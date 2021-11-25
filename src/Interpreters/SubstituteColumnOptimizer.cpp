#include <Interpreters/SubstituteColumnOptimizer.h>
#include <Interpreters/TreeCNFConverter.h>
#include <Interpreters/ComparisonGraph.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Parsers/ASTSelectQuery.h>
#include <Poco/Logger.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Storages/IStorage.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

constexpr auto COMPONENT_PART = "__component_";
constexpr UInt64 COLUMN_PENALTY = 10 * 1024 * 1024;
constexpr Int64 INDEX_PRICE = -1'000'000'000'000'000'000;

class ComponentMatcher
{
public:
    using Visitor = InDepthNodeVisitor<ComponentMatcher, true>;

    struct Data
    {
        const ComparisonGraph & graph;
        std::set<UInt64> & components;
        std::unordered_map<String, String> & old_name;
        std::unordered_map<String, UInt64> & component;
        UInt64 & current_id;

        Data(const ComparisonGraph & graph_,
             std::set<UInt64> & components_,
             std::unordered_map<String, String> & old_name_,
             std::unordered_map<String, UInt64> & component_,
             UInt64 & current_id_)
            : graph(graph_)
            , components(components_)
            , old_name(old_name_)
            , component(component_)
            , current_id(current_id_)
        {
        }
    };

    static void visit(ASTPtr & ast, Data & data)
    {
        if (auto id = data.graph.getComponentId(ast))
        {
            const String name = COMPONENT_PART + std::to_string(*id) + "_" + std::to_string(++data.current_id);
            data.old_name[name] = ast->getAliasOrColumnName();
            data.component[name] = *id;
            data.components.insert(*id);
            ast = std::make_shared<ASTIdentifier>(name);
        }
    }

    static bool needChildVisit(const ASTPtr &, const ASTPtr &)
    {
        return true;
    }
};

using ComponentVisitor = ComponentMatcher::Visitor;

struct ColumnPrice
{
    Int64 compressed_size;
    Int64 uncompressed_size;

    ColumnPrice(const Int64 compressed_size_, const Int64 uncompressed_size_)
        : compressed_size(compressed_size_)
        , uncompressed_size(uncompressed_size_)
    {
    }

    ColumnPrice() : ColumnPrice(0, 0) {}

    bool operator<(const ColumnPrice & that) const
    {
        return std::tie(compressed_size, uncompressed_size) < std::tie(that.compressed_size, that.uncompressed_size);
    }

    ColumnPrice & operator+=(const ColumnPrice & that)
    {
        compressed_size += that.compressed_size;
        uncompressed_size += that.uncompressed_size;
        return *this;
    }

    ColumnPrice & operator-=(const ColumnPrice & that)
    {
        compressed_size -= that.compressed_size;
        uncompressed_size -= that.uncompressed_size;
        return *this;
    }
};

using ColumnPriceByName = std::unordered_map<String, ColumnPrice>;

class SubstituteColumnMatcher
{
public:
    using Visitor = InDepthNodeVisitor<SubstituteColumnMatcher, false>;

    struct Data
    {
        std::unordered_map<UInt64, ASTPtr> id_to_expression_map;
        std::unordered_map<String, UInt64> name_to_component_id;
        std::unordered_map<String, String> old_name;
        bool is_select;
    };

    static void visit(ASTPtr & ast, Data & data)
    {
        const auto * identifier = ast->as<ASTIdentifier>();
        if (identifier && data.name_to_component_id.contains(identifier->name()))
        {
            const String & name = identifier->name();
            const auto component_id = data.name_to_component_id.at(name);
            auto new_ast = data.id_to_expression_map.at(component_id)->clone();

            if (data.is_select)
                new_ast->setAlias(data.old_name.at(name));

            ast = new_ast;
        }
    }

    static bool needChildVisit(const ASTPtr &, const ASTPtr &)
    {
        return true;
    }
};

using SubstituteColumnVisitor = SubstituteColumnMatcher::Visitor;

ColumnPrice calculatePrice(
    const ColumnPriceByName & column_prices,
    const IdentifierNameSet & identifiers)
{
    ColumnPrice result(0, 0);
    for (const auto & ident : identifiers)
    {
        auto it = column_prices.find(ident);
        if (it != column_prices.end())
            result += it->second;
    }

    return result;
}

/// We need to choose one expression in each component,
/// so that total price of all read columns will be minimal.
/// Bruteforce equal ASTs in each component and calculate
/// price of all columns on which ast depends.
/// TODO: branch-and-bound
void bruteforce(
    const ComparisonGraph & graph,
    const std::vector<UInt64> & components,
    size_t current_component,
    const ColumnPriceByName & column_prices,
    ColumnPrice current_price,
    std::vector<ASTPtr> & expressions_stack,
    ColumnPrice & min_price,
    std::vector<ASTPtr> & min_expressions)
{
    if (current_component == components.size())
    {
        if (current_price < min_price)
        {
            min_price = current_price;
            min_expressions = expressions_stack;
        }
    }
    else
    {
        for (const auto & ast : graph.getComponent(components[current_component]))
        {
            IdentifierNameSet identifiers;
            ast->collectIdentifierNames(identifiers);
            ColumnPrice expression_price = calculatePrice(column_prices, identifiers);

            expressions_stack.push_back(ast);
            current_price += expression_price;

            ColumnPriceByName new_prices(column_prices);
            /// Update prices of already counted columns.
            for (const auto & identifier : identifiers)
                new_prices[identifier] = ColumnPrice(0, 0);

            bruteforce(graph,
                       components,
                       current_component + 1,
                       new_prices,
                       current_price,
                       expressions_stack,
                       min_price,
                       min_expressions);

            current_price -= expression_price;
            expressions_stack.pop_back();
        }
    }
}

}


SubstituteColumnOptimizer::SubstituteColumnOptimizer(
    ASTSelectQuery * select_query_,
    const StorageMetadataPtr & metadata_snapshot_,
    const ConstStoragePtr & storage_)
    : select_query(select_query_)
    , metadata_snapshot(metadata_snapshot_)
    , storage(storage_)
{
}

void SubstituteColumnOptimizer::perform()
{
    if (!storage)
        return;

    const auto column_sizes = storage->getColumnSizes();
    if (column_sizes.empty())
        return;

    const auto & compare_graph = metadata_snapshot->getConstraints().getGraph();

    // Fill aliases
    if (select_query->select())
    {
        auto * list = select_query->refSelect()->as<ASTExpressionList>();
        if (!list)
            throw Exception("List of selected columns must be ASTExpressionList", ErrorCodes::LOGICAL_ERROR);

        for (ASTPtr & ast : list->children)
            ast->setAlias(ast->getAliasOrColumnName());
    }

    auto run_for_all = [&](const auto func)
    {
        if (select_query->where())
            func(select_query->refWhere(), false);
        if (select_query->prewhere())
            func(select_query->refPrewhere(), false);
        if (select_query->select())
            func(select_query->refSelect(), true);
        if (select_query->having())
            func(select_query->refHaving(), false);
    };

    std::set<UInt64> components;
    std::unordered_map<String, String> old_name;
    std::unordered_map<String, UInt64> name_to_component;

    UInt64 counter_id = 0;

    ComponentVisitor::Data component_data(
        compare_graph, components, old_name, name_to_component, counter_id);

    IdentifierNameSet identifiers;
    auto preprocess = [&](ASTPtr & ast, bool)
    {
        ComponentVisitor(component_data).visit(ast);
        ast->collectIdentifierNames(identifiers);
    };

    run_for_all(preprocess);

    const auto primary_key = metadata_snapshot->getColumnsRequiredForPrimaryKey();
    const std::unordered_set<std::string_view> primary_key_set(std::begin(primary_key), std::end(primary_key));
    ColumnPriceByName column_prices;

    for (const auto & [column_name, column_size] : column_sizes)
        column_prices[column_name] = ColumnPrice(column_size.data_compressed + COLUMN_PENALTY, column_size.data_uncompressed);

    for (const auto & column_name : primary_key)
        column_prices[column_name] = ColumnPrice(INDEX_PRICE, INDEX_PRICE);

    for (const auto & column_name : identifiers)
        column_prices[column_name] = ColumnPrice(0, 0);

    std::unordered_map<UInt64, ASTPtr> id_to_expression_map;
    std::vector<UInt64> components_list;

    for (const UInt64 component_id : components)
    {
        auto component = compare_graph.getComponent(component_id);
        if (component.size() == 1)
            id_to_expression_map[component_id] = component.front();
        else
            components_list.push_back(component_id);
    }

    std::vector<ASTPtr> expressions_stack;
    ColumnPrice min_price(std::numeric_limits<Int64>::max(), std::numeric_limits<Int64>::max());
    std::vector<ASTPtr> min_expressions;

    bruteforce(compare_graph,
               components_list,
               0,
               column_prices,
               ColumnPrice(0, 0),
               expressions_stack,
               min_price,
               min_expressions);

    for (size_t i = 0; i < components_list.size(); ++i)
        id_to_expression_map[components_list[i]] = min_expressions[i];

    auto process = [&](ASTPtr & ast, bool is_select)
    {
        SubstituteColumnVisitor::Data substitute_data{id_to_expression_map, name_to_component, old_name, is_select};
        SubstituteColumnVisitor(substitute_data).visit(ast);
    };

    run_for_all(process);
}

}
