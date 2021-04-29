#include <Storages/MergeTree/SubstituteColumnOptimizer.h>
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

const String COMPONENT = "__constraint_component_";
constexpr UInt64 COLUMN_PENALTY = 10 * 1024 * 1024;
//constexpr size_t MAX_COMPONENTS_FOR_BRUTEFORCE = 10;

class ComponentMatcher
{
public:
    using Visitor = InDepthNodeVisitor<ComponentMatcher, true>;

    struct Data
    {
        const ComparisonGraph & graph;
        std::set<UInt64> & components;

        Data(const ComparisonGraph & graph_, std::set<UInt64> & components_)
            : graph(graph_), components(components_)
        {
        }
    };

    static void visit(ASTPtr & ast, Data & data)
    {
        const auto id  = data.graph.getComponentId(ast);
        if (id)
        {
            ast = std::make_shared<ASTIdentifier>(COMPONENT + std::to_string(id.value()));
            data.components.insert(id.value());
        }
    }

    static bool needChildVisit(const ASTPtr &, const ASTPtr &)
    {
        return true;
    }
};

using ComponentVisitor = ComponentMatcher::Visitor;


void collectIdentifiers(const ASTPtr & ast, std::unordered_set<String> & identifiers)
{
    const auto * identifier = ast->as<ASTIdentifier>();
    if (identifier)
        identifiers.insert(identifier->name());
    else
    {
        for (const auto & child : ast->children)
            collectIdentifiers(child, identifiers);
    }
}

struct ColumnPrice
{
    size_t compressed_size;
    size_t uncompressed_size;

    ColumnPrice(const size_t compressed_size_, const size_t uncompressed_size_)
        : compressed_size(compressed_size_)
        , uncompressed_size(uncompressed_size_)
    {}

    ColumnPrice()
        : ColumnPrice(0, 0)
    {}

    bool operator<(const ColumnPrice & that) const
    {
        return std::tie(compressed_size, uncompressed_size) < std::tie(that.compressed_size, that.uncompressed_size);
    }

    ColumnPrice operator+(ColumnPrice that) const
    {
        that += *this;
        return that;
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

class SubstituteColumnMatcher
{
public:
    using Visitor = InDepthNodeVisitor<SubstituteColumnMatcher, false>;

    struct Data
    {
        std::unordered_map<String, ASTPtr> id_to_expression_map;
    };

    static void visit(ASTPtr & ast, Data & data)
    {
        const auto * identifier = ast->as<ASTIdentifier>();
        if (identifier && data.id_to_expression_map.contains(identifier->name()))
            ast = data.id_to_expression_map.at(identifier->name())->clone();
    }

    static bool needChildVisit(const ASTPtr &, const ASTPtr &)
    {
        return true;
    }
};

using SubstituteColumnVisitor = SubstituteColumnMatcher::Visitor;

ColumnPrice calculatePrice(
    const std::unordered_map<std::string, ColumnPrice> & column_prices,
    std::unordered_set<String> identifiers)
{
    ColumnPrice result(0, 0);
    for (const auto & ident : identifiers)
        result = result + column_prices.at(ident);
    return result;
}

// TODO: branch-and-bound
void bruteforce(
    const ComparisonGraph & graph,
    const std::vector<UInt64> & components,
    size_t current_component,
    const std::unordered_map<std::string, ColumnPrice> & column_prices,
    ColumnPrice current_price,
    std::vector<ASTPtr> & expressions_stack,
    ColumnPrice & min_price,
    std::vector<ASTPtr> & min_expressions)
{
    if (current_component == components.size())
    {
        Poco::Logger::get("New price").information(std::to_string(current_price.compressed_size));
        for (const auto & ast : expressions_stack)
            Poco::Logger::get("AST").information(ast->getColumnName());
        if (current_price < min_price)
        {
            min_price = current_price;
            min_expressions = expressions_stack;
            Poco::Logger::get("New price").information("UPDATE");
        }
    }
    else
    {
        for (const auto & ast : graph.getComponent(components[current_component]))
        {
            std::unordered_set<String> identifiers;
            collectIdentifiers(ast, identifiers);
            ColumnPrice expression_price = calculatePrice(column_prices, identifiers);
            Poco::Logger::get("EXPRPRICE").information( ast->getColumnName()+ " " + std::to_string(expression_price.compressed_size));

            expressions_stack.push_back(ast);
            current_price += expression_price;

            std::unordered_map<std::string, ColumnPrice> new_prices(column_prices);
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
    Aliases & /*aliases_*/,
    const NameSet & /*source_columns_set_*/,
    const std::vector<TableWithColumnNamesAndTypes> & /*tables_with_columns_*/,
    const StorageMetadataPtr & metadata_snapshot_,
    const ConstStoragePtr & storage_)
    : select_query(select_query_)
    /* , aliases(aliases_)
    , source_columns_set(source_columns_set_)
    , tables_with_columns(tables_with_columns_)*/
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
    {
        Poco::Logger::get("SubstituteColumnOptimizer").information("skip: column sizes not available");
        return;
    }

    const auto compare_graph = metadata_snapshot->getConstraints().getGraph();

    auto run_for_all = [&](const auto func) {
        if (select_query->where())
            func(select_query->refWhere());
        if (select_query->prewhere())
            func(select_query->refPrewhere());
        if (select_query->select())
            func(select_query->refSelect());
        if (select_query->having())
            func(select_query->refHaving());
    };

    std::set<UInt64> components;
    ComponentVisitor::Data component_data(compare_graph, components);
    std::unordered_set<String> identifiers;
    auto preprocess = [&](ASTPtr & ast) {
        ComponentVisitor(component_data).visit(ast);
        collectIdentifiers(ast, identifiers);
        Poco::Logger::get("kek").information(ast->dumpTree());
    };

    run_for_all(preprocess);

    const auto primary_key = metadata_snapshot->getColumnsRequiredForPrimaryKey();
    const std::unordered_set<std::string_view> primary_key_set(std::begin(primary_key), std::end(primary_key));
    std::unordered_map<std::string, ColumnPrice> column_prices;
    for (const auto & [column_name, column_size] : column_sizes)
    {
        column_prices[column_name] = ColumnPrice(
            column_size.data_compressed + COLUMN_PENALTY, column_size.data_uncompressed);
        Poco::Logger::get("COLUMNS").information(column_name + " " + std::to_string(column_size.data_compressed) + " " + std::to_string(column_size.data_uncompressed));
    }
    for (const auto & column_name : primary_key)
    {
        column_prices[column_name] = ColumnPrice(0, 0);
        Poco::Logger::get("PK COLUMNS").information(column_name + " " + std::to_string(column_prices[column_name].compressed_size));
    }
    for (const auto & column_name : identifiers)
    {
        column_prices[column_name] = ColumnPrice(0, 0);
        Poco::Logger::get("ID COLUMNS").information(column_name + " " + std::to_string(column_prices[column_name].compressed_size));
    }

    std::unordered_map<String, ASTPtr> id_to_expression_map;
    std::vector<UInt64> components_list;
    for (const UInt64 component : components)
        if (compare_graph.getComponent(component).size() == 1)
            id_to_expression_map[COMPONENT + std::to_string(component)] = compare_graph.getComponent(component).front();
        else
            components_list.push_back(component);

    /*if (components_list.size() > MAX_COMPONENTS_FOR_BRUTEFORCE)
    {
        Poco::Logger::get("SubstituteColumnOptimizer").information("skip: too many components for bruteforce");
        return;
    }*/

    std::vector<ASTPtr> expressions_stack;
    ColumnPrice min_price(std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max());
    std::vector<ASTPtr> min_expressions;
    bruteforce(compare_graph,
               components_list,
               0,
               column_prices,
               ColumnPrice(0, 0),
               expressions_stack,
               min_price,
               min_expressions);

    for (size_t i = 0; i < min_expressions.size(); ++i)
        id_to_expression_map[COMPONENT + std::to_string(components_list[i])] = min_expressions[i];

    auto process = [&](ASTPtr & ast) {
        SubstituteColumnVisitor::Data substitute_data{id_to_expression_map};
        SubstituteColumnVisitor(substitute_data).visit(ast);
    };

    run_for_all(process);
}

}
