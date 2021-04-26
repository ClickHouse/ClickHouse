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

class ComponentMatcher
{
public:
    using Visitor = InDepthNodeVisitor<ComponentMatcher, true>;

    struct Data
    {
        const ComparisonGraph & graph;

        Data(const ComparisonGraph & graph_)
            : graph(graph_)
        {
        }
    };

    static void visit(ASTPtr & ast, Data & data)
    {
        const auto id  = data.graph.getComponentId(ast);
        if (id)
            ast = std::make_shared<ASTIdentifier>(COMPONENT + std::to_string(id.value()));
    }

    static bool needChildVisit(const ASTPtr &, const ASTPtr &)
    {
        return true;
    }
};

using ComponentVisitor = ComponentMatcher::Visitor;


class IdentifierSetMatcher
{
public:
    using Visitor = InDepthNodeVisitor<IdentifierSetMatcher, true>;

    struct Data
    {
        std::unordered_set<String> identifiers;
    };

    static void visit(ASTPtr & ast, Data & data)
    {
        const auto * identifier = ast->as<ASTIdentifier>();
        if (identifier)
            data.identifiers.insert(identifier->name());
    }

    static bool needChildVisit(const ASTPtr &, const ASTPtr &)
    {
        return true;
    }
};

using IdentifierSetVisitor = IdentifierSetMatcher::Visitor;


class SubstituteColumnMatcher
{
public:
    using Visitor = InDepthNodeVisitor<SubstituteColumnMatcher, false>;

    struct Data
    {
        const ComparisonGraph & graph;
        const std::unordered_set<String> & identifiers;
        ConstStoragePtr storage;

        Data(const ComparisonGraph & graph_,
             const std::unordered_set<String> & identifiers_,
             const ConstStoragePtr & storage_)
            : graph(graph_)
            , identifiers(identifiers_)
            , storage(storage_)
        {
        }
    };

    static void visit(ASTPtr & ast, Data & data)
    {
        const auto * identifier = ast->as<ASTIdentifier>();
        if (identifier && identifier->name().starts_with(COMPONENT))
        {
            const std::size_t id = std::stoll(identifier->name().substr(COMPONENT.size(), identifier->name().size()));
            // like TreeRewriter
            struct ColumnSizeTuple
            {
                size_t compressed_size;
                size_t uncompressed_size;
                const ASTPtr & ast;

                bool operator<(const ColumnSizeTuple & that) const
                {
                    return std::tie(compressed_size, uncompressed_size) < std::tie(that.compressed_size, that.uncompressed_size);
                }
            };

            const auto column_sizes = data.storage->getColumnSizes();

            std::vector<ColumnSizeTuple> columns;
            for (const auto & equal_ast : data.graph.getComponent(id))
            {
                if (const auto it = column_sizes.find(equal_ast->getColumnName()); it != std::end(column_sizes))
                    columns.push_back({it->second.data_compressed, it->second.data_uncompressed, equal_ast});
            }

            if (!columns.empty())
                ast = std::min_element(std::begin(columns), std::end(columns))->ast->clone();
        }
    }

    static bool needChildVisit(const ASTPtr &, const ASTPtr &)
    {
        return true;
    }
};

using SubstituteColumnVisitor = SubstituteColumnMatcher::Visitor;
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

    ComponentVisitor::Data component_data(compare_graph);
    IdentifierSetVisitor::Data identifier_data;
    auto preprocess = [&](ASTPtr & ast) {
        ComponentVisitor(component_data).visit(ast);
        IdentifierSetVisitor(identifier_data).visit(ast);
    };

    auto process = [&](ASTPtr & ast) {
        SubstituteColumnVisitor::Data substitute_data(compare_graph, identifier_data.identifiers, storage);
        SubstituteColumnVisitor(substitute_data).visit(ast);
    };

    ASTPtr old_query = select_query->clone();

    run_for_all(preprocess);
    run_for_all(process);


}

}
