#include <Storages/MergeTree/SubstituteColumnOptimizer.h>
#include <Parsers/ASTConstraintDeclaration.h>
#include <Interpreters/TreeCNFConverter.h>
#include <Interpreters/ComparisonGraph.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTConstraintDeclaration.h>
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
class SubstituteColumnMatcher
{
public:
    using Visitor = InDepthNodeVisitor<SubstituteColumnMatcher, true>;

    struct Data
    {
        const ComparisonGraph & graph;
        ConstStoragePtr storage;

        Data(const ComparisonGraph & graph_, const ConstStoragePtr & storage_)
            : graph(graph_), storage(storage_)
        {
        }
    };

    static void visit(ASTPtr & ast, Data & data)
    {
        const auto column_sizes = data.storage->getColumnSizes();

        // like TreeRewriter
        struct ColumnSizeTuple
        {
            size_t compressed_size;
            size_t uncompressed_size;
            const ASTPtr & ast;

            bool operator<(const ColumnSizeTuple & that) const
            {
                return std::tie(compressed_size, uncompressed_size)
                       < std::tie(that.compressed_size, that.uncompressed_size);
            }
        };

        std::vector<ColumnSizeTuple> columns;
        for (const auto & equal_ast : data.graph.getEqual(ast))
        {
            if (const auto it = column_sizes.find(equal_ast->getColumnName()); it != std::end(column_sizes))
                columns.push_back({
                    it->second.data_compressed,
                    it->second.data_uncompressed,
                    equal_ast});
        }

        if (!columns.empty())
            ast = std::min_element(std::begin(columns), std::end(columns))->ast->clone();
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
    SubstituteColumnVisitor::Data data(compare_graph, storage);
    if (select_query->where())
        SubstituteColumnVisitor(data).visit(select_query->refWhere());
    if (select_query->prewhere())
        SubstituteColumnVisitor(data).visit(select_query->refPrewhere());
    if (select_query->select())
        SubstituteColumnVisitor(data).visit(select_query->refSelect());
    if (select_query->having())
        SubstituteColumnVisitor(data).visit(select_query->refHaving());
}

}
