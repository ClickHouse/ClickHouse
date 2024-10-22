#include <Processors/Sources/RecursiveCTESource.h>

#include <Storages/IStorage.h>

#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Transforms/SquashingTransform.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/Context.h>

#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/TableNode.h>

#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_recursive_cte_evaluation_depth;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_DEEP_RECURSION;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

namespace
{

std::vector<TableNode *> collectTableNodesWithStorage(const StoragePtr & storage, IQueryTreeNode * root)
{
    std::vector<TableNode *> result;

    std::vector<IQueryTreeNode *> nodes_to_process;
    nodes_to_process.push_back(root);

    while (!nodes_to_process.empty())
    {
        auto * subtree_node = nodes_to_process.back();
        nodes_to_process.pop_back();

        auto * table_node = subtree_node->as<TableNode>();
        if (table_node && table_node->getStorageID() == storage->getStorageID())
            result.push_back(table_node);

        for (auto & child : subtree_node->getChildren())
        {
            if (child)
                nodes_to_process.push_back(child.get());
        }
    }

    return result;
}

}

class RecursiveCTEChunkGenerator
{
public:
    RecursiveCTEChunkGenerator(Block header_, QueryTreeNodePtr recursive_cte_union_node_)
        : header(std::move(header_))
        , recursive_cte_union_node(std::move(recursive_cte_union_node_))
    {
        auto & recursive_cte_union_node_typed = recursive_cte_union_node->as<UnionNode &>();
        chassert(recursive_cte_union_node_typed.hasRecursiveCTETable());

        auto & recursive_cte_table = recursive_cte_union_node_typed.getRecursiveCTETable();
        recursive_table_nodes = collectTableNodesWithStorage(recursive_cte_table->storage, recursive_cte_union_node.get());
        if (recursive_table_nodes.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "UNION query {} is not recursive", recursive_cte_union_node->formatASTForErrorMessage());

        size_t recursive_cte_union_node_queries_size = recursive_cte_union_node_typed.getQueries().getNodes().size();
        chassert(recursive_cte_union_node_queries_size > 1);

        non_recursive_query = recursive_cte_union_node_typed.getQueries().getNodes()[0];
        recursive_query = recursive_cte_union_node_typed.getQueries().getNodes()[1];

        if (recursive_cte_union_node_queries_size > 2)
        {
            auto working_union_query = std::make_shared<UnionNode>(recursive_cte_union_node_typed.getMutableContext(),
                recursive_cte_union_node_typed.getUnionMode());
            auto & working_union_query_subqueries = working_union_query->getQueries().getNodes();

            for (size_t i = 1; i < recursive_cte_union_node_queries_size; ++i)
                working_union_query_subqueries.push_back(recursive_cte_union_node_typed.getQueries().getNodes()[i]);

            recursive_query = std::move(working_union_query);
        }

        recursive_query_context = recursive_query->as<QueryNode>() ? recursive_query->as<QueryNode &>().getMutableContext() :
            recursive_query->as<UnionNode &>().getMutableContext();

        const auto & recursive_query_projection_columns = recursive_query->as<QueryNode>() ? recursive_query->as<QueryNode &>().getProjectionColumns() :
            recursive_query->as<UnionNode &>().computeProjectionColumns();

        if (recursive_cte_table->columns.size() != recursive_query_projection_columns.size())
            throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
            "Recursive CTE subquery {}. Expected projection columns to have same size in recursive and non recursive subquery.",
            recursive_cte_union_node->formatASTForErrorMessage());

        working_temporary_table_holder = recursive_cte_table->holder;
        working_temporary_table_storage = recursive_cte_table->storage;

        intermediate_temporary_table_holder = std::make_shared<TemporaryTableHolder>(
            recursive_query_context,
            ColumnsDescription{NamesAndTypesList{recursive_cte_table->columns.begin(), recursive_cte_table->columns.end()}},
            ConstraintsDescription{},
            nullptr /*query*/,
            true /*create_for_global_subquery*/);
        intermediate_temporary_table_storage = intermediate_temporary_table_holder->getTable();
    }

    Chunk generate()
    {
        Chunk current_chunk;

        while (!finished)
        {
            if (!executor.has_value())
                buildStepExecutor();

            while (current_chunk.getNumRows() == 0 && executor->pull(current_chunk))
            {
            }

            read_rows_during_recursive_step += current_chunk.getNumRows();

            if (current_chunk.getNumRows() > 0)
                break;

            executor.reset();

            if (read_rows_during_recursive_step == 0)
            {
                finished = true;
                truncateTemporaryTable(intermediate_temporary_table_storage);
                continue;
            }

            read_rows_during_recursive_step = 0;

            for (auto & recursive_table_node : recursive_table_nodes)
                recursive_table_node->updateStorage(intermediate_temporary_table_storage, recursive_query_context);

            truncateTemporaryTable(working_temporary_table_storage);

            std::swap(intermediate_temporary_table_holder, working_temporary_table_holder);
            std::swap(intermediate_temporary_table_storage, working_temporary_table_storage);
        }

        return current_chunk;
    }

private:
    void buildStepExecutor()
    {
        const auto & recursive_subquery_settings = recursive_query_context->getSettingsRef();

        if (recursive_step > recursive_subquery_settings[Setting::max_recursive_cte_evaluation_depth])
            throw Exception(
                ErrorCodes::TOO_DEEP_RECURSION,
                "Maximum recursive CTE evaluation depth ({}) exceeded, during evaluation of {}. Consider raising "
                "max_recursive_cte_evaluation_depth setting.",
                recursive_subquery_settings[Setting::max_recursive_cte_evaluation_depth],
                recursive_cte_union_node->formatASTForErrorMessage());

        auto & query_to_execute = recursive_step > 0 ? recursive_query : non_recursive_query;
        ++recursive_step;

        SelectQueryOptions select_query_options;
        select_query_options.merge_tree_enable_remove_parts_from_snapshot_optimization = false;

        const auto & recursive_table_name = recursive_cte_union_node->as<UnionNode &>().getCTEName();
        recursive_query_context->addOrUpdateExternalTable(recursive_table_name, working_temporary_table_holder);

        auto interpreter = std::make_unique<InterpreterSelectQueryAnalyzer>(query_to_execute, recursive_query_context, select_query_options);
        auto pipeline_builder = interpreter->buildQueryPipeline();

        pipeline_builder.addSimpleTransform([&](const Block & in_header)
        {
            return std::make_shared<MaterializingTransform>(in_header);
        });

        auto convert_to_temporary_tables_header_actions_dag = ActionsDAG::makeConvertingActions(
            pipeline_builder.getHeader().getColumnsWithTypeAndName(),
            header.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Position);
        auto convert_to_temporary_tables_header_actions = std::make_shared<ExpressionActions>(std::move(convert_to_temporary_tables_header_actions_dag));
        pipeline_builder.addSimpleTransform([&](const Block & input_header)
        {
            return std::make_shared<ExpressionTransform>(input_header, convert_to_temporary_tables_header_actions);
        });

        /// TODO: Support squashing transform

        auto intermediate_temporary_table_storage_sink = intermediate_temporary_table_storage->write(
            {},
            intermediate_temporary_table_storage->getInMemoryMetadataPtr(),
            recursive_query_context,
            false /*async_insert*/);

        pipeline_builder.addChain(Chain(std::move(intermediate_temporary_table_storage_sink)));

        pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));
        pipeline.setProgressCallback(recursive_query_context->getProgressCallback());
        pipeline.setProcessListElement(recursive_query_context->getProcessListElement());

        executor.emplace(pipeline);
    }

    void truncateTemporaryTable(StoragePtr & temporary_table)
    {
        /// TODO: Support proper locking
        TableExclusiveLockHolder table_exclusive_lock;
        temporary_table->truncate({},
            temporary_table->getInMemoryMetadataPtr(),
            recursive_query_context,
            table_exclusive_lock);
    }

    Block header;
    QueryTreeNodePtr recursive_cte_union_node;
    std::vector<TableNode *> recursive_table_nodes;

    QueryTreeNodePtr non_recursive_query;
    QueryTreeNodePtr recursive_query;
    ContextMutablePtr recursive_query_context;

    TemporaryTableHolderPtr working_temporary_table_holder;
    StoragePtr working_temporary_table_storage;

    TemporaryTableHolderPtr intermediate_temporary_table_holder;
    StoragePtr intermediate_temporary_table_storage;

    QueryPipeline pipeline;
    std::optional<PullingAsyncPipelineExecutor> executor;

    size_t recursive_step = 0;
    size_t read_rows_during_recursive_step = 0;
    bool finished = false;
};

RecursiveCTESource::RecursiveCTESource(Block header, QueryTreeNodePtr recursive_cte_union_node_)
    : ISource(header)
    , generator(std::make_unique<RecursiveCTEChunkGenerator>(std::move(header), std::move(recursive_cte_union_node_)))
{}

RecursiveCTESource::~RecursiveCTESource() = default;

Chunk RecursiveCTESource::generate()
{
    return generator->generate();
}

}
