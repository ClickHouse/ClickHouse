#include <Processors/Sources/RecursiveCTESource.h>

#include <Storages/IStorage.h>

#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Transforms/SquashingTransform.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>

#include <QueryPipeline/Chain.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>

#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/TableNode.h>

#include <Core/Settings.h>
#include <Core/Defines.h>

#include <Columns/IColumn.h>

#include <Common/SipHash.h>
#include <Common/HashTable/Hash.h>
#include <Common/PODArray.h>

#include <unordered_map>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_recursive_cte_evaluation_depth;
    extern const SettingsUInt64 min_insert_block_size_rows;
    extern const SettingsUInt64 min_insert_block_size_bytes;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_DEEP_RECURSION;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

namespace
{

std::vector<TableNode *> collectTableNodesWithTemporaryTableName(const std::string & temporary_table_name, IQueryTreeNode * root)
{
    std::vector<TableNode *> result;

    std::vector<IQueryTreeNode *> nodes_to_process;
    nodes_to_process.push_back(root);

    while (!nodes_to_process.empty())
    {
        auto * subtree_node = nodes_to_process.back();
        nodes_to_process.pop_back();

        auto * table_node = subtree_node->as<TableNode>();
        if (table_node && table_node->getTemporaryTableName() == temporary_table_name)
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
    RecursiveCTEChunkGenerator(SharedHeader header_, QueryTreeNodePtr recursive_cte_union_node_)
        : header(std::move(header_))
        , recursive_cte_union_node(std::move(recursive_cte_union_node_))
    {
        auto & recursive_cte_union_node_typed = recursive_cte_union_node->as<UnionNode &>();
        chassert(recursive_cte_union_node_typed.hasRecursiveCTETable());

        auto & recursive_cte_table = recursive_cte_union_node_typed.getRecursiveCTETable();

        const auto & cte_name = recursive_cte_union_node_typed.getCTEName();
        recursive_table_nodes = collectTableNodesWithTemporaryTableName(cte_name, recursive_cte_union_node.get());
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

        /// USING KEY: keyed evaluation. The recursion accumulates one row per key, only
        /// changed rows form the next step's working table, and the result is the
        /// accumulated keyed table at convergence (not the concatenation of all steps).
        if (!recursive_cte_table->key_columns.empty())
        {
            keyed = true;
            settled_temporary_table_holder = recursive_cte_table->settled_holder;
            settled_temporary_table_storage = recursive_cte_table->settled_storage;

            const auto & table_columns = recursive_cte_table->columns;
            for (const auto & key_column_name : recursive_cte_table->key_columns)
            {
                for (size_t i = 0; i < table_columns.size(); ++i)
                {
                    if (table_columns[i].name == key_column_name)
                    {
                        key_column_indices.push_back(i);
                        break;
                    }
                }
            }

            if (key_column_indices.size() != recursive_cte_table->key_columns.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Recursive CTE {} USING KEY columns were not resolved to projection columns",
                    recursive_cte_union_node->formatASTForErrorMessage());

            settled_table_nodes = collectTableNodesWithTemporaryTableName(cte_name + "_settled", recursive_cte_union_node.get());
            accumulated_columns = header->cloneEmptyColumns();
        }
    }

    Chunk generate()
    {
        if (keyed)
            return generateKeyed();

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
                recursive_subquery_settings[Setting::max_recursive_cte_evaluation_depth].value,
                recursive_cte_union_node->formatASTForErrorMessage());

        auto & query_to_execute = recursive_step > 0 ? recursive_query : non_recursive_query;
        ++recursive_step;

        SelectQueryOptions select_query_options;
        select_query_options.merge_tree_enable_remove_parts_from_snapshot_optimization = false;

        const auto & recursive_table_name = recursive_cte_union_node->as<UnionNode &>().getCTEName();
        recursive_query_context->addOrUpdateExternalTable(recursive_table_name, working_temporary_table_holder);
        if (keyed && settled_temporary_table_holder)
            recursive_query_context->addOrUpdateExternalTable(recursive_table_name + "_settled", settled_temporary_table_holder);

        auto interpreter = std::make_unique<InterpreterSelectQueryAnalyzer>(query_to_execute, recursive_query_context, select_query_options);
        auto pipeline_builder = interpreter->buildQueryPipeline();

        pipeline_builder.addSimpleTransform([&](const SharedHeader & in_header)
        {
            return std::make_shared<MaterializingTransform>(in_header);
        });

        auto convert_to_temporary_tables_header_actions_dag = ActionsDAG::makeConvertingActions(
            pipeline_builder.getHeader().getColumnsWithTypeAndName(),
            header->getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Position,
            interpreter->getContext());
        auto convert_to_temporary_tables_header_actions = std::make_shared<ExpressionActions>(std::move(convert_to_temporary_tables_header_actions_dag));
        pipeline_builder.addSimpleTransform([&](const SharedHeader & input_header)
        {
            return std::make_shared<ExpressionTransform>(input_header, convert_to_temporary_tables_header_actions);
        });

        /// Keyed evaluation consumes the step output directly (the working table is rebuilt
        /// from the changed rows after the step), so no intermediate table sink is needed.
        if (!keyed)
        {
            /// Squash small chunks before writing them into the intermediate table. Each recursive
            /// step appends one StorageMemory block per produced chunk, and the next step reads the
            /// working table block by block, so without squashing deep recursions accumulate a lot
            /// of tiny blocks and the per-step reads degrade.
            pipeline_builder.addSimpleTransform([&](const SharedHeader & in_header)
            {
                return std::make_shared<SimpleSquashingChunksTransform>(
                    in_header,
                    recursive_subquery_settings[Setting::min_insert_block_size_rows],
                    recursive_subquery_settings[Setting::min_insert_block_size_bytes]);
            });

            auto intermediate_temporary_table_storage_sink = intermediate_temporary_table_storage->write(
                {},
                intermediate_temporary_table_storage->getInMemoryMetadataPtr(recursive_query_context, false),
                recursive_query_context,
                false /*async_insert*/);

            pipeline_builder.addChain(Chain(std::move(intermediate_temporary_table_storage_sink)));
        }

        pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));
        pipeline.setProgressCallback(recursive_query_context->getProgressCallback());
        pipeline.setProcessListElement(recursive_query_context->getProcessListElement());

        executor.emplace(pipeline);
    }

    Chunk generateKeyed()
    {
        if (!keyed_evaluated)
        {
            runKeyedEvaluation();
            keyed_evaluated = true;
        }

        if (keyed_result_columns.empty() || keyed_result_offset >= keyed_result_columns[0]->size())
            return {};

        size_t total_rows = keyed_result_columns[0]->size();
        size_t chunk_rows = std::min<size_t>(DEFAULT_BLOCK_SIZE, total_rows - keyed_result_offset);

        Columns chunk_columns;
        chunk_columns.reserve(keyed_result_columns.size());
        for (const auto & result_column : keyed_result_columns)
            chunk_columns.push_back(result_column->cut(keyed_result_offset, chunk_rows));

        keyed_result_offset += chunk_rows;
        return Chunk(std::move(chunk_columns), chunk_rows);
    }

    void runKeyedEvaluation()
    {
        while (true)
        {
            buildStepExecutor();

            MutableColumns delta_columns = header->cloneEmptyColumns();

            Chunk chunk;
            while (executor->pull(chunk))
            {
                if (chunk.getNumRows() > 0)
                    upsertChunkIntoAccumulated(chunk, delta_columns);
                chunk.clear();
            }

            executor.reset();

            size_t delta_rows = delta_columns.empty() ? 0 : delta_columns[0]->size();
            if (delta_rows == 0)
                break;

            /// The next step's working table (the frontier) is only the changed rows.
            truncateTemporaryTable(working_temporary_table_storage);
            pushBlockIntoStorage(working_temporary_table_storage, header->cloneWithColumns(std::move(delta_columns)));
            for (auto & recursive_table_node : recursive_table_nodes)
                recursive_table_node->updateStorage(working_temporary_table_storage, recursive_query_context);

            /// Refresh the settled table (one row per key, the current accumulated state),
            /// if any recursive member references it.
            if (!settled_table_nodes.empty())
            {
                truncateTemporaryTable(settled_temporary_table_storage);
                pushBlockIntoStorage(settled_temporary_table_storage, buildAccumulatedBlock());
                for (auto & settled_table_node : settled_table_nodes)
                    settled_table_node->updateStorage(settled_temporary_table_storage, recursive_query_context);
            }
        }

        /// The result of a keyed recursive CTE is the accumulated keyed table at convergence.
        keyed_result_columns = buildAccumulatedBlock().getColumns();
    }

    void upsertChunkIntoAccumulated(const Chunk & chunk, MutableColumns & delta_columns)
    {
        const auto & chunk_columns = chunk.getColumns();
        size_t rows = chunk.getNumRows();
        size_t columns_size = accumulated_columns.size();

        for (size_t row = 0; row < rows; ++row)
        {
            SipHash key_hash;
            for (auto key_column_index : key_column_indices)
                chunk_columns[key_column_index]->updateHashWithValue(row, key_hash);
            UInt128 key = key_hash.get128();

            auto it = accumulated_index.find(key);
            if (it != accumulated_index.end())
            {
                size_t existing_row = it->second;

                bool row_changed = false;
                for (size_t i = 0; i < columns_size; ++i)
                {
                    if (accumulated_columns[i]->compareAt(existing_row, row, *chunk_columns[i], /*nan_direction_hint*/ 1) != 0)
                    {
                        row_changed = true;
                        break;
                    }
                }

                /// A row identical to the accumulated one does not change the state and is
                /// not propagated to the next step's working table. This is what terminates
                /// cycles and refutes re-derived rows without an explicit cycle guard.
                if (!row_changed)
                    continue;

                if (accumulated_live[existing_row])
                {
                    accumulated_live[existing_row] = static_cast<UInt8>(0);
                    ++accumulated_dead;
                }
            }

            size_t new_row_index = accumulated_columns[0]->size();
            for (size_t i = 0; i < columns_size; ++i)
            {
                accumulated_columns[i]->insertFrom(*chunk_columns[i], row);
                delta_columns[i]->insertFrom(*chunk_columns[i], row);
            }
            accumulated_row_hashes.push_back(key);
            accumulated_live.push_back(static_cast<UInt8>(1));
            accumulated_index[key] = new_row_index;
        }

        compactAccumulatedIfNeeded();
    }

    void compactAccumulatedIfNeeded()
    {
        size_t accumulated_size = accumulated_live.size();
        if (accumulated_size < 8192 || accumulated_dead * 2 < accumulated_size)
            return;

        MutableColumns compacted_columns = header->cloneEmptyColumns();
        PaddedPODArray<UInt128> compacted_row_hashes;
        compacted_row_hashes.reserve(accumulated_size - accumulated_dead);

        accumulated_index.clear();

        for (size_t row = 0; row < accumulated_size; ++row)
        {
            if (!accumulated_live[row])
                continue;

            size_t new_row_index = compacted_columns[0]->size();
            for (size_t i = 0; i < compacted_columns.size(); ++i)
                compacted_columns[i]->insertFrom(*accumulated_columns[i], row);
            compacted_row_hashes.push_back(accumulated_row_hashes[row]);
            accumulated_index[accumulated_row_hashes[row]] = new_row_index;
        }

        accumulated_columns = std::move(compacted_columns);
        accumulated_row_hashes = std::move(compacted_row_hashes);
        accumulated_live.assign(accumulated_row_hashes.size(), static_cast<UInt8>(1));
        accumulated_dead = 0;
    }

    Block buildAccumulatedBlock()
    {
        /// Deep-copies the live accumulated rows: the returned block must not share column
        /// data with accumulated_columns, which continue to be mutated by later steps.
        Columns result_columns;
        result_columns.reserve(accumulated_columns.size());
        ssize_t result_size_hint = static_cast<ssize_t>(accumulated_live.size() - accumulated_dead);
        for (const auto & accumulated_column : accumulated_columns)
            result_columns.push_back(accumulated_column->filter(accumulated_live, result_size_hint));
        return header->cloneWithColumns(std::move(result_columns));
    }

    void pushBlockIntoStorage(StoragePtr & storage, Block block)
    {
        if (!block.rows())
            return;

        auto sink = storage->write(
            {},
            storage->getInMemoryMetadataPtr(recursive_query_context, false),
            recursive_query_context,
            false /*async_insert*/);

        QueryPipeline push_pipeline(std::move(sink));
        PushingPipelineExecutor push_executor(push_pipeline);
        push_executor.start();
        push_executor.push(Chunk(block.getColumns(), block.rows()));
        push_executor.finish();
    }

    void truncateTemporaryTable(StoragePtr & temporary_table)
    {
        /// TODO: Support proper locking
        TableExclusiveLockHolder table_exclusive_lock;
        temporary_table->truncate({},
            temporary_table->getInMemoryMetadataPtr(recursive_query_context, false),
            recursive_query_context,
            table_exclusive_lock);
    }

    SharedHeader header;
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

    /// USING KEY (keyed recursive CTE) state.
    bool keyed = false;
    std::vector<size_t> key_column_indices;
    std::vector<TableNode *> settled_table_nodes;
    TemporaryTableHolderPtr settled_temporary_table_holder;
    StoragePtr settled_temporary_table_storage;

    /// Accumulated keyed state: one live row per key. Updated rows are appended and the
    /// previous row is marked dead; compaction reclaims dead rows when they dominate.
    MutableColumns accumulated_columns;
    PaddedPODArray<UInt128> accumulated_row_hashes;
    IColumn::Filter accumulated_live;
    size_t accumulated_dead = 0;
    std::unordered_map<UInt128, size_t, UInt128Hash> accumulated_index;

    bool keyed_evaluated = false;
    Columns keyed_result_columns;
    size_t keyed_result_offset = 0;
};

RecursiveCTESource::RecursiveCTESource(SharedHeader header, QueryTreeNodePtr recursive_cte_union_node_)
    : ISource(header)
    , generator(std::make_unique<RecursiveCTEChunkGenerator>(std::move(header), std::move(recursive_cte_union_node_)))
{}

RecursiveCTESource::~RecursiveCTESource() = default;

Chunk RecursiveCTESource::generate()
{
    return generator->generate();
}

}
