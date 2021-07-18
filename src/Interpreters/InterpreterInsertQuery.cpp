#include <Interpreters/InterpreterInsertQuery.h>

#include <Access/AccessFlags.h>
#include <DataStreams/AddingDefaultBlockOutputStream.h>
#include <DataStreams/CheckConstraintsBlockOutputStream.h>
#include <DataStreams/CountingBlockOutputStream.h>
#include <DataStreams/InputStreamFromASTInsertQuery.h>
#include <DataStreams/NullAndDoCopyBlockInputStream.h>
#include <DataStreams/PushingToViewsBlockOutputStream.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <DataStreams/copyData.h>
#include <IO/ConnectionTimeoutsContext.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterWatchQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Processors/Sources/SinkToOutputStream.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageMaterializedView.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/checkStackSize.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/TranslateQualifiedNamesVisitor.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/processColumnTransformers.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnNullable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int ILLEGAL_COLUMN;
    extern const int DUPLICATE_COLUMN;
}

InterpreterInsertQuery::InterpreterInsertQuery(
    const ASTPtr & query_ptr_, ContextPtr context_, bool allow_materialized_, bool no_squash_, bool no_destination_)
    : WithContext(context_)
    , query_ptr(query_ptr_)
    , allow_materialized(allow_materialized_)
    , no_squash(no_squash_)
    , no_destination(no_destination_)
{
    checkStackSize();
}


StoragePtr InterpreterInsertQuery::getTable(ASTInsertQuery & query)
{
    if (query.table_function)
    {
        const auto & factory = TableFunctionFactory::instance();
        TableFunctionPtr table_function_ptr = factory.get(query.table_function, getContext());
        return table_function_ptr->execute(query.table_function, getContext(), table_function_ptr->getName());
    }

    query.table_id = getContext()->resolveStorageID(query.table_id);
    return DatabaseCatalog::instance().getTable(query.table_id, getContext());
}

Block InterpreterInsertQuery::getSampleBlock(
    const ASTInsertQuery & query,
    const StoragePtr & table,
    const StorageMetadataPtr & metadata_snapshot) const
{
    Block table_sample_non_materialized = metadata_snapshot->getSampleBlockNonMaterialized();
    /// If the query does not include information about columns
    if (!query.columns)
    {
        if (no_destination)
            return metadata_snapshot->getSampleBlockWithVirtuals(table->getVirtuals());
        else
            return table_sample_non_materialized;
    }

    Block table_sample = metadata_snapshot->getSampleBlock();

    const auto columns_ast = processColumnTransformers(getContext()->getCurrentDatabase(), table, metadata_snapshot, query.columns);

    /// Form the block based on the column names from the query
    Block res;
    for (const auto & identifier : columns_ast->children)
    {
        std::string current_name = identifier->getColumnName();

        /// The table does not have a column with that name
        if (!table_sample.has(current_name))
            throw Exception("No such column " + current_name + " in table " + table->getStorageID().getNameForLogs(),
                ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        if (!allow_materialized && !table_sample_non_materialized.has(current_name))
            throw Exception("Cannot insert column " + current_name + ", because it is MATERIALIZED column.", ErrorCodes::ILLEGAL_COLUMN);
        if (res.has(current_name))
            throw Exception("Column " + current_name + " specified more than once", ErrorCodes::DUPLICATE_COLUMN);

        res.insert(ColumnWithTypeAndName(table_sample.getByName(current_name).type, current_name));
    }
    return res;
}


/** A query that just reads all data without any complex computations or filetering.
  * If we just pipe the result to INSERT, we don't have to use too many threads for read.
  */
static bool isTrivialSelect(const ASTPtr & select)
{
    if (auto * select_query = select->as<ASTSelectQuery>())
    {
        const auto & tables = select_query->tables();

        if (!tables)
            return false;

        const auto & tables_in_select_query = tables->as<ASTTablesInSelectQuery &>();

        if (tables_in_select_query.children.size() != 1)
            return false;

        const auto & child = tables_in_select_query.children.front();
        const auto & table_element = child->as<ASTTablesInSelectQueryElement &>();
        const auto & table_expr = table_element.table_expression->as<ASTTableExpression &>();

        if (table_expr.subquery)
            return false;

        /// Note: how to write it in more generic way?
        return (!select_query->distinct
            && !select_query->limit_with_ties
            && !select_query->prewhere()
            && !select_query->where()
            && !select_query->groupBy()
            && !select_query->having()
            && !select_query->orderBy()
            && !select_query->limitBy());
    }
    /// This query is ASTSelectWithUnionQuery subquery
    return false;
};


BlockIO InterpreterInsertQuery::execute()
{
    const Settings & settings = getContext()->getSettingsRef();
    auto & query = query_ptr->as<ASTInsertQuery &>();

    BlockIO res;

    StoragePtr table = getTable(query);
    auto table_lock = table->lockForShare(getContext()->getInitialQueryId(), settings.lock_acquire_timeout);
    auto metadata_snapshot = table->getInMemoryMetadataPtr();

    auto query_sample_block = getSampleBlock(query, table, metadata_snapshot);
    if (!query.table_function)
        getContext()->checkAccess(AccessType::INSERT, query.table_id, query_sample_block.getNames());

    bool is_distributed_insert_select = false;

    if (query.select && table->isRemote() && settings.parallel_distributed_insert_select)
    {
        // Distributed INSERT SELECT
        if (auto maybe_pipeline = table->distributedWrite(query, getContext()))
        {
            res.pipeline = std::move(*maybe_pipeline);
            is_distributed_insert_select = true;
        }
    }

    BlockOutputStreams out_streams;
    if (!is_distributed_insert_select || query.watch)
    {
        size_t out_streams_size = 1;
        if (query.select)
        {
            bool is_trivial_insert_select = false;

            if (settings.optimize_trivial_insert_select)
            {
                const auto & select_query = query.select->as<ASTSelectWithUnionQuery &>();
                const auto & selects = select_query.list_of_selects->children;
                const auto & union_modes = select_query.list_of_modes;

                /// ASTSelectWithUnionQuery is not normalized now, so it may pass some queries which can be Trivial select queries
                const auto mode_is_all = [](const auto & mode) { return mode == ASTSelectWithUnionQuery::Mode::ALL; };

                is_trivial_insert_select =
                    std::all_of(union_modes.begin(), union_modes.end(), std::move(mode_is_all))
                    && std::all_of(selects.begin(), selects.end(), isTrivialSelect);
            }

            if (is_trivial_insert_select)
            {
                /** When doing trivial INSERT INTO ... SELECT ... FROM table,
                  * don't need to process SELECT with more than max_insert_threads
                  * and it's reasonable to set block size for SELECT to the desired block size for INSERT
                  * to avoid unnecessary squashing.
                  */

                Settings new_settings = getContext()->getSettings();

                new_settings.max_threads = std::max<UInt64>(1, settings.max_insert_threads);

                if (table->prefersLargeBlocks())
                {
                    if (settings.min_insert_block_size_rows)
                        new_settings.max_block_size = settings.min_insert_block_size_rows;
                    if (settings.min_insert_block_size_bytes)
                        new_settings.preferred_block_size_bytes = settings.min_insert_block_size_bytes;
                }

                auto new_context = Context::createCopy(context);
                new_context->setSettings(new_settings);

                InterpreterSelectWithUnionQuery interpreter_select{
                    query.select, new_context, SelectQueryOptions(QueryProcessingStage::Complete, 1)};
                res = interpreter_select.execute();
            }
            else
            {
                /// Passing 1 as subquery_depth will disable limiting size of intermediate result.
                InterpreterSelectWithUnionQuery interpreter_select{
                    query.select, getContext(), SelectQueryOptions(QueryProcessingStage::Complete, 1)};
                res = interpreter_select.execute();
            }

            res.pipeline.dropTotalsAndExtremes();

            if (table->supportsParallelInsert() && settings.max_insert_threads > 1)
                out_streams_size = std::min(size_t(settings.max_insert_threads), res.pipeline.getNumStreams());

            res.pipeline.resize(out_streams_size);

            /// Allow to insert Nullable into non-Nullable columns, NULL values will be added as defaults values.
            if (getContext()->getSettingsRef().insert_null_as_default)
            {
                const auto & input_columns = res.pipeline.getHeader().getColumnsWithTypeAndName();
                const auto & query_columns = query_sample_block.getColumnsWithTypeAndName();
                const auto & output_columns = metadata_snapshot->getColumns();

                if (input_columns.size() == query_columns.size())
                {
                    for (size_t col_idx = 0; col_idx < query_columns.size(); ++col_idx)
                    {
                        /// Change query sample block columns to Nullable to allow inserting nullable columns, where NULL values will be substituted with
                        /// default column values (in AddingDefaultBlockOutputStream), so all values will be cast correctly.
                        if (input_columns[col_idx].type->isNullable() && !query_columns[col_idx].type->isNullable() && output_columns.hasDefault(query_columns[col_idx].name))
                            query_sample_block.setColumn(col_idx, ColumnWithTypeAndName(makeNullable(query_columns[col_idx].column), makeNullable(query_columns[col_idx].type), query_columns[col_idx].name));
                    }
                }
            }
        }
        else if (query.watch)
        {
            InterpreterWatchQuery interpreter_watch{ query.watch, getContext() };
            res = interpreter_watch.execute();
            res.pipeline.init(Pipe(std::make_shared<SourceFromInputStream>(std::move(res.in))));
        }

        for (size_t i = 0; i < out_streams_size; i++)
        {
            /// We create a pipeline of several streams, into which we will write data.
            BlockOutputStreamPtr out;

            /// NOTE: we explicitly ignore bound materialized views when inserting into Kafka Storage.
            ///       Otherwise we'll get duplicates when MV reads same rows again from Kafka.
            if (table->noPushingToViews() && !no_destination)
                out = table->write(query_ptr, metadata_snapshot, getContext());
            else
                out = std::make_shared<PushingToViewsBlockOutputStream>(table, metadata_snapshot, getContext(), query_ptr, no_destination);

            /// Note that we wrap transforms one on top of another, so we write them in reverse of data processing order.

            /// Checking constraints. It must be done after calculation of all defaults, so we can check them on calculated columns.
            if (const auto & constraints = metadata_snapshot->getConstraints(); !constraints.empty())
                out = std::make_shared<CheckConstraintsBlockOutputStream>(
                    query.table_id, out, out->getHeader(), metadata_snapshot->getConstraints(), getContext());

            bool null_as_default = query.select && getContext()->getSettingsRef().insert_null_as_default;

            /// Actually we don't know structure of input blocks from query/table,
            /// because some clients break insertion protocol (columns != header)
            out = std::make_shared<AddingDefaultBlockOutputStream>(
                out, query_sample_block, metadata_snapshot->getColumns(), getContext(), null_as_default);

            /// It's important to squash blocks as early as possible (before other transforms),
            ///  because other transforms may work inefficient if block size is small.

            /// Do not squash blocks if it is a sync INSERT into Distributed, since it lead to double bufferization on client and server side.
            /// Client-side bufferization might cause excessive timeouts (especially in case of big blocks).
            if (!(settings.insert_distributed_sync && table->isRemote()) && !no_squash && !query.watch)
            {
                bool table_prefers_large_blocks = table->prefersLargeBlocks();

                out = std::make_shared<SquashingBlockOutputStream>(
                    out,
                    out->getHeader(),
                    table_prefers_large_blocks ? settings.min_insert_block_size_rows : settings.max_block_size,
                    table_prefers_large_blocks ? settings.min_insert_block_size_bytes : 0);
            }

            auto out_wrapper = std::make_shared<CountingBlockOutputStream>(out);
            out_wrapper->setProcessListElement(getContext()->getProcessListElement());
            out_streams.emplace_back(std::move(out_wrapper));
        }
    }

    /// What type of query: INSERT or INSERT SELECT or INSERT WATCH?
    if (is_distributed_insert_select)
    {
        /// Pipeline was already built.
    }
    else if (query.select || query.watch)
    {
        const auto & header = out_streams.at(0)->getHeader();
        auto actions_dag = ActionsDAG::makeConvertingActions(
                res.pipeline.getHeader().getColumnsWithTypeAndName(),
                header.getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Position);
        auto actions = std::make_shared<ExpressionActions>(actions_dag, ExpressionActionsSettings::fromContext(getContext(), CompileExpressions::yes));

        res.pipeline.addSimpleTransform([&](const Block & in_header) -> ProcessorPtr
        {
            return std::make_shared<ExpressionTransform>(in_header, actions);
        });

        res.pipeline.setSinks([&](const Block &, QueryPipeline::StreamType type) -> ProcessorPtr
        {
            if (type != QueryPipeline::StreamType::Main)
                return nullptr;

            auto stream = std::move(out_streams.back());
            out_streams.pop_back();

            return std::make_shared<SinkToOutputStream>(std::move(stream));
        });

        if (!allow_materialized)
        {
            for (const auto & column : metadata_snapshot->getColumns())
                if (column.default_desc.kind == ColumnDefaultKind::Materialized && header.has(column.name))
                    throw Exception("Cannot insert column " + column.name + ", because it is MATERIALIZED column.", ErrorCodes::ILLEGAL_COLUMN);
        }
    }
    else if (query.data && !query.has_tail) /// can execute without additional data
    {
        // res.out = std::move(out_streams.at(0));
        res.in = std::make_shared<InputStreamFromASTInsertQuery>(query_ptr, nullptr, query_sample_block, getContext(), nullptr);
        res.in = std::make_shared<NullAndDoCopyBlockInputStream>(res.in, out_streams.at(0));
    }
    else
        res.out = std::move(out_streams.at(0));

    res.pipeline.addStorageHolder(table);
    if (const auto * mv = dynamic_cast<const StorageMaterializedView *>(table.get()))
    {
        if (auto inner_table = mv->tryGetTargetTable())
            res.pipeline.addStorageHolder(inner_table);
    }

    return res;
}


StorageID InterpreterInsertQuery::getDatabaseTable() const
{
    return query_ptr->as<ASTInsertQuery &>().table_id;
}


void InterpreterInsertQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr &, ContextPtr context_) const
{
    elem.query_kind = "Insert";
    const auto & insert_table = context_->getInsertionTable();
    if (!insert_table.empty())
    {
        elem.query_databases.insert(insert_table.getDatabaseName());
        elem.query_tables.insert(insert_table.getFullNameNotQuoted());
    }
}

}
