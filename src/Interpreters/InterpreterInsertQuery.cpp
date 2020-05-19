#include <Interpreters/InterpreterInsertQuery.h>

#include <DataStreams/AddingDefaultBlockOutputStream.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>
#include <DataStreams/CheckConstraintsBlockOutputStream.h>
#include <DataStreams/ConvertingBlockInputStream.h>
#include <DataStreams/CountingBlockOutputStream.h>
#include <DataStreams/InputStreamFromASTInsertQuery.h>
#include <DataStreams/NullAndDoCopyBlockInputStream.h>
#include <DataStreams/NullBlockOutputStream.h>
#include <DataStreams/OwningBlockInputStream.h>
#include <DataStreams/PushingToViewsBlockOutputStream.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <DataStreams/copyData.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterWatchQuery.h>
#include <Access/AccessFlags.h>
#include <Interpreters/JoinedTables.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/queryToString.h>
#include <Storages/Kafka/StorageKafka.h>
#include <Storages/StorageDistributed.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/checkStackSize.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int ILLEGAL_COLUMN;
    extern const int DUPLICATE_COLUMN;
    extern const int LOGICAL_ERROR;
}


InterpreterInsertQuery::InterpreterInsertQuery(
    const ASTPtr & query_ptr_, const Context & context_, bool allow_materialized_, bool no_squash_, bool no_destination_)
    : query_ptr(query_ptr_)
    , context(context_)
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
        const auto * table_function = query.table_function->as<ASTFunction>();
        const auto & factory = TableFunctionFactory::instance();
        TableFunctionPtr table_function_ptr = factory.get(table_function->name, context);
        return table_function_ptr->execute(query.table_function, context, table_function_ptr->getName());
    }

    query.table_id = context.resolveStorageID(query.table_id);
    return DatabaseCatalog::instance().getTable(query.table_id);
}

Block InterpreterInsertQuery::getSampleBlock(const ASTInsertQuery & query, const StoragePtr & table) const
{
    Block table_sample_non_materialized = table->getSampleBlockNonMaterialized();
    /// If the query does not include information about columns
    if (!query.columns)
    {
        if (no_destination)
            return table->getSampleBlockWithVirtuals();
        else
            return table_sample_non_materialized;
    }

    Block table_sample = table->getSampleBlock();
    /// Form the block based on the column names from the query
    Block res;
    for (const auto & identifier : query.columns->children)
    {
        std::string current_name = identifier->getColumnName();

        /// The table does not have a column with that name
        if (!table_sample.has(current_name))
            throw Exception("No such column " + current_name + " in table " + query.table_id.getNameForLogs(), ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

        if (!allow_materialized && !table_sample_non_materialized.has(current_name))
            throw Exception("Cannot insert column " + current_name + ", because it is MATERIALIZED column.", ErrorCodes::ILLEGAL_COLUMN);
        if (res.has(current_name))
            throw Exception("Column " + current_name + " specified more than once", ErrorCodes::DUPLICATE_COLUMN);

        res.insert(ColumnWithTypeAndName(table_sample.getByName(current_name).type, current_name));
    }
    return res;
}


BlockIO InterpreterInsertQuery::execute()
{
    const Settings & settings = context.getSettingsRef();
    auto & query = query_ptr->as<ASTInsertQuery &>();

    BlockIO res;

    StoragePtr table = getTable(query);
    auto table_lock = table->lockStructureForShare(
            true, context.getInitialQueryId(), context.getSettingsRef().lock_acquire_timeout);

    auto query_sample_block = getSampleBlock(query, table);
    if (!query.table_function)
        context.checkAccess(AccessType::INSERT, query.table_id, query_sample_block.getNames());

    BlockInputStreams in_streams;
    BlockOutputStreams out_streams;
    bool is_distributed_insert_select = false;

    if (query.select && table->isRemote() && settings.parallel_distributed_insert_select)
    {
        // Distributed INSERT SELECT
        std::shared_ptr<StorageDistributed> storage_src;
        auto & select = query.select->as<ASTSelectWithUnionQuery &>();
        auto new_query = std::dynamic_pointer_cast<ASTInsertQuery>(query.clone());
        if (select.list_of_selects->children.size() == 1)
        {
            auto & select_query = select.list_of_selects->children.at(0)->as<ASTSelectQuery &>();
            JoinedTables joined_tables(Context(context), select_query);

            if (joined_tables.tablesCount() == 1)
            {
                storage_src = std::dynamic_pointer_cast<StorageDistributed>(joined_tables.getLeftTableStorage());
                if (storage_src)
                {
                    const auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
                    select_with_union_query->list_of_selects = std::make_shared<ASTExpressionList>();

                    auto new_select_query = std::dynamic_pointer_cast<ASTSelectQuery>(select_query.clone());
                    select_with_union_query->list_of_selects->children.push_back(new_select_query);

                    new_select_query->replaceDatabaseAndTable(storage_src->getRemoteDatabaseName(), storage_src->getRemoteTableName());

                    new_query->select = select_with_union_query;
                }
            }
        }

        auto storage_dst = std::dynamic_pointer_cast<StorageDistributed>(table);

        if (storage_src && storage_dst && storage_src->cluster_name == storage_dst->cluster_name)
        {
            is_distributed_insert_select = true;

            const auto & cluster = storage_src->getCluster();
            const auto & shards_info = cluster->getShardsInfo();

            String new_query_str = queryToString(new_query);
            for (size_t shard_index : ext::range(0, shards_info.size()))
            {
                const auto & shard_info = shards_info[shard_index];
                if (shard_info.isLocal())
                {
                    InterpreterInsertQuery interpreter(new_query, context);
                    auto block_io = interpreter.execute();
                    in_streams.push_back(block_io.in);
                }
                else
                {
                    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(settings);
                    auto connections = shard_info.pool->getMany(timeouts, &settings, PoolMode::GET_ONE);
                    if (connections.empty() || connections.front().isNull())
                        throw Exception(
                            "Expected exactly one connection for shard " + toString(shard_info.shard_num), ErrorCodes::LOGICAL_ERROR);

                    ///  INSERT SELECT query returns empty block
                    auto in_stream = std::make_shared<RemoteBlockInputStream>(std::move(connections), new_query_str, Block{}, context);
                    in_streams.push_back(in_stream);
                }
                out_streams.push_back(std::make_shared<NullBlockOutputStream>(Block()));
            }
        }
    }

    if (!is_distributed_insert_select || query.watch)
    {
        size_t out_streams_size = 1;
        if (query.select)
        {
            /// Passing 1 as subquery_depth will disable limiting size of intermediate result.
            InterpreterSelectWithUnionQuery interpreter_select{ query.select, context, SelectQueryOptions(QueryProcessingStage::Complete, 1)};

            if (table->supportsParallelInsert() && settings.max_insert_threads > 1)
            {
                in_streams = interpreter_select.executeWithMultipleStreams(res.pipeline);
                out_streams_size = std::min(size_t(settings.max_insert_threads), in_streams.size());
            }
            else
            {
                res = interpreter_select.execute();
                in_streams.emplace_back(res.in);
                res.in = nullptr;
                res.out = nullptr;
            }
        }
        else if (query.watch)
        {
            InterpreterWatchQuery interpreter_watch{ query.watch, context };
            res = interpreter_watch.execute();
            in_streams.emplace_back(res.in);
            res.in = nullptr;
            res.out = nullptr;
        }

        for (size_t i = 0; i < out_streams_size; i++)
        {
            /// We create a pipeline of several streams, into which we will write data.
            BlockOutputStreamPtr out;

            /// NOTE: we explicitly ignore bound materialized views when inserting into Kafka Storage.
            ///       Otherwise we'll get duplicates when MV reads same rows again from Kafka.
            if (table->noPushingToViews() && !no_destination)
                out = table->write(query_ptr, context);
            else
                out = std::make_shared<PushingToViewsBlockOutputStream>(table, context, query_ptr, no_destination);

            /// Do not squash blocks if it is a sync INSERT into Distributed, since it lead to double bufferization on client and server side.
            /// Client-side bufferization might cause excessive timeouts (especially in case of big blocks).
            if (!(context.getSettingsRef().insert_distributed_sync && table->isRemote()) && !no_squash && !query.watch)
            {
                out = std::make_shared<SquashingBlockOutputStream>(
                    out,
                    out->getHeader(),
                    context.getSettingsRef().min_insert_block_size_rows,
                    context.getSettingsRef().min_insert_block_size_bytes);
            }

            /// Actually we don't know structure of input blocks from query/table,
            /// because some clients break insertion protocol (columns != header)
            out = std::make_shared<AddingDefaultBlockOutputStream>(
                out, query_sample_block, out->getHeader(), table->getColumns().getDefaults(), context);

            if (const auto & constraints = table->getConstraints(); !constraints.empty())
                out = std::make_shared<CheckConstraintsBlockOutputStream>(
                    query.table_id, out, query_sample_block, table->getConstraints(), context);

            auto out_wrapper = std::make_shared<CountingBlockOutputStream>(out);
            out_wrapper->setProcessListElement(context.getProcessListElement());
            out = std::move(out_wrapper);
            out_streams.emplace_back(std::move(out));
        }
    }

    /// What type of query: INSERT or INSERT SELECT or INSERT WATCH?
    if (query.select || query.watch)
    {
        for (auto & in_stream : in_streams)
        {
            in_stream = std::make_shared<ConvertingBlockInputStream>(
                in_stream, out_streams.at(0)->getHeader(), ConvertingBlockInputStream::MatchColumnsMode::Position);
        }

        Block in_header = in_streams.at(0)->getHeader();
        if (in_streams.size() > 1)
        {
            for (size_t i = 1; i < in_streams.size(); ++i)
                assertBlocksHaveEqualStructure(in_streams[i]->getHeader(), in_header, query.select ? "INSERT SELECT" : "INSERT WATCH");
        }

        res.in = std::make_shared<NullAndDoCopyBlockInputStream>(in_streams, out_streams);

        if (!allow_materialized)
        {
            for (const auto & column : table->getColumns())
                if (column.default_desc.kind == ColumnDefaultKind::Materialized && in_header.has(column.name))
                    throw Exception("Cannot insert column " + column.name + ", because it is MATERIALIZED column.", ErrorCodes::ILLEGAL_COLUMN);
        }
    }
    else if (query.data && !query.has_tail) /// can execute without additional data
    {
        // res.out = std::move(out_streams.at(0));
        res.in = std::make_shared<InputStreamFromASTInsertQuery>(query_ptr, nullptr, query_sample_block, context, nullptr);
        res.in = std::make_shared<NullAndDoCopyBlockInputStream>(res.in, out_streams.at(0));
    }
    else
        res.out = std::move(out_streams.at(0));
    res.pipeline.addStorageHolder(table);

    return res;
}


StorageID InterpreterInsertQuery::getDatabaseTable() const
{
    return query_ptr->as<ASTInsertQuery &>().table_id;
}

}
