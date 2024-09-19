#include <unordered_set>
#include <IO/WriteHelpers.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Storages/KVStorageUtils.h>
#include <Storages/KeyDescription.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageRedis.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <Common/Exception.h>
#include <Common/RemoteHostFilter.h>
#include <Common/checkStackSize.h>
#include <Common/logger_useful.h>
#include <Common/parseAddress.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int INTERNAL_REDIS_ERROR;
}

class RedisDataSource : public ISource
{
public:
    RedisDataSource(
        StorageRedis & storage_,
        const Block & header,
        FieldVectorPtr keys_,
        FieldVector::const_iterator begin_,
        FieldVector::const_iterator end_,
        const size_t max_block_size_)
        : ISource(header)
        , storage(storage_)
        , primary_key_pos(getPrimaryKeyPos(header, storage.getPrimaryKey()))
        , keys(keys_)
        , begin(begin_)
        , end(end_)
        , it(begin)
        , max_block_size(max_block_size_)
    {
    }

    RedisDataSource(StorageRedis & storage_, const Block & header, const size_t max_block_size_, const String & pattern_ = "*")
        : ISource(header)
        , storage(storage_)
        , primary_key_pos(getPrimaryKeyPos(header, storage.getPrimaryKey()))
        , iterator(-1)
        , pattern(pattern_)
        , max_block_size(max_block_size_)
    {
    }

    String getName() const override { return storage.getName(); }

    Chunk generate() override
    {
        if (keys)
            return generateWithKeys();
        return generateFullScan();
    }

    Chunk generateWithKeys()
    {
        const auto & sample_block = getPort().getHeader();
        if (it >= end)
        {
            it = {};
            return {};
        }

        const auto & key_column_type = sample_block.getByName(storage.getPrimaryKey().at(0)).type;
        auto raw_keys = serializeKeysToRawString(it, end, key_column_type, max_block_size);

        return storage.getBySerializedKeys(raw_keys, nullptr);
    }

    /// TODO scan may get duplicated keys when Redis is rehashing, it is a very rare case.
    Chunk generateFullScan()
    {
        checkStackSize();

        /// redis scan ending
        if (iterator == 0)
            return {};

        RedisArray scan_keys;
        RedisIterator next_iterator;

        std::tie(next_iterator, scan_keys) = storage.scan(iterator == -1 ? 0 : iterator, pattern, max_block_size);
        iterator = next_iterator;

        /// redis scan can return nothing
        if (scan_keys.isNull() || scan_keys.size() == 0)
            return generateFullScan();

        const auto & sample_block = getPort().getHeader();
        MutableColumns columns = sample_block.cloneEmptyColumns();

        RedisArray values = storage.multiGet(scan_keys);
        for (size_t i = 0; i < scan_keys.size() && !values.get<RedisBulkString>(i).isNull(); i++)
        {
            fillColumns(scan_keys.get<RedisBulkString>(i).value(),
                        values.get<RedisBulkString>(i).value(),
                        primary_key_pos, sample_block, columns
            );
        }

        Block block = sample_block.cloneWithColumns(std::move(columns));
        return Chunk(block.getColumns(), block.rows());
    }

private:
    StorageRedis & storage;

    size_t primary_key_pos;

    /// For key scan
    FieldVectorPtr keys = nullptr;
    FieldVector::const_iterator begin;
    FieldVector::const_iterator end;
    FieldVector::const_iterator it;

    /// For full scan
    RedisIterator iterator;
    String pattern;

    const size_t max_block_size;
};


class RedisSink : public SinkToStorage
{
public:
    RedisSink(StorageRedis & storage_, const StorageMetadataPtr & metadata_snapshot_);

    void consume(Chunk & chunk) override;
    String getName() const override { return "RedisSink"; }

private:
    StorageRedis & storage;
    StorageMetadataPtr metadata_snapshot;
    size_t primary_key_pos = 0;
};

RedisSink::RedisSink(StorageRedis & storage_, const StorageMetadataPtr & metadata_snapshot_)
    : SinkToStorage(metadata_snapshot_->getSampleBlock())
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
{
    for (const auto & column : getHeader())
    {
        if (column.name == storage.getPrimaryKey()[0])
            break;
        ++primary_key_pos;
    }
}

void RedisSink::consume(Chunk & chunk)
{
    auto rows = chunk.getNumRows();
    auto block = getHeader().cloneWithColumns(chunk.getColumns());

    WriteBufferFromOwnString wb_key;
    WriteBufferFromOwnString wb_value;

    RedisArray data;
    for (size_t i = 0; i < rows; ++i)
    {
        wb_key.restart();
        wb_value.restart();

        size_t idx = 0;
        for (const auto & elem : block)
        {
            elem.type->getDefaultSerialization()->serializeBinary(*elem.column, i, idx == primary_key_pos ? wb_key : wb_value, {});
            ++idx;
        }
        data.add(wb_key.str());
        data.add(wb_value.str());
    }

    storage.multiSet(data);
}

StorageRedis::StorageRedis(
    const StorageID & table_id_,
    const RedisConfiguration & configuration_,
    ContextPtr context_,
    const StorageInMemoryMetadata & storage_metadata,
    const String & primary_key_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , table_id(table_id_)
    , configuration(configuration_)
    , log(getLogger("StorageRedis"))
    , primary_key(primary_key_)
{
    pool = std::make_shared<RedisPool>(configuration.pool_size);
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageRedis::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    storage_snapshot->check(column_names);

    FieldVectorPtr keys;
    bool all_scan = false;

    Block header = storage_snapshot->metadata->getSampleBlock();
    auto primary_key_data_type = header.getByName(primary_key).type;

    std::tie(keys, all_scan) = getFilterKeys(primary_key, primary_key_data_type, query_info, context_);

    if (all_scan)
    {
        return Pipe(std::make_shared<RedisDataSource>(*this, header, max_block_size));
    }
    else
    {
        if (keys->empty())
            return {};

        Pipes pipes;

        ::sort(keys->begin(), keys->end());
        keys->erase(std::unique(keys->begin(), keys->end()), keys->end());

        size_t num_keys = keys->size();
        size_t num_threads = std::min<size_t>(num_streams, keys->size());

        num_threads = std::min<size_t>(num_threads, configuration.pool_size);
        assert(num_keys <= std::numeric_limits<uint32_t>::max());

        for (size_t thread_idx = 0; thread_idx < num_threads; ++thread_idx)
        {
            size_t begin = num_keys * thread_idx / num_threads;
            size_t end = num_keys * (thread_idx + 1) / num_threads;

            pipes.emplace_back(
                std::make_shared<RedisDataSource>(*this, header, keys, keys->begin() + begin, keys->begin() + end, max_block_size));
        }
        return Pipe::unitePipes(std::move(pipes));
    }
}

namespace
{
    //  host:port, db_index, password, pool_size
    RedisConfiguration getRedisConfiguration(ASTs & engine_args, ContextPtr context)
    {
        RedisConfiguration configuration;

        if (engine_args.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad arguments count when creating Redis table engine");

        if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, context))
        {
            validateNamedCollection(
                *named_collection,
                ValidateKeysMultiset<RedisEqualKeysSet>{"host", "port", "hostname", "password", "db_index", "pool_size"},
                {});

            configuration.host = named_collection->getAny<String>({"host", "hostname"});
            configuration.port = static_cast<uint32_t>(named_collection->getOrDefault<UInt64>("port", 6379));
            configuration.password = named_collection->getOrDefault<String>("password", DEFAULT_REDIS_PASSWORD);
            configuration.db_index = static_cast<uint32_t>(named_collection->getOrDefault<UInt64>("db_index", DEFAULT_REDIS_DB_INDEX));
            configuration.pool_size = static_cast<uint32_t>(named_collection->getOrDefault<UInt64>("pool_size", DEFAULT_REDIS_POOL_SIZE));
        }
        else
        {
            for (auto & engine_arg : engine_args)
                engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context);

            /// 6379 is the default Redis port.
            auto parsed_host_port = parseAddress(checkAndGetLiteralArgument<String>(engine_args[0], "host:port"), 6379);
            configuration.host = parsed_host_port.first;
            configuration.port = parsed_host_port.second;

            if (engine_args.size() > 1)
                configuration.db_index = static_cast<uint32_t>(checkAndGetLiteralArgument<UInt64>(engine_args[1], "db_index"));
            else
                configuration.db_index = DEFAULT_REDIS_DB_INDEX;
            if (engine_args.size() > 2)
                configuration.password = checkAndGetLiteralArgument<String>(engine_args[2], "password");
            else
                configuration.password = DEFAULT_REDIS_PASSWORD;
            if (engine_args.size() > 3)
                configuration.pool_size = static_cast<uint32_t>(checkAndGetLiteralArgument<UInt64>(engine_args[3], "pool_size"));
            else
                configuration.pool_size = DEFAULT_REDIS_POOL_SIZE;
        }

        context->getRemoteHostFilter().checkHostAndPort(configuration.host, toString(configuration.port));
        return configuration;
    }

    StoragePtr createStorageRedis(const StorageFactory::Arguments & args)
    {
        auto configuration = getRedisConfiguration(args.engine_args, args.getLocalContext());

        StorageInMemoryMetadata metadata;
        metadata.setColumns(args.columns);
        metadata.setConstraints(args.constraints);
        metadata.setComment(args.comment);

        if (!args.storage_def->primary_key)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "StorageRedis must require one column in primary key");

        auto primary_key_desc = KeyDescription::getKeyFromAST(args.storage_def->primary_key->ptr(), metadata.columns, args.getContext());
        auto primary_key_names = primary_key_desc.expression->getRequiredColumns();

        if (primary_key_names.size() != 1)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "StorageRedis must require one column in primary key");
        }

        return std::make_shared<StorageRedis>(args.table_id, configuration, args.getContext(), metadata, primary_key_names[0]);
    }
}

Chunk StorageRedis::getBySerializedKeys(const std::vector<std::string> & keys, PaddedPODArray<UInt8> * null_map) const
{
    RedisArray redis_keys;
    for (const auto & key : keys)
        redis_keys.add(key);
    return getBySerializedKeys(redis_keys, null_map);
}

Chunk StorageRedis::getBySerializedKeys(const RedisArray & keys, PaddedPODArray<UInt8> * null_map) const
{
    Block sample_block = getInMemoryMetadataPtr()->getSampleBlock();

    size_t primary_key_pos = getPrimaryKeyPos(sample_block, getPrimaryKey());
    MutableColumns columns = sample_block.cloneEmptyColumns();

    RedisArray values = multiGet(keys);
    if (values.isNull() || values.size() == 0)
        return {};

    if (null_map)
    {
        null_map->clear();
        null_map->resize_fill(keys.size(), 1);
    }

    for (size_t i = 0; i < values.size(); ++i)
    {
        if (!values.get<RedisBulkString>(i).isNull())
        {
            fillColumns(keys.get<RedisBulkString>(i).value(),
                        values.get<RedisBulkString>(i).value(),
                        primary_key_pos, sample_block, columns
            );
        }
        else /// key not found
        {
            if (null_map)
            {
                (*null_map)[i] = 0;
                for (size_t col_idx = 0; col_idx < sample_block.columns(); ++col_idx)
                {
                    columns[col_idx]->insert(sample_block.getByPosition(col_idx).type->getDefault());
                }
            }
        }
    }

    size_t num_rows = columns.at(0)->size();
    return Chunk(std::move(columns), num_rows);
}

std::pair<RedisIterator, RedisArray> StorageRedis::scan(RedisIterator iterator, const String & pattern, uint64_t max_count)
{
    auto connection = getRedisConnection(pool, configuration);
    RedisCommand scan("SCAN");
    scan << toString(iterator) << "MATCH" << pattern << "COUNT" << toString(max_count);

    const auto & result = connection->client->execute<RedisArray>(scan);
    RedisIterator next = parse<RedisIterator>(result.get<RedisBulkString>(0).value());

    return {next, result.get<RedisArray>(1)};
}

RedisArray StorageRedis::multiGet(const RedisArray & keys) const
{
    auto connection = getRedisConnection(pool, configuration);

    RedisCommand cmd_mget("MGET");
    for (size_t i = 0; i < keys.size(); ++i)
        cmd_mget.add(keys.get<RedisBulkString>(i));

    return connection->client->execute<RedisArray>(cmd_mget);
}

void StorageRedis::multiSet(const RedisArray & data) const
{
    auto connection = getRedisConnection(pool, configuration);

    RedisCommand cmd_mget("MSET");
    for (size_t i = 0; i < data.size(); ++i)
        cmd_mget.add(data.get<RedisBulkString>(i));

    auto ret = connection->client->execute<RedisSimpleString>(cmd_mget);
    if (ret != "OK")
        throw Exception(ErrorCodes::INTERNAL_REDIS_ERROR, "Fail to write to redis table {}, for {}", table_id.getFullNameNotQuoted(), ret);
}

RedisInteger StorageRedis::multiDelete(const RedisArray & keys) const
{
    auto connection = getRedisConnection(pool, configuration);

    RedisCommand cmd("DEL");
    for (size_t i = 0; i < keys.size(); ++i)
        cmd.add(keys.get<RedisBulkString>(i));

    auto ret = connection->client->execute<RedisInteger>(cmd);
    if (ret != static_cast<RedisInteger>(keys.size()))
        LOG_DEBUG(
            log,
            "Try to delete {} rows but actually deleted {} rows from redis table {}.",
            keys.size(),
            ret,
            table_id.getFullNameNotQuoted());

    return ret;
}

Chunk StorageRedis::getByKeys(const ColumnsWithTypeAndName & keys, PaddedPODArray<UInt8> & null_map, const Names &) const
{
    if (keys.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "StorageRedis supports only one key, got: {}", keys.size());

    auto raw_keys = serializeKeysToRawString(keys[0]);

    if (raw_keys.size() != keys[0].column->size())
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Assertion failed: {} != {}", raw_keys.size(), keys[0].column->size());

    return getBySerializedKeys(raw_keys, &null_map);
}

Block StorageRedis::getSampleBlock(const Names &) const
{
    return getInMemoryMetadataPtr()->getSampleBlock();
}

SinkToStoragePtr StorageRedis::write(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr /*context*/,
    bool /*async_insert*/)
{
    return std::make_shared<RedisSink>(*this, metadata_snapshot);
}

void StorageRedis::truncate(const ASTPtr & query, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &)
{
    auto connection = getRedisConnection(pool, configuration);

    auto * truncate_query = query->as<ASTDropQuery>();
    assert(truncate_query != nullptr);

    RedisCommand cmd_flush_db("FLUSHDB");
    if (!truncate_query->sync)
        cmd_flush_db.add("ASYNC");

    auto ret = connection->client->execute<RedisSimpleString>(cmd_flush_db);

    if (ret != "OK")
        throw Exception(ErrorCodes::INTERNAL_REDIS_ERROR, "Fail to truncate redis table {}, for {}", table_id.getFullNameNotQuoted(), ret);
}

void StorageRedis::checkMutationIsPossible(const MutationCommands & commands, const Settings & /* settings */) const
{
    if (commands.empty())
        return;

    if (commands.size() > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mutations cannot be combined for StorageRedis");

    const auto command_type = commands.front().type;
    if (command_type != MutationCommand::Type::UPDATE && command_type != MutationCommand::Type::DELETE)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only DELETE and UPDATE mutation supported for StorageRedis");
}

void StorageRedis::mutate(const MutationCommands & commands, ContextPtr context_)
{
    if (commands.empty())
        return;

    assert(commands.size() == 1);

    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto storage = getStorageID();
    auto storage_ptr = DatabaseCatalog::instance().getTable(storage, context_);

    if (commands.front().type == MutationCommand::Type::DELETE)
    {
        MutationsInterpreter::Settings settings(true);
        settings.return_all_columns = true;
        settings.return_mutated_rows = true;

        auto interpreter = std::make_unique<MutationsInterpreter>(storage_ptr, metadata_snapshot, commands, context_, settings);
        auto pipeline = QueryPipelineBuilder::getPipeline(interpreter->execute());
        PullingPipelineExecutor executor(pipeline);

        auto sink = std::make_shared<RedisSink>(*this, metadata_snapshot);

        auto header = interpreter->getUpdatedHeader();
        auto primary_key_pos = header.getPositionByName(primary_key);

        Block block;
        while (executor.pull(block))
        {
            auto & column_type_name = block.getByPosition(primary_key_pos);

            auto column = column_type_name.column;
            auto size = column->size();

            RedisArray keys;
            WriteBufferFromOwnString wb_key;
            for (size_t i = 0; i < size; ++i)
            {
                wb_key.restart();
                column_type_name.type->getDefaultSerialization()->serializeBinary(*column, i, wb_key, {});
                keys.add(wb_key.str());
            }
            multiDelete(keys);
        }
        return;
    }

    assert(commands.front().type == MutationCommand::Type::UPDATE);
    if (commands.front().column_to_update_expression.contains(primary_key))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Primary key cannot be updated (cannot update column {})", primary_key);

    MutationsInterpreter::Settings settings(true);
    settings.return_all_columns = true;
    settings.return_mutated_rows = true;

    auto interpreter = std::make_unique<MutationsInterpreter>(storage_ptr, metadata_snapshot, commands, context_, settings);
    auto pipeline = QueryPipelineBuilder::getPipeline(interpreter->execute());
    PullingPipelineExecutor executor(pipeline);

    auto sink = std::make_shared<RedisSink>(*this, metadata_snapshot);

    Block block;
    while (executor.pull(block))
    {
        Chunk chunk(block.getColumns(), block.rows());
        sink->consume(chunk);
    }
}

/// TODO support ttl
void registerStorageRedis(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_sort_order = true,
        .supports_parallel_insert = true,
        .source_access_type = AccessType::REDIS,
    };

    factory.registerStorage("Redis", createStorageRedis, features);
}

}
