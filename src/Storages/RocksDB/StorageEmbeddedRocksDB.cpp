#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/RocksDB/StorageEmbeddedRocksDB.h>
#include <Storages/MutationCommands.h>

#include <DataTypes/DataTypesNumber.h>

#include <Storages/StorageFactory.h>
#include <Storages/KVStorageUtils.h>

#include <Parsers/ASTCreateQuery.h>

#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/ISource.h>

#include <Interpreters/castColumn.h>
#include <Interpreters/Context.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/MutationsInterpreter.h>

#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sources/NullSource.h>

#include <Poco/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Core/Settings.h>
#include <Storages/AlterCommands.h>
#include <Storages/RocksDB/RocksDBSettings.h>
#include <IO/SharedThreadPools.h>
#include <Disks/DiskLocal.h>
#include <base/sort.h>

#include <rocksdb/advanced_options.h>
#include <rocksdb/compression_type.h>
#include <rocksdb/convenience.h>
#include <rocksdb/env.h>
#include <rocksdb/options.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/db_ttl.h>

#include <cstddef>
#include <filesystem>
#include <memory>
#include <utility>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ROCKSDB_ERROR;
    extern const int NOT_IMPLEMENTED;
}

using FieldVectorPtr = std::shared_ptr<FieldVector>;
using RocksDBOptions = std::unordered_map<std::string, std::string>;

static RocksDBOptions getOptionsFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & path)
{
    RocksDBOptions options;

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(path, keys);

    for (const auto & key : keys)
    {
        const String key_path = path + "." + key;
        options[key] = config.getString(key_path);
    }

    return options;
}

class EmbeddedRocksDBSource : public ISource
{
public:
    EmbeddedRocksDBSource(
        const StorageEmbeddedRocksDB & storage_,
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

    EmbeddedRocksDBSource(
        const StorageEmbeddedRocksDB & storage_,
        const Block & header,
        std::unique_ptr<rocksdb::Iterator> iterator_,
        const size_t max_block_size_)
        : ISource(header)
        , storage(storage_)
        , primary_key_pos(getPrimaryKeyPos(header, storage.getPrimaryKey()))
        , iterator(std::move(iterator_))
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

    Chunk generateFullScan()
    {
        if (!iterator->Valid())
            return {};

        const auto & sample_block = getPort().getHeader();
        MutableColumns columns = sample_block.cloneEmptyColumns();

        for (size_t rows = 0; iterator->Valid() && rows < max_block_size; ++rows, iterator->Next())
        {
            fillColumns(iterator->key(), iterator->value(), primary_key_pos, getPort().getHeader(), columns);
        }

        if (!iterator->status().ok())
        {
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Engine {} got error while seeking key value data: {}",
                getName(), iterator->status().ToString());
        }
        Block block = sample_block.cloneWithColumns(std::move(columns));
        return Chunk(block.getColumns(), block.rows());
    }

private:
    const StorageEmbeddedRocksDB & storage;

    size_t primary_key_pos;

    /// For key scan
    FieldVectorPtr keys = nullptr;
    FieldVector::const_iterator begin;
    FieldVector::const_iterator end;
    FieldVector::const_iterator it;

    /// For full scan
    std::unique_ptr<rocksdb::Iterator> iterator = nullptr;

    const size_t max_block_size;
};


StorageEmbeddedRocksDB::StorageEmbeddedRocksDB(const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata_,
        LoadingStrictnessLevel mode,
        ContextPtr context_,
        std::unique_ptr<RocksDBSettings> settings_,
        const String & primary_key_,
        Int32 ttl_,
        String rocksdb_dir_,
        bool read_only_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , log(getLogger(fmt::format("StorageEmbeddedRocksDB ({})", getStorageID().getNameForLogs())))
    , primary_key{primary_key_}
    , rocksdb_dir(std::move(rocksdb_dir_))
    , ttl(ttl_)
    , read_only(read_only_)
{
    setInMemoryMetadata(metadata_);
    setSettings(std::move(settings_));
    if (rocksdb_dir.empty())
    {
        rocksdb_dir = context_->getPath() + relative_data_path_;
    }
    if (mode < LoadingStrictnessLevel::ATTACH)
    {
        fs::create_directories(rocksdb_dir);
    }
    initDB();
}

StorageEmbeddedRocksDB::~StorageEmbeddedRocksDB() = default;

void StorageEmbeddedRocksDB::truncate(const ASTPtr &, const StorageMetadataPtr & , ContextPtr, TableExclusiveLockHolder &)
{
    std::lock_guard lock(rocksdb_ptr_mx);
    rocksdb_ptr->Close();
    rocksdb_ptr = nullptr;

    (void)fs::remove_all(rocksdb_dir);
    fs::create_directories(rocksdb_dir);
    initDB();
}

void StorageEmbeddedRocksDB::checkMutationIsPossible(const MutationCommands & commands, const Settings & /* settings */) const
{
    if (commands.empty())
        return;

    if (commands.size() > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mutations cannot be combined for EmbeddedRocksDB");

    const auto command_type = commands.front().type;
    if (command_type != MutationCommand::Type::UPDATE && command_type != MutationCommand::Type::DELETE)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only DELETE and UPDATE mutation supported for EmbeddedRocksDB");
}

void StorageEmbeddedRocksDB::mutate(const MutationCommands & commands, ContextPtr context_)
{
    if (commands.empty())
        return;

    assert(commands.size() == 1);

    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto storage = getStorageID();
    auto storage_ptr = DatabaseCatalog::instance().getTable(storage, context_);

    if (commands.front().type == MutationCommand::Type::DELETE)
    {
        MutationsInterpreter::Settings mutation_settings(true);
        mutation_settings.return_all_columns = true;
        mutation_settings.return_mutated_rows = true;

        auto interpreter = std::make_unique<MutationsInterpreter>(
            storage_ptr,
            metadata_snapshot,
            commands,
            context_,
            mutation_settings);

        auto pipeline = QueryPipelineBuilder::getPipeline(interpreter->execute());
        PullingPipelineExecutor executor(pipeline);

        auto header = interpreter->getUpdatedHeader();
        auto primary_key_pos = header.getPositionByName(primary_key);

        Block block;
        while (executor.pull(block))
        {
            auto & column_type_name = block.getByPosition(primary_key_pos);

            auto column = column_type_name.column;
            auto size = column->size();

            rocksdb::WriteBatch batch;
            WriteBufferFromOwnString wb_key;
            for (size_t i = 0; i < size; ++i)
            {
                wb_key.restart();

                column_type_name.type->getDefaultSerialization()->serializeBinary(*column, i, wb_key, {});
                auto status = batch.Delete(wb_key.str());
                if (!status.ok())
                    throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB write error: {}", status.ToString());
            }

            auto status = rocksdb_ptr->Write(rocksdb::WriteOptions(), &batch);
            if (!status.ok())
                throw Exception(ErrorCodes::ROCKSDB_ERROR, "RocksDB write error: {}", status.ToString());
        }

        return;
    }

    assert(commands.front().type == MutationCommand::Type::UPDATE);
    if (commands.front().column_to_update_expression.contains(primary_key))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Primary key cannot be updated (cannot update column {})", primary_key);

    MutationsInterpreter::Settings mutation_settings(true);
    mutation_settings.return_all_columns = true;
    mutation_settings.return_mutated_rows = true;

    auto interpreter = std::make_unique<MutationsInterpreter>(
        storage_ptr,
        metadata_snapshot,
        commands,
        context_,
        mutation_settings);

    auto pipeline = QueryPipelineBuilder::getPipeline(interpreter->execute());
    PullingPipelineExecutor executor(pipeline);

    auto sink = std::make_shared<EmbeddedRocksDBSink>(*this, metadata_snapshot);

    Block block;
    while (executor.pull(block))
    {
        auto chunk = Chunk(block.getColumns(), block.rows());
        sink->consume(chunk);
    }
}

void StorageEmbeddedRocksDB::drop()
{
    std::lock_guard lock(rocksdb_ptr_mx);
    rocksdb_ptr->Close();
    rocksdb_ptr = nullptr;
}

bool StorageEmbeddedRocksDB::optimize(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Names & /* deduplicate_by_columns */,
    bool cleanup,
    ContextPtr /*context*/)
{
    if (partition)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Partition cannot be specified when optimizing table of type EmbeddedRocksDB");

    if (final)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "FINAL cannot be specified when optimizing table of type EmbeddedRocksDB");

    if (deduplicate)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DEDUPLICATE cannot be specified when optimizing table of type EmbeddedRocksDB");

    if (cleanup)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "CLEANUP cannot be specified when optimizing table of type EmbeddedRocksDB");

    std::shared_lock lock(rocksdb_ptr_mx);
    rocksdb::CompactRangeOptions compact_options;
    auto status = rocksdb_ptr->CompactRange(compact_options, nullptr, nullptr);
    if (!status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "Compaction failed: {}", status.ToString());
    return true;
}

static_assert(rocksdb::DEBUG_LEVEL == 0);
static_assert(rocksdb::HEADER_LEVEL == 5);
static constexpr std::array<std::pair<DB::LogsLevel, Poco::Message::Priority>, 6> rocksdb_logger_map = {
    std::make_pair(DB::LogsLevel::debug, Poco::Message::Priority::PRIO_DEBUG),
    std::make_pair(DB::LogsLevel::information, Poco::Message::Priority::PRIO_INFORMATION),
    std::make_pair(DB::LogsLevel::warning, Poco::Message::Priority::PRIO_WARNING),
    std::make_pair(DB::LogsLevel::error, Poco::Message::Priority::PRIO_ERROR),
    std::make_pair(DB::LogsLevel::fatal, Poco::Message::Priority::PRIO_FATAL),
    /// Same as default logger does for HEADER_LEVEL
    std::make_pair(DB::LogsLevel::information, Poco::Message::Priority::PRIO_INFORMATION),
};
class StorageEmbeddedRocksDBLogger : public rocksdb::Logger
{
public:
    explicit StorageEmbeddedRocksDBLogger(const rocksdb::InfoLogLevel log_level, LoggerRawPtr log_)
        : rocksdb::Logger(log_level)
        , log(log_)
    {}

    void Logv(const char * format, va_list ap) override
        __attribute__((format(printf, 2, 0)))
    {
        Logv(rocksdb::InfoLogLevel::DEBUG_LEVEL, format, ap);
    }

    void Logv(const rocksdb::InfoLogLevel log_level, const char * format, va_list ap) override
        __attribute__((format(printf, 3, 0)))
    {
        if (log_level < GetInfoLogLevel())
            return;

        auto level = rocksdb_logger_map[log_level];

        /// stack buffer was enough
        {
            va_list backup_ap;
            va_copy(backup_ap, ap);
            std::array<char, 1024> stack;
            if (vsnprintf(stack.data(), stack.size(), format, backup_ap) < static_cast<int>(stack.size()))
            {
                va_end(backup_ap);
                LOG_IMPL(log, level.first, level.second, "{}", stack.data());
                return;
            }
            va_end(backup_ap);
        }

        /// let's try with a bigger dynamic buffer (but not too huge, since
        /// some of rocksdb internal code has also such a limitation, i..e
        /// HdfsLogger)
        {
            va_list backup_ap;
            va_copy(backup_ap, ap);
            static constexpr int buffer_size = 30000;
            std::unique_ptr<char[]> buffer(new char[buffer_size]);
            if (vsnprintf(buffer.get(), buffer_size, format, backup_ap) >= buffer_size)
                buffer[buffer_size - 1] = 0;
            va_end(backup_ap);
            LOG_IMPL(log, level.first, level.second, "{}", buffer.get());
        }
    }

private:
    LoggerRawPtr log;
};

void StorageEmbeddedRocksDB::initDB()
{
    rocksdb::Status status;
    rocksdb::Options base;

    base.create_if_missing = true;
    base.compression = rocksdb::CompressionType::kZSTD;
    base.statistics = rocksdb::CreateDBStatistics();
    /// It is too verbose by default, and in fact we don't care about rocksdb logs at all.
    base.info_log_level = rocksdb::ERROR_LEVEL;

    rocksdb::Options merged = base;
    rocksdb::BlockBasedTableOptions table_options;

    const auto & config = getContext()->getConfigRef();
    if (config.has("rocksdb.options"))
    {
        auto config_options = getOptionsFromConfig(config, "rocksdb.options");
        status = rocksdb::GetDBOptionsFromMap({}, merged, config_options, &merged);
        if (!status.ok())
        {
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to merge rocksdb options from 'rocksdb.options' at: {}: {}",
                rocksdb_dir, status.ToString());
        }
    }
    if (config.has("rocksdb.column_family_options"))
    {
        auto column_family_options = getOptionsFromConfig(config, "rocksdb.column_family_options");
        status = rocksdb::GetColumnFamilyOptionsFromMap({}, merged, column_family_options, &merged);
        if (!status.ok())
        {
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to merge rocksdb options from 'rocksdb.column_family_options' at: {}: {}",
                rocksdb_dir, status.ToString());
        }
    }
    if (config.has("rocksdb.block_based_table_options"))
    {
        auto block_based_table_options = getOptionsFromConfig(config, "rocksdb.block_based_table_options");
        status = rocksdb::GetBlockBasedTableOptionsFromMap({}, table_options, block_based_table_options, &table_options);
        if (!status.ok())
        {
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to merge rocksdb options from 'rocksdb.block_based_table_options' at: {}: {}",
                rocksdb_dir, status.ToString());
        }
    }

    if (config.has("rocksdb.tables"))
    {
        auto table_name = getStorageID().getTableName();

        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys("rocksdb.tables", keys);

        for (const auto & key : keys)
        {
            const String key_prefix = "rocksdb.tables." + key;
            if (config.getString(key_prefix + ".name") != table_name)
                continue;

            String config_key = key_prefix + ".options";
            if (config.has(config_key))
            {
                auto table_config_options = getOptionsFromConfig(config, config_key);
                status = rocksdb::GetDBOptionsFromMap({}, merged, table_config_options, &merged);
                if (!status.ok())
                {
                    throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to merge rocksdb options from '{}' at: {}: {}",
                        config_key, rocksdb_dir, status.ToString());
                }
            }

            config_key = key_prefix + ".column_family_options";
            if (config.has(config_key))
            {
                auto table_column_family_options = getOptionsFromConfig(config, config_key);
                status = rocksdb::GetColumnFamilyOptionsFromMap({}, merged, table_column_family_options, &merged);
                if (!status.ok())
                {
                    throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to merge rocksdb options from '{}' at: {}: {}",
                        config_key, rocksdb_dir, status.ToString());
                }
            }

            config_key = key_prefix + ".block_based_table_options";
            if (config.has(config_key))
            {
                auto block_based_table_options = getOptionsFromConfig(config, config_key);
                status = rocksdb::GetBlockBasedTableOptionsFromMap({}, table_options, block_based_table_options, &table_options);
                if (!status.ok())
                {
                    throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to merge rocksdb options from '{}' at: {}: {}",
                        config_key, rocksdb_dir, status.ToString());
                }
            }
        }
    }

    merged.info_log = std::make_shared<StorageEmbeddedRocksDBLogger>(merged.info_log_level, log.get());
    merged.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    if (ttl > 0)
    {
        rocksdb::DBWithTTL * db;
        status = rocksdb::DBWithTTL::Open(merged, rocksdb_dir, &db, ttl, read_only);
        if (!status.ok())
        {
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to open rocksdb path at: {}: {}",
                rocksdb_dir, status.ToString());
        }
        rocksdb_ptr = std::unique_ptr<rocksdb::DBWithTTL>(db);
    }
    else
    {
        rocksdb::DB * db;
        if (read_only)
            status = rocksdb::DB::OpenForReadOnly(merged, rocksdb_dir, &db);
        else
            status = rocksdb::DB::Open(merged, rocksdb_dir, &db);

        if (!status.ok())
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to open rocksdb path at: {}: {}", rocksdb_dir, status.ToString());

        rocksdb_ptr = std::unique_ptr<rocksdb::DB>(db);
    }
}

class ReadFromEmbeddedRocksDB : public SourceStepWithFilter
{
public:
    std::string getName() const override { return "ReadFromEmbeddedRocksDB"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    void applyFilters(ActionDAGNodes added_filter_nodes) override;

    ReadFromEmbeddedRocksDB(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        Block sample_block,
        const StorageEmbeddedRocksDB & storage_,
        size_t max_block_size_,
        size_t num_streams_)
        : SourceStepWithFilter(DataStream{.header = std::move(sample_block)}, column_names_, query_info_, storage_snapshot_, context_)
        , storage(storage_)
        , max_block_size(max_block_size_)
        , num_streams(num_streams_)
    {
    }

private:
    const StorageEmbeddedRocksDB & storage;

    size_t max_block_size;
    size_t num_streams;

    FieldVectorPtr keys;
    bool all_scan = false;
};

void StorageEmbeddedRocksDB::read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t max_block_size,
        size_t num_streams)
{
    storage_snapshot->check(column_names);
    Block sample_block = storage_snapshot->metadata->getSampleBlock();

    auto reading = std::make_unique<ReadFromEmbeddedRocksDB>(
        column_names, query_info, storage_snapshot, context_, std::move(sample_block), *this, max_block_size, num_streams);

    query_plan.addStep(std::move(reading));
}

void ReadFromEmbeddedRocksDB::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    const auto & sample_block = getOutputStream().header;

    if (all_scan)
    {
        auto iterator = std::unique_ptr<rocksdb::Iterator>(storage.rocksdb_ptr->NewIterator(rocksdb::ReadOptions()));
        iterator->SeekToFirst();
        auto source = std::make_shared<EmbeddedRocksDBSource>(storage, sample_block, std::move(iterator), max_block_size);
        source->setStorageLimits(query_info.storage_limits);
        pipeline.init(Pipe(std::move(source)));
    }
    else
    {
        if (keys->empty())
        {
            pipeline.init(Pipe(std::make_shared<NullSource>(sample_block)));
            return;
        }

        ::sort(keys->begin(), keys->end());
        keys->erase(std::unique(keys->begin(), keys->end()), keys->end());

        Pipes pipes;

        size_t num_keys = keys->size();
        size_t num_threads = std::min<size_t>(num_streams, keys->size());

        assert(num_keys <= std::numeric_limits<uint32_t>::max());
        assert(num_threads <= std::numeric_limits<uint32_t>::max());

        for (size_t thread_idx = 0; thread_idx < num_threads; ++thread_idx)
        {
            size_t begin = num_keys * thread_idx / num_threads;
            size_t end = num_keys * (thread_idx + 1) / num_threads;

            auto source = std::make_shared<EmbeddedRocksDBSource>(
                    storage, sample_block, keys, keys->begin() + begin, keys->begin() + end, max_block_size);
            source->setStorageLimits(query_info.storage_limits);
            pipes.emplace_back(std::move(source));
        }
        pipeline.init(Pipe::unitePipes(std::move(pipes)));
    }
}

void ReadFromEmbeddedRocksDB::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    const auto & sample_block = getOutputStream().header;
    auto primary_key_data_type = sample_block.getByName(storage.primary_key).type;
    std::tie(keys, all_scan) = getFilterKeys(storage.primary_key, primary_key_data_type, filter_actions_dag, context);
}

SinkToStoragePtr StorageEmbeddedRocksDB::write(
    const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr  query_context, bool /*async_insert*/)
{
    if (getSettings().optimize_for_bulk_insert)
    {
        LOG_DEBUG(log, "Using bulk insert");
        return std::make_shared<EmbeddedRocksDBBulkSink>(query_context, *this, metadata_snapshot);
    }

    LOG_DEBUG(log, "Using regular insert");
    return std::make_shared<EmbeddedRocksDBSink>(*this, metadata_snapshot);
}

static StoragePtr create(const StorageFactory::Arguments & args)
{
    // TODO custom RocksDBSettings, table function
    auto engine_args = args.engine_args;
    if (engine_args.size() > 3)
    {
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Engine {} requires at most 3 parameters. "
                        "({} given). Correct usage: EmbeddedRocksDB([ttl, rocksdb_dir, read_only])",
                        args.engine_name, engine_args.size());
    }

    Int32 ttl{0};
    String rocksdb_dir;
    bool read_only{false};
    if (!engine_args.empty())
        ttl = static_cast<Int32>(checkAndGetLiteralArgument<UInt64>(engine_args[0], "ttl"));
    if (engine_args.size() > 1)
        rocksdb_dir = checkAndGetLiteralArgument<String>(engine_args[1], "rocksdb_dir");
    if (engine_args.size() > 2)
        read_only = checkAndGetLiteralArgument<bool>(engine_args[2], "read_only");

    StorageInMemoryMetadata metadata;
    metadata.setColumns(args.columns);
    metadata.setConstraints(args.constraints);
    metadata.setComment(args.comment);

    if (!args.storage_def->primary_key)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "StorageEmbeddedRocksDB must require one column in primary key");

    metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->primary_key->ptr(), metadata.columns, args.getContext());
    auto primary_key_names = metadata.getColumnsRequiredForPrimaryKey();
    if (primary_key_names.size() != 1)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "StorageEmbeddedRocksDB must require one column in primary key");
    }
    auto settings = std::make_unique<RocksDBSettings>();
    settings->loadFromQuery(*args.storage_def, args.getContext());
    if (args.storage_def->settings)
        metadata.settings_changes = args.storage_def->settings->ptr();
    else
    {
        /// A workaround because embedded rocksdb doesn't have default immutable settings
        /// But InterpreterAlterQuery requires settings_changes to be set to run ALTER MODIFY
        /// SETTING queries. So we just add a setting with its default value.
        auto settings_changes = std::make_shared<ASTSetQuery>();
        settings_changes->is_standalone = false;
        settings_changes->changes.insertSetting("optimize_for_bulk_insert", settings->optimize_for_bulk_insert.value);
        metadata.settings_changes = settings_changes;
    }
    return std::make_shared<StorageEmbeddedRocksDB>(args.table_id, args.relative_data_path, metadata, args.mode, args.getContext(), std::move(settings), primary_key_names[0], ttl, std::move(rocksdb_dir), read_only);
}

std::shared_ptr<rocksdb::Statistics> StorageEmbeddedRocksDB::getRocksDBStatistics() const
{
    std::shared_lock lock(rocksdb_ptr_mx);
    if (!rocksdb_ptr)
        return nullptr;
    return rocksdb_ptr->GetOptions().statistics;
}

std::vector<rocksdb::Status> StorageEmbeddedRocksDB::multiGet(const std::vector<rocksdb::Slice> & slices_keys, std::vector<String> & values) const
{
    std::shared_lock lock(rocksdb_ptr_mx);
    if (!rocksdb_ptr)
        return {};
    return rocksdb_ptr->MultiGet(rocksdb::ReadOptions(), slices_keys, &values);
}

Chunk StorageEmbeddedRocksDB::getByKeys(
    const ColumnsWithTypeAndName & keys,
    PaddedPODArray<UInt8> & null_map,
    const Names &) const
{
    if (keys.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "StorageEmbeddedRocksDB supports only one key, got: {}", keys.size());

    auto raw_keys = serializeKeysToRawString(keys[0]);

    if (raw_keys.size() != keys[0].column->size())
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Assertion failed: {} != {}", raw_keys.size(), keys[0].column->size());

    return getBySerializedKeys(raw_keys, &null_map);
}

Block StorageEmbeddedRocksDB::getSampleBlock(const Names &) const
{
    return getInMemoryMetadataPtr()->getSampleBlock();
}

Chunk StorageEmbeddedRocksDB::getBySerializedKeys(
    const std::vector<std::string> & keys,
    PaddedPODArray<UInt8> * null_map) const
{
    std::vector<String> values;
    Block sample_block = getInMemoryMetadataPtr()->getSampleBlock();

    size_t primary_key_pos = getPrimaryKeyPos(sample_block, getPrimaryKey());

    MutableColumns columns = sample_block.cloneEmptyColumns();

    /// Convert from vector of string to vector of string refs (rocksdb::Slice), because multiGet api expects them.
    std::vector<rocksdb::Slice> slices_keys;
    slices_keys.reserve(keys.size());
    for (const auto & key : keys)
        slices_keys.emplace_back(key);

    auto statuses = multiGet(slices_keys, values);
    if (null_map)
    {
        null_map->clear();
        null_map->resize_fill(statuses.size(), 1);
    }

    for (size_t i = 0; i < statuses.size(); ++i)
    {
        if (statuses[i].ok())
        {
            fillColumns(slices_keys[i], values[i], primary_key_pos, sample_block, columns);
        }
        else if (statuses[i].IsNotFound())
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
        else
        {
            throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "rocksdb error {}", statuses[i].ToString());
        }
    }

    size_t num_rows = columns.at(0)->size();
    return Chunk(std::move(columns), num_rows);
}

std::optional<UInt64> StorageEmbeddedRocksDB::totalRows(const Settings & query_settings) const
{
    if (!query_settings.optimize_trivial_approximate_count_query)
        return {};
    std::shared_lock lock(rocksdb_ptr_mx);
    if (!rocksdb_ptr)
        return {};
    UInt64 estimated_rows;
    if (!rocksdb_ptr->GetIntProperty("rocksdb.estimate-num-keys", &estimated_rows))
        return {};
    return estimated_rows;
}

std::optional<UInt64> StorageEmbeddedRocksDB::totalBytes(const Settings & /*settings*/) const
{
    std::shared_lock lock(rocksdb_ptr_mx);
    if (!rocksdb_ptr)
        return {};
    UInt64 estimated_bytes;
    if (!rocksdb_ptr->GetAggregatedIntProperty("rocksdb.estimate-live-data-size", &estimated_bytes))
        return {};
    return estimated_bytes;
}

void StorageEmbeddedRocksDB::alter(
    const AlterCommands & params,
    ContextPtr query_context,
    AlterLockHolder & holder)
{
    IStorage::alter(params, query_context, holder);
    auto new_metadata = getInMemoryMetadataPtr();
    if (new_metadata->settings_changes)
    {
        const auto & settings_changes = new_metadata->settings_changes->as<const ASTSetQuery &>();
        auto new_settings = std::make_unique<RocksDBSettings>();
        new_settings->applyChanges(settings_changes.changes);
        setSettings(std::move(new_settings));
    }
}

void registerStorageEmbeddedRocksDB(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .supports_sort_order = true,
        .supports_ttl = true,
        .supports_parallel_insert = true,
    };

    factory.registerStorage("EmbeddedRocksDB", create, features);
}

void StorageEmbeddedRocksDB::checkAlterIsPossible(const AlterCommands & commands, ContextPtr /* context */) const
{
    for (const auto & command : commands)
        if (!command.isCommentAlter() && !command.isSettingsAlter())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}", command.type, getName());
}

}
