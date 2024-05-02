#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/RocksDB/StorageEmbeddedRocksDB.h>
#include <Storages/RocksDB/EmbeddedRocksDBSink.h>
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
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <base/sort.h>

#include <rocksdb/table.h>
#include <rocksdb/convenience.h>
#include <rocksdb/utilities/db_ttl.h>

#include <cstddef>
#include <filesystem>
#include <utility>


namespace fs = std::filesystem;

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
        KeyIterator key_iterator_,
        const size_t max_block_size_)
        : ISource(header)
        , storage(storage_)
        , key_iterator(std::move(key_iterator_))
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
        , iterator(std::move(iterator_))
        , max_block_size(max_block_size_)
    {
    }

    String getName() const override { return storage.getName(); }

    Chunk generate() override
    {
        if (key_iterator.has_value())
            return generateWithKeys();
        return generateFullScan();
    }

    Chunk generateWithKeys()
    {
        if (key_iterator.value().atEnd())
            return {};
        auto raw_keys = serializeKeysToRawString(key_iterator.value(), storage.getPrimaryKeyTypes(), max_block_size);
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
            fillColumns(iterator->key(), storage.getPrimaryKeyPos(), getPort().getHeader(), columns);
            fillColumns(iterator->value(), storage.getValueColumnPos(), getPort().getHeader(), columns);
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

    /// For key scan
    std::optional<KeyIterator> key_iterator = std::nullopt;

    /// For full scan
    std::unique_ptr<rocksdb::Iterator> iterator = nullptr;

    const size_t max_block_size;
};


StorageEmbeddedRocksDB::StorageEmbeddedRocksDB(const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata_,
        LoadingStrictnessLevel mode,
        ContextPtr context_,
        Names primary_key_,
        Int32 ttl_,
        String rocksdb_dir_,
        bool read_only_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , primary_key{std::move(primary_key_)}
    , rocksdb_dir(std::move(rocksdb_dir_))
    , ttl(ttl_)
    , read_only(read_only_)
{
    setInMemoryMetadata(metadata_);
    if (rocksdb_dir.empty())
    {
        rocksdb_dir = context_->getPath() + relative_data_path_;
    }
    if (mode < LoadingStrictnessLevel::ATTACH)
    {
        fs::create_directories(rocksdb_dir);
    }

    const auto sample_block = getInMemoryMetadataPtr()->getSampleBlock();
    std::vector<bool> is_pk(sample_block.columns());
    primary_key_pos.reserve(primary_key.size());
    for (const auto& key_name : primary_key)
    {
        primary_key_pos.push_back(sample_block.getPositionByName(key_name));
        is_pk[primary_key_pos.back()] = true;
    }

    value_column_pos.reserve(is_pk.size() - primary_key_pos.size());
    for (size_t i = 0; i < is_pk.size(); ++i)
    {
        if (!is_pk[i])
            value_column_pos.push_back(i);
    }

    primary_key_types.reserve(primary_key.size());
    for (const auto pos : primary_key_pos)
    {
        const auto & column_type_name = sample_block.getByPosition(pos);
        primary_key_types.push_back(column_type_name.type);
    }

    initDB();
}

StorageEmbeddedRocksDB::~StorageEmbeddedRocksDB() = default;

void StorageEmbeddedRocksDB::truncate(const ASTPtr &, const StorageMetadataPtr & , ContextPtr, TableExclusiveLockHolder &)
{
    std::lock_guard lock(rocksdb_ptr_mx);
    rocksdb_ptr->Close();
    rocksdb_ptr = nullptr;

    fs::remove_all(rocksdb_dir);
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
        MutationsInterpreter::Settings settings(true);
        settings.return_all_columns = true;
        settings.return_mutated_rows = true;

        auto interpreter = std::make_unique<MutationsInterpreter>(
            storage_ptr,
            metadata_snapshot,
            commands,
            context_,
            settings);

        auto pipeline = QueryPipelineBuilder::getPipeline(interpreter->execute());
        PullingPipelineExecutor executor(pipeline);

        auto sink = std::make_shared<EmbeddedRocksDBSink>(*this, metadata_snapshot);

        Block block;
        while (executor.pull(block))
        {
            std::vector<ColumnPtr> columns;
            std::vector<DataTypePtr> types;
            columns.reserve(primary_key_pos.size());
            types.reserve(primary_key_pos.size());
            for (const auto pos : primary_key_pos)
            {
                auto & column_type_name = block.getByPosition(pos);
                columns.push_back(column_type_name.column);
                types.push_back(column_type_name.type);
            }

            const auto size = block.rows();
            rocksdb::WriteBatch batch;
            WriteBufferFromOwnString wb_key;
            for (size_t i = 0; i < size; ++i)
            {
                wb_key.restart();

                for (size_t j = 0; j < columns.size(); ++j)
                {
                    types[j]->getDefaultSerialization()->serializeBinary(*columns[j], i, wb_key, {});
                }
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
    for (const auto& key : primary_key)
    {
        if (commands.front().column_to_update_expression.contains(key))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Primary key cannot be updated (cannot update column {})", primary_key[0]);
    }

    MutationsInterpreter::Settings settings(true);
    settings.return_all_columns = true;
    settings.return_mutated_rows = true;

    auto interpreter = std::make_unique<MutationsInterpreter>(
        storage_ptr,
        metadata_snapshot,
        commands,
        context_,
        settings);

    auto pipeline = QueryPipelineBuilder::getPipeline(interpreter->execute());
    PullingPipelineExecutor executor(pipeline);

    auto sink = std::make_shared<EmbeddedRocksDBSink>(*this, metadata_snapshot);

    Block block;
    while (executor.pull(block))
    {
        sink->consume(Chunk{block.getColumns(), block.rows()});
    }
}

void StorageEmbeddedRocksDB::drop()
{
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
        status = rocksdb::GetDBOptionsFromMap(merged, config_options, &merged);
        if (!status.ok())
        {
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to merge rocksdb options from 'rocksdb.options' at: {}: {}",
                rocksdb_dir, status.ToString());
        }
    }
    if (config.has("rocksdb.column_family_options"))
    {
        auto column_family_options = getOptionsFromConfig(config, "rocksdb.column_family_options");
        status = rocksdb::GetColumnFamilyOptionsFromMap(merged, column_family_options, &merged);
        if (!status.ok())
        {
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to merge rocksdb options from 'rocksdb.column_family_options' at: {}: {}",
                rocksdb_dir, status.ToString());
        }
    }
    if (config.has("rocksdb.block_based_table_options"))
    {
        auto block_based_table_options = getOptionsFromConfig(config, "rocksdb.block_based_table_options");
        status = rocksdb::GetBlockBasedTableOptionsFromMap(table_options, block_based_table_options, &table_options);
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
                status = rocksdb::GetDBOptionsFromMap(merged, table_config_options, &merged);
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
                status = rocksdb::GetColumnFamilyOptionsFromMap(merged, table_column_family_options, &merged);
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
                status = rocksdb::GetBlockBasedTableOptionsFromMap(table_options, block_based_table_options, &table_options);
                if (!status.ok())
                {
                    throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to merge rocksdb options from '{}' at: {}: {}",
                        config_key, rocksdb_dir, status.ToString());
                }
            }
        }
    }

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
        {
            status = rocksdb::DB::OpenForReadOnly(merged, rocksdb_dir, &db);
        }
        else
        {
            status = rocksdb::DB::Open(merged, rocksdb_dir, &db);
        }
        if (!status.ok())
        {
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to open rocksdb path at: {}: {}",
                rocksdb_dir, status.ToString());
        }
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
    // TODO: Use this for all scan or key scan.
    [[maybe_unused]] size_t num_streams;

    FieldVectorsPtr key_values;
    bool all_scan = true;
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
        if (key_values == nullptr)
        {
            pipeline.init(Pipe(std::make_shared<NullSource>(sample_block)));
            return;
        }

        // Use single thread to do the key scan since currently the key number of key scan method tends to be small, so it's not worthy to
        // spawn threads to scan. If multi-threaded scan is necessary, we can easily create multiple KeyIterator with each containing part
        // of the work.
        auto source = std::make_shared<EmbeddedRocksDBSource>(
            storage, sample_block, KeyIterator(key_values), max_block_size);
        source->setStorageLimits(query_info.storage_limits);
        pipeline.init(Pipe(std::move(source)));
    }
}

void ReadFromEmbeddedRocksDB::applyFilters(ActionDAGNodes added_filter_nodes)
{
    filter_actions_dag = ActionsDAG::buildFilterActionsDAG(added_filter_nodes.nodes);
    std::tie(key_values, all_scan) = getFilterKeys(storage.primary_key, storage.getPrimaryKeyTypes(), filter_actions_dag, context);
}

SinkToStoragePtr StorageEmbeddedRocksDB::write(
    const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr /*context*/, bool /*async_insert*/)
{
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

    if (!args.storage_def->primary_key)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "StorageEmbeddedRocksDB must require one column in primary key");

    metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->primary_key->ptr(), metadata.columns, args.getContext());
    auto primary_key_names = metadata.getColumnsRequiredForPrimaryKey();
    if (primary_key_names.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "StorageEmbeddedRocksDB must require at least one column in primary key");
    return std::make_shared<StorageEmbeddedRocksDB>(args.table_id, args.relative_data_path, metadata, args.mode, args.getContext(),
        std::move(primary_key_names), ttl, std::move(rocksdb_dir), read_only);
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
    if (keys.size() != primary_key.size())
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Key column number mismatch, should be {}, is {}.",
                primary_key.size(), keys.size());
    for (size_t i = 0; i < keys.size(); ++i)
        if (!keys[i].type->equals(*primary_key_types[i]))
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Primary key type mismatch: {} vs {}.",
                    primary_key_types[i]->getName(), keys[i].type->getName());

    std::vector<std::string> raw_keys;
    raw_keys.reserve(keys[0].column->size());
    for (size_t i = 0; i < keys[0].column->size(); ++i)
    {
        std::string & serialized_key = raw_keys.emplace_back();
        WriteBufferFromString wb(serialized_key);
        for (const auto& key : keys)
        {
            Field field;
            key.column->get(i, field);
            key.type->getDefaultSerialization()->serializeBinary(field, wb, {});
        }
        wb.finalize();
    }

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
            fillColumns(slices_keys[i], getPrimaryKeyPos(), sample_block, columns);
            fillColumns(values[i], getValueColumnPos(), sample_block, columns);
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

std::optional<UInt64> StorageEmbeddedRocksDB::totalRows(const Settings & settings) const
{
    if (!settings.optimize_trivial_approximate_count_query)
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

void registerStorageEmbeddedRocksDB(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_sort_order = true,
        .supports_ttl = true,
        .supports_parallel_insert = true,
    };

    factory.registerStorage("EmbeddedRocksDB", create, features);
}
}
