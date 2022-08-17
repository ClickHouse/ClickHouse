#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/RocksDB/StorageEmbeddedRocksDB.h>
#include <Storages/RocksDB/EmbeddedRocksDBSink.h>

#include <DataTypes/DataTypesNumber.h>

#include <Storages/StorageFactory.h>
#include <Storages/KVStorageUtils.h>

#include <Parsers/ASTCreateQuery.h>

#include <QueryPipeline/Pipe.h>
#include <Processors/ISource.h>

#include <Interpreters/castColumn.h>
#include <Interpreters/Context.h>
#include <Interpreters/TreeRewriter.h>

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
#include <shared_mutex>


namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ROCKSDB_ERROR;
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
            throw Exception("Engine " + getName() + " got error while seeking key value data: " + iterator->status().ToString(),
                ErrorCodes::ROCKSDB_ERROR);
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
        bool attach,
        ContextPtr context_,
        const String & primary_key_,
        Int32 ttl_,
        bool read_only_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , primary_key{primary_key_}
    , ttl(ttl_)
    , read_only(read_only_)
{
    setInMemoryMetadata(metadata_);
    rocksdb_dir = context_->getPath() + relative_data_path_;
    if (!attach)
    {
        fs::create_directories(rocksdb_dir);
    }
    initDB();
}

void StorageEmbeddedRocksDB::truncate(const ASTPtr &, const StorageMetadataPtr & , ContextPtr, TableExclusiveLockHolder &)
{
    std::lock_guard lock(rocksdb_ptr_mx);
    rocksdb_ptr->Close();
    rocksdb_ptr = nullptr;

    fs::remove_all(rocksdb_dir);
    fs::create_directories(rocksdb_dir);
    initDB();
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
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to merge rocksdb options from 'rocksdb.options' at: {}: {}",
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
        }
    }

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

Pipe StorageEmbeddedRocksDB::read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t max_block_size,
        unsigned num_streams)
{
    storage_snapshot->check(column_names);

    FieldVectorPtr keys;
    bool all_scan = false;

    Block sample_block = storage_snapshot->metadata->getSampleBlock();
    auto primary_key_data_type = sample_block.getByName(primary_key).type;
    std::tie(keys, all_scan) = getFilterKeys(primary_key, primary_key_data_type, query_info, context_);
    if (all_scan)
    {
        auto iterator = std::unique_ptr<rocksdb::Iterator>(rocksdb_ptr->NewIterator(rocksdb::ReadOptions()));
        iterator->SeekToFirst();
        return Pipe(std::make_shared<EmbeddedRocksDBSource>(*this, sample_block, std::move(iterator), max_block_size));
    }
    else
    {
        if (keys->empty())
            return {};

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

            pipes.emplace_back(std::make_shared<EmbeddedRocksDBSource>(
                    *this, sample_block, keys, keys->begin() + begin, keys->begin() + end, max_block_size));
        }
        return Pipe::unitePipes(std::move(pipes));
    }
}

SinkToStoragePtr StorageEmbeddedRocksDB::write(
    const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr /*context*/)
{
    return std::make_shared<EmbeddedRocksDBSink>(*this, metadata_snapshot);
}

static StoragePtr create(const StorageFactory::Arguments & args)
{
    // TODO custom RocksDBSettings, table function
    auto engine_args = args.engine_args;
    if (engine_args.size() > 2)
    {
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Engine {} requires at most 2 parameters. ({} given). Correct usage: EmbeddedRocksDB([ttl, read_only])",
            args.engine_name, engine_args.size());
    }

    Int32 ttl{0};
    bool read_only{false};
    if (!engine_args.empty())
        ttl = checkAndGetLiteralArgument<UInt64>(engine_args[0], "ttl");
    if (engine_args.size() > 1)
        read_only = checkAndGetLiteralArgument<bool>(engine_args[1], "read_only");

    StorageInMemoryMetadata metadata;
    metadata.setColumns(args.columns);
    metadata.setConstraints(args.constraints);

    if (!args.storage_def->primary_key)
        throw Exception("StorageEmbeddedRocksDB must require one column in primary key", ErrorCodes::BAD_ARGUMENTS);

    metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->primary_key->ptr(), metadata.columns, args.getContext());
    auto primary_key_names = metadata.getColumnsRequiredForPrimaryKey();
    if (primary_key_names.size() != 1)
    {
        throw Exception("StorageEmbeddedRocksDB must require one column in primary key", ErrorCodes::BAD_ARGUMENTS);
    }
    return std::make_shared<StorageEmbeddedRocksDB>(args.table_id, args.relative_data_path, metadata, args.attach, args.getContext(), primary_key_names[0], ttl, read_only);
}

std::shared_ptr<rocksdb::Statistics> StorageEmbeddedRocksDB::getRocksDBStatistics() const
{
    std::shared_lock<std::shared_mutex> lock(rocksdb_ptr_mx);
    if (!rocksdb_ptr)
        return nullptr;
    return rocksdb_ptr->GetOptions().statistics;
}

std::vector<rocksdb::Status> StorageEmbeddedRocksDB::multiGet(const std::vector<rocksdb::Slice> & slices_keys, std::vector<String> & values) const
{
    std::shared_lock<std::shared_mutex> lock(rocksdb_ptr_mx);
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
    auto metadata = getInMemoryMetadataPtr();
    return metadata ? metadata->getSampleBlock() : Block();
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

void registerStorageEmbeddedRocksDB(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_sort_order = true,
        .supports_parallel_insert = true,
    };

    factory.registerStorage("EmbeddedRocksDB", create, features);
}


}
