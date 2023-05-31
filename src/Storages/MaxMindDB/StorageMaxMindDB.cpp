#include "StorageMaxMindDB.h"

#if USE_MAXMINDDB
#    include <DataTypes/DataTypeNullable.h>
#    include <Formats/FormatFactory.h>
#    include <Interpreters/Context.h>
#    include <Parsers/ASTCreateQuery.h>
#    include <Storages/KVStorageUtils.h>
#    include <Storages/MaxMindDB/utils.h>
#    include <Storages/StorageFactory.h>
#    include <Storages/checkAndGetLiteralArgument.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TYPE_MISMATCH;
    extern const int UNSUPPORTED_METHOD;
}

class MaxMindDBSource : public ISource
{
public:
    MaxMindDBSource(
        const StorageMaxMindDB & storage_,
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

    String getName() const override { return storage.getName(); }

    Chunk generate() override { return generateWithKeys(); }

    Chunk generateWithKeys()
    {
        if (it >= end)
        {
            it = {};
            return {};
        }

        std::vector<std::string> str_keys = serializeKeysToString(it, end, max_block_size);
        return storage.getBySerializedKeys(str_keys, nullptr);
    }

private:
    const StorageMaxMindDB & storage;

    size_t primary_key_pos;

    /// Only for key scan, full scan is not supported for MaxMindDB
    FieldVectorPtr keys;
    FieldVector::const_iterator begin;
    FieldVector::const_iterator end;
    FieldVector::const_iterator it;

    const size_t max_block_size;
};

StorageMaxMindDB::StorageMaxMindDB(
    const StorageID & table_id_,
    const StorageInMemoryMetadata & metadata,
    ContextPtr context_,
    const String & primary_key_,
    String mmdb_file_path_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , primary_key{primary_key_}
    , mmdb_file_path(mmdb_file_path_)
    , format_settings(getFormatSettings(getContext()))
    , json_format_settings{.settings = format_settings}
{
    setInMemoryMetadata(metadata);

    initDB();

    // json_format_settings.settings = format_settings;
}

StorageMaxMindDB::~StorageMaxMindDB()
{
    finalizeDB();
}

void StorageMaxMindDB::initDB()
{
    mmdb_ptr = std::make_unique<MMDB_s>();
    int status = MMDB_open(mmdb_file_path.c_str(), MMDB_MODE_MMAP, mmdb_ptr.get());
    if (status != MMDB_SUCCESS)
    {
        mmdb_ptr.reset();
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to open maxminddb path at: {}: {}", mmdb_file_path, MMDB_strerror(status));
    }
}

void StorageMaxMindDB::finalizeDB()
{
    if (mmdb_ptr)
    {
        MMDB_close(mmdb_ptr.get());
        mmdb_ptr.reset();
    }
}

Block StorageMaxMindDB::getSampleBlock(const Names &) const
{
    return getInMemoryMetadataPtr()->getSampleBlock();
}

Pipe StorageMaxMindDB::read(
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

    Block sample_block = getInMemoryMetadataPtr()->getSampleBlock();
    auto primary_key_data_type = sample_block.getByName(primary_key).type;
    std::tie(keys, all_scan) = getFilterKeys(primary_key, primary_key_data_type, query_info, context_);

    if (all_scan)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "MaxMindDB do not support full scan");

    if (!keys || keys->empty())
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

        pipes.emplace_back(std::make_shared<MaxMindDBSource>(
            *this, sample_block, keys, keys->begin() + begin, keys->begin() + end, max_block_size));
    }
    return Pipe::unitePipes(std::move(pipes));
}


Chunk StorageMaxMindDB::getByKeys(const ColumnsWithTypeAndName & keys, PaddedPODArray<UInt8> & null_map, const Names &) const
{
    if (keys.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "StorageMaxMindDB supports only one key, got: {}", keys.size());

    auto str_keys = serializeKeysToString(keys[0]);
    if (str_keys.size() != keys[0].column->size())
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Assertion failed: {} != {}", str_keys.size(), keys[0].column->size());

    return getBySerializedKeys(str_keys, &null_map);
}


Chunk StorageMaxMindDB::getBySerializedKeys(const std::vector<std::string> & keys, PaddedPODArray<UInt8> * null_map) const
{
    Block sample_block = getInMemoryMetadataPtr()->getSampleBlock();
    MutableColumns columns = sample_block.cloneEmptyColumns();

    if (keys.empty())
        return Chunk(std::move(columns), 0);


    size_t primary_key_pos = getPrimaryKeyPos(sample_block, getPrimaryKey());

    if (null_map)
    {
        null_map->clear();
        null_map->resize_fill(keys.size(), 1);
    }

    std::vector<String> values(keys.size());
    for (size_t i = 0; i < keys.size(); ++i)
    {
        bool found = lookupDB(keys[i], values[i]);
        if (found)
            fillKeyAndValueColumns(keys[i], values[i], primary_key_pos, sample_block, columns);
        else if (null_map)
        {
            (*null_map)[i] = 0;
            for (size_t col_idx = 0; col_idx < sample_block.columns(); ++col_idx)
                columns[col_idx]->insert(sample_block.getByPosition(col_idx).type->getDefault());
        }
    }

    size_t num_rows = columns.at(0)->size();
    return Chunk(std::move(columns), num_rows);
}

bool StorageMaxMindDB::lookupDB(const std::string & key, std::string & value) const
{
    int gai_error, mmdb_error;
    MMDB_lookup_result_s result = MMDB_lookup_string(mmdb_ptr.get(), key.c_str(), &gai_error, &mmdb_error);
    if (gai_error != 0)
    {
        // throw Exception(ErrorCodes::LOGICAL_ERROR, "Call getaddrinfo for {} failed because {}", key, gai_strerror(gai_error));
        return false;
    }
    if (mmdb_error != MMDB_SUCCESS)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got an error from the maxminddb library because {}", MMDB_strerror(mmdb_error));
    }
    if (!result.found_entry)
    {
        return false;
    }

    MMDB_entry_data_list_s * entry_data_list = nullptr;
    int status = MMDB_get_entry_data_list(&result.entry, &entry_data_list);
    if (status != MMDB_SUCCESS)
    {
        MMDB_free_entry_data_list(entry_data_list);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MMDB get_entry_data_list failed because {}", MMDB_strerror(mmdb_error));
    }

    if (!entry_data_list)
    {
        return false;
    }

    JSONBuilder::ItemPtr json;
    std::tie(json, std::ignore) = dumpMMDBEntryDataList(entry_data_list);

    WriteBufferFromString buf(value);
    JSONBuilder::FormatContext format_context{.out = buf};
    json->format(json_format_settings, format_context);
    return true;
}

static StoragePtr create(const StorageFactory::Arguments & args)
{
    auto engine_args = args.engine_args;
    if (engine_args.size() != 1)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Engine {} requires at most 3 parameters. "
            "({} given). Correct usage: MaxMindDB(mmdd_file_path)",
            args.engine_name,
            engine_args.size());

    String mmdb_file_path = checkAndGetLiteralArgument<String>(engine_args[0], "mmdb_file_path");

    StorageInMemoryMetadata metadata;
    metadata.setColumns(args.columns);
    metadata.setConstraints(args.constraints);

    if (!args.storage_def->primary_key)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "StorageMaxMindDB must require one column in primary key");
    metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->primary_key->ptr(), metadata.columns, args.getContext());

    auto primary_key_names = metadata.getColumnsRequiredForPrimaryKey();
    if (primary_key_names.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "StorageMaxMindDB must require one column in primary key");

    /// Check columns
    auto name_and_type_list = metadata.columns.getOrdinary();
    if (name_and_type_list.size() != 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "StorageMaxMindDB must require two ordinary columns");

    for (const auto & name_and_type : name_and_type_list)
    {
        if (name_and_type.name == primary_key_names[0])
        {
            /// Check data type of key column
            auto key_type_without_lc = removeLowCardinality(name_and_type.type);
            if (!isString(key_type_without_lc) && !isIPv4(key_type_without_lc) && !isIPv6(key_type_without_lc))
                throw Exception(
                    ErrorCodes::TYPE_MISMATCH, "Expected key to be String, IPv4 or IPv6 type but {} give", name_and_type.type->getName());
        }
        else
        {
            /// Check data type of value column
            const auto & value_type = name_and_type.type;
            DataTypePtr raw_value_type = removeNullable(removeLowCardinality(value_type));
            if (!isString(raw_value_type))
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected value to be String but {} give", name_and_type.type->getName());
        }
    }
    return std::make_shared<StorageMaxMindDB>(args.table_id, metadata, args.getContext(), primary_key_names[0], std::move(mmdb_file_path));
}

void registerStorageMaxMindDB(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_sort_order = true,
    };

    factory.registerStorage("MaxMindDB", create, features);
}

}


#endif
