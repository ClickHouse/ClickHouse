#include "StorageMaxMindDB.h"

#if USE_MAXMINDDB
#    include <DataTypes/DataTypeNullable.h>
#    include <Interpreters/Context.h>
#    include <Parsers/ASTCreateQuery.h>
#    include <Storages/KVStorageUtils.h>
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
}

static std::vector<std::string>
serializeKeysToString(FieldVector::const_iterator & it_, FieldVector::const_iterator end_, size_t max_block_size_)
{
    size_t num_keys = end_ - it_;
    std::vector<std::string> result;
    result.reserve(num_keys);

    size_t rows_processed = 0;
    while (it_ < end_ && (max_block_size_ == 0 || rows_processed < max_block_size_))
    {
        const auto & field = *it_;
        auto type = field.getType();
        if (type != Field::Types::IPv4 && type != Field::Types::IPv6 && type != Field::Types::String)
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected key to be String, IPv4 or IPv6 type but {} give", fieldTypeToString(type));

        result.emplace_back(toString(*it_));
        ++it_;
        ++rows_processed;
    }

    return result;
}

static std::vector<std::string> serializeKeysToString(const ColumnWithTypeAndName & keys)
{
    if (!keys.column)
        return {};

    auto type_without_lc = removeLowCardinality(keys.type);
    if (!isString(type_without_lc) && !isIPv4(type_without_lc) && !isIPv6(type_without_lc))
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected key to be String, IPv4 or IPv6 type but {} give", keys.type->getName());

    size_t num_keys = keys.column->size();
    std::vector<std::string> result;
    result.reserve(num_keys);

    for (size_t i = 0; i < num_keys; ++i)
        result.emplace_back(toString((*keys.column)[i]));
    return result;
}


static void fillColumns(const String & key, const String & value, size_t key_pos, const Block & header, MutableColumns & columns)
{
    ReadBufferFromString key_buffer(key);
    ReadBufferFromString value_buffer(value);
    for (size_t i = 0; i < header.columns(); ++i)
    {
        const auto & serialization = header.getByPosition(i).type->getDefaultSerialization();
        serialization->deserializeTextEscaped(*columns[i], i == key_pos ? key_buffer : value_buffer, {});
    }
}


class MaxMindDBSource : public ISource
{
public:
    MaxMindDBSource(
        const StorageMaxMindDB & storage_,
        const Block & header,
        FieldVector::const_iterator begin_,
        FieldVector::const_iterator end_,
        const size_t max_block_size_)
        : ISource(header)
        , storage(storage_)
        , primary_key_pos(getPrimaryKeyPos(header, storage.getPrimaryKey()))
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
    : IStorage(table_id_), WithContext(context_->getGlobalContext()), primary_key{primary_key_}, mmdb_file_path(mmdb_file_path_)
{
    setInMemoryMetadata(metadata);

    initDB();
}

void StorageMaxMindDB::initDB()
{
    MMDB_s mmdb;
    int status = MMDB_open(mmdb_file_path.c_str(), MMDB_MODE_MMAP, &mmdb);
    if (status != MMDB_SUCCESS)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to open maxminddb path at: {}: {}", mmdb_file_path, MMDB_strerror(status));
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

        pipes.emplace_back(
            std::make_shared<MaxMindDBSource>(*this, sample_block, keys->begin() + begin, keys->begin() + end, max_block_size));
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
            fillColumns(keys[i], values[i], primary_key_pos, sample_block, columns);
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
