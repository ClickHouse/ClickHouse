#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Storages/StorageFactory.h>
#include <Storages/Rocksdb/StorageEmbeddedRocksdb.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTCreateQuery.h>
#include <DataTypes/NestedUtils.h>
#include <Common/typeid_cast.h>
#include <Common/StringUtils/StringUtils.h>

#include <IO/WriteBufferFromString.h>
#include <DataStreams/IBlockOutputStream.h>

#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Interpreters/Context.h>
#include <Interpreters/TreeRewriter.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnString.h>
#include <Processors/Pipe.h>
#include <ext/enumerate.h>

#include <Poco/File.h>
#include <Poco/Path.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SYSTEM_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


// returns keys may be filter by condition
static bool traverseASTFilter(const String & primary_key, const ASTPtr & elem, FieldVector & res)
{
    const auto * function = elem->as<ASTFunction>();
    if (!function)
        return false;

    if (function->name == "and")
    {
        for (const auto & child : function->arguments->children)
            if (traverseASTFilter(primary_key, child, res))
                return true;
        return false;
    }
    else if (function->name == "equals" || function->name == "in")
    {
        const auto & args = function->arguments->as<ASTExpressionList &>();
        const IAST * value;

        if (args.children.size() != 2)
            return false;

        const ASTIdentifier * ident;
        if ((ident = args.children.at(0)->as<ASTIdentifier>()))
            value = args.children.at(1).get();
        else if ((ident = args.children.at(1)->as<ASTIdentifier>()))
            value = args.children.at(0).get();
        else
            return false;

        if (ident->name != primary_key)
            return false;

        if (const auto * literal = value->as<ASTLiteral>())
        {
            if (function->name == "equals")
            {
                res.push_back(literal->value);
                return true;
            }
            else if (function->name == "in" && literal->value.getType() == Field::Types::Tuple)
            {
                auto tuple = literal->value.safeGet<Tuple>();
                for (const auto & f : tuple)
                {
                    res.push_back(f);
                }
                return true;
            }
            else return false;
        }
    }
    return false;
}


/** Retrieve from the query a condition of the form `key = 'key'`, `key in ('xxx_'), from conjunctions in the WHERE clause.
  * TODO support key like search
  */
static std::pair<FieldVector, bool> getFilterKeys(const String & primary_key, const ASTPtr & query)
{
    const auto & select = query->as<ASTSelectQuery &>();
    if (!select.where())
    {
        return std::make_pair(FieldVector{}, true);
    }
    FieldVector res;
    auto matched_keys = traverseASTFilter(primary_key, select.where(), res);
    return std::make_pair(res, !matched_keys);
}


class EmbeddedRocksdbBlockOutputStream : public IBlockOutputStream
{
public:
    explicit EmbeddedRocksdbBlockOutputStream(
        StorageEmbeddedRocksdb & storage_,
        const StorageMetadataPtr & metadata_snapshot_)
        : storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
    {}

    Block getHeader() const override { return metadata_snapshot->getSampleBlock(); }

    void write(const Block & block) override
    {
        metadata_snapshot->check(block, true);
        auto rows = block.rows();

        WriteBufferFromOwnString wb_key;
        WriteBufferFromOwnString wb_value;

        rocksdb::WriteBatch batch;
        auto columns = metadata_snapshot->getColumns();

        for (size_t i = 0; i < rows; i++)
        {
            wb_key.restart();
            wb_value.restart();

            for (const auto & col : columns)
            {
                const auto & type = block.getByName(col.name).type;
                const auto & column = block.getByName(col.name).column;
                if (col.name == storage.primary_key)
                    type->serializeBinary(*column, i, wb_key);
                else
                    type->serializeBinary(*column, i, wb_value);
            }
            batch.Put(wb_key.str(), wb_value.str());
        }
        auto status = storage.rocksdb_ptr->Write(rocksdb::WriteOptions(), &batch);
        if (!status.ok())
            throw Exception("Rocksdb write error: " + status.ToString(), ErrorCodes::SYSTEM_ERROR);
    }

private:
    StorageEmbeddedRocksdb & storage;
    StorageMetadataPtr metadata_snapshot;
};

StorageEmbeddedRocksdb::StorageEmbeddedRocksdb(const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata_,
        bool attach,
        Context & context_,
        const String & primary_key_)
    : IStorage(table_id_), primary_key{primary_key_}
{
    setInMemoryMetadata(metadata_);
    rocksdb_dir = context_.getPath() + relative_data_path_ + "/rocksdb";
    if (!attach)
    {
        Poco::File(rocksdb_dir).createDirectories();
    }
    initDb();
}

void StorageEmbeddedRocksdb::truncate(const ASTPtr &, const StorageMetadataPtr & , const Context &, TableExclusiveLockHolder &)
{
    rocksdb_ptr->Close();
    Poco::File(rocksdb_dir).remove(true);
    Poco::File(rocksdb_dir).createDirectories();
    initDb();
}

void StorageEmbeddedRocksdb::initDb()
{
    rocksdb::Options options;
    rocksdb::DB * db;
    options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(options, rocksdb_dir, &db);

    if (status != rocksdb::Status::OK())
        throw Exception("Fail to open rocksdb path at: " +  rocksdb_dir, ErrorCodes::SYSTEM_ERROR);
    rocksdb_ptr = std::unique_ptr<rocksdb::DB>(db);
}


Pipe StorageEmbeddedRocksdb::read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        const Context & /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());
    Block sample_block = metadata_snapshot->getSampleBlock();
    MutableColumns columns = sample_block.cloneEmptyColumns();

    FieldVector keys;
    bool all_scan = false;
    std::tie(keys, all_scan) = getFilterKeys(primary_key, query_info.query);

    // TODO pipline
    if (all_scan)
    {
        auto it = std::unique_ptr<rocksdb::Iterator>(rocksdb_ptr->NewIterator(rocksdb::ReadOptions()));
        for (it->SeekToFirst(); it->Valid(); it->Next())
        {
            ReadBufferFromString key_buffer(it->key());
            ReadBufferFromString value_buffer(it->value());

            for (const auto [idx, column_type] : ext::enumerate(sample_block.getColumnsWithTypeAndName()))
            {
                if (column_type.name == primary_key)
                    column_type.type->deserializeBinary(*columns[idx], key_buffer);
                else
                    column_type.type->deserializeBinary(*columns[idx], value_buffer);
            }
        }
        assert(it->status().ok());
    }
    else
    {
        std::vector<rocksdb::Slice> slices_keys;
        std::vector<String> values;

        WriteBufferFromOwnString wb;
        UInt64 offset = 0;
        for (const auto & key : keys)
        {
            sample_block.getByName(primary_key).type->serializeBinary(key, wb);
            auto str_ref = wb.stringRef();
            slices_keys.emplace_back(str_ref.data + offset, str_ref.size - offset);
            offset = str_ref.size;
        }

        auto statuses = rocksdb_ptr->MultiGet(rocksdb::ReadOptions(), slices_keys, &values);
        for (size_t i = 0; i < statuses.size(); ++i)
        {
            if (statuses[i].ok())
            {
                ReadBufferFromString key_buffer(slices_keys[i]);
                ReadBufferFromString value_buffer(values[i]);

                for (const auto [idx, column_type] : ext::enumerate(sample_block.getColumnsWithTypeAndName()))
                {
                    if (column_type.name == primary_key)
                        column_type.type->deserializeBinary(*columns[idx], key_buffer);
                    else
                        column_type.type->deserializeBinary(*columns[idx], value_buffer);
                }
            }
        }
    }

    UInt64 num_rows = columns.at(0)->size();
    Chunk chunk(std::move(columns), num_rows);
    return Pipe(std::make_shared<SourceFromSingleChunk>(sample_block, std::move(chunk)));
}

BlockOutputStreamPtr StorageEmbeddedRocksdb::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, const Context & /*context*/)
{
    return std::make_shared<EmbeddedRocksdbBlockOutputStream>(*this, metadata_snapshot);
}


static StoragePtr create(const StorageFactory::Arguments & args)
{
    // TODO RocksdbSettings
    if (!args.engine_args.empty())
        throw Exception(
            "Engine " + args.engine_name + " doesn't support any arguments (" + toString(args.engine_args.size()) + " given)",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    StorageInMemoryMetadata metadata;
    metadata.setColumns(args.columns);
    metadata.setConstraints(args.constraints);

    if (!args.storage_def->primary_key)
        throw Exception("StorageEmbeddedRocksdb must require one primary key", ErrorCodes::BAD_ARGUMENTS);

    metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->primary_key->ptr(), metadata.columns, args.context);
    auto primary_key_names = metadata.getColumnsRequiredForPrimaryKey();
    if (primary_key_names.size() != 1)
    {
        throw Exception("StorageEmbeddedRocksdb must require one primary key", ErrorCodes::BAD_ARGUMENTS);
    }
    return StorageEmbeddedRocksdb::create(args.table_id, args.relative_data_path, metadata, args.attach, args.context, primary_key_names[0]);
}


void registerStorageEmbeddedRocksdb(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_sort_order = true,
    };

    factory.registerStorage("EmbeddedRocksdb", create, features);
}


}
