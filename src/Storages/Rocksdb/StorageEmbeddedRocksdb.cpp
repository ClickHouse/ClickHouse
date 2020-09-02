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
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeString.h>
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

#include <Poco/File.h>
#include <Poco/Path.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SYSTEM_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


static bool extractKeyImpl(const IAST & elem, Strings & res)
{
    const auto * function = elem.as<ASTFunction>();
    if (!function)
        return false;

    if (function->name == "equals" || function->name == "in")
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

        if (ident->name != "key")
            return false;

        if (const auto * literal = value->as<ASTLiteral>())
        {
            if (literal->value.getType() == Field::Types::String)
            {
                res.push_back(literal->value.safeGet<String>());
                return true;
            }
            else if (literal->value.getType() == Field::Types::Tuple)
            {
                auto tuple = literal->value.safeGet<Tuple>();
                for (const auto & f : tuple)
                {
                    res.push_back(f.safeGet<String>());
                }
                return true;
            }
            else return false;
        }
    }
    return false;
}


/** Retrieve from the query a condition of the form `key = 'key'` or `key in ('xxx_') or `key like 'xxx%'`, from conjunctions in the WHERE clause.
  */
static std::pair<Strings, bool> extractKey(const ASTPtr & query)
{
    const auto & select = query->as<ASTSelectQuery &>();
    if (!select.where())
    {
        return std::make_pair(Strings{}, true);
    }
    Strings res;
    extractKeyImpl(*select.where(), res);
    return std::make_pair(res, false);
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
        auto key_col = block.getByName("key");
        auto val_col = block.getByName("value");

        const ColumnString * keys = checkAndGetColumn<ColumnString>(key_col.column.get());

        WriteBufferFromOwnString wb_value;

        for (size_t i = 0; i < rows; i++)
        {
            StringRef key = keys->getDataAt(i);
            val_col.type->serializeBinary(*val_col.column, i, wb_value);

            auto status = storage.rocksdb_ptr->rocksdb->Put(rocksdb::WriteOptions(), key.toString(), wb_value.str());
            if (!status.ok())
                throw Exception("Rocksdb write error: " + status.ToString(), ErrorCodes::SYSTEM_ERROR);

            wb_value.restart();
        }

        rocksdb::Iterator* it = storage.rocksdb_ptr->rocksdb->NewIterator(rocksdb::ReadOptions());
        for (it->SeekToFirst(); it->Valid(); it->Next())
        {
            LOG_DEBUG(&Poco::Logger::get("StorageEmbeddedRocksdb"), "Iterator `{}` returns `{}`, {}",
                it->key().ToString(), it->value().ToString(), it->key().size());
        }
        // Check for any errors found during the scan
        assert(it->status().ok());
        delete it;
    }

private:
    StorageEmbeddedRocksdb & storage;
    StorageMetadataPtr metadata_snapshot;
};

StorageEmbeddedRocksdb::StorageEmbeddedRocksdb(const StorageFactory::Arguments & args)
    : IStorage(args.table_id)
{
    //must contains two columns, key and value
    if (args.columns.size() != 2)
        throw Exception("Storage " + getName() + " requires exactly 2 columns", ErrorCodes::BAD_ARGUMENTS);

    if (!args.columns.has("key") || !args.columns.has("value"))
        throw Exception("Storage " + getName() + " requires columns are: key and value", ErrorCodes::BAD_ARGUMENTS);

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(args.columns);
    storage_metadata.setConstraints(args.constraints);
    setInMemoryMetadata(storage_metadata);

    rocksdb_dir = args.context.getPath() + args.relative_data_path + "/rocksdb";
    if (!args.attach)
    {
        Poco::File(rocksdb_dir).createDirectories();
    }
    initDb();
}

void StorageEmbeddedRocksdb::truncate(const ASTPtr &, const StorageMetadataPtr & , const Context &, TableExclusiveLockHolder &)
{
    if (rocksdb_ptr)
    {
        rocksdb_ptr->shutdown();
    }
    Poco::File(rocksdb_dir).remove(true);
    Poco::File(rocksdb_dir).createDirectories();
    initDb();
}

void StorageEmbeddedRocksdb::initDb()
{
    rocksdb::Options options;
    rocksdb::BlockBasedTableOptions table_options;

    rocksdb::DB * db;
    options.create_if_missing = true;
    options.statistics = rocksdb::CreateDBStatistics();
    auto cache = rocksdb::NewLRUCache(256 << 20);
    table_options.block_cache = cache;
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    rocksdb::Status status = rocksdb::DB::Open(options, rocksdb_dir, &db);

    if (status != rocksdb::Status::OK())
        throw Exception("Fail to open rocksdb path at: " +  rocksdb_dir, ErrorCodes::SYSTEM_ERROR);
    rocksdb_ptr = std::make_shared<Rocksdb>(db);
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

    size_t key_pos = 0;
    size_t value_pos = 1;
    if (sample_block.getByPosition(0).name != "key")
        std::swap(key_pos, value_pos);

    MutableColumns columns = sample_block.cloneEmptyColumns();

    Strings keys;
    bool all_scan = false;
    std::tie(keys, all_scan) = extractKey(query_info.query);
    if (keys.empty() && !all_scan)
        throw Exception("StorageEmbeddedRocksdb engine must contain condition like key = 'key' or key in tuple(String) or key like 'xxx%' in WHERE clause or empty WHERE clause for all key value scan.", ErrorCodes::BAD_ARGUMENTS);

    // TODO pipline
    if (all_scan)
    {
        auto it = rocksdb_ptr->rocksdb->NewIterator(rocksdb::ReadOptions());
        for (it->SeekToFirst(); it->Valid(); it->Next())
        {
            ReadBufferFromString rvalue(it->value().ToString());
            columns[key_pos]->insert(it->key().ToString());
            sample_block.getByName("value").type->deserializeBinary(*columns[value_pos], rvalue);
        }
        assert(it->status().ok());
        delete it;
    }
    else
    {
        Strings values;
        std::vector<rocksdb::Slice> slices_keys;
        for (auto & key : keys)
        {
            slices_keys.push_back(key);
        }
        auto statuses = rocksdb_ptr->rocksdb->MultiGet(rocksdb::ReadOptions(), slices_keys, &values);
        for (size_t i = 0; i < statuses.size(); ++i)
        {
            if (statuses[i].ok())
            {
                ReadBufferFromString rvalue(values[i]);
                columns[key_pos]->insert(keys[i]);
                sample_block.getByName("value").type->deserializeBinary(*columns[value_pos], rvalue);
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

StorageEmbeddedRocksdb::~StorageEmbeddedRocksdb()
{
    if (rocksdb_ptr)
    {
        rocksdb_ptr->shutdown();
    }
}


void registerStorageEmbeddedRocksdb(StorageFactory & factory)
{
    factory.registerStorage("EmbeddedRocksdb", [](const StorageFactory::Arguments & args)
    {
        if (!args.engine_args.empty())
            throw Exception(
                "Engine " + args.engine_name + " doesn't support any arguments (" + toString(args.engine_args.size()) + " given)",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        return StorageEmbeddedRocksdb::create(args);
    });
}


}
