#include <Storages/RocksDB/StorageEmbeddedRocksDB.h>
#include <Storages/RocksDB/EmbeddedRocksDBBlockOutputStream.h>
#include <Storages/RocksDB/EmbeddedRocksDBBlockInputStream.h>

#include <DataTypes/DataTypesNumber.h>
#include <Storages/StorageFactory.h>

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTCreateQuery.h>

#include <IO/WriteBufferFromString.h>

#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Pipe.h>

#include <Interpreters/Context.h>
#include <Interpreters/Set.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/TreeRewriter.h>

#include <Poco/File.h>
#include <Poco/Path.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>

#include <rocksdb/db.h>
#include <rocksdb/table.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ROCKSDB_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


// returns keys may be filter by condition
static bool traverseASTFilter(const String & primary_key, const DataTypePtr & primary_key_type, const ASTPtr & elem, const PreparedSets & sets, FieldVector & res)
{
    const auto * function = elem->as<ASTFunction>();
    if (!function)
        return false;

    if (function->name == "and")
    {
        // one child has the key filter condition is ok
        for (const auto & child : function->arguments->children)
            if (traverseASTFilter(primary_key, primary_key_type, child, sets, res))
                return true;
        return false;
    }
    else if (function->name == "or")
    {
        // make sure every child has the key filter condition
        FieldVector child_res;
        for (const auto & child : function->arguments->children)
        {
            if (!traverseASTFilter(primary_key, primary_key_type, child, sets, child_res))
                return false;
        }
        res.insert(res.end(), child_res.begin(), child_res.end());
        return true;
    }
    else if (function->name == "equals" || function->name == "in")
    {
        const auto & args = function->arguments->as<ASTExpressionList &>();
        const ASTIdentifier * ident;
        const IAST * value;

        if (args.children.size() != 2)
            return false;

        if (function->name == "in")
        {
            ident = args.children.at(0)->as<ASTIdentifier>();
            if (!ident)
                return false;

            if (ident->name() != primary_key)
                return false;
            value = args.children.at(1).get();

            PreparedSetKey set_key;
            if ((value->as<ASTSubquery>() || value->as<ASTIdentifier>()))
                set_key = PreparedSetKey::forSubquery(*value);
            else
                set_key = PreparedSetKey::forLiteral(*value, {primary_key_type});

            auto set_it = sets.find(set_key);
            if (set_it == sets.end())
                return false;
            SetPtr prepared_set = set_it->second;

            if (!prepared_set->hasExplicitSetElements())
                return false;

            prepared_set->checkColumnsNumber(1);
            const auto & set_column = *prepared_set->getSetElements()[0];
            for (size_t row = 0; row < set_column.size(); ++row)
            {
                res.push_back(set_column[row]);
            }
            return true;
        }
        else
        {
            if ((ident = args.children.at(0)->as<ASTIdentifier>()))
                value = args.children.at(1).get();
            else if ((ident = args.children.at(1)->as<ASTIdentifier>()))
                value = args.children.at(0).get();
            else
                return false;

            if (ident->name() != primary_key)
                return false;

            //function->name == "equals"
            if (const auto * literal = value->as<ASTLiteral>())
            {
                res.push_back(literal->value);
                return true;
            }
        }
    }
    return false;
}


/** Retrieve from the query a condition of the form `key = 'key'`, `key in ('xxx_'), from conjunctions in the WHERE clause.
  * TODO support key like search
  */
static std::pair<FieldVector, bool> getFilterKeys(const String & primary_key, const DataTypePtr & primary_key_type, const SelectQueryInfo & query_info)
{
    const auto & select = query_info.query->as<ASTSelectQuery &>();
    if (!select.where())
    {
        return std::make_pair(FieldVector{}, true);
    }
    FieldVector res;
    auto matched_keys = traverseASTFilter(primary_key, primary_key_type, select.where(), query_info.sets, res);
    return std::make_pair(res, !matched_keys);
}


class EmbeddedRocksDBSource : public SourceWithProgress
{
public:
    EmbeddedRocksDBSource(
        const StorageEmbeddedRocksDB & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const FieldVector & keys_,
        const size_t start_,
        const size_t end_,
        const size_t max_block_size_)
        : SourceWithProgress(metadata_snapshot_->getSampleBlock())
        , storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , start(start_)
        , end(end_)
        , max_block_size(max_block_size_)
    {
        // slice the keys
        if (end > start)
        {
            keys.resize(end - start);
            std::copy(keys_.begin() + start, keys_.begin() + end, keys.begin());
        }
    }

    String getName() const override
    {
        return storage.getName();
    }

    Chunk generate() override
    {
        if (processed_keys >= keys.size() || (start == end))
            return {};

        std::vector<rocksdb::Slice> slices_keys;
        slices_keys.reserve(keys.size());
        std::vector<String> values;
        std::vector<WriteBufferFromOwnString> wbs(keys.size());

        const auto & sample_block = metadata_snapshot->getSampleBlock();
        const auto & key_column = sample_block.getByName(storage.primary_key);
        auto columns = sample_block.cloneEmptyColumns();
        size_t primary_key_pos = sample_block.getPositionByName(storage.primary_key);

        for (size_t i = processed_keys; i < std::min(keys.size(), processed_keys + max_block_size); ++i)
        {
            key_column.type->serializeBinary(keys[i], wbs[i]);
            auto str_ref = wbs[i].stringRef();
            slices_keys.emplace_back(str_ref.data, str_ref.size);
        }

        auto statuses = storage.rocksdb_ptr->MultiGet(rocksdb::ReadOptions(), slices_keys, &values);
        for (size_t i = 0; i < statuses.size(); ++i)
        {
            if (statuses[i].ok())
            {
                ReadBufferFromString key_buffer(slices_keys[i]);
                ReadBufferFromString value_buffer(values[i]);

                size_t idx = 0;
                for (const auto & elem : sample_block)
                {
                    elem.type->deserializeBinary(*columns[idx], idx == primary_key_pos ? key_buffer : value_buffer);
                    ++idx;
                }
            }
        }
        processed_keys += max_block_size;

        UInt64 num_rows = columns.at(0)->size();
        return Chunk(std::move(columns), num_rows);
    }

private:
    const StorageEmbeddedRocksDB & storage;

    const StorageMetadataPtr metadata_snapshot;
    const size_t start;
    const size_t end;
    const size_t max_block_size;
    FieldVector keys;

    size_t processed_keys = 0;
};


StorageEmbeddedRocksDB::StorageEmbeddedRocksDB(const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata_,
        bool attach,
        Context & context_,
        const String & primary_key_)
    : IStorage(table_id_), primary_key{primary_key_}
{
    setInMemoryMetadata(metadata_);
    rocksdb_dir = context_.getPath() + relative_data_path_;
    if (!attach)
    {
        Poco::File(rocksdb_dir).createDirectories();
    }
    initDb();
}

void StorageEmbeddedRocksDB::truncate(const ASTPtr &, const StorageMetadataPtr & , const Context &, TableExclusiveLockHolder &)
{
    rocksdb_ptr->Close();
    Poco::File(rocksdb_dir).remove(true);
    Poco::File(rocksdb_dir).createDirectories();
    initDb();
}

void StorageEmbeddedRocksDB::initDb()
{
    rocksdb::Options options;
    rocksdb::DB * db;
    options.create_if_missing = true;
    options.compression = rocksdb::CompressionType::kZSTD;
    rocksdb::Status status = rocksdb::DB::Open(options, rocksdb_dir, &db);

    if (status != rocksdb::Status::OK())
        throw Exception("Fail to open rocksdb path at: " +  rocksdb_dir + ": " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);
    rocksdb_ptr = std::unique_ptr<rocksdb::DB>(db);
}


Pipe StorageEmbeddedRocksDB::read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        const Context & /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t max_block_size,
        unsigned num_streams)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());
    FieldVector keys;
    bool all_scan = false;

    auto primary_key_data_type = metadata_snapshot->getSampleBlock().getByName(primary_key).type;
    std::tie(keys, all_scan) = getFilterKeys(primary_key, primary_key_data_type, query_info);
    if (all_scan)
    {
        auto reader = std::make_shared<EmbeddedRocksDBBlockInputStream>(
                *this, metadata_snapshot, max_block_size);
        return Pipe(std::make_shared<SourceFromInputStream>(reader));
    }
    else
    {
        if (keys.empty())
            return {};

        std::sort(keys.begin(), keys.end());
        auto unique_iter = std::unique(keys.begin(), keys.end());
        if (unique_iter != keys.end())
            keys.erase(unique_iter, keys.end());

        Pipes pipes;
        size_t start = 0;
        size_t end;

        const size_t num_threads = std::min(size_t(num_streams), keys.size());
        const size_t batch_per_size = ceil(keys.size() * 1.0 / num_threads);

        for (size_t t = 0; t < num_threads; ++t)
        {
            if (start >= keys.size())
                start = end = 0;
            else
                end = start + batch_per_size > keys.size() ? keys.size() : start + batch_per_size;

            pipes.emplace_back(
                std::make_shared<EmbeddedRocksDBSource>(*this, metadata_snapshot, keys, start, end, max_block_size));
            start += batch_per_size;
        }
        return Pipe::unitePipes(std::move(pipes));
    }
}

BlockOutputStreamPtr StorageEmbeddedRocksDB::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, const Context & /*context*/)
{
    return std::make_shared<EmbeddedRocksDBBlockOutputStream>(*this, metadata_snapshot);
}


static StoragePtr create(const StorageFactory::Arguments & args)
{
    // TODO custom RocksDBSettings, table function
    if (!args.engine_args.empty())
        throw Exception(
            "Engine " + args.engine_name + " doesn't support any arguments (" + toString(args.engine_args.size()) + " given)",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    StorageInMemoryMetadata metadata;
    metadata.setColumns(args.columns);
    metadata.setConstraints(args.constraints);

    if (!args.storage_def->primary_key)
        throw Exception("StorageEmbeddedRocksDB must require one column in primary key", ErrorCodes::BAD_ARGUMENTS);

    metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->primary_key->ptr(), metadata.columns, args.context);
    auto primary_key_names = metadata.getColumnsRequiredForPrimaryKey();
    if (primary_key_names.size() != 1)
    {
        throw Exception("StorageEmbeddedRocksDB must require one column in primary key", ErrorCodes::BAD_ARGUMENTS);
    }
    return StorageEmbeddedRocksDB::create(args.table_id, args.relative_data_path, metadata, args.attach, args.context, primary_key_names[0]);
}


void registerStorageEmbeddedRocksDB(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_sort_order = true,
    };

    factory.registerStorage("EmbeddedRocksDB", create, features);
}


}
