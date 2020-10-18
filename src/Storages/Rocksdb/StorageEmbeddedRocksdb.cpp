#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Storages/StorageFactory.h>
#include <Storages/Rocksdb/StorageEmbeddedRocksdb.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
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
#include <Interpreters/Set.h>
#include <Interpreters/PreparedSets.h>
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
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


// returns keys may be filter by condition
static bool traverseASTFilter(const String & primary_key, const ASTPtr & elem, const PreparedSets & sets, FieldVector & res)
{
    const auto * function = elem->as<ASTFunction>();
    if (!function)
        return false;

    if (function->name == "and")
    {
        for (const auto & child : function->arguments->children)
            if (traverseASTFilter(primary_key, child, sets, res))
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


        if (function->name == "in" && ((value->as<ASTSubquery>() || value->as<ASTIdentifier>())))
        {
            auto set_it = sets.find(PreparedSetKey::forSubquery(*value));
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

        if (const auto * literal = value->as<ASTLiteral>())
        {
            if (function->name == "equals")
            {
                res.push_back(literal->value);
                return true;
            }
            else if (function->name == "in")
            {
                if (literal->value.getType() == Field::Types::Tuple)
                {
                    auto tuple = literal->value.safeGet<Tuple>();
                    for (const auto & f : tuple)
                    {
                        res.push_back(f);
                    }
                    return true;
                }
            }
            else return false;
        }
    }
    return false;
}


/** Retrieve from the query a condition of the form `key = 'key'`, `key in ('xxx_'), from conjunctions in the WHERE clause.
  * TODO support key like search
  */
static std::pair<FieldVector, bool> getFilterKeys(const String & primary_key, const SelectQueryInfo & query_info)
{
    const auto & select = query_info.query->as<ASTSelectQuery &>();
    if (!select.where())
    {
        return std::make_pair(FieldVector{}, true);
    }
    FieldVector res;
    auto matched_keys = traverseASTFilter(primary_key, select.where(), query_info.sets, res);
    return std::make_pair(res, !matched_keys);
}


class EmbeddedRocksdbSource : public SourceWithProgress
{
public:
    EmbeddedRocksdbSource(
        const StorageEmbeddedRocksdb & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const FieldVector & keys_,
        const size_t start_,
        const size_t end_,
        const size_t rocksdb_batch_read_size_)
        : SourceWithProgress(metadata_snapshot_->getSampleBlock())
        , storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , start(start_)
        , end(end_)
        , rocksdb_batch_read_size(rocksdb_batch_read_size_)
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

        for (size_t i = processed_keys; i < std::min(keys.size(), processed_keys + size_t(rocksdb_batch_read_size)); ++i)
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

                for (const auto [idx, column_type] : ext::enumerate(sample_block.getColumnsWithTypeAndName()))
                {
                    column_type.type->deserializeBinary(*columns[idx], idx == primary_key_pos? key_buffer: value_buffer);
                }
            }
        }
        processed_keys += rocksdb_batch_read_size;

        UInt64 num_rows = columns.at(0)->size();
        return Chunk(std::move(columns), num_rows);
    }

private:
    const StorageEmbeddedRocksdb & storage;

    const StorageMetadataPtr metadata_snapshot;
    const size_t start;
    const size_t end;
    const size_t rocksdb_batch_read_size;
    FieldVector keys;

    size_t processed_keys = 0;
};


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
        unsigned num_streams)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());
    Block sample_block = metadata_snapshot->getSampleBlock();
    size_t primary_key_pos = sample_block.getPositionByName(primary_key);

    FieldVector keys;
    bool all_scan = false;

    std::tie(keys, all_scan) = getFilterKeys(primary_key, query_info);
    if (all_scan)
    {
        MutableColumns columns = sample_block.cloneEmptyColumns();
        auto it = std::unique_ptr<rocksdb::Iterator>(rocksdb_ptr->NewIterator(rocksdb::ReadOptions()));
        for (it->SeekToFirst(); it->Valid(); it->Next())
        {
            ReadBufferFromString key_buffer(it->key());
            ReadBufferFromString value_buffer(it->value());

            for (const auto [idx, column_type] : ext::enumerate(sample_block.getColumnsWithTypeAndName()))
            {
                column_type.type->deserializeBinary(*columns[idx], idx == primary_key_pos? key_buffer: value_buffer);
            }
        }

        if (!it->status().ok())
        {
            throw Exception("Engine " + getName() + " got error while seeking key value datas: " + it->status().ToString(),
                ErrorCodes::LOGICAL_ERROR);
        }
        UInt64 num_rows = columns.at(0)->size();
        Chunk chunk(std::move(columns), num_rows);
        return Pipe(std::make_shared<SourceFromSingleChunk>(sample_block, std::move(chunk)));
    }
    else
    {
        if (keys.empty())
            return {};

        Pipes pipes;
        size_t start = 0;
        size_t end;

        const size_t num_threads = std::min(size_t(num_streams), keys.size());
        const size_t batch_per_size = ceil(keys.size() * 1.0 / num_threads);

        // TODO settings
        static constexpr size_t rocksdb_batch_read_size = 81920;

        for (size_t t = 0; t < num_threads; ++t)
        {
            if (start >= keys.size())
                start = end = 0;
            else
                end = start + batch_per_size > keys.size() ? keys.size() : start + batch_per_size;

            pipes.emplace_back(
                std::make_shared<EmbeddedRocksdbSource>(*this, metadata_snapshot, keys, start, end, rocksdb_batch_read_size));
            start += batch_per_size;
        }
        return Pipe::unitePipes(std::move(pipes));
    }
}

BlockOutputStreamPtr StorageEmbeddedRocksdb::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, const Context & /*context*/)
{
    return std::make_shared<EmbeddedRocksdbBlockOutputStream>(*this, metadata_snapshot);
}


static StoragePtr create(const StorageFactory::Arguments & args)
{
    // TODO custom RocksdbSettings
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
