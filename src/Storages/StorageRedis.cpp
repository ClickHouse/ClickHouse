#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageRedis.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <unordered_set>
#include <Core/Settings.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Sources/MongoDBSource.h>
#include <QueryPipeline/Pipe.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/parseAddress.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INVALID_REDIS_STORAGE_TYPE;
    extern const int NOT_IMPLEMENTED;
}

StorageRedis::StorageRedis(
    const StorageID & table_id_,
    const Configuration & configuration_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment_) : ta
{

}


Pipe StorageRedis::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    connectIfNotConnected();

    storage_snapshot->check(column_names);

    Block sample_block;
    for (const String & column_name : column_names)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);
        sample_block.insert({column_data.type, column_data.name});
    }

    return Pipe(std::make_shared<MongoDBSource>(
        connection, createCursor(database_name, collection_name, sample_block), sample_block, max_block_size));
}


SinkToStoragePtr StorageRedis::write(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    ContextPtr /*context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method write is unsupported for StorageRedis");
}

StorageRedis::Configuration StorageRedis::getConfiguration(ASTs engine_args, ContextPtr context)
{
    Configuration configuration;

    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, context))
    {
        validateNamedCollection(
            *named_collection,
            ValidateKeysMultiset<RedisEqualKeysSet>{"host", "port", "hostname", "password", "db_id", "storage_type"},
            {});

        configuration.host = named_collection->getAny<String>({"host", "hostname"});
        configuration.port = static_cast<UInt16>(named_collection->get<UInt32>("port"));
        configuration.password = named_collection->get<String>("password");
        configuration.db_id = named_collection->getAny<String>({"db_id"});
        configuration.storage_type = toStorageType(named_collection->getOrDefault<String>("storage_type", ""));
    }
    else
    {
        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context);

        /// 6379 is the default Redis port.
        auto parsed_host_port = parseAddress(checkAndGetLiteralArgument<String>(engine_args[0], "host:port"), 6379);

        configuration.host = parsed_host_port.first;
        configuration.port = parsed_host_port.second;
        configuration.db_id = checkAndGetLiteralArgument<String>(engine_args[1], "db_id");
        configuration.password = checkAndGetLiteralArgument<String>(engine_args[2], "password");
        configuration.storage_type = toStorageType(checkAndGetLiteralArgument<String>(engine_args[3], "storage_type"));
    }

    context->getRemoteHostFilter().checkHostAndPort(configuration.host, toString(configuration.port));

    return configuration;
}

void StorageRedis::connectIfNotConnected()
{

}


class StorageRedisSink : public SinkToStorage
{
public:
    explicit StorageRedisSink(
        const std::string & collection_name_,
        const std::string & db_name_,
        const StorageMetadataPtr & metadata_snapshot_,
        std::shared_ptr<Poco::MongoDB::Connection> connection_)
        : SinkToStorage(metadata_snapshot_->getSampleBlock())
        , collection_name(collection_name_)
        , db_name(db_name_)
        , metadata_snapshot{metadata_snapshot_}
        , connection(connection_)
    {
    }

    String getName() const override { return "StorageRedisSink"; }

    void consume(Chunk chunk) override
    {
        Poco::MongoDB::Database db(db_name);
        Poco::MongoDB::Document::Ptr index = new Poco::MongoDB::Document();

        auto block = getHeader().cloneWithColumns(chunk.detachColumns());

        size_t num_rows = block.rows();
        size_t num_cols = block.columns();

        const auto columns = block.getColumns();
        const auto data_types = block.getDataTypes();
        const auto data_names = block.getNames();

        std::vector<std::string> row(num_cols);
        for (const auto i : collections::range(0, num_rows))
        {
            for (const auto j : collections::range(0, num_cols))
            {
                WriteBufferFromOwnString ostr;
                data_types[j]->getDefaultSerialization()->serializeText(*columns[j], i, ostr, FormatSettings{});
                row[j] = ostr.str();
                index->add(data_names[j], row[j]);
            }
        }
        Poco::SharedPtr<Poco::MongoDB::InsertRequest> insert_request = db.createInsertRequest(collection_name);
        insert_request->documents().push_back(index);
        connection->sendRequest(*insert_request);
    }

private:
    String collection_name;
    String db_name;
    StorageMetadataPtr metadata_snapshot;
    std::shared_ptr<Poco::MongoDB::Connection> connection;
};


using StorageType = StorageRedis::StorageType;

String StorageRedis::toString(StorageType storage_type)
{
    static const std::unordered_map<StorageType, String> type_to_str_map
        = {{StorageType::SIMPLE, "simple"},
           {StorageType::LIST, "list"},
           {StorageType::SET, "set"},
           {StorageType::HASH, "hash"},
           {StorageType::ZSET, "zset"}};

    auto iter = type_to_str_map.find(storage_type);
    return iter->second;
}

StorageType StorageRedis::toStorageType(const String & storage_type)
{
    static const std::unordered_map<std::string, StorageType> str_to_type_map
        = {{"simple", StorageType::SIMPLE},
           {"list", StorageType::LIST},
           {"set", StorageType::SET},
           {"hash", StorageType::HASH},
           {"zset", StorageType::ZSET}};

    auto iter = str_to_type_map.find(storage_type);
    if (iter == str_to_type_map.end())
    {
        throw Exception(ErrorCodes::INVALID_REDIS_STORAGE_TYPE, "invalid redis storage type: {}", storage_type);
    }
    return iter->second;
}

void registerStorageRedis(StorageFactory & factory)
{
    factory.registerStorage(
        "MongoDB",
        [](const StorageFactory::Arguments & args)
        {
            auto configuration = StorageRedis::getConfiguration(args.engine_args, args.getLocalContext());

            return std::make_shared<StorageRedis>(
                args.table_id,
                configuration.host,
                configuration.port,
                configuration.database,
                configuration.table,
                configuration.username,
                configuration.password,
                configuration.options,
                args.columns,
                args.constraints,
                args.comment);
        },
        {
            .source_access_type = AccessType::MONGO,
        });
}

}
