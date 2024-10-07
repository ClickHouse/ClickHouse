#include "config.h"

#if USE_MONGODB
#include <Storages/StorageMongoDBPocoLegacy.h>
#include <Storages/StorageMongoDBPocoLegacySocketFactory.h>
#include <Storages/StorageFactory.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/NamedCollectionsHelpers.h>

#include <Poco/MongoDB/Connection.h>
#include <Poco/MongoDB/Cursor.h>
#include <Poco/MongoDB/Database.h>
#include <Poco/URI.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Common/parseAddress.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/RemoteHostFilter.h>
#include <IO/Operators.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/Sources/MongoDBPocoLegacySource.h>
#include <base/range.h>
#include <unordered_set>

#include <Parsers/ASTLiteral.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <DataTypes/DataTypeArray.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int MONGODB_CANNOT_AUTHENTICATE;
}

StorageMongoDBPocoLegacy::StorageMongoDBPocoLegacy(
    const StorageID & table_id_,
    const std::string & host_,
    uint16_t port_,
    const std::string & database_name_,
    const std::string & collection_name_,
    const std::string & username_,
    const std::string & password_,
    const std::string & options_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment)
    : IStorage(table_id_)
    , database_name(database_name_)
    , collection_name(collection_name_)
    , username(username_)
    , password(password_)
    , uri("mongodb://" + host_ + ":" + std::to_string(port_) + "/" + database_name_ + "?" + options_)
{
    LOG_WARNING(getLogger("StorageMongoDB (" + table_id_.table_name + ")"), "The deprecated MongoDB integartion implementation is used, this will be removed in next releases.");

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}


void StorageMongoDBPocoLegacy::connectIfNotConnected()
{
    std::lock_guard lock{connection_mutex};
    if (!connection)
    {
        StorageMongoDBPocoLegacySocketFactory factory;
        connection = std::make_shared<Poco::MongoDB::Connection>(uri, factory);
    }

    if (!authenticated)
    {
        Poco::URI poco_uri(uri);
        auto query_params = poco_uri.getQueryParameters();
        auto auth_source = std::find_if(query_params.begin(), query_params.end(),
                                        [&](const std::pair<std::string, std::string> & param) { return param.first == "authSource"; });
        auto auth_db = database_name;
        if (auth_source != query_params.end())
            auth_db = auth_source->second;

        if (!username.empty() && !password.empty())
        {
            Poco::MongoDB::Database poco_db(auth_db);
            if (!poco_db.authenticate(*connection, username, password, Poco::MongoDB::Database::AUTH_SCRAM_SHA1))
                throw Exception(ErrorCodes::MONGODB_CANNOT_AUTHENTICATE, "Cannot authenticate in MongoDB, incorrect user or password");
        }

        authenticated = true;
    }
}

class StorageMongoDBLegacySink : public SinkToStorage
{
public:
    explicit StorageMongoDBLegacySink(
        const std::string & collection_name_,
        const std::string & db_name_,
        const StorageMetadataPtr & metadata_snapshot_,
        std::shared_ptr<Poco::MongoDB::Connection> connection_)
        : SinkToStorage(metadata_snapshot_->getSampleBlock())
        , collection_name(collection_name_)
        , db_name(db_name_)
        , metadata_snapshot{metadata_snapshot_}
        , connection(connection_)
        , is_wire_protocol_old(isMongoDBWireProtocolOld(*connection_, db_name))
    {
    }

    String getName() const override { return "StorageMongoDBLegacySink"; }

    void consume(Chunk & chunk) override
    {
        Poco::MongoDB::Database db(db_name);
        Poco::MongoDB::Document::Vector documents;

        auto block = getHeader().cloneWithColumns(chunk.getColumns());

        size_t num_rows = block.rows();
        size_t num_cols = block.columns();

        const auto columns = block.getColumns();
        const auto data_types = block.getDataTypes();
        const auto data_names = block.getNames();

        documents.reserve(num_rows);

        for (const auto i : collections::range(0, num_rows))
        {
            Poco::MongoDB::Document::Ptr document = new Poco::MongoDB::Document();

            for (const auto j : collections::range(0, num_cols))
            {
                insertValueIntoMongoDB(*document, data_names[j], *data_types[j], *columns[j], i);
            }

            documents.push_back(std::move(document));
        }

        if (is_wire_protocol_old)
        {
            Poco::SharedPtr<Poco::MongoDB::InsertRequest> insert_request = db.createInsertRequest(collection_name);
            insert_request->documents() = std::move(documents);
            connection->sendRequest(*insert_request);
        }
        else
        {
            Poco::SharedPtr<Poco::MongoDB::OpMsgMessage> insert_request = db.createOpMsgMessage(collection_name);
            insert_request->setCommandName(Poco::MongoDB::OpMsgMessage::CMD_INSERT);
            insert_request->documents() = std::move(documents);
            connection->sendRequest(*insert_request);
        }
    }

private:

    void insertValueIntoMongoDB(
        Poco::MongoDB::Document & document,
        const std::string & name,
        const IDataType & data_type,
        const IColumn & column,
        size_t idx)
    {
        WhichDataType which(data_type);

        if (which.isArray())
        {
            const ColumnArray & column_array = assert_cast<const ColumnArray &>(column);
            const ColumnArray::Offsets & offsets = column_array.getOffsets();

            size_t offset = offsets[idx - 1];
            size_t next_offset = offsets[idx];

            const IColumn & nested_column = column_array.getData();

            const auto * array_type = assert_cast<const DataTypeArray *>(&data_type);
            const DataTypePtr & nested_type = array_type->getNestedType();

            Poco::MongoDB::Array::Ptr array = new Poco::MongoDB::Array();
            for (size_t i = 0; i + offset < next_offset; ++i)
            {
                insertValueIntoMongoDB(*array, Poco::NumberFormatter::format(i), *nested_type, nested_column, i + offset);
            }

            document.add(name, array);
            return;
        }

        /// MongoDB does not support UInt64 type, so just cast it to Int64
        if (which.isNativeUInt())
            document.add(name, static_cast<Poco::Int64>(column.getUInt(idx)));
        else if (which.isNativeInt())
            document.add(name, static_cast<Poco::Int64>(column.getInt(idx)));
        else if (which.isFloat32())
            document.add(name, static_cast<Float64>(column.getFloat32(idx)));
        else if (which.isFloat64())
            document.add(name, column.getFloat64(idx));
        else if (which.isDate())
            document.add(name, Poco::Timestamp(DateLUT::instance().fromDayNum(DayNum(column.getUInt(idx))) * 1000000));
        else if (which.isDateTime())
            document.add(name, Poco::Timestamp(column.getUInt(idx) * 1000000));
        else
        {
            WriteBufferFromOwnString ostr;
            data_type.getDefaultSerialization()->serializeText(column, idx, ostr, FormatSettings{});
            document.add(name, ostr.str());
        }
    }

    String collection_name;
    String db_name;
    StorageMetadataPtr metadata_snapshot;
    std::shared_ptr<Poco::MongoDB::Connection> connection;

    const bool is_wire_protocol_old;
};

Pipe StorageMongoDBPocoLegacy::read(
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
        sample_block.insert({ column_data.type, column_data.name });
    }

    return Pipe(std::make_shared<MongoDBPocoLegacySource>(connection, database_name, collection_name, Poco::MongoDB::Document{}, sample_block, max_block_size));
}


SinkToStoragePtr StorageMongoDBPocoLegacy::write(const ASTPtr & /* query */, const StorageMetadataPtr & metadata_snapshot, ContextPtr /* context */, bool /*async_insert*/)
{
    connectIfNotConnected();
    return std::make_shared<StorageMongoDBLegacySink>(collection_name, database_name, metadata_snapshot, connection);
}

StorageMongoDBPocoLegacy::Configuration StorageMongoDBPocoLegacy::getConfiguration(ASTs engine_args, ContextPtr context)
{
    Configuration configuration;

    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, context))
    {
        validateNamedCollection(
            *named_collection,
            ValidateKeysMultiset<MongoDBEqualKeysSet>{"host", "port", "user", "username", "password", "database", "db", "collection", "table"},
            {"options"});

        configuration.host = named_collection->getAny<String>({"host", "hostname"});
        configuration.port = static_cast<UInt16>(named_collection->get<UInt64>("port"));
        configuration.username = named_collection->getAny<String>({"user", "username"});
        configuration.password = named_collection->get<String>("password");
        configuration.database = named_collection->getAny<String>({"database", "db"});
        configuration.table = named_collection->getAny<String>({"collection", "table"});
        configuration.options = named_collection->getOrDefault<String>("options", "");
    }
    else
    {
        if (engine_args.size() < 5 || engine_args.size() > 6)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Storage MongoDB requires from 5 to 6 parameters: "
                            "MongoDB('host:port', database, collection, 'user', 'password' [, 'options']).");

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context);

        /// 27017 is the default MongoDB port.
        auto parsed_host_port = parseAddress(checkAndGetLiteralArgument<String>(engine_args[0], "host:port"), 27017);

        configuration.host = parsed_host_port.first;
        configuration.port = parsed_host_port.second;
        configuration.database = checkAndGetLiteralArgument<String>(engine_args[1], "database");
        configuration.table = checkAndGetLiteralArgument<String>(engine_args[2], "table");
        configuration.username = checkAndGetLiteralArgument<String>(engine_args[3], "username");
        configuration.password = checkAndGetLiteralArgument<String>(engine_args[4], "password");

        if (engine_args.size() >= 6)
            configuration.options = checkAndGetLiteralArgument<String>(engine_args[5], "database");
    }

    context->getRemoteHostFilter().checkHostAndPort(configuration.host, toString(configuration.port));

    return configuration;
}


void registerStorageMongoDBPocoLegacy(StorageFactory & factory)
{
    factory.registerStorage("MongoDB", [](const StorageFactory::Arguments & args)
    {
        auto configuration = StorageMongoDBPocoLegacy::getConfiguration(args.engine_args, args.getLocalContext());

        return std::make_shared<StorageMongoDBPocoLegacy>(
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
#endif
