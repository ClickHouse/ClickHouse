#include "MongoDBDictionarySource.h"
#include "DictionarySourceFactory.h"
#include "DictionaryStructure.h"
#include "registerDictionaries.h"

namespace DB
{

void registerDictionarySourceMongoDB(DictionarySourceFactory & factory)
{
    auto create_mongo_db_dictionary = [](
        const DictionaryStructure & dict_struct,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & root_config_prefix,
        Block & sample_block,
        ContextPtr,
        const std::string & /* default_database */,
        bool /* created_from_ddl */)
    {
        const auto config_prefix = root_config_prefix + ".mongodb";
        return std::make_unique<MongoDBDictionarySource>(dict_struct,
            config.getString(config_prefix + ".uri", ""),
            config.getString(config_prefix + ".host", ""),
            config.getUInt(config_prefix + ".port", 0),
            config.getString(config_prefix + ".user", ""),
            config.getString(config_prefix + ".password", ""),
            config.getString(config_prefix + ".method", ""),
            config.getString(config_prefix + ".db", ""),
            config.getString(config_prefix + ".collection"),
            sample_block);
    };

    factory.registerSource("mongodb", create_mongo_db_dictionary);
}

}

#include <common/logger_useful.h>
#include <Poco/MongoDB/Array.h>
#include <Poco/MongoDB/Connection.h>
#include <Poco/MongoDB/Cursor.h>
#include <Poco/MongoDB/Database.h>
#include <Poco/MongoDB/ObjectId.h>
#include <Poco/URI.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Version.h>

// only after poco
// naming conflict:
// Poco/MongoDB/BSONWriter.h:54: void writeCString(const std::string & value);
// src/IO/WriteHelpers.h:146 #define writeCString(s, buf)
#include <IO/WriteHelpers.h>
#include <DataStreams/MongoDBBlockInputStream.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int UNSUPPORTED_METHOD;
    extern const int MONGODB_CANNOT_AUTHENTICATE;
}


static const UInt64 max_block_size = 8192;


MongoDBDictionarySource::MongoDBDictionarySource(
    const DictionaryStructure & dict_struct_,
    const std::string & uri_,
    const std::string & host_,
    UInt16 port_,
    const std::string & user_,
    const std::string & password_,
    const std::string & method_,
    const std::string & db_,
    const std::string & collection_,
    const Block & sample_block_)
    : dict_struct{dict_struct_}
    , uri{uri_}
    , host{host_}
    , port{port_}
    , user{user_}
    , password{password_}
    , method{method_}
    , db{db_}
    , collection{collection_}
    , sample_block{sample_block_}
    , connection{std::make_shared<Poco::MongoDB::Connection>()}
{
    if (!uri.empty())
    {
        Poco::URI poco_uri(uri);

        // Parse database from URI. This is required for correctness -- the
        // cursor is created using database name and colleciton name, so we have
        // to specify them properly.
        db = poco_uri.getPath();
        // getPath() may return a leading slash, remove it.
        if (!db.empty() && db[0] == '/')
        {
            db.erase(0, 1);
        }

        // Parse some other parts from URI, for logging and display purposes.
        host = poco_uri.getHost();
        port = poco_uri.getPort();
        user = poco_uri.getUserInfo();
        if (size_t separator = user.find(':'); separator != std::string::npos)
        {
            user.resize(separator);
        }

        // Connect with URI.
        Poco::MongoDB::Connection::SocketFactory socket_factory;
        connection->connect(uri, socket_factory);
    }
    else
    {
        // Connect with host/port/user/etc.
        connection->connect(host, port);
        if (!user.empty())
        {
#if POCO_VERSION >= 0x01070800
            Poco::MongoDB::Database poco_db(db);
            if (!poco_db.authenticate(*connection, user, password, method.empty() ? Poco::MongoDB::Database::AUTH_SCRAM_SHA1 : method))
                throw Exception(ErrorCodes::MONGODB_CANNOT_AUTHENTICATE, "Cannot authenticate in MongoDB, incorrect user or password");
#else
            authenticate(*connection, db, user, password);
#endif
        }
    }
}


MongoDBDictionarySource::MongoDBDictionarySource(const MongoDBDictionarySource & other)
    : MongoDBDictionarySource{
        other.dict_struct, other.uri, other.host, other.port, other.user, other.password, other.method, other.db, other.collection, other.sample_block}
{
}

MongoDBDictionarySource::~MongoDBDictionarySource() = default;

BlockInputStreamPtr MongoDBDictionarySource::loadAll()
{
    return std::make_shared<MongoDBBlockInputStream>(connection, createCursor(db, collection, sample_block), sample_block, max_block_size);
}

BlockInputStreamPtr MongoDBDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    if (!dict_struct.id)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'id' is required for selective loading");

    auto cursor = createCursor(db, collection, sample_block);

    /** NOTE: While building array, Poco::MongoDB requires passing of different unused element names, along with values.
      * In general, Poco::MongoDB is quite inefficient and bulky.
      */

    Poco::MongoDB::Array::Ptr ids_array(new Poco::MongoDB::Array);
    for (const UInt64 id : ids)
        ids_array->add(DB::toString(id), Int32(id));

    cursor->query().selector().addNewDocument(dict_struct.id->name).add("$in", ids_array);

    return std::make_shared<MongoDBBlockInputStream>(connection, std::move(cursor), sample_block, max_block_size);
}


BlockInputStreamPtr MongoDBDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    if (!dict_struct.key)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'key' is required for selective loading");

    auto cursor = createCursor(db, collection, sample_block);

    Poco::MongoDB::Array::Ptr keys_array(new Poco::MongoDB::Array);

    for (const auto row_idx : requested_rows)
    {
        auto & key = keys_array->addNewDocument(DB::toString(row_idx));

        const auto & key_attributes = *dict_struct.key;
        for (size_t attribute_index = 0; attribute_index < key_attributes.size(); ++attribute_index)
        {
            const auto & key_attribute = key_attributes[attribute_index];

            switch (key_attribute.underlying_type)
            {
                case AttributeUnderlyingType::UInt8:
                case AttributeUnderlyingType::UInt16:
                case AttributeUnderlyingType::UInt32:
                case AttributeUnderlyingType::UInt64:
                case AttributeUnderlyingType::Int8:
                case AttributeUnderlyingType::Int16:
                case AttributeUnderlyingType::Int32:
                case AttributeUnderlyingType::Int64:
                {
                    key.add(key_attribute.name, Int32(key_columns[attribute_index]->get64(row_idx)));
                    break;
                }
                case AttributeUnderlyingType::Float32:
                case AttributeUnderlyingType::Float64:
                {
                    key.add(key_attribute.name, key_columns[attribute_index]->getFloat64(row_idx));
                    break;
                }
                case AttributeUnderlyingType::String:
                {
                    String loaded_str(get<String>((*key_columns[attribute_index])[row_idx]));
                    /// Convert string to ObjectID
                    if (key_attribute.is_object_id)
                    {
                        Poco::MongoDB::ObjectId::Ptr loaded_id(new Poco::MongoDB::ObjectId(loaded_str));
                        key.add(key_attribute.name, loaded_id);
                    }
                    else
                    {
                        key.add(key_attribute.name, loaded_str);
                    }
                    break;
                }
                default:
                    throw Exception("Unsupported dictionary attribute type for MongoDB dictionary source", ErrorCodes::NOT_IMPLEMENTED);
            }
        }
    }

    /// If more than one key we should use $or
    cursor->query().selector().add("$or", keys_array);

    return std::make_shared<MongoDBBlockInputStream>(connection, std::move(cursor), sample_block, max_block_size);
}

std::string MongoDBDictionarySource::toString() const
{
    return "MongoDB: " + db + '.' + collection + ',' + (user.empty() ? " " : " " + user + '@') + host + ':' + DB::toString(port);
}

}
