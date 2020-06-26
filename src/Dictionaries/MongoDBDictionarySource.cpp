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
        const Context &,
        bool /* check_config */)
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
#include <Common/FieldVisitors.h>
#include <ext/enumerate.h>
#include "MongoDBBlockInputStream.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int MONGODB_CANNOT_AUTHENTICATE;
}


static const UInt64 max_block_size = 8192;


#if POCO_VERSION < 0x01070800
/// See https://pocoproject.org/forum/viewtopic.php?f=10&t=6326&p=11426&hilit=mongodb+auth#p11485
void authenticate(Poco::MongoDB::Connection & connection, const std::string & database, const std::string & user, const std::string & password)
{
    Poco::MongoDB::Database db(database);

    /// Challenge-response authentication.
    std::string nonce;

    /// First step: request nonce.
    {
        auto command = db.createCommand();
        command->setNumberToReturn(1);
        command->selector().add<Int32>("getnonce", 1);

        Poco::MongoDB::ResponseMessage response;
        connection.sendRequest(*command, response);

        if (response.documents().empty())
            throw Exception(
                "Cannot authenticate in MongoDB: server returned empty response for 'getnonce' command",
                ErrorCodes::MONGODB_CANNOT_AUTHENTICATE);

        auto doc = response.documents()[0];
        try
        {
            double ok = doc->get<double>("ok", 0);
            if (ok != 1)
                throw Exception(
                    "Cannot authenticate in MongoDB: server returned response for 'getnonce' command that"
                    " has field 'ok' missing or having wrong value",
                    ErrorCodes::MONGODB_CANNOT_AUTHENTICATE);

            nonce = doc->get<std::string>("nonce", "");
            if (nonce.empty())
                throw Exception(
                    "Cannot authenticate in MongoDB: server returned response for 'getnonce' command that"
                    " has field 'nonce' missing or empty",
                    ErrorCodes::MONGODB_CANNOT_AUTHENTICATE);
        }
        catch (Poco::NotFoundException & e)
        {
            throw Exception(
                "Cannot authenticate in MongoDB: server returned response for 'getnonce' command that has missing required field: "
                    + e.displayText(),
                ErrorCodes::MONGODB_CANNOT_AUTHENTICATE);
        }
    }

    /// Second step: use nonce to calculate digest and send it back to the server.
    /// Digest is hex_md5(n.nonce + username + hex_md5(username + ":mongo:" + password))
    {
        std::string first = user + ":mongo:" + password;

        Poco::MD5Engine md5;
        md5.update(first);
        std::string digest_first(Poco::DigestEngine::digestToHex(md5.digest()));
        std::string second = nonce + user + digest_first;
        md5.reset();
        md5.update(second);
        std::string digest_second(Poco::DigestEngine::digestToHex(md5.digest()));

        auto command = db.createCommand();
        command->setNumberToReturn(1);
        command->selector()
            .add<Int32>("authenticate", 1)
            .add<std::string>("user", user)
            .add<std::string>("nonce", nonce)
            .add<std::string>("key", digest_second);

        Poco::MongoDB::ResponseMessage response;
        connection.sendRequest(*command, response);

        if (response.empty())
            throw Exception(
                "Cannot authenticate in MongoDB: server returned empty response for 'authenticate' command",
                ErrorCodes::MONGODB_CANNOT_AUTHENTICATE);

        auto doc = response.documents()[0];
        try
        {
            double ok = doc->get<double>("ok", 0);
            if (ok != 1)
                throw Exception(
                    "Cannot authenticate in MongoDB: server returned response for 'authenticate' command that"
                    " has field 'ok' missing or having wrong value",
                    ErrorCodes::MONGODB_CANNOT_AUTHENTICATE);
        }
        catch (Poco::NotFoundException & e)
        {
            throw Exception(
                "Cannot authenticate in MongoDB: server returned response for 'authenticate' command that has missing required field: "
                    + e.displayText(),
                ErrorCodes::MONGODB_CANNOT_AUTHENTICATE);
        }
    }
}
#endif


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
                throw Exception("Cannot authenticate in MongoDB, incorrect user or password", ErrorCodes::MONGODB_CANNOT_AUTHENTICATE);
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


std::unique_ptr<Poco::MongoDB::Cursor> createCursor(const std::string & database, const std::string & collection, const Block & sample_block_to_select)
{
    auto cursor = std::make_unique<Poco::MongoDB::Cursor>(database, collection);

    /// Looks like selecting _id column is implicit by default.
    if (!sample_block_to_select.has("_id"))
        cursor->query().returnFieldSelector().add("_id", 0);

    for (const auto & column : sample_block_to_select)
        cursor->query().returnFieldSelector().add(column.name, 1);
    return cursor;
}


BlockInputStreamPtr MongoDBDictionarySource::loadAll()
{
    return std::make_shared<MongoDBBlockInputStream>(connection, createCursor(db, collection, sample_block), sample_block, max_block_size);
}


BlockInputStreamPtr MongoDBDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    if (!dict_struct.id)
        throw Exception{"'id' is required for selective loading", ErrorCodes::UNSUPPORTED_METHOD};

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
        throw Exception{"'key' is required for selective loading", ErrorCodes::UNSUPPORTED_METHOD};

    auto cursor = createCursor(db, collection, sample_block);

    Poco::MongoDB::Array::Ptr keys_array(new Poco::MongoDB::Array);

    for (const auto row_idx : requested_rows)
    {
        auto & key = keys_array->addNewDocument(DB::toString(row_idx));

        for (const auto attr : ext::enumerate(*dict_struct.key))
        {
            switch (attr.second.underlying_type)
            {
                case AttributeUnderlyingType::utUInt8:
                case AttributeUnderlyingType::utUInt16:
                case AttributeUnderlyingType::utUInt32:
                case AttributeUnderlyingType::utUInt64:
                case AttributeUnderlyingType::utUInt128:
                case AttributeUnderlyingType::utInt8:
                case AttributeUnderlyingType::utInt16:
                case AttributeUnderlyingType::utInt32:
                case AttributeUnderlyingType::utInt64:
                case AttributeUnderlyingType::utDecimal32:
                case AttributeUnderlyingType::utDecimal64:
                case AttributeUnderlyingType::utDecimal128:
                    key.add(attr.second.name, Int32(key_columns[attr.first]->get64(row_idx)));
                    break;

                case AttributeUnderlyingType::utFloat32:
                case AttributeUnderlyingType::utFloat64:
                    key.add(attr.second.name, key_columns[attr.first]->getFloat64(row_idx));
                    break;

                case AttributeUnderlyingType::utString:
                    String loaded_str(get<String>((*key_columns[attr.first])[row_idx]));
                    /// Convert string to ObjectID
                    if (attr.second.is_object_id)
                    {
                        Poco::MongoDB::ObjectId::Ptr loaded_id(new Poco::MongoDB::ObjectId(loaded_str));
                        key.add(attr.second.name, loaded_id);
                    }
                    else
                    {
                        key.add(attr.second.name, loaded_str);
                    }
                    break;
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
