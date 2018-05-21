#include <Common/config.h>
#if USE_POCO_MONGODB
#include <Poco/Util/AbstractConfiguration.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
    #include <Poco/MongoDB/Connection.h>
    #include <Poco/MongoDB/Database.h>
    #include <Poco/MongoDB/Cursor.h>
    #include <Poco/MongoDB/Array.h>
    #include <Poco/MongoDB/ObjectId.h>
#pragma GCC diagnostic pop

#include <Poco/Version.h>

// only after poco
// naming conflict:
// Poco/MongoDB/BSONWriter.h:54: void writeCString(const std::string& value);
// dbms/src/IO/WriteHelpers.h:146 #define writeCString(s, buf)
#include <Dictionaries/MongoDBDictionarySource.h>
#include <Dictionaries/MongoDBBlockInputStream.h>
#include <Common/FieldVisitors.h>
#include <ext/enumerate.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int WRONG_PASSWORD;
    extern const int MONGODB_CANNOT_AUTHENTICATE;
}


static const size_t max_block_size = 8192;


#if POCO_VERSION < 0x01070800
/// See https://pocoproject.org/forum/viewtopic.php?f=10&t=6326&p=11426&hilit=mongodb+auth#p11485
static void authenticate(Poco::MongoDB::Connection & connection,
    const std::string & database, const std::string & user, const std::string & password)
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
            throw Exception("Cannot authenticate in MongoDB: server returned empty response for 'getnonce' command",
                ErrorCodes::MONGODB_CANNOT_AUTHENTICATE);

        auto doc = response.documents()[0];
        try
        {
            double ok = doc->get<double>("ok", 0);
            if (ok != 1)
                throw Exception("Cannot authenticate in MongoDB: server returned response for 'getnonce' command that"
                    " has field 'ok' missing or having wrong value", ErrorCodes::MONGODB_CANNOT_AUTHENTICATE);

            nonce = doc->get<std::string>("nonce", "");
            if (nonce.empty())
                throw Exception("Cannot authenticate in MongoDB: server returned response for 'getnonce' command that"
                    " has field 'nonce' missing or empty", ErrorCodes::MONGODB_CANNOT_AUTHENTICATE);
        }
        catch (Poco::NotFoundException & e)
        {
            throw Exception("Cannot authenticate in MongoDB: server returned response for 'getnonce' command that has missing required field: "
                + e.displayText(), ErrorCodes::MONGODB_CANNOT_AUTHENTICATE);
        }
    }

    /// Second step: use nonce to calculate digest and send it back to the server.
    /// Digest is hex_md5(n.nonce + username + hex_md5(username + ":mongo:" + password))
    {
        std::string first = user + ":mongo:" + password;

        Poco::MD5Engine md5;
        md5.update(first);
        std::string digest_first(Poco::DigestEngine::digestToHex(md5.digest()));
        std::string second    =  nonce + user + digest_first;
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
            throw Exception("Cannot authenticate in MongoDB: server returned empty response for 'authenticate' command",
                ErrorCodes::MONGODB_CANNOT_AUTHENTICATE);

        auto doc = response.documents()[0];
        try
        {
            double ok = doc->get<double>("ok", 0);
            if (ok != 1)
                throw Exception("Cannot authenticate in MongoDB: server returned response for 'authenticate' command that"
                    " has field 'ok' missing or having wrong value", ErrorCodes::MONGODB_CANNOT_AUTHENTICATE);
        }
        catch (Poco::NotFoundException & e)
        {
            throw Exception("Cannot authenticate in MongoDB: server returned response for 'authenticate' command that has missing required field: "
                + e.displayText(), ErrorCodes::MONGODB_CANNOT_AUTHENTICATE);
        }
    }
}
#endif


MongoDBDictionarySource::MongoDBDictionarySource(
    const DictionaryStructure & dict_struct, const std::string & host, UInt16 port,
    const std::string & user, const std::string & password,
    const std::string & method,
    const std::string & db, const std::string & collection,
    const Block & sample_block)
    : dict_struct{dict_struct}, host{host}, port{port}, user{user}, password{password},
        method{method},
        db{db}, collection{collection}, sample_block{sample_block},
        connection{std::make_shared<Poco::MongoDB::Connection>(host, port)}
{
    if (!user.empty())
    {
#if POCO_VERSION >= 0x01070800
        Poco::MongoDB::Database poco_db(db);
        poco_db.authenticate(*connection, user, password, method.empty() ? Poco::MongoDB::Database::AUTH_SCRAM_SHA1 : method);
#else
        authenticate(*connection, db, user, password);
#endif

    }
}


MongoDBDictionarySource::MongoDBDictionarySource(
    const DictionaryStructure & dict_struct, const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix, Block & sample_block)
    : MongoDBDictionarySource(
        dict_struct,
        config.getString(config_prefix + ".host"),
        config.getUInt(config_prefix + ".port"),
        config.getString(config_prefix + ".user", ""),
        config.getString(config_prefix + ".password", ""),
        config.getString(config_prefix + ".method", ""),
        config.getString(config_prefix + ".db", ""),
        config.getString(config_prefix + ".collection"),
        sample_block)
{
}


MongoDBDictionarySource::MongoDBDictionarySource(const MongoDBDictionarySource & other)
    : MongoDBDictionarySource{
        other.dict_struct, other.host, other.port, other.user, other.password,
        other.method,
        other.db, other.collection, other.sample_block}
{
}


MongoDBDictionarySource::~MongoDBDictionarySource() = default;


static std::unique_ptr<Poco::MongoDB::Cursor> createCursor(
    const std::string & database, const std::string & collection, const Block & sample_block_to_select)
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
    return std::make_shared<MongoDBBlockInputStream>(
        connection, createCursor(db, collection, sample_block), sample_block, max_block_size);
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

    cursor->query().selector().addNewDocument(dict_struct.id->name)
        .add("$in", ids_array);

    return std::make_shared<MongoDBBlockInputStream>(
        connection, std::move(cursor), sample_block, max_block_size);
}


BlockInputStreamPtr MongoDBDictionarySource::loadKeys(
    const Columns & key_columns, const std::vector<size_t> & requested_rows)
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
                case AttributeUnderlyingType::UInt8:
                case AttributeUnderlyingType::UInt16:
                case AttributeUnderlyingType::UInt32:
                case AttributeUnderlyingType::UInt64:
                case AttributeUnderlyingType::UInt128:
                case AttributeUnderlyingType::Int8:
                case AttributeUnderlyingType::Int16:
                case AttributeUnderlyingType::Int32:
                case AttributeUnderlyingType::Int64:
                    key.add(attr.second.name, Int32(key_columns[attr.first]->get64(row_idx)));
                    break;

                case AttributeUnderlyingType::Float32:
                case AttributeUnderlyingType::Float64:
                    key.add(attr.second.name, applyVisitor(FieldVisitorConvertToNumber<Float64>(), (*key_columns[attr.first])[row_idx]));
                    break;

                case AttributeUnderlyingType::String:
                    String _str(get<String>((*key_columns[attr.first])[row_idx]));
                    /// Convert string to ObjectID
                    if (attr.second.is_object_id)
                    {
                        Poco::MongoDB::ObjectId::Ptr _id(new Poco::MongoDB::ObjectId(_str));
                        key.add(attr.second.name, _id);
                    }
                    else
                    {
                        key.add(attr.second.name, _str);
                    }
                    break;
            }
        }
    }

    /// If more than one key we should use $or
    cursor->query().selector().add("$or", keys_array);

    return std::make_shared<MongoDBBlockInputStream>(
        connection, std::move(cursor), sample_block, max_block_size);
}


std::string MongoDBDictionarySource::toString() const
{
    return "MongoDB: " + db + '.' + collection + ',' + (user.empty() ? " " : " " + user + '@') + host + ':' + DB::toString(port);
}

}

#endif
