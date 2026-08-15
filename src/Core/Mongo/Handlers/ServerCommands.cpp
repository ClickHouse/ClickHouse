#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>
#include <Core/Mongo/Handlers/ServerCommands.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/config_version.h>
#include <Common/quoteString.h>

#include <fmt/format.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace DB::MongoProtocol
{

namespace
{

/// The command a document invokes is its first key, the same way the registry dispatches on it.
String commandName(const Document & command)
{
    auto keys = command.getDocumentKeys();
    if (keys.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mongo command document is empty");
    return keys[0];
}

/// The database of a command that applies to the whole database rather than to a collection.
String databaseName(const Document & command, const String & command_name)
{
    auto json = command.getRapidJSONRepresentation();
    auto database_it = json.FindMember("$db");
    if (database_it == json.MemberEnd() || !database_it->value.IsString())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mongo command '{}' does not contain the '$db' database name", command_name);

    String database = database_it->value.GetString();
    if (database.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty Mongo database name in the command '{}'", command_name);
    for (char symbol : database)
        if (!isWordCharASCII(symbol) && symbol != '-')
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Mongo database name '{}'", database);
    return database;
}

}

std::vector<Document> ServerCommandsHandler::handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    auto command = commandName(documents[0].documents[0]);

    bson_t * bson_doc = bson_new();

    if (command == "buildInfo" || command == "buildinfo")
    {
        /// A driver reads the version to decide which features it may use. ClickHouse is not a
        /// Mongo server, so the version it reports is its own, and `maxWireVersion` of `isMaster`
        /// is what actually gates the protocol features.
        BSON_APPEND_UTF8(bson_doc, "version", VERSION_STRING);
        BSON_APPEND_UTF8(bson_doc, "gitVersion", VERSION_GITHASH);
        BSON_APPEND_BOOL(bson_doc, "debug", false);
        BSON_APPEND_INT32(bson_doc, "maxBsonObjectSize", static_cast<int32_t>(MAX_BSON_OBJECT_SIZE));

        bson_t version_array;
        bson_append_array_begin(bson_doc, "versionArray", -1, &version_array);
        for (size_t i = 0; i < 4; ++i)
        {
            const Int32 version[] = {Int32(VERSION_MAJOR), Int32(VERSION_MINOR), Int32(VERSION_PATCH), 0};
            auto key = std::to_string(i);
            bson_append_int32(&version_array, key.c_str(), static_cast<int>(key.size()), version[i]);
        }
        bson_append_array_end(bson_doc, &version_array);
    }
    else if (command == "killCursors")
    {
        /// The whole result of a `find` is returned in the first batch and the cursor is already
        /// exhausted, so every cursor a client asks about is one this server does not know.
        bson_t killed;
        bson_append_array_begin(bson_doc, "cursorsKilled", -1, &killed);
        bson_append_array_end(bson_doc, &killed);

        bson_t not_found;
        bson_append_array_begin(bson_doc, "cursorsNotFound", -1, &not_found);
        bson_append_array_end(bson_doc, &not_found);

        bson_t alive;
        bson_append_array_begin(bson_doc, "cursorsAlive", -1, &alive);
        bson_append_array_end(bson_doc, &alive);

        bson_t unknown;
        bson_append_array_begin(bson_doc, "cursorsUnknown", -1, &unknown);
        bson_append_array_end(bson_doc, &unknown);
    }
    else if (command == "connectionStatus")
    {
        /// The authenticated principal of this connection: what `saslStart` authenticated the
        /// session as, and the ClickHouse roles of that user. Before a successful `saslStart`
        /// both arrays are empty, which is how Mongo reports an anonymous connection. ClickHouse
        /// users and roles are not scoped by a database, so the database of both is `admin` -
        /// the database Mongo keeps the cluster-wide principals in.
        String user = executor->getAuthenticatedUserName();
        std::vector<String> roles;
        if (!user.empty())
            roles = splitByNewline(executor->execute("SELECT arrayJoin(currentRoles())"));

        bson_t authenticated_users;
        bson_t authenticated_user_roles;
        bson_t authentication_info;
        bson_init(&authentication_info);

        bson_append_array_begin(&authentication_info, "authenticatedUsers", -1, &authenticated_users);
        if (!user.empty())
        {
            bson_t user_doc;
            bson_init(&user_doc);
            BSON_APPEND_UTF8(&user_doc, "user", user.c_str());
            BSON_APPEND_UTF8(&user_doc, "db", "admin");
            bson_append_document(&authenticated_users, "0", 1, &user_doc);
            bson_destroy(&user_doc);
        }
        bson_append_array_end(&authentication_info, &authenticated_users);

        bson_append_array_begin(&authentication_info, "authenticatedUserRoles", -1, &authenticated_user_roles);
        size_t index = 0;
        for (const auto & role : roles)
        {
            if (role.empty())
                continue;

            bson_t role_doc;
            bson_init(&role_doc);
            BSON_APPEND_UTF8(&role_doc, "role", role.c_str());
            BSON_APPEND_UTF8(&role_doc, "db", "admin");

            auto key_str = std::to_string(index);
            ++index;
            bson_append_document(&authenticated_user_roles, key_str.c_str(), static_cast<int>(key_str.size()), &role_doc);
            bson_destroy(&role_doc);
        }
        bson_append_array_end(&authentication_info, &authenticated_user_roles);

        BSON_APPEND_DOCUMENT(bson_doc, "authInfo", &authentication_info);
        bson_destroy(&authentication_info);
    }

    BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);

    std::vector<Document> result;
    result.emplace_back(bson_doc);
    return result;
}

std::vector<Document> DropDatabaseHandler::handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    auto database = databaseName(documents[0].documents[0], "dropDatabase");

    /// Mongo answers a `dropDatabase` of a database that does not exist with success, so the drop
    /// has to tolerate it rather than report an error the client does not expect.
    executor->execute(fmt::format("DROP DATABASE IF EXISTS {}", backQuoteIfNeed(database)));

    bson_t * bson_doc = bson_new();
    BSON_APPEND_UTF8(bson_doc, "dropped", database.c_str());
    BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);

    std::vector<Document> result;
    result.emplace_back(bson_doc);
    return result;
}

void registerServerCommandsHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<ServerCommandsHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);

    auto drop_database = std::make_shared<DropDatabaseHandler>();
    for (const auto & identifier : drop_database->getIdentifiers())
        registry->addHandler(identifier, drop_database);
}

}
