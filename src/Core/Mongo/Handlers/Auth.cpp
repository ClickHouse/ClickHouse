#include <Core/Mongo/Document.h>
#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/Auth.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>

#include <Common/Base64.h>
#include <Common/Exception.h>

#include <rapidjson/document.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
}

namespace DB::MongoProtocol
{

namespace
{

/// Reads a required string member of a JSON object.
String getStringMember(const rapidjson::Value & value, const char * name)
{
    auto it = value.FindMember(name);
    if (it == value.MemberEnd() || !it->value.IsString())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Field '{}' is missing in the 'saslStart' command", name);
    return it->value.GetString();
}

}

std::vector<Document> AuthHandler::handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    const auto & doc = documents[0].documents[0];
    auto json = doc.getRapidJSONRepresentation();

    /// Only the `PLAIN` SASL mechanism is supported: it is the only one that gives us the
    /// cleartext password needed to authenticate a ClickHouse user.
    auto mechanism = getStringMember(json, "mechanism");
    if (mechanism != "PLAIN")
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED, "Authentication mechanism '{}' is not supported, use 'PLAIN' instead", mechanism);

    /// In the legacy extended JSON representation of BSON a binary field becomes
    /// `{"$binary": "<base64>", "$type": "..."}`.
    auto payload_it = json.FindMember("payload");
    if (payload_it == json.MemberEnd() || !payload_it->value.IsObject())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Field 'payload' is missing in the 'saslStart' command");
    auto encoded_payload = getStringMember(payload_it->value, "$binary");

    /// The `PLAIN` message is `authzid \0 authcid \0 password`, where `authcid` is the user
    /// name sent by the client. It must not be confused with `$db`, which is only the
    /// database the client authenticates against.
    auto payload = base64Decode(encoded_payload);

    auto authzid_end = payload.find('\0');
    if (authzid_end == String::npos)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Malformed 'PLAIN' authentication payload");
    auto authcid_end = payload.find('\0', authzid_end + 1);
    if (authcid_end == String::npos)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Malformed 'PLAIN' authentication payload");

    String authzid = payload.substr(0, authzid_end);
    String authcid = payload.substr(authzid_end + 1, authcid_end - authzid_end - 1);
    String password = payload.substr(authcid_end + 1);

    /// `authzid` is the identity to act as. It is empty unless the client asks for
    /// impersonation, which for us is the same as authenticating that user directly.
    String user = authcid.empty() ? authzid : authcid;
    if (user.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty user name in the 'PLAIN' authentication payload");

    executor->authenticate(user, password);

    bson_t * bson_doc = bson_new();

    BSON_APPEND_BOOL(bson_doc, "done", true);
    BSON_APPEND_INT32(bson_doc, "conversationId", 1);
    BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);

    std::vector<Document> result;
    result.emplace_back(bson_doc);
    return result;
}

void registerAuthHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<AuthHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
