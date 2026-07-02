#include <Core/Mongo/Handler.h>
#include <Core/Mongo/Handlers/Auth.h>
#include <Core/Mongo/Handlers/HandlerRegistry.h>

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include "Core/Mongo/Document.h"

namespace DB::MongoProtocol
{

namespace
{

const std::string BASE64_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

std::string base64Decode(const std::string & encoded)
{
    std::vector<unsigned char> output;
    std::vector<int> decoding_table(256, -1);
    for (size_t i = 0; i < BASE64_CHARS.size(); ++i)
    {
        decoding_table[static_cast<unsigned char>(BASE64_CHARS[i])] = static_cast<int>(i);
    }

    int val = 0;
    int valb = -8;
    for (unsigned char c : encoded)
    {
        if (c == '=')
            break; // Stop processing padding
        if (decoding_table[c] == -1)
            continue;
        val = (val << 6) + decoding_table[c];
        valb += 6;
        if (valb >= 0)
        {
            output.push_back(static_cast<unsigned char>((val >> valb) & 0xFF));
            valb -= 8;
        }
    }
    return std::string(output.begin(), output.end());
}


}

std::vector<Document> AuthHandler::handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor)
{
    const auto & doc = documents[0].documents[0];
    String user_password_encoded;
    {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        const auto & json_representation = doc.getRapidJsonRepresentation();

        json_representation["payload"].GetObject()["$binary"].Accept(writer);
        user_password_encoded = buffer.GetString();
    }
    String user;
    {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        const auto & json_representation = doc.getRapidJsonRepresentation();

        json_representation["$db"].Accept(writer);
        user = buffer.GetString();
    }
    auto user_password = base64Decode(user_password_encoded);
    auto password = user_password.substr(user.size());
    executor->authenticate(user.substr(1, user.size() - 2), password);

    bson_t * bson_doc = bson_new();

    BSON_APPEND_BOOL(bson_doc, "done", true);
    BSON_APPEND_INT32(bson_doc, "conversationId", 1);
    BSON_APPEND_DOUBLE(bson_doc, "ok", 1.0);

    auto result_doc = Document(bson_doc);
    return {result_doc};
}

void registerAuthHandler(HandlerRegitstry * registry)
{
    auto handler = std::make_shared<AuthHandler>();
    for (const auto & identifier : handler->getIdentifiers())
        registry->addHandler(identifier, handler);
}

}
