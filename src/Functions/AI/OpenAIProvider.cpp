#include <Functions/AI/OpenAIProvider.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>

#include <sstream>

namespace DB
{

namespace ErrorCodes
{
    extern const int RECEIVED_ERROR_FROM_REMOTE_IO_SERVER;
}

String extractProviderError(const std::string & response_body, int status_code)
{
    try
    {
        Poco::JSON::Parser err_parser;
        auto err_json = err_parser.parse(response_body);
        auto err_obj = err_json.extract<Poco::JSON::Object::Ptr>();
        if (err_obj && err_obj->has("error"))
        {
            auto err = err_obj->getObject("error");
            if (err)
            {
                String msg = err->optValue<String>("message", "");
                String type = err->optValue<String>("type", "");
                if (!msg.empty())
                    return fmt::format("HTTP {} [{}]: {}", status_code, type, msg);
            }
        }
    }
    catch (...) {} // NOLINT(bugprone-empty-catch) Ok: best-effort JSON parsing
    size_t max_len = 256;
    return fmt::format("HTTP {} (response truncated to {} chars): {}", status_code, max_len,
        response_body.substr(0, std::min(response_body.size(), max_len)));
}


OpenAIProvider::OpenAIProvider(const String & endpoint_, const String & api_key_)
    : endpoint(endpoint_)
    , api_key(api_key_)
    , uri(endpoint_)
{
}

Poco::URI OpenAIProvider::deriveEmbeddingURI() const
{
    String path = uri.getPath();
    size_t pos = path.find("/chat/completions");
    if (pos != String::npos)
    {
        String new_path = path.substr(0, pos) + "/embeddings";
        Poco::URI embedding_uri(uri);
        embedding_uri.setPath(new_path);
        return embedding_uri;
    }
    if (path.find("/embeddings") != String::npos)
        return uri;

    Poco::URI embedding_uri(uri);
    embedding_uri.setPath("/v1/embeddings");
    return embedding_uri;
}

AIEmbeddingResponse OpenAIProvider::embed(const AIEmbeddingRequest & request, const ConnectionTimeouts & timeouts)
{
    Poco::URI embedding_uri = deriveEmbeddingURI();

    Poco::JSON::Object::Ptr root = new Poco::JSON::Object;

    if (request.inputs.size() == 1)
    {
        root->set("input", sanitizeTextForAI(request.inputs[0]));
    }
    else
    {
        Poco::JSON::Array::Ptr input_array = new Poco::JSON::Array;
        for (const auto & text : request.inputs)
            input_array->add(sanitizeTextForAI(text));
        root->set("input", input_array);
    }

    root->set("model", request.model);
    if (request.dimensions > 0)
        root->set("dimensions", static_cast<Int64>(request.dimensions));

    std::ostringstream body_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    root->stringify(body_stream);
    String body = body_stream.str();

    auto session = makeHTTPSession(HTTPConnectionGroupType::HTTP, embedding_uri, timeouts, ProxyConfiguration{});

    Poco::Net::HTTPRequest http_request(
        Poco::Net::HTTPRequest::HTTP_POST,
        embedding_uri.getPathAndQuery(),
        Poco::Net::HTTPMessage::HTTP_1_1);
    http_request.setContentType("application/json");
    if (!api_key.empty())
        http_request.set("Authorization", "Bearer " + api_key);
    http_request.setContentLength(body.size());

    auto & out_stream = session->sendRequest(http_request);
    out_stream << body;

    Poco::Net::HTTPResponse http_response;
    auto & in_stream = session->receiveResponse(http_response);

    std::string response_body;
    {
        std::ostringstream ss; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        ss << in_stream.rdbuf();
        response_body = ss.str();
    }

    auto status = http_response.getStatus();
    if (status != Poco::Net::HTTPResponse::HTTP_OK)
    {
        throw Exception(
            ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER,
            "AI embedding provider error: {}", extractProviderError(response_body, static_cast<int>(status)));
    }

    Poco::JSON::Parser parser;
    auto json_result = parser.parse(response_body);
    const auto & json_obj = json_result.extract<Poco::JSON::Object::Ptr>();

    AIEmbeddingResponse response;
    response.embeddings.resize(request.inputs.size());

    auto data_arr = json_obj->getArray("data");
    if (data_arr)
    {
        for (unsigned i = 0; i < data_arr->size(); ++i)
        {
            auto item = data_arr->getObject(i);
            if (!item)
                continue;

            unsigned idx = item->optValue<unsigned>("index", i);
            if (idx >= response.embeddings.size())
                continue;

            auto embedding_arr = item->getArray("embedding");
            if (embedding_arr)
            {
                response.embeddings[idx].reserve(embedding_arr->size());
                for (unsigned j = 0; j < embedding_arr->size(); ++j)
                    response.embeddings[idx].push_back(static_cast<Float32>(embedding_arr->getElement<double>(j)));
            }
        }
    }

    if (json_obj->has("usage"))
    {
        auto usage = json_obj->getObject("usage");
        if (usage)
            response.input_tokens = usage->optValue<UInt64>("prompt_tokens", 0);
    }

    return response;
}

}
