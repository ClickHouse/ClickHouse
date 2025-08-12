#include <Functions/FunctionStringToLLM.h>
#include <Common/HTTPConnectionPool.h>
#include <Common/JSONBuilder.h>
#include <string>
#include <Core/Settings.h>
#include <Common/threadPoolCallbackRunner.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Interpreters/Context.h>
#include <Poco/URI.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <filesystem>
#include <Interpreters/LLM/ModelEntity.h>
#if 0
namespace DB
{
struct LLMModel
{
    String name;
    String base_url;
};

String RemoteLLMUtils::process(const ContextPtr context, const String & model_name, const String & prompt, const String & result_name)
{
    auto model = context->getModelEntity(model_name);
    auto remote_model = std::static_pointer_cast<RemoteModelEntity>(model);
    Poco::JSON::Object::Ptr request_body = new Poco::JSON::Object;
    request_body->set("model", remote_model->getName());
    request_body->set("prompt", prompt);
    request_body->set("stream", false);

    std::ostringstream oss;  // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    request_body->stringify(oss);
    const std::string body_str = oss.str();

    DB::HTTPHeaderEntries headers {};
    headers.emplace_back("Content-Type", "application/json");

    Poco::URI url(fmt::format("{}{}", "", ""));
    auto wb = DB::BuilderRWBufferFromHTTP(url)
        .withConnectionGroup(DB::HTTPConnectionGroupType::HTTP)
        .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
        .withSettings(context->getReadSettings())
        .withTimeouts(DB::ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings()))
        .withHostFilter(&context->getRemoteHostFilter())
        .withHeaders(headers)
        .withOutCallback([body_str](std::ostream & os) { os << body_str; })
        .withSkipNotFound(false)
        .create({});

    String json_str;
    readJSONObjectPossiblyInvalid(json_str, *wb);

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var json = parser.parse(json_str);
    const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();
    auto response_str = object->get(result_name).extract<std::string>();
    return response_str;
}

}
#endif
