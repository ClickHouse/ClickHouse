#include <Interpreters/LLM/OllamaModelEntity.h>
#include <Interpreters/LLM/ModelEntityFactory.h>
#include <Interpreters/LLM/PromptRender.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Net/HTTPSession.h>
#include <IO/HTTPHeaderEntries.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Interpreters/Context.h>


namespace DB
{

OllamaModelEntity::OllamaModelEntity(const OllamaModelEntity & o)
    : IModelEntity(o)
    , completion_endpoint(o.completion_endpoint)
    , api_key(o.api_key)
    , parameters(nlohmann::json::parse(R"({"temperature":0.2})"))
{
}

bool OllamaModelEntity::configChanged(const Poco::Util::AbstractConfiguration & config, const String & name_)
{
    auto config_prefix = "llm_models.";
    auto completion_endpoint_ = config.getString(config_prefix + name_ + ".completion_endpoint");
    auto api_key_ = config.getString(config_prefix + name_ + ".api_key");
    auto parameters_string_ = config.getString(config_prefix + name_ + ".parameters", "");
    return completion_endpoint_ != completion_endpoint || api_key_ != api_key || parameters_string_ != parameters_string;
}

void OllamaModelEntity::reload(const Poco::Util::AbstractConfiguration & config, const String & name_)
{
    auto config_prefix = "llm_models.";
    name = name_;
    type = config.getString(config_prefix + name + ".type");
    completion_endpoint = config.getString(config_prefix + name + ".completion_endpoint");
    api_key = config.getString(config_prefix + name + ".api_key");
    parameters_string = config.getString(config_prefix + name_ + ".parameters", "");
    if (!parameters_string.empty())
    {
        auto input_parameters = nlohmann::json::parse(parameters_string);
        parameters = input_parameters;
    }
}

void OllamaModelEntity::processCompletion(const ContextPtr context, const nlohmann::json & model, const String & user_prompt,
    const ColumnString::Chars & data,
    const ColumnString::Offsets & offsets,
    size_t offset,
    size_t size,
    ColumnString::Chars & res_data,
    ColumnString::Offsets & res_offsets) const
{

    /// Make request to llm.
    WriteBufferFromOwnString prompt_buffer;
    PromptTemplate::render(prompt_buffer, user_prompt, data, offsets, offset, size);
    auto request = makeRequest(model, prompt_buffer.str(), size);

    String response_string;
    /// Send the request and receive response
    try
    {
        DB::HTTPHeaderEntries headers {};
        headers.emplace_back("Content-Type", "application/json");
        const auto request_string = request.dump();
        Poco::URI url(getCompletionURL());
        auto wb = DB::BuilderRWBufferFromHTTP(url)
            .withConnectionGroup(DB::HTTPConnectionGroupType::HTTP)
            .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
            .withSettings(context->getReadSettings())
            .withTimeouts(DB::ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings()))
            .withHostFilter(&context->getRemoteHostFilter())
            .withHeaders(headers)
            .withOutCallback([&request_string](std::ostream & os) { os << request_string; })
            .withSkipNotFound(false)
            .create({});
        readStringUntilEOF(response_string, *wb);
    }
    catch (const Exception & exception)
    {
        LOG_ERROR(log, "Failed to send request to the endpoint {}. The reason for the failure is: {}.", getCompletionURL(), exception.displayText());
        throw;
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Cannot send request to {}", getCompletionURL()));
        throw;
    }

    /// Check response.
    auto response = nlohmann::json::parse(response_string);
    if (response.contains("error"))
    {
        auto reason = response["error"].dump();
        LOG_ERROR(log, "The request was refused due to exception: {}", reason);
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}", reason);
    }
    else if ((response.contains("done_reason") && response["done_reason"] != "stop") || (response.contains("done") && !response["done"].is_null() && response["done"].get<bool>() != true))
    {
        auto reason = response.contains("done_reason") ? response["done_reason"].dump() : "internal error";
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The Ollama API return error {}", reason);
    }
    parseReply(response, res_data, res_offsets, offset, size);
}

nlohmann::json OllamaModelEntity::makeRequest(const nlohmann::json & model, const String & user_prompt, size_t batch_size_) const
{
    nlohmann::json request_payload = {
        {"model", name},
        {"prompt", user_prompt},
        {"stream", false},
        {"format", {{"type", "object"},
                    {"properties", {{"items", {{"type", "array"}, {"minItems", batch_size_}, {"maxItems", batch_size_}, {"items", {{"type", "string"}}}}}}},
                    {"required", {"items"}}}}
    };
    if (model.contains("parameters"))
        request_payload.update(model["parameters"]);
    else
        request_payload.update(parameters);
    return request_payload;
}

void OllamaModelEntity::parseReply(const nlohmann::json & parsed, ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets, size_t offset, size_t size) const
{
    if (!parsed.contains("repsonse") || !parsed["response"].is_string())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "key 'response' missing or not a string");
#if 0
    nlohmann::json responses;
    try
    {

    }
    catch (const Exception & exception)
    {

    }
#endif
    auto responses = nlohmann::json::parse(parsed["response"].get<std::string>());
    if (!responses.contains("items"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "");
    auto items = responses["items"];
    assert(size == items.size());
    size_t res_curr_offset = res_data.size();
    for (size_t i = 0; i < items.size(); ++i)
    {
        std::string reply;
        const auto & item = items.at(i);
        if (item.is_string())
            reply = item.get<std::string>();
        else
            reply = item.dump();
        auto dst_size = reply.size();
        res_data.resize(res_curr_offset + dst_size);

        // Copy
        std::memcpy(&res_data[res_curr_offset], reply.data(), dst_size);
        res_data[res_curr_offset + dst_size] = 0;

        res_curr_offset += dst_size + 1;
        res_offsets[offset + i] = res_curr_offset;
    }
}

void registerRemoteModelEntity(ModelEntityFactory & factory)
{
    factory.registerModelEntity("ollama", [] (const std::string & type)
    {
        return std::make_shared<OllamaModelEntity>(type);
    });
}

}
