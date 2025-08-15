#include <Interpreters/LLM/OpenAIModelEntity.h>
#include <Interpreters/LLM/ModelEntityFactory.h>
#include <Interpreters/LLM/PromptRender.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Net/HTTPSession.h>
#include <IO/HTTPHeaderEntries.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Interpreters/Context.h>
#include <Client/AI/AIClientFactory.h>
#include <Interpreters/LLM/IModelEntity.h>
#include <ai/openai.h>
#include <ai/types/client.h>
#include <ai/types/generate_options.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int REMOTE_LLM_API_ERROR;
}

bool OpenAIModelEntity::configChanged(const Poco::Util::AbstractConfiguration & raw_config, const String & name_)
{
    auto new_config = buildModelConfiguration(raw_config, name_);
    return new_config != config;
}

void OpenAIModelEntity::complete(GenerateContext & generate_context) const
{
    WriteBufferFromOwnString prompt_buffer;
    PromptTemplate::render(prompt_buffer,
        generate_context.user_prompt,
        generate_context.input_data,
        generate_context.input_data_offsets,
        generate_context.offset,
        generate_context.rows);

    auto client = AIClientFactory::createClient(config.ai_config);
    nlohmann::json format = {
        {"type", "json_schema"},
        {"json_schema", {
            {"name", "clickhouse"},
            {"strict", false},
            {"schema", {
                {"type", "object"},
                {"properties", {
                    {"items", {
                        {"type", "array"},
                        {"minItems", generate_context.rows},
                        {"maxItems", generate_context.rows},
                        {"items", {
                            {"type", "string"}
                        }}
                    }}
                }}
            }}
        }}
    };
    ai::GenerateOptions generate_options
    (
        generate_context.model_name,
        IModelEntity::systemPrompt(),
        prompt_buffer.str(),
        { format }
    );
    auto generate_result = client.client->generate_text(generate_options);
    if (generate_result.error)
        throw Exception(ErrorCodes::REMOTE_LLM_API_ERROR, "The request was refused due to exception {}", generate_result.error.value());

    /// Check response.
    nlohmann::json response;
    try
    {
        response = nlohmann::json::parse(generate_result.text);
    }
    catch (const Exception & e)
    {
        auto message = fmt::format("Failed to parse response, exception {}", e.displayText());
        LOG_ERROR(log, "{}", message);
        throw Exception(ErrorCodes::REMOTE_LLM_API_ERROR, "{}", message);
    }
    buildResult(response, generate_context.output_data, generate_context.output_data_offsets, generate_context.offset, generate_context.rows);
}

void OpenAIModelEntity::buildResult(const nlohmann::json & responses, ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets, size_t offset, size_t rows) const
{

    if (!responses.contains("items"))
        throw Exception(ErrorCodes::REMOTE_LLM_API_ERROR, "The `items` is missing.");
    const auto & items = responses["items"];
    if (!items.is_array())
        throw Exception(ErrorCodes::REMOTE_LLM_API_ERROR, "The 'items' is not an array.");
    size_t total_bytes = 0;
    for (size_t i = 0; i < rows; ++i)
    {
        if (!items[i].is_string())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The `item` type mismatch: expected string, got {}", items[i].type());
        total_bytes += items[i].get_ref<const std::string&>().size() + 1;
    }
    size_t res_curr_offset = res_data.size();
    res_data.reserve(res_curr_offset + total_bytes);
    for (size_t i = 0; i < items.size(); ++i)
    {
        const auto item = items[i].get_ref<const std::string&>();
        auto item_size = item.size();
        res_data.resize(res_curr_offset + item_size + 1);
        std::memcpy(&res_data[res_curr_offset], item.data(), item_size);
        res_data[res_curr_offset + item_size] = 0;

        res_curr_offset += item_size + 1;
        res_offsets[offset + i] = res_curr_offset;
    }
    chassert(res_curr_offset == res_data.size());
}

void registerRemoteModelEntity(ModelEntityFactory & factory)
{
    auto register_model = [&] (const std::string & provider, const std::string & model_name)
    {
        auto id = IModelEntity::generateModelID(provider, model_name);
        factory.registerModelEntity(id,
            [] (const ModelConfiguration & model_config) { return std::make_shared<OpenAIModelEntity>(model_config); });
    };
    using namespace ai::openai::models;
    auto openai_models = { kGpt4, kGpt4o, kGpt4oMini, kGpt4Turbo, kGpt41, kGpt41Mini };
    for (const auto model_name : openai_models)
    {
        register_model("openai", model_name);
    }
}

}
