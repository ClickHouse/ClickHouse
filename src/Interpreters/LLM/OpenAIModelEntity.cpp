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

static void buildCompleteResult(const nlohmann::json & responses, ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets, size_t offset, size_t rows)
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
    buildCompleteResult(response, generate_context.output_data, generate_context.output_data_offsets, generate_context.offset, generate_context.rows);
}

static void buildEmbeddingResult(const nlohmann::json & embeddings_json, PaddedPODArray<float> & res_data, PaddedPODArray<unsigned long> & res_offsets, size_t, size_t rows)
{
    if (!embeddings_json.is_array())
        throw Exception(ErrorCodes::REMOTE_LLM_API_ERROR, "The 'data' is not an array.");
    assert(embeddings_json.size() == rows);

    size_t total_bytes = embeddings_json.at(0)["embedding"].size() * rows * sizeof(float);
    res_data.reserve(total_bytes);

    size_t total_length = 0;
    for (const auto & element : embeddings_json)
    {
        const auto & embedding = element["embedding"];
        auto length = embedding.size();
        for (size_t i = 0; i < length; ++i)
            res_data.push_back((embedding.at(i).get<float>()));
        res_offsets.push_back(total_length + length);
        total_length += length;
    }
}

void OpenAIModelEntity::embedding(EmbeddedContext & embedding_context) const
{
    auto client = AIClientFactory::createClient(config.ai_config);
    auto offset = embedding_context.offset;
    auto & offsets = embedding_context.input_data_offsets;
    auto & data = embedding_context.input_data;
    size_t prev_offset = (offset == 0) ? 0 : offsets[offset - 1];
    size_t limit = offset + embedding_context.rows;

    nlohmann::json input = nlohmann::json::array();
    for (size_t i = offset; i < limit; ++i)
    {
        std::string_view element(reinterpret_cast<const char*>(&data[prev_offset]), offsets[i] - prev_offset - 1);
        prev_offset = offsets[i];
        input.push_back(element);
    }

    ai::EmbeddingOptions embedding_options
    (
        embedding_context.model_name,
        std::move(input),
         embedding_context.dimensions
    );
    auto embedding_result = client.client->embeddings(embedding_options);
    if (embedding_result.error)
        throw Exception(ErrorCodes::REMOTE_LLM_API_ERROR, "The request was refused due to exception {}", embedding_result.error.value());

    /// Check response.
    buildEmbeddingResult(embedding_result.data, embedding_context.output_data, embedding_context.output_data_offsets, embedding_context.offset, embedding_context.rows);
}


void registerOpenAIModelEntity(ModelEntityFactory & factory)
{
    auto register_model = [&] (const std::string & provider)
    {
        factory.registerModelEntity(provider,
            [] (const ModelConfiguration & model_config) { return std::make_shared<OpenAIModelEntity>(model_config); });
    };
    register_model("openai");
}

}
