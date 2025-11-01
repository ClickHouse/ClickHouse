#include <Interpreters/LLM/AnthropicModelEntity.h>
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
#include <boost/algorithm/string.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int REMOTE_LLM_API_ERROR;
}

static void buildCompleteResult(const std::vector<std::string> & results, ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets, size_t offset, size_t rows)
{
    if (results.size() != rows)
        throw Exception(ErrorCodes::REMOTE_LLM_API_ERROR, "Unexpected number of results returned, expected {}, got {}.", rows, results.size());
    size_t total_bytes = 0;
    for (size_t i = 0; i < rows; ++i)
    {
        total_bytes += results[i].size() + 1;
    }
    size_t res_curr_offset = res_data.size();
    res_data.reserve(res_curr_offset + total_bytes);
    for (size_t i = 0; i < results.size(); ++i)
    {
        const auto item = results[i];
        auto item_size = item.size();
        res_data.resize(res_curr_offset + item_size + 1);
        std::memcpy(&res_data[res_curr_offset], item.data(), item_size);
        res_data[res_curr_offset + item_size] = 0;

        res_curr_offset += item_size + 1;
        res_offsets[offset + i] = res_curr_offset;
    }
    chassert(res_curr_offset == res_data.size());
}

void AnthropicModelEntity::complete(GenerateContext & generate_context) const
{
    WriteBufferFromOwnString prompt_buffer;
    PromptTemplate::render(prompt_buffer,
        generate_context.user_prompt,
        generate_context.input_data,
        generate_context.input_data_offsets,
        generate_context.offset,
        generate_context.rows);

    auto client = AIClientFactory::createClient(config.ai_config);
    ai::GenerateOptions generate_options
    (
        generate_context.model_name,
        IModelEntity::systemPrompt(),
        prompt_buffer.str()
    );
    auto generate_result = client.client->generate_text(generate_options);
    if (generate_result.error)
        throw Exception(ErrorCodes::REMOTE_LLM_API_ERROR, "The request was refused due to exception {}", generate_result.error.value());

    std::vector<std::string> results;
    boost::split(results, generate_result.text, boost::is_any_of("\n\n"), boost::token_compress_on);

    buildCompleteResult(results, generate_context.output_data, generate_context.output_data_offsets, generate_context.offset, generate_context.rows);
}

void registerAnthropicModelEntity(ModelEntityFactory & factory)
{
    auto register_model = [&] (const std::string & provider)
    {
        factory.registerModelEntity(provider,
            [] (const ModelConfiguration & model_config) { return std::make_shared<AnthropicModelEntity>(model_config); });
    };
    register_model("anthropic");
}

}
