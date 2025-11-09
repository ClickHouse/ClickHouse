#include "Client/ClientBase.h"
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToLLM.h>
#include <Interpreters/LLM/PromptRender.h>
#include <Interpreters/LLM/IModelEntity.h>
#include <Interpreters/Context.h>
#include <Poco/Net/HTTPSession.h>
#include <IO/HTTPHeaderEntries.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <nlohmann/json.hpp>

namespace DB
{
class LLMCompleteImpl
{
public:
    static void vector(
        const ContextPtr context,
        const String & model_detail_raw,
        const String & prompt_detail_raw,
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        if (input_rows_count == 0)
            return;

        res_offsets.resize(input_rows_count);
        auto model_json = nlohmann::json::parse(model_detail_raw);
        auto prompt_json = nlohmann::json::parse(prompt_detail_raw);
        auto model_name = model_json["model_name"];
        auto user_prompt = prompt_json["prompt"];
        auto model = context->getModelEntity(model_name);
        auto batch_size = model_json.contains("batch_size") ? static_cast<size_t>(model_json["batch_size"]) : model->getBatchSize();
        batch_size = !batch_size ? model->getBatchSize() : batch_size;
        batch_size = std::min<size_t>(batch_size, input_rows_count);
        size_t start_index = 0;
        do
        {
            auto left = input_rows_count - start_index;
            auto size = left > batch_size ? batch_size : left;
            GenerateContext generate_context
            {
                .context = context,
                .model_name = model->getModelName(),
                .model = model_json,
                .user_prompt = user_prompt,
                .input_data = data,
                .input_data_offsets = offsets,
                .offset = start_index,
                .rows =  size,
                .output_data = res_data,
                .output_data_offsets = res_offsets,
            };
            model->complete(generate_context);
            start_index += size;
        } while (start_index < input_rows_count);
    }
};

struct NameLLMComplete
{
	static constexpr auto name = "llmComplete";
};

using FunctionLLMComplete = FunctionStringToLLMGenerateText<LLMCompleteImpl, NameLLMComplete>;

REGISTER_FUNCTION(llmComplete)
{
    factory.registerFunction<FunctionLLMComplete>({}, FunctionFactory::Case::Insensitive);
}

}
