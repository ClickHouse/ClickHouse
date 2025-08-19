#include "Client/ClientBase.h"
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToLLM.h>
#include <Interpreters/LLM/PromptRender.h>
#include <Interpreters/LLM/IModelEntity.h>
#include <Interpreters/Context.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <nlohmann/json.hpp>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

class LLMEmbeddingImpl
{
public:
#pragma clang optimize off
    static void vector(
        const ContextPtr context,
        const String & model_detail_raw,
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        PaddedPODArray<float> & res_data,
        PaddedPODArray<unsigned long> & res_offsets,
        size_t input_rows_count)
    {
        if (input_rows_count == 0)
            return;

        auto model_json = nlohmann::json::parse(model_detail_raw);
        auto model_name = model_json["model_name"];
        auto dimensions = model_json.contains("dimensions") ? model_json["dimensions"].get<int>() : 0;
        auto model = context->getModelEntity(model_name);
        auto batch_size = model_json.contains("batch_size") ? static_cast<size_t>(model_json["batch_size"]) : model->getBatchSize();
        batch_size = !batch_size ? model->getBatchSize() : batch_size;
        batch_size = std::min<size_t>(batch_size, input_rows_count);

        res_offsets.reserve(input_rows_count);
        size_t start_index = 0;
        do
        {
            auto left = input_rows_count - start_index;
            auto size = left > batch_size ? batch_size : left;
            EmbeddedContext embedding_context
            {
                .context = context,
                .model_name = model->getModelName(),
                .model = model_json,
                .input_data = data,
                .input_data_offsets = offsets,
                .offset = start_index,
                .rows =  size,
                .output_data = res_data,
                .output_data_offsets = res_offsets,
                .dimensions =  dimensions,
            };
            model->embedding(embedding_context);
            start_index += size;
        } while (start_index < input_rows_count);
    }
#pragma clang optimize on
};

struct NameLLMEmbedding
{
	static constexpr auto name = "llmEmbedding";
};

using FunctionLLMEmbedding = FunctionStringToLLMEmbedding<LLMEmbeddingImpl, NameLLMEmbedding>;

REGISTER_FUNCTION(llmEmbedding)
{
    factory.registerFunction<FunctionLLMEmbedding>({}, FunctionFactory::Case::Insensitive);
}

}
