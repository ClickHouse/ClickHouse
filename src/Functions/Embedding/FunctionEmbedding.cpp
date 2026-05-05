#include <Functions/Embedding/EmbeddingModelRegistry.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_nlp_functions;
    extern const SettingsString embedding_model;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SUPPORT_IS_DISABLED;
}

/// embedding(text [, dims]) -> Array(Float32)
///
/// Computes a text embedding using the model specified by the `embedding_model` setting.
/// The setting must be set to a model id registered under
/// <embedding><model>...</model></embedding> in the server config.
class FunctionEmbedding : public IFunction
{
public:
    static constexpr auto name = "embedding";

    static FunctionPtr create(ContextPtr context)
    {
        if (!context->getSettingsRef()[Setting::allow_experimental_nlp_functions])
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Natural language processing function '{}' is experimental. "
                "Set `allow_experimental_nlp_functions` setting to enable it", name);

        String model_id = context->getSettingsRef()[Setting::embedding_model];
        return std::make_shared<FunctionEmbedding>(model_id);
    }

    explicit FunctionEmbedding(String model_id_) : model_id(std::move(model_id_)) {}

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 1 or 2 arguments, got {}", getName(), arguments.size());

        /// Nullable(String) would require returning Nullable(Array(Float32)), which ClickHouse
        /// forbids. Force the caller to handle NULLs explicitly (coalesce(x, ''), IS NOT NULL, etc.)
        /// rather than silently coerce NULL to an empty array and lose information.
        if (!isString(arguments[0].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {} (Nullable(String) is not supported; use coalesce())",
                arguments[0].type->getName(), getName());

        if (arguments.size() == 2 && !isNativeInteger(arguments[1].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}, expected integer",
                arguments[1].type->getName(), getName());

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const auto * col_str = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        if (!col_str)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}", arguments[0].column->getName(), getName());

        auto model = EmbeddingModelRegistry::instance().getModel(model_id);
        size_t dims = model->getDefaultDims();
        if (arguments.size() == 2)
            dims = arguments[1].column->getUInt(0);
        dims = model->validateDims(dims);

        const size_t rows = col_str->size();
        std::vector<std::string_view> texts;
        texts.reserve(rows);
        for (size_t i = 0; i < rows; ++i)
        {
            auto sv = col_str->getDataAt(i);
            texts.emplace_back(sv.data(), sv.size());
        }

        auto embeddings = model->embedBatch(texts, dims);

        auto col_res_data    = ColumnFloat32::create();
        auto col_res_offsets = ColumnArray::ColumnOffsets::create();
        auto & res_data    = col_res_data->getData();
        auto & res_offsets = col_res_offsets->getData();
        res_data.reserve(rows * dims);
        res_offsets.reserve(rows);

        size_t current_offset = 0;
        for (const auto & emb : embeddings)
        {
            res_data.insert(emb.begin(), emb.end());
            current_offset += emb.size();
            res_offsets.push_back(current_offset);
        }

        return ColumnArray::create(std::move(col_res_data), std::move(col_res_offsets));
    }

private:
    String model_id;
};

REGISTER_FUNCTION(Embedding)
{
    FunctionDocumentation::Description description = R"(
Computes a text embedding using the model specified by the `embedding_model` setting.
By default, uses the built-in static embedding model. Set `embedding_model` to a model ID
configured in the server's <embedding> config section to use a different model.

Supports Matryoshka Representation Learning (MRL) — embeddings can be truncated to any
prefix dimension without retraining.
)";
    FunctionDocumentation::Syntax syntax = "embedding(text [, dims])";
    FunctionDocumentation::Arguments args = {
        {"text", "Input text to embed.", {"String"}},
        {"dims", "Output embedding dimensions (default: model-specific).", {"UInt32"}}
    };
    FunctionDocumentation::ReturnedValue ret = {"L2-normalized embedding vector.", {"Array(Float32)"}};
    FunctionDocumentation::Category cat = FunctionDocumentation::Category::NLP;
    factory.registerFunction<FunctionEmbedding>(
        {description, syntax, args, {}, ret, {}, {26, 4}, cat});
}

}
