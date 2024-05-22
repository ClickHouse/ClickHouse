#include "model_storage.h"

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#include "Common/FunctionDocumentation.h"
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Common/re2.h>

#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>

#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
extern const int ILLEGAL_COLUMN;
extern const int NO_ELEMENTS_IN_CONFIG;
}

/// Evaluate GGML model.
/// - Arguments: TBD
/// - Result: TBD
class FunctionGGMLEvaluate final : public IFunction, WithContext
{
public:
    static constexpr auto name = "ggmlEvaluate";
    static constexpr auto ggmlConfigSection = "ggml";

    explicit FunctionGGMLEvaluate(ContextPtr context_) : WithContext(context_), log(getLogger(getName())) { }

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionGGMLEvaluate>(context_); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool isDeterministic() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 3)
            throw Exception(
                ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function {} expects exactly 3 arguments. Got {}", getName(), arguments.size());
        if (arguments.size() > 3)
            throw Exception(
                ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
                "Function {} expects exactly 3 arguments. Got {}",
                getName(),
                arguments.size());
        if (!isString(arguments[0].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, expected a string.",
                arguments[0].type->getName(),
                getName());
        const auto * name_col = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
        if (!name_col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be a constant string", getName());
        if (!isMap(arguments[1].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}, expected a map.",
                arguments[1].type->getName(),
                getName());
        if (!isString(arguments[2].type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of third argument of function {}, expected a string.",
                arguments[2].type->getName(),
                getName());

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        UNUSED(result_type);

        LOG_DEBUG(log, "Start ggmlEvaluate with {} rows", input_rows_count);

        if (input_rows_count == 0)
            return arguments[0].column;

        auto model_name = getModelName(*arguments[0].column);

        auto model_params = getModelParams(*arguments[1].column);

        auto model = getModel(model_name);

        const auto & vals = *arguments[2].column;
        auto col_res = ColumnString::create();
        col_res->reserve(input_rows_count);
        UInt64 totalsize = 0;
        std::vector<String> result_raw(input_rows_count);

        for (size_t j = 0; j < input_rows_count; ++j)
        {
            std::string val;
            if (!vals[j].tryGet(val))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Could not convert {}'th input to string", j);
            result_raw[j] = model->eval(val, model_params);
            totalsize += result_raw[j].size() + 1;
        }

        col_res->getChars().resize(totalsize);
        col_res->getOffsets().resize(input_rows_count);
        auto * data_ptr = col_res->getChars().data();
        UInt64 offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            memcpy(data_ptr + offset, result_raw[i].data(), result_raw[i].size());
            data_ptr[offset + result_raw[i].size()] = '\0';
            offset += result_raw[i].size() + 1;
            col_res->getOffsets()[i] = offset;
        }

        return col_res;
    }

private:
    std::string getModelName(const DB::IColumn & col) const
    {
        if (!col.hasEqualValues())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Ambiguous model name: column contains different values");
        std::string res;
        if (!col[0].tryGet(res))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Could not convert argument to string");
        return res;
    }

    GgmlModelParams getModelParams(const DB::IColumn & col) const
    {
        if (!col.hasEqualValues())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Ambiguous model parameters: column contains different values");
        Map mp;
        if (!col[0].tryGet(mp))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Could not convert argument to map");
        GgmlModelParams res;
        for (const auto & val : mp)
        {
            Tuple t = val.safeGet<Tuple>();
            res[t[0].safeGet<String>()] = t[1];
        }
        return res;
    }

    std::shared_ptr<IGgmlModel> getModel(const std::string & model_name) const
    {
        auto & storage = getContext()->getGgmlModelStorage();
        auto model = storage.get(model_name);

        if (!getContext()->getConfigRef().has(ggmlConfigSection))
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "no key '{}' in config", ggmlConfigSection);
        ConfigPtr model_config{getContext()->getConfigRef().createView(ggmlConfigSection)};

        LOG_DEBUG(log, "Start loading model");
        model->load(model_config);
        LOG_DEBUG(log, "Model loaded");

        return model;
    }

    LoggerPtr log;
};


REGISTER_FUNCTION(GGMLEvaluate)
{
    factory.registerFunction<FunctionGGMLEvaluate>(
        FunctionDocumentation{
            .description = R"(
Evaluates ggml-compatible model.

Requires that all models planned to be used have been described in the server config beforehand in a following manner:
<clickhouse>                                      // Root of config
    <ggml>                                        // Block containing models descriptions
        <gptj>                                    // Model description block. Here, `gptj` is the name model is to be referred to by.
            <path>/path/to/models/gptj.bin</path> // Path to model file. Model has to be ggml-compatible.
            <arch>gptj</arch>                     // General architecture to use while evaluating the model
            <hparams>                             // Hyper-params specific to provided pair of architecture + model file.
                ...
            </hparams>
        </gptj>
        ...
    </ggml>
    ...
</clickhouse>

Takes 3 arguments:
- Model name. A corresponding entry in server config file must be present.
- Map of parameters. Supported ones:
    - n_predict - Specifies upper bound on the amoun of tokens generated by function.
    - seed      - Specifies what seed to used while initializing the model // GGMLTODO
- String. The data to evaluate the model on.

The result of this function is a string that represents a continuation of the value acquired from the third argument with at most <n_predict> more space-separated tokens.

[example:walrus]
            )",
            .examples
            = {{"walrus",
                "SELECT ggmlEvaluate('gpt2', map('n_predict', 4), 'Did you know that walruses')",
                "Did you know that walruses are actually mammals?"}}},
        FunctionFactory::CaseInsensitive);
}

}
