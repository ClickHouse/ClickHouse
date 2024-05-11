#include "model_storage.h"

#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

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

    explicit FunctionGGMLEvaluate(ContextPtr context_) : WithContext(context_), log(getLogger(getName())) {}

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
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function {} expects exactly 3 arguments", getName());
        if (arguments.size() > 3)
            throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION, "Function {} expects exactly 3 arguments", getName());
        // TODO : validate types
        // const auto * name_col = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        // if (!name_col)
        //     throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument of function {} must be a string", getName());
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
            result_raw[j] = model->eval(model_params, val);
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
        Tuple t;
        if (!col[0].tryGet(t))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Could not convert argument to map");
        UInt64 n_predict = t[0].safeGet<UInt64>();
        GgmlModelParams res = { n_predict };
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
    factory.registerFunction<FunctionGGMLEvaluate>();
}

}
