#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>

#include <BridgeHelper/CatBoostLibraryBridgeHelper.h>
#include <BridgeHelper/IBridgeHelper.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>

#include <DataTypes/DataTypeString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_COLUMN;
}

/// Evaluate CatBoost model.
/// - Arguments: float features first, then categorical features.
/// - Result: Float64.
class FunctionGGMLEvaluate final : public IFunction, WithContext
{
private:
    mutable std::unique_ptr<CatBoostLibraryBridgeHelper> bridge_helper;

public:
    static constexpr auto name = "ggmlEvaluate";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionGGMLEvaluate>(context_); }

    explicit FunctionGGMLEvaluate(ContextPtr context_) : WithContext(context_) {}
    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool isDeterministic() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function {} expects exactly 1 argument", getName());
        if (arguments.size() > 1)
            throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION, "Function {} expects exactly 1 argument", getName());
        // const auto * name_col = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        // if (!name_col)
        //     throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument of function {} must be a string", getName());
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        std::cout << "GGML!!!" << std::endl;
        std::cout << "input_rows_count is : " << input_rows_count << std::endl;
        std::cout << "result_type is : " << result_type->getName() << std::endl;

        ColumnPtr res = arguments[0].column;
        return res;
    }
};


REGISTER_FUNCTION(GGMLEvaluate)
{
    factory.registerFunction<FunctionGGMLEvaluate>();
}

}
