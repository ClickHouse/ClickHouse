#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
}

namespace
{

/// Return true if the column is nullable.
class FunctionIsNullable : public IFunction
{
public:
    static constexpr auto name = "isNullable";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionIsNullable>(context->getSettingsRef()[Setting::allow_experimental_analyzer]);
    }

    explicit FunctionIsNullable(bool use_analyzer_) : use_analyzer(use_analyzer_) {}

    String getName() const override
    {
        return name;
    }

    ColumnPtr getConstantResultForNonConstArguments(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const override
    {
        /// isNullable(column) triggers a bug in old analyzer when it is replaced to constant.
        if (!use_analyzer)
            return nullptr;

        const ColumnWithTypeAndName & elem = arguments[0];
        if (elem.type->onlyNull() || canContainNull(*elem.type))
            return result_type->createColumnConst(1, UInt8(1));

        return result_type->createColumnConst(1, UInt8(0));
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool useDefaultImplementationForNothing() const override { return false; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & elem = arguments[0];
        return ColumnUInt8::create(input_rows_count, isColumnNullable(*elem.column) || elem.type->isLowCardinalityNullable());
    }

private:
    bool use_analyzer;
};

}

REGISTER_FUNCTION(IsNullable)
{
    FunctionDocumentation::Description description = R"(
Checks whether the argument's data type is `Nullable` (i.e it allows `NULL` values).
    )";
    FunctionDocumentation::Syntax syntax = "isNullable(x)";
    FunctionDocumentation::Arguments arguments = {
        {"x", "A value of any data type.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if `x` is of a `Nullable` data type, otherwise `0`.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
CREATE TABLE tab (
    ordinary_col UInt32,
    nullable_col Nullable(UInt32)
)
ENGINE = MergeTree
ORDER BY tuple();
INSERT INTO tab (ordinary_col, nullable_col) VALUES (1,1), (2, 2), (3,3);
SELECT isNullable(ordinary_col), isNullable(nullable_col) FROM tab;
        )",
        R"(
┌───isNullable(ordinary_col)──┬───isNullable(nullable_col)──┐
│                           0 │                           1 │
│                           0 │                           1 │
│                           0 │                           1 │
└─────────────────────────────┴─────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Null;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionIsNullable>(documentation);
}

}

