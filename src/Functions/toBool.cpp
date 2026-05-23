#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/CastOverloadResolver.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>


namespace DB
{
namespace
{
    class FunctionToBool final : public IFunction
    {
    public:
        static constexpr auto name = "toBool";

        static FunctionPtr create(ContextPtr)
        {
            return std::make_shared<FunctionToBool>();
        }

        std::string getName() const override
        {
            return name;
        }

        size_t getNumberOfArguments() const override { return 1; }
        bool useDefaultImplementationForConstants() const override { return true; }
        bool useDefaultImplementationForNulls() const override { return false; }
        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

        String getSignatureString() const override
        {
            return
                "(Nullable(Any)) -> Nullable(typeFromString('Bool'))"
                " OR (Any) -> typeFromString('Bool')";
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t) const override
        {
            const String target_name = result_type->getName();
            ColumnsWithTypeAndName cast_args
            {
                arguments[0],
                {
                    DataTypeString().createColumnConst(arguments[0].column->size(), target_name),
                    std::make_shared<DataTypeString>(),
                    ""
                }
            };

            auto func_cast = createInternalCast(arguments[0], result_type, CastType::nonAccurate, {}, nullptr);
            return func_cast->execute(cast_args, result_type, arguments[0].column->size(), /* dry_run = */ false);
        }
    };
}

REGISTER_FUNCTION(ToBool)
{
    FunctionDocumentation::Description description = R"(
Converts an input value to a value of type Bool.
    )";
    FunctionDocumentation::Syntax syntax = "toBool(expr)";
    FunctionDocumentation::Arguments arguments = {
        {"expr", "Expression returning a number or a string. For strings, accepts 'true' or 'false' (case-insensitive).", {"(U)Int*", "Float*", "String", "Expression"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `true` or `false` based on evaluation of the argument.", {"Bool"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT
    toBool(toUInt8(1)),
    toBool(toInt8(-1)),
    toBool(toFloat32(1.01)),
    toBool('true'),
    toBool('false'),
    toBool('FALSE')
FORMAT Vertical
        )",
        R"(
toBool(toUInt8(1)):      true
toBool(toInt8(-1)):      true
toBool(toFloat32(1.01)): true
toBool('true'):          true
toBool('false'):         false
toBool('FALSE'):         false
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 2};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToBool>(documentation);
}

}
