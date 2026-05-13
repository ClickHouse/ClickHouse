#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>
#include <Common/Primality.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
}

namespace
{

class FunctionIsPrime : public IFunction
{
public:
    static constexpr auto name = "isPrime";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionIsPrime>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool isDeterministic() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"n", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeUInt), nullptr, "UInt8/UInt16/UInt32/UInt64"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args);
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn * column = arguments[0].column.get();
        auto result = ColumnUInt8::create(input_rows_count);
        auto & result_data = result->getData();

        if (!castTypeToEither<ColumnUInt8, ColumnUInt16, ColumnUInt32, ColumnUInt64>(
                column,
                [&](const auto & col)
                {
                    const auto & data = col.getData();
                    for (size_t i = 0; i < input_rows_count; ++i)
                        result_data[i] = Primality::isPrime(data[i]);
                    return true;
                }))
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());

        return result;
    }
};

}

REGISTER_FUNCTION(IsPrime)
{
    FunctionDocumentation::Description description = R"(
Returns `1` if the argument is a prime number, otherwise `0`.

Uses an exact lookup bitmap for small values and a deterministic [Miller-Rabin test](https://en.wikipedia.org/wiki/Miller-Rabin_primality_test)
for larger values. The result is exact for every supported input type.

For wider unsigned integer types (`UInt128`, `UInt256`), use [`isProbablePrime`](/sql-reference/functions/math-functions#isProbablePrime) instead.
    )";
    FunctionDocumentation::Syntax syntax = "isPrime(n)";
    FunctionDocumentation::Arguments arguments
        = {{"n", "Unsigned integer to test for primality.", {"UInt8", "UInt16", "UInt32", "UInt64"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if `n` is prime, `0` otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples
        = {{"Prime number", "SELECT isPrime(17)", "1"},
           {"Composite number", "SELECT isPrime(18)", "0"},
           {"Large `UInt64` prime", "SELECT isPrime(18446744073709551557)", "1"},
           {"Maximum `UInt64` value", "SELECT isPrime(18446744073709551615)", "0"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Mathematical;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionIsPrime>(documentation);
}

}
