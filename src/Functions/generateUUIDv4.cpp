#include <DataTypes/DataTypeUUID.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsRandom.h>

namespace DB
{

#define DECLARE_SEVERAL_IMPLEMENTATIONS(...) \
DECLARE_DEFAULT_CODE      (__VA_ARGS__) \
DECLARE_X86_64_V3_SPECIFIC_CODE(__VA_ARGS__)

DECLARE_SEVERAL_IMPLEMENTATIONS(

class FunctionGenerateUUIDv4 : public IFunction
{
public:
    static constexpr auto name = "generateUUIDv4";

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool isVariadic() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args;
        FunctionArgumentDescriptors optional_args{
            {"expr", nullptr, nullptr, "any type"}
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeUUID>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_res = ColumnVector<UUID>::create();
        typename ColumnVector<UUID>::Container & vec_to = col_res->getData();

        size_t size = input_rows_count;
        vec_to.resize(size);

        /// RandImpl is target-dependent and is not the same in different TargetSpecific namespaces.
        RandImpl::execute(reinterpret_cast<char *>(vec_to.data()), vec_to.size() * sizeof(UUID));

        for (UUID & uuid : vec_to)
        {
            /// https://tools.ietf.org/html/rfc4122#section-4.4

            UUIDHelpers::getHighBytes(uuid) = (UUIDHelpers::getHighBytes(uuid) & 0xffffffffffff0fffull) | 0x0000000000004000ull;
            UUIDHelpers::getLowBytes(uuid) = (UUIDHelpers::getLowBytes(uuid) & 0x3fffffffffffffffull) | 0x8000000000000000ull;
        }

        return col_res;
    }
};

) // DECLARE_SEVERAL_IMPLEMENTATIONS
#undef DECLARE_SEVERAL_IMPLEMENTATIONS

class FunctionGenerateUUIDv4 : public TargetSpecific::Default::FunctionGenerateUUIDv4
{
public:
    explicit FunctionGenerateUUIDv4(ContextPtr context) : selector(context)
    {
        selector.registerImplementation<TargetArch::Default,
            TargetSpecific::Default::FunctionGenerateUUIDv4>();

#if USE_MULTITARGET_CODE
        selector.registerImplementation<TargetArch::x86_64_v3,
            TargetSpecific::x86_64_v3::FunctionGenerateUUIDv4>();
#endif
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return selector.selectAndExecute(arguments, result_type, input_rows_count);
    }

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionGenerateUUIDv4>(context);
    }

private:
    ImplementationSelector<IFunction> selector;
};

REGISTER_FUNCTION(GenerateUUIDv4)
{
    /// generateUUIDv4 documentation
    FunctionDocumentation::Description description = R"(Generates a [version 4](https://tools.ietf.org/html/rfc4122#section-4.4) [UUID](../data-types/uuid.md).)";
    FunctionDocumentation::Syntax syntax = "generateUUIDv4([expr])";
    FunctionDocumentation::Arguments arguments = {
        {"expr", "Optional. An arbitrary expression used to bypass [common subexpression elimination](/sql-reference/functions/overview#common-subexpression-elimination) if the function is called multiple times in a query. The value of the expression has no effect on the returned UUID."}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a UUIDv4.", {"UUID"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT generateUUIDv4(number) FROM numbers(3);
        )",
        R"(
┌─generateUUIDv4(number)───────────────┐
│ fcf19b77-a610-42c5-b3f5-a13c122f65b6 │
│ 07700d36-cb6b-4189-af1d-0972f23dc3bc │
│ 68838947-1583-48b0-b9b7-cf8268dd343d │
└──────────────────────────────────────┘
        )"
    },
    {
        "Common subexpression elimination",
        R"(
SELECT generateUUIDv4(1), generateUUIDv4(1);
        )",
        R"(
┌─generateUUIDv4(1)────────────────────┬─generateUUIDv4(2)────────────────────┐
│ 2d49dc6e-ddce-4cd0-afb8-790956df54c1 │ 2d49dc6e-ddce-4cd0-afb8-790956df54c1 │
└──────────────────────────────────────┴──────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::UUID;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionGenerateUUIDv4>(documentation);
}

}


