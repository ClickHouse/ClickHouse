#include <DataTypes/DataTypeUUID.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsRandom.h>
#include <Functions/UUIDv7Utils.h>

namespace DB
{

namespace
{

uint64_t getTimestampMillisecond()
{
    timespec tp;
    clock_gettime(CLOCK_REALTIME, &tp);/// NOLINT(cert-err33-c)
    const uint64_t sec = tp.tv_sec;
    return sec * 1000 + tp.tv_nsec / 1000000;
}


#define DECLARE_SEVERAL_IMPLEMENTATIONS(...) \
DECLARE_DEFAULT_CODE      (__VA_ARGS__) \
DECLARE_X86_64_V3_SPECIFIC_CODE(__VA_ARGS__)

DECLARE_SEVERAL_IMPLEMENTATIONS(

class FunctionGenerateUUIDv7Base : public IFunction
{
public:
    static constexpr auto name = "generateUUIDv7";

    String getName() const final {  return name; }
    size_t getNumberOfArguments() const final { return 0; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const final { return false; }
    bool useDefaultImplementationForNulls() const final { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const final { return false; }
    bool isVariadic() const final { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args;
        FunctionArgumentDescriptors optional_args{
            {"expr", nullptr, nullptr, "Arbitrary expression"}
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeUUID>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_res = ColumnVector<UUID>::create();
        typename ColumnVector<UUID>::Container & vec_to = col_res->getData();

        if (input_rows_count)
        {
            vec_to.resize(input_rows_count);

            /// Not all random bytes produced here are required for the UUIDv7 but it's the simplest way to get the required number of them by using RandImpl
            RandImpl::execute(reinterpret_cast<char *>(vec_to.data()), vec_to.size() * sizeof(UUID));

            /// Note: For performance reasons, clock_gettime is called once per chunk instead of once per UUID. This reduces precision but
            /// it still complies with the UUID standard.
            uint64_t timestamp = getTimestampMillisecond();
            for (UUID & uuid : vec_to)
            {
                UUIDv7Utils::Data data;
                data.generate(uuid, timestamp);
            }
        }
        return col_res;
    }
};
) // DECLARE_SEVERAL_IMPLEMENTATIONS
#undef DECLARE_SEVERAL_IMPLEMENTATIONS

class FunctionGenerateUUIDv7Base : public TargetSpecific::Default::FunctionGenerateUUIDv7Base
{
public:
    using Self = FunctionGenerateUUIDv7Base;
    using Parent = TargetSpecific::Default::FunctionGenerateUUIDv7Base;

    explicit FunctionGenerateUUIDv7Base(ContextPtr context)
        : selector(context)
    {
        selector.registerImplementation<TargetArch::Default, Parent>();

#if USE_MULTITARGET_CODE
        using Parentv3 = TargetSpecific::x86_64_v3::FunctionGenerateUUIDv7Base;
        selector.registerImplementation<TargetArch::x86_64_v3, Parentv3>();
#endif
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return selector.selectAndExecute(arguments, result_type, input_rows_count);
    }

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<Self>(context);
    }

private:
    ImplementationSelector<IFunction> selector;
};

REGISTER_FUNCTION(GenerateUUIDv7)
{
    /// generateUUIDv7 documentation
    FunctionDocumentation::Description description = R"(
Generates a [version 7](https://datatracker.ietf.org/doc/html/draft-peabody-dispatch-new-uuid-format-04) [UUID](../data-types/uuid.md).

See section ["UUIDv7 generation"](#uuidv7-generation) for details on UUID structure, counter management, and concurrency guarantees.

:::note
As of September 2025, version 7 UUIDs are in draft status and their layout may change in future.
:::
    )";
    FunctionDocumentation::Syntax syntax = "generateUUIDv7([expr])";
    FunctionDocumentation::Arguments arguments = {
        {"expr", "Optional. An arbitrary expression used to bypass [common subexpression elimination](/sql-reference/functions/overview#common-subexpression-elimination) if the function is called multiple times in a query. The value of the expression has no effect on the returned UUID.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a UUIDv7.", {"UUID"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT generateUUIDv7(number) FROM numbers(3);
        )",
        R"(
┌─generateUUIDv7(number)───────────────┐
│ 019947fb-5766-7ed0-b021-d906f8f7cebb │
│ 019947fb-5766-7ed0-b021-d9072d0d1e07 │
│ 019947fb-5766-7ed0-b021-d908dca2cf63 │
└──────────────────────────────────────┘
        )"
    },
    {
        "Common subexpression elimination",
        R"(
SELECT generateUUIDv7(1), generateUUIDv7(1);
        )",
        R"(
┌─generateUUIDv7(1)────────────────────┬─generateUUIDv7(1)────────────────────┐
│ 019947ff-0f87-7d88-ace0-8b5b3a66e0c1 │ 019947ff-0f87-7d88-ace0-8b5b3a66e0c1 │
└──────────────────────────────────────┴──────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {24, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::UUID;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionGenerateUUIDv7Base>(documentation);
}
}
}
