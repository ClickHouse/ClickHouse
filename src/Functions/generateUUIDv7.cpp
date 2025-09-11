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
DECLARE_AVX2_SPECIFIC_CODE(__VA_ARGS__)

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
        using ParentAVX2 = TargetSpecific::AVX2::FunctionGenerateUUIDv7Base;
        selector.registerImplementation<TargetArch::AVX2, ParentAVX2>();
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
    FunctionDocumentation::Description description = R"(Generates a UUID of version 7. The generated UUID contains the current Unix timestamp in milliseconds (48 bits), followed by version "7" (4 bits), a counter (42 bit, including a variant field "2", 2 bit) to distinguish UUIDs within a millisecond, and a random field (32 bits). For any given timestamp (unix_ts_ms), the counter starts at a random value and is incremented by 1 for each new UUID until the timestamp changes. In case the counter overflows, the timestamp field is incremented by 1 and the counter is reset to a random new start value. Function generateUUIDv7 guarantees that the counter field within a timestamp increments monotonically across all function invocations in concurrently running threads and queries.)";
    FunctionDocumentation::Syntax syntax = "generateUUIDv7()";
    FunctionDocumentation::Arguments arguments = {{"expression", "Optional. The expression is used to bypass common subexpression elimination if the function is called multiple times in a query but otherwise ignored."}};
    FunctionDocumentation::ReturnedValue returned_value = {"A value of type UUID version 7."};
    FunctionDocumentation::Examples examples = {{"single", "SELECT generateUUIDv7()", ""}, {"multiple", "SELECT generateUUIDv7(1), generateUUIDv7(2)", ""}};
    FunctionDocumentation::IntroducedIn introduced_in = {24, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::UUID;

    factory.registerFunction<FunctionGenerateUUIDv7Base>({description, syntax, arguments, returned_value, examples, introduced_in, category});
}
}
}
