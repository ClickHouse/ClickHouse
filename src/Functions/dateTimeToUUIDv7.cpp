#include <Columns/ColumnsDateTime.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsRandom.h>
#include <Functions/UUIDv7Utils.h>
#include <Common/TargetSpecific.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{


uint64_t dateTimeToMillisecond(UInt32 date_time)
{
    return static_cast<uint64_t>(date_time) * 1000;
}


#define DECLARE_SEVERAL_IMPLEMENTATIONS(...) \
DECLARE_DEFAULT_CODE      (__VA_ARGS__) \
DECLARE_X86_64_V3_SPECIFIC_CODE(__VA_ARGS__)

DECLARE_SEVERAL_IMPLEMENTATIONS(

class FunctionDateTimeToUUIDv7Base : public IFunction
{
public:
    static constexpr auto name = "dateTimeToUUIDv7";

    String getName() const final {  return name; }
    size_t getNumberOfArguments() const final { return 1; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const final { return false; }
    bool useDefaultImplementationForNulls() const final { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const final { return false; }
    bool isVariadic() const final { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"value", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isDateTime), nullptr, "DateTime"}
        };

        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeUUID>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & , size_t input_rows_count) const override
    {
        auto col_res = ColumnVector<UUID>::create();
        typename ColumnVector<UUID>::Container & vec_to = col_res->getData();

        if (input_rows_count)
        {
            vec_to.resize(input_rows_count);

            /// Fills in all memory stored by the column of UUIDs with random bytes. Timestamp and other bits are set later.
            RandImpl::execute(reinterpret_cast<char *>(vec_to.data()), vec_to.size() * sizeof(UUID));

            const auto & col_src = *arguments[0].column;
            if (const auto * col_src_non_const = typeid_cast<const ColumnDateTime *>(&col_src))
            {
                const auto & src_data = col_src_non_const->getData();
                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    uint64_t timestamp = dateTimeToMillisecond(src_data[i]);
                    UUIDv7Utils::Data data;
                    data.generate(vec_to[i], timestamp);
                }
            }
            else if (const auto * col_src_const = typeid_cast<const ColumnConst *>(&col_src))
            {
                const auto src_data = col_src_const->getValue<UInt32>();
                uint64_t timestamp = dateTimeToMillisecond(src_data);
                for (UUID & uuid : vec_to)
                {
                    UUIDv7Utils::Data data;
                    data.generate(uuid, timestamp);
                }
            }
            else
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal argument for function {}", name);

        }
        return col_res;
    }
};

) // DECLARE_SEVERAL_IMPLEMENTATIONS
#undef DECLARE_SEVERAL_IMPLEMENTATIONS

class FunctionDateTimeToUUIDv7 : public TargetSpecific::Default::FunctionDateTimeToUUIDv7Base
{
public:
    using Self = FunctionDateTimeToUUIDv7;
    using Parent = TargetSpecific::Default::FunctionDateTimeToUUIDv7Base;

    explicit FunctionDateTimeToUUIDv7(ContextPtr context)
        : selector(context)
    {
        selector.registerImplementation<TargetArch::Default, Parent>();

#if USE_MULTITARGET_CODE
        using ParentAVX2 = TargetSpecific::x86_64_v3::FunctionDateTimeToUUIDv7Base;
        selector.registerImplementation<TargetArch::x86_64_v3, ParentAVX2>();
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


REGISTER_FUNCTION(DateTimeToUUIDv7)
{
    /// dateTimeToUUIDv7 documentation
    FunctionDocumentation::Description description = R"(
Converts a [DateTime](../data-types/datetime.md) value to a [UUIDv7](https://en.wikipedia.org/wiki/UUID#Version_7) at the given time.

See section ["UUIDv7 generation"](#uuidv7-generation) for details on UUID structure, counter management, and concurrency guarantees.

:::note
As of September 2025, version 7 UUIDs are in draft status and their layout may change in future.
:::
    )";
    FunctionDocumentation::Syntax syntax = "dateTimeToUUIDv7(value)";
    FunctionDocumentation::Arguments arguments = {
        {"value", "Date with time.", {"DateTime"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a UUIDv7.", {"UUID"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT dateTimeToUUIDv7(toDateTime('2021-08-15 18:57:56', 'Asia/Shanghai'));
        )",
        R"(
┌─dateTimeToUUIDv7(toDateTime('2021-08-15 18:57:56', 'Asia/Shanghai'))─┐
│ 018f05af-f4a8-778f-beee-1bedbc95c93b                                   │
└─────────────────────────────────────────────────────────────────────────┘
        )"
    },
    {
        "multiple UUIDs for the same timestamp",
        R"(
SELECT dateTimeToUUIDv7(toDateTime('2021-08-15 18:57:56'));
SELECT dateTimeToUUIDv7(toDateTime('2021-08-15 18:57:56'));
        )",
        R"(
┌─dateTimeToUUIDv7(t⋯08-15 18:57:56'))─┐
│ 017b4b2d-7720-76ed-ae44-bbcc23a8c550 │
└──────────────────────────────────────┘
┌─dateTimeToUUIDv7(t⋯08-15 18:57:56'))─┐
│ 017b4b2d-7720-76ed-ae44-bbcf71ed0fd3 │
└──────────────────────────────────────┘
       )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::UUID;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionDateTimeToUUIDv7>(documentation);

}
}
}
