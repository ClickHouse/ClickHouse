#include <Columns/ColumnsDateTime.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsRandom.h>
#include <Common/SharedMutex.h>
#include <Common/TargetSpecific.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/* Bit layouts of UUIDv7

 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|                           unix_ts_ms                          |
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|          unix_ts_ms           |  ver  |   counter_high_bits   |
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|var|                   counter_low_bits                        |
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|                            rand_b                             |
└─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┘
*/
/// bit counts
constexpr auto rand_a_bits_count = 12;
constexpr auto rand_b_bits_count = 62;
constexpr auto rand_b_low_bits_count = 32;
constexpr auto counter_high_bits_count = rand_a_bits_count;
constexpr auto counter_low_bits_count = 30;
constexpr auto bits_in_counter = counter_high_bits_count + counter_low_bits_count;
constexpr uint64_t counter_limit = (1ull << bits_in_counter);

/// bit masks for UUIDv7 components
constexpr uint64_t variant_2_mask  = (2ull << rand_b_bits_count);
constexpr uint64_t rand_a_bits_mask = (1ull << rand_a_bits_count) - 1;
constexpr uint64_t rand_b_bits_mask = (1ull << rand_b_bits_count) - 1;
constexpr uint64_t rand_b_with_counter_bits_mask = (1ull << rand_b_low_bits_count) - 1;
constexpr uint64_t counter_low_bits_mask = (1ull << counter_low_bits_count) - 1;
constexpr uint64_t counter_high_bits_mask = rand_a_bits_mask;

uint64_t dateTimeToMillisecond(UInt32 date_time)
{
    return static_cast<uint64_t>(date_time) * 1000;
}

void setTimestampAndVersion(UUID & uuid, uint64_t timestamp)
{
    UUIDHelpers::getHighBytes(uuid) = (UUIDHelpers::getHighBytes(uuid) & rand_a_bits_mask) | (timestamp << 16) | 0x7000;
}

void setVariant(UUID & uuid)
{
    UUIDHelpers::getLowBytes(uuid) = (UUIDHelpers::getLowBytes(uuid) & rand_b_bits_mask) | variant_2_mask;
}

struct CounterFields
{
    uint64_t last_timestamp = 0;
    uint64_t counter = 0;

    void resetCounter(const UUID & uuid)
    {
        const uint64_t counter_low_bits = (UUIDHelpers::getLowBytes(uuid) >> rand_b_low_bits_count) & counter_low_bits_mask;
        const uint64_t counter_high_bits = UUIDHelpers::getHighBytes(uuid) & counter_high_bits_mask;
        counter = (counter_high_bits << 30) | counter_low_bits;
    }

    void incrementCounter(UUID & uuid)
    {
        if (++counter == counter_limit) [[unlikely]]
        {
            ++last_timestamp;
            resetCounter(uuid);
            setTimestampAndVersion(uuid, last_timestamp);
            setVariant(uuid);
        }
        else
        {
            UUIDHelpers::getHighBytes(uuid) = (last_timestamp << 16) | 0x7000 | (counter >> counter_low_bits_count);
            UUIDHelpers::getLowBytes(uuid) = (UUIDHelpers::getLowBytes(uuid) & rand_b_with_counter_bits_mask) | variant_2_mask | ((counter & counter_low_bits_mask) << rand_b_low_bits_count);
        }
    }

    void generate(UUID & uuid, uint64_t timestamp)
    {
        const bool need_to_increment_counter = (last_timestamp == timestamp) || ((last_timestamp > timestamp) & (last_timestamp < timestamp + 10000));
        if (need_to_increment_counter)
        {
            incrementCounter(uuid);
        }
        else
        {
            last_timestamp = timestamp;
            resetCounter(uuid);
            setTimestampAndVersion(uuid, last_timestamp);
            setVariant(uuid);
        }
    }
};

struct Data
{
    /// Guarantee counter monotonicity within one timestamp across all threads generating UUIDv7 simultaneously.
    static inline CounterFields fields;
    static inline SharedMutex mutex; /// works a little bit faster than std::mutex here
    std::lock_guard<SharedMutex> guard;

    Data()
        : guard(mutex)
    {}

    void generate(UUID & uuid, uint64_t timestamp)
    {
        fields.generate(uuid, timestamp);
    }
};

}

#define DECLARE_SEVERAL_IMPLEMENTATIONS(...) \
DECLARE_DEFAULT_CODE      (__VA_ARGS__) \
DECLARE_AVX2_SPECIFIC_CODE(__VA_ARGS__)

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
        FunctionArgumentDescriptors mandatory_args {
            {"value", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isDateTime), nullptr, "DateTime"}
        };
        FunctionArgumentDescriptors optional_args;

        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

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
                    Data data;
                    data.generate(vec_to[i], timestamp);
                }
            }
            else if (const auto * col_src_const = typeid_cast<const ColumnConst *>(&col_src))
            {
                const auto src_data = col_src_const->getValue<UInt32>();
                uint64_t timestamp = dateTimeToMillisecond(src_data);
                for (UUID & uuid : vec_to)
                {
                    Data data;
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
        using ParentAVX2 = TargetSpecific::AVX2::FunctionDateTimeToUUIDv7Base;
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


REGISTER_FUNCTION(DateTimeToUUIDv7)
{
    FunctionDocumentation::Description description = R"(Converts a [DateTime](../data-types/datetime.md) value to the first [UUIDv7](https://en.wikipedia.org/wiki/UUID#Version_7) at the giving time.)";
    FunctionDocumentation::Syntax syntax = "dateTimeToUUIDv7(value)";
    FunctionDocumentation::Arguments arguments = {
        {"value", "Date with time.", {"DateTime"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Input value converted to", {"UUID"}};
    FunctionDocumentation::Examples examples = {{"simple", "SELECT dateTimeToUUIDv7(toDateTime('2021-08-15 18:57:56', 'Asia/Shanghai'))", "6832626392367104000"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::UUID;

    factory.registerFunction<FunctionDateTimeToUUIDv7>({description, syntax, arguments, returned_value, examples, introduced_in, category});

}
}
