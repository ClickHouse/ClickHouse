#include <Common/thread_local_rng.h>
#include <DataTypes/DataTypeUUID.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsRandom.h>

namespace DB
{

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
// constexpr uint64_t rand_a_bits_mask = (1ull << rand_a_bits_count) - 1;
// constexpr uint64_t rand_b_bits_mask = (1ull << rand_b_bits_count) - 1;
constexpr uint64_t rand_b_with_counter_bits_mask = (1ull << rand_b_low_bits_count) - 1;
constexpr uint64_t counter_low_bits_mask = (1ull << counter_low_bits_count) - 1;
// constexpr uint64_t counter_high_bits_mask = rand_a_bits_mask;

uint64_t getTimestampMillisecond()
{
    timespec tp;
    clock_gettime(CLOCK_REALTIME, &tp);
    const uint64_t sec = tp.tv_sec;
    return sec * 1000 + tp.tv_nsec / 1000000;
}

uint64_t getRandomCounter() {
    return thread_local_rng() & ((1ull << bits_in_counter) - 1);
}

struct UUIDv7VarParts
{
    uint64_t timestamp;
    uint64_t counter;
};

void setParts(UUID & uuid, UUIDv7VarParts & parts)
{
    const uint64_t high = (parts.timestamp << 16) | 0x7000 | (parts.counter >> counter_low_bits_count);

    uint64_t low = (UUIDHelpers::getLowBytes(uuid) & rand_b_with_counter_bits_mask) | variant_2_mask;
    low |= ((parts.counter & counter_low_bits_mask) << rand_b_low_bits_count);

    UUIDHelpers::getHighBytes(uuid) = high;
    UUIDHelpers::getLowBytes(uuid) = low;
}

struct UUIDRange
{
    UUIDv7VarParts begin; /// inclusive
    UUIDv7VarParts end;   /// exclusive
};

/// To get the range of `input_rows_count` UUIDv7 from `max(available, now)`:
/// 1. calculate UUIDv7 by current timestamp (`now`) and a random counter
/// 2. `begin = max(available, now)`
/// 3. Calculate `end = begin + input_rows_count` handling `counter` overflow
UUIDRange getRangeOfAvailableParts(const UUIDv7VarParts & available, size_t input_rows_count)
{
    /// 1. `now`
    UUIDv7VarParts begin = {.timestamp = getTimestampMillisecond(), .counter = getRandomCounter()};

    /// 2. `begin`
    bool available_is_now_or_later = begin.timestamp <= available.timestamp;
    bool available_is_too_far_ahead = begin.timestamp + 10000 <= available.timestamp;
    if (available_is_now_or_later && !available_is_too_far_ahead)
    {
        begin.timestamp = available.timestamp;
        begin.counter = available.counter;
    }

    /// 3. `end = begin + input_rows_count`
    UUIDv7VarParts end;
    const uint64_t counter_nums_in_current_timestamp_left = (counter_limit - begin.counter);
    if (input_rows_count >= counter_nums_in_current_timestamp_left) [[unlikely]]
        /// if counter numbers in current timestamp is not enough for rows --> depending on how many elements input_rows_count overflows, forward timestamp by at least 1 tick
        end.timestamp = begin.timestamp + 1 + (input_rows_count - counter_nums_in_current_timestamp_left) / (counter_limit);
    else
        end.timestamp = begin.timestamp;

    end.counter = (begin.counter + input_rows_count) & (1ull << bits_in_counter) - 1;

    return {begin, end};
}


struct Data
{
    /// Guarantee counter monotonicity within one timestamp across all threads generating UUIDv7 simultaneously.
    static inline UUIDv7VarParts lowest_available_parts;
    static inline SharedMutex mutex; /// works a little bit faster than std::mutex here
    std::lock_guard<SharedMutex> guard;

    Data()
        : guard(mutex)
    {}

    UUIDv7VarParts reserveRange(size_t input_rows_count)
    {
        UUIDRange range = getRangeOfAvailableParts(lowest_available_parts, input_rows_count);

        lowest_available_parts = range.end;

        return range.begin;
    }
};

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

            UUIDv7VarParts available_part;
            {
                Data data;
                available_part = data.reserveRange(input_rows_count); /// returns begin of available snowflake ids range
            }

            for (UUID & uuid : vec_to)
            {
                setParts(uuid, available_part);
                if (available_part.counter + 1 >= counter_limit)
                {
                    /// handle overflow
                    available_part.counter = 0;
                    ++available_part.timestamp;
                }
                else
                {
                    ++available_part.counter;
                }
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

    explicit FunctionGenerateUUIDv7Base(ContextPtr context) : selector(context)
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
    FunctionDocumentation::Syntax syntax = "SELECT generateUUIDv7()";
    FunctionDocumentation::Arguments arguments = {{"expression", "The expression is used to bypass common subexpression elimination if the function is called multiple times in a query but otherwise ignored. Optional."}};
    FunctionDocumentation::ReturnedValue returned_value = "A value of type UUID version 7.";
    FunctionDocumentation::Examples examples = {{"single", "SELECT generateUUIDv7()", ""}, {"multiple", "SELECT generateUUIDv7(1), generateUUIDv7(2)", ""}};
    FunctionDocumentation::Categories categories = {"UUID"};

    factory.registerFunction<FunctionGenerateUUIDv7Base>({description, syntax, arguments, returned_value, examples, categories});
}

}
