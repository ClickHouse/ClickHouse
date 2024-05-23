#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRandom.h>
#include <Functions/FunctionHelpers.h>
#include <Core/ServerUUID.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include "base/types.h"


namespace DB
{

namespace
{

/* Snowflake ID
  https://en.wikipedia.org/wiki/Snowflake_ID

 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|0|                         timestamp                           |
├─┼                 ┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|                   |     machine_id    |    machine_seq_num    |
└─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┘

- The first 41 (+ 1 top zero bit) bits is the timestamp (millisecond since Unix epoch 1 Jan 1970)
- The middle 10 bits are the machine ID
- The last 12 bits are a counter to disambiguate multiple snowflakeIDs generated within the same millisecond by differen processes
*/

/// bit counts
constexpr auto timestamp_bits_count = 41;
constexpr auto machine_id_bits_count = 10;
constexpr auto machine_seq_num_bits_count = 12;

/// bits masks for Snowflake ID components
// constexpr uint64_t timestamp_mask = ((1ULL << timestamp_bits_count) - 1) << (machine_id_bits_count + machine_seq_num_bits_count); // unused
constexpr uint64_t machine_id_mask = ((1ULL << machine_id_bits_count) - 1) << machine_seq_num_bits_count;
constexpr uint64_t machine_seq_num_mask = (1ULL << machine_seq_num_bits_count) - 1;

/// max values
constexpr uint64_t max_machine_seq_num = machine_seq_num_mask;

uint64_t getMachineID()
{
    UUID server_uuid = ServerUUID::get();
    /// hash into 64 bits
    uint64_t hi = UUIDHelpers::getHighBytes(server_uuid);
    uint64_t lo = UUIDHelpers::getLowBytes(server_uuid);
    /// return only 10 bits
    return (((hi * 11) ^ (lo * 17)) & machine_id_mask) >> machine_seq_num_bits_count;
}

uint64_t getTimestamp()
{
    auto now = std::chrono::system_clock::now();
    auto ticks_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    return static_cast<uint64_t>(ticks_since_epoch) & ((1ULL << timestamp_bits_count) - 1);
}

struct SnowflakeComponents {
    uint64_t timestamp;
    uint64_t machind_id;
    uint64_t machine_seq_num;
};

SnowflakeComponents toComponents(uint64_t snowflake) {
    return {
        .timestamp = (snowflake >> (machine_id_bits_count + machine_seq_num_bits_count)),
        .machind_id = ((snowflake & machine_id_mask) >> machine_seq_num_bits_count),
        .machine_seq_num = (snowflake & machine_seq_num_mask)
    };
}

uint64_t toSnowflakeID(SnowflakeComponents components) {
    return (components.timestamp << (machine_id_bits_count + machine_seq_num_bits_count) |
            components.machind_id << (machine_seq_num_bits_count) |
            components.machine_seq_num);
}

struct RangeOfSnowflakeIDs {
    /// [begin, end)
    SnowflakeComponents begin, end;
};

/* Get range of `input_rows_count` Snowflake IDs from `max(available, now)`

1. Calculate Snowflake ID by current timestamp (`now`)
2. `begin = max(available, now)`
3. Calculate `end = begin + input_rows_count` handling `machine_seq_num` overflow
*/
RangeOfSnowflakeIDs getRangeOfAvailableIDs(const SnowflakeComponents& available, size_t input_rows_count)
{
    /// 1. `now`
    SnowflakeComponents begin = {
        .timestamp = getTimestamp(),
        .machind_id = getMachineID(),
        .machine_seq_num = 0
    };

    /// 2. `begin`
    if (begin.timestamp <= available.timestamp)
    {
        begin.timestamp = available.timestamp;
        begin.machine_seq_num = available.machine_seq_num;
    }

    /// 3. `end = begin + input_rows_count`
    SnowflakeComponents end;
    const uint64_t seq_nums_in_current_timestamp_left = (max_machine_seq_num - begin.machine_seq_num + 1);
    if (input_rows_count >= seq_nums_in_current_timestamp_left)
        /// if sequence numbers in current timestamp is not enough for rows => update timestamp
        end.timestamp = begin.timestamp + 1 + (input_rows_count - seq_nums_in_current_timestamp_left) / (max_machine_seq_num + 1);
    else
        end.timestamp = begin.timestamp;

    end.machind_id = begin.machind_id;
    end.machine_seq_num = (begin.machine_seq_num + input_rows_count) & machine_seq_num_mask;

    return {begin, end};
}

struct GlobalCounterPolicy
{
    static constexpr auto name = "generateSnowflakeID";
    static constexpr auto doc_description = R"(Generates a Snowflake ID. The generated Snowflake ID contains the current Unix timestamp in milliseconds 41 (+ 1 top zero bit) bits, followed by machine id (10 bits), a counter (12 bits) to distinguish IDs within a millisecond. For any given timestamp (unix_ts_ms), the counter starts at 0 and is incremented by 1 for each new Snowflake ID until the timestamp changes. In case the counter overflows, the timestamp field is incremented by 1 and the counter is reset to 0. Function generateSnowflakeID guarantees that the counter field within a timestamp increments monotonically across all function invocations in concurrently running threads and queries.)";

    /// Guarantee counter monotonicity within one timestamp across all threads generating Snowflake IDs simultaneously.
    struct Data
    {
        static inline std::atomic<uint64_t> lowest_available_snowflake_id = 0;

        SnowflakeComponents reserveRange(size_t input_rows_count)
        {
            uint64_t available_snowflake_id = lowest_available_snowflake_id.load();
            RangeOfSnowflakeIDs range;
            do
            {
                range = getRangeOfAvailableIDs(toComponents(available_snowflake_id), input_rows_count);
            }
            while (!lowest_available_snowflake_id.compare_exchange_weak(available_snowflake_id, toSnowflakeID(range.end)));
            /// if `compare_exhange` failed    => another thread updated `lowest_available_snowflake_id` and we should try again
            ///                      completed => range of IDs [begin, end) is reserved, can return the beginning of the range

            return range.begin;
        }
    };
};

struct ThreadLocalCounterPolicy
{
    static constexpr auto name = "generateSnowflakeIDThreadMonotonic";
    static constexpr auto doc_description = R"(Generates a Snowflake ID. The generated Snowflake ID contains the current Unix timestamp in milliseconds 41 (+ 1 top zero bit) bits, followed by machine id (10 bits), a counter (12 bits) to distinguish IDs within a millisecond. For any given timestamp (unix_ts_ms), the counter starts at 0 and is incremented by 1 for each new Snowflake ID until the timestamp changes. In case the counter overflows, the timestamp field is incremented by 1 and the counter is reset to 0. This function behaves like generateSnowflakeID but gives no guarantee on counter monotony across different simultaneous requests. Monotonicity within one timestamp is guaranteed only within the same thread calling this function to generate Snowflake IDs.)";

    /// Guarantee counter monotonicity within one timestamp within the same thread. Faster than GlobalCounterPolicy if a query uses multiple threads.
    struct Data
    {
        static inline thread_local uint64_t lowest_available_snowflake_id = 0;

        SnowflakeComponents reserveRange(size_t input_rows_count)
        {
            RangeOfSnowflakeIDs range = getRangeOfAvailableIDs(toComponents(lowest_available_snowflake_id), input_rows_count);
            lowest_available_snowflake_id = toSnowflakeID(range.end);
            return range.begin;
        }
    };
};

}

template <typename FillPolicy>
class FunctionGenerateSnowflakeID : public IFunction, public FillPolicy
{
public:
    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<FunctionGenerateSnowflakeID>(); }

    String getName() const override { return FillPolicy::name; }
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
            {"expr", nullptr, nullptr, "Arbitrary Expression"}
        };
        validateFunctionArgumentTypes(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & /*arguments*/, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_res = ColumnVector<UInt64>::create();
        typename ColumnVector<UInt64>::Container & vec_to = col_res->getData();

        vec_to.resize(input_rows_count);

        if (input_rows_count != 0)
        {
            typename FillPolicy::Data data;
            /// get the begin of available snowflake ids range
            SnowflakeComponents snowflake_id = data.reserveRange(input_rows_count);

            for (UInt64 & to_row : vec_to)
            {
                to_row = toSnowflakeID(snowflake_id);
                if (snowflake_id.machine_seq_num++ == max_machine_seq_num)
                {
                    snowflake_id.machine_seq_num = 0;
                    ++snowflake_id.timestamp;
                }
            }
        }

        return col_res;
    }

};

template<typename FillPolicy>
void registerSnowflakeIDGenerator(auto& factory)
{
    static constexpr auto doc_syntax_format = "{}([expression])";
    static constexpr auto example_format = "SELECT {}()";
    static constexpr auto multiple_example_format = "SELECT {f}(1), {f}(2)";

    FunctionDocumentation::Description doc_description = FillPolicy::doc_description;
    FunctionDocumentation::Syntax doc_syntax = fmt::format(doc_syntax_format, FillPolicy::name);
    FunctionDocumentation::Arguments doc_arguments = {{"expression", "The expression is used to bypass common subexpression elimination if the function is called multiple times in a query but otherwise ignored. Optional."}};
    FunctionDocumentation::ReturnedValue doc_returned_value = "A value of type UInt64";
    FunctionDocumentation::Examples doc_examples = {{"uuid", fmt::format(example_format, FillPolicy::name), ""}, {"multiple", fmt::format(multiple_example_format, fmt::arg("f", FillPolicy::name)), ""}};
    FunctionDocumentation::Categories doc_categories = {"Snowflake ID"};

    factory.template registerFunction<FunctionGenerateSnowflakeID<FillPolicy>>({doc_description, doc_syntax, doc_arguments, doc_returned_value, doc_examples, doc_categories}, FunctionFactory::CaseInsensitive);
}

REGISTER_FUNCTION(GenerateSnowflakeID)
{
    registerSnowflakeIDGenerator<GlobalCounterPolicy>(factory);
    registerSnowflakeIDGenerator<ThreadLocalCounterPolicy>(factory);
}

}
