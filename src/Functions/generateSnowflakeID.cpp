#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRandom.h>
#include <Functions/FunctionHelpers.h>
#include <Core/ServerUUID.h>
#include <Common/Logger.h>
#include <Common/ErrorCodes.h>
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
- The last 12 bits are a counter to disambiguate multiple snowflakeIDs generated within the same millisecond by different processes
*/

/// bit counts
constexpr auto timestamp_bits_count = 41;
constexpr auto machine_id_bits_count = 10;
constexpr auto machine_seq_num_bits_count = 12;

/// bits masks for Snowflake ID components
constexpr uint64_t machine_id_mask = ((1ull << machine_id_bits_count) - 1) << machine_seq_num_bits_count;
constexpr uint64_t machine_seq_num_mask = (1ull << machine_seq_num_bits_count) - 1;

/// max values
constexpr uint64_t max_machine_seq_num = machine_seq_num_mask;

uint64_t getTimestamp()
{
    auto now = std::chrono::system_clock::now();
    auto ticks_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    return static_cast<uint64_t>(ticks_since_epoch) & ((1ull << timestamp_bits_count) - 1);
}

uint64_t getMachineIdImpl()
{
    UUID server_uuid = ServerUUID::get();
    /// hash into 64 bits
    uint64_t hi = UUIDHelpers::getHighBytes(server_uuid);
    uint64_t lo = UUIDHelpers::getLowBytes(server_uuid);
    /// return only 10 bits
    return (((hi * 11) ^ (lo * 17)) & machine_id_mask) >> machine_seq_num_bits_count;
}

uint64_t getMachineId()
{
    static uint64_t machine_id = getMachineIdImpl();
    return machine_id;
}

struct SnowflakeId
{
    uint64_t timestamp;
    uint64_t machine_id;
    uint64_t machine_seq_num;
};

SnowflakeId toSnowflakeId(uint64_t snowflake)
{
    return {.timestamp = (snowflake >> (machine_id_bits_count + machine_seq_num_bits_count)),
            .machine_id = ((snowflake & machine_id_mask) >> machine_seq_num_bits_count),
            .machine_seq_num = (snowflake & machine_seq_num_mask)};
}

uint64_t fromSnowflakeId(SnowflakeId components)
{
    return (components.timestamp << (machine_id_bits_count + machine_seq_num_bits_count) |
            components.machine_id << (machine_seq_num_bits_count) |
            components.machine_seq_num);
}

struct SnowflakeIdRange
{
    SnowflakeId begin; /// inclusive
    SnowflakeId end;   /// exclusive
};

/// To get the range of `input_rows_count` Snowflake IDs from `max(available, now)`:
/// 1. calculate Snowflake ID by current timestamp (`now`)
/// 2. `begin = max(available, now)`
/// 3. Calculate `end = begin + input_rows_count` handling `machine_seq_num` overflow
SnowflakeIdRange getRangeOfAvailableIds(const SnowflakeId & available, uint64_t machine_id, size_t input_rows_count)

{
    /// 1. `now`
    SnowflakeId begin = {.timestamp = getTimestamp(), .machine_id = machine_id, .machine_seq_num = 0};

    /// 2. `begin`
    if (begin.timestamp <= available.timestamp)
    {
        begin.timestamp = available.timestamp;
        begin.machine_seq_num = available.machine_seq_num;
    }

    /// 3. `end = begin + input_rows_count`
    SnowflakeId end;
    const uint64_t seq_nums_in_current_timestamp_left = (max_machine_seq_num - begin.machine_seq_num + 1);
    if (input_rows_count >= seq_nums_in_current_timestamp_left)
        /// if sequence numbers in current timestamp is not enough for rows --> depending on how many elements input_rows_count overflows, forward timestamp by at least 1 tick
        end.timestamp = begin.timestamp + 1 + (input_rows_count - seq_nums_in_current_timestamp_left) / (max_machine_seq_num + 1);
    else
        end.timestamp = begin.timestamp;

    end.machine_id = begin.machine_id;
    end.machine_seq_num = (begin.machine_seq_num + input_rows_count) & machine_seq_num_mask;

    return {begin, end};
}

struct Data
{
    /// Guarantee counter monotonicity within one timestamp across all threads generating Snowflake IDs simultaneously.
    static inline std::atomic<uint64_t> lowest_available_snowflake_id = 0;

    SnowflakeId reserveRange(uint64_t machine_id, size_t input_rows_count)
    {
        uint64_t available_snowflake_id = lowest_available_snowflake_id.load();
        SnowflakeIdRange range;
        do
        {
            range = getRangeOfAvailableIds(toSnowflakeId(available_snowflake_id), machine_id, input_rows_count);
        }
        while (!lowest_available_snowflake_id.compare_exchange_weak(available_snowflake_id, fromSnowflakeId(range.end)));
        /// CAS failed --> another thread updated `lowest_available_snowflake_id` and we re-try
        ///     else --> our thread reserved ID range [begin, end) and return the beginning of the range

        return range.begin;
    }
};

}

class FunctionGenerateSnowflakeID : public IFunction
{
public:
    static constexpr auto name = "generateSnowflakeID";

    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<FunctionGenerateSnowflakeID>(); }

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
            {"expr", nullptr, nullptr, "Arbitrary expression"},
            {"machine_id", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeUInt), static_cast<FunctionArgumentDescriptor::ColumnValidator>(&isColumnConst), "const UInt*"}
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_res = ColumnVector<UInt64>::create();
        typename ColumnVector<UInt64>::Container & vec_to = col_res->getData();

        if (input_rows_count > 0)
        {
            vec_to.resize(input_rows_count);

            uint64_t machine_id = getMachineId();
            if (arguments.size() == 2)
            {
                machine_id = arguments[1].column->getUInt(0);
                machine_id &= (1ull << machine_id_bits_count) - 1;
            }

            Data data;
            SnowflakeId snowflake_id = data.reserveRange(machine_id, input_rows_count);

            for (UInt64 & to_row : vec_to)
            {
                to_row = fromSnowflakeId(snowflake_id);
                if (snowflake_id.machine_seq_num == max_machine_seq_num)
                {
                    /// handle overflow
                    snowflake_id.machine_seq_num = 0;
                    ++snowflake_id.timestamp;
                }
                else
                {
                    ++snowflake_id.machine_seq_num;
                }
            }
        }

        return col_res;
    }

};

REGISTER_FUNCTION(GenerateSnowflakeID)
{
    FunctionDocumentation::Description description = R"(Generates a Snowflake ID. The generated Snowflake ID contains the current Unix timestamp in milliseconds (41 + 1 top zero bits), followed by a machine id (10 bits), and a counter (12 bits) to distinguish IDs within a millisecond. For any given timestamp (unix_ts_ms), the counter starts at 0 and is incremented by 1 for each new Snowflake ID until the timestamp changes. In case the counter overflows, the timestamp field is incremented by 1 and the counter is reset to 0. Function generateSnowflakeID guarantees that the counter field within a timestamp increments monotonically across all function invocations in concurrently running threads and queries.)";
    FunctionDocumentation::Syntax syntax = "generateSnowflakeID([expression, [machine_id]])";
    FunctionDocumentation::Arguments arguments = {
        {"expression", "The expression is used to bypass common subexpression elimination if the function is called multiple times in a query but otherwise ignored. Optional."},
        {"machine_id", "A machine ID, the lowest 10 bits are used. Optional."}
    };
    FunctionDocumentation::ReturnedValue returned_value = "A value of type UInt64";
    FunctionDocumentation::Examples examples = {{"no_arguments", "SELECT generateSnowflakeID()", "7201148511606784000"}, {"with_machine_id", "SELECT generateSnowflakeID(1)", "7201148511606784001"}, {"with_expression_and_machine_id", "SELECT generateSnowflakeID('some_expression', 1)", "7201148511606784002"}};
    FunctionDocumentation::Categories categories = {"Snowflake ID"};

    factory.registerFunction<FunctionGenerateSnowflakeID>({description, syntax, arguments, returned_value, examples, categories});
}

}
