#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRandom.h>
#include <Functions/FunctionHelpers.h>
#include <Core/ServerUUID.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

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
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤

- The first 41 (+ 1 top zero bit) bits is the timestamp (millisecond since Unix epoch 1 Jan 1970)
- The middle 10 bits are the machine ID
- The last 12 bits are a counter to disambiguate multiple snowflakeIDs generated within the same millisecond by differen processes
*/

constexpr auto timestamp_bits_count = 41;
constexpr auto machine_id_bits_count = 10;
constexpr auto machine_seq_num_bits_count = 12;

constexpr int64_t timestamp_mask = ((1LL << timestamp_bits_count) - 1) << (machine_id_bits_count + machine_seq_num_bits_count);
constexpr int64_t machine_id_mask = ((1LL << machine_id_bits_count) - 1) << machine_seq_num_bits_count;
constexpr int64_t machine_seq_num_mask = (1LL << machine_seq_num_bits_count) - 1;
constexpr int64_t max_machine_seq_num = machine_seq_num_mask;

Int64 getMachineID()
{
    UUID server_uuid = ServerUUID::get();
    /// hash into 64 bits
    UInt64 hi = UUIDHelpers::getHighBytes(server_uuid);
    UInt64 lo = UUIDHelpers::getLowBytes(server_uuid);
    return ((hi * 11) ^ (lo * 17)) & machine_id_mask;
}

Int64 getTimestamp()
{
    auto now = std::chrono::system_clock::now();
    auto ticks_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    return ticks_since_epoch & ((1LL << timestamp_bits_count) - 1);
}

}

class FunctionSnowflakeID : public IFunction
{
private:
    mutable std::atomic<Int64> lowest_available_snowflake_id = 0; /// atomic to avoid a mutex

public:
    static constexpr auto name = "generateSnowflakeID";
    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<FunctionSnowflakeID>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool isVariadic() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!arguments.empty()) {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 0.",
                getName(), arguments.size());
        }
        return std::make_shared<DataTypeInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & /*arguments*/, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_res = ColumnVector<Int64>::create();
        typename ColumnVector<Int64>::Container & vec_to = col_res->getData();

        vec_to.resize(input_rows_count);

        if (input_rows_count == 0) {
            return col_res;
        }

        const Int64 machine_id = getMachineID();
        Int64 current_timestamp = getTimestamp();
        Int64 current_machine_seq_num;

        Int64 available_snowflake_id, next_available_snowflake_id;

        const Int64 input_rows_count_signed = static_cast<Int64>(input_rows_count);

        do
        {
            available_snowflake_id = lowest_available_snowflake_id.load();
            const Int64 available_timestamp = (available_snowflake_id & timestamp_mask) >> (machine_id_bits_count + machine_seq_num_bits_count);
            const Int64 available_machine_seq_num = available_snowflake_id & machine_seq_num_mask;

            if (current_timestamp > available_timestamp)
            {
                /// handle overflow
                current_machine_seq_num = 0;
            }
            else
            {
                current_timestamp = available_timestamp;
                current_machine_seq_num = available_machine_seq_num;
            }

            /// calculate new lowest_available_snowflake_id
            const Int64 seq_nums_in_current_timestamp_left = (max_machine_seq_num - current_machine_seq_num + 1);
            Int64 new_timestamp;
            if (input_rows_count_signed >= seq_nums_in_current_timestamp_left)
                new_timestamp = current_timestamp + 1 + (input_rows_count_signed - seq_nums_in_current_timestamp_left) / max_machine_seq_num;
            else
                new_timestamp = current_timestamp;
            const Int64 new_machine_seq_num = (current_machine_seq_num + input_rows_count_signed) & machine_seq_num_mask;
            next_available_snowflake_id = (new_timestamp << (machine_id_bits_count + machine_seq_num_bits_count)) | machine_id | new_machine_seq_num;
        }
        while (!lowest_available_snowflake_id.compare_exchange_strong(available_snowflake_id, next_available_snowflake_id));
        /// failed CAS     => another thread updated `lowest_available_snowflake_id`
        /// successful CAS => we have our range of exclusive values

        for (Int64 & to_row : vec_to)
        {
            to_row = (current_timestamp << (machine_id_bits_count + machine_seq_num_bits_count)) | machine_id | current_machine_seq_num;
            if (current_machine_seq_num++ == max_machine_seq_num)
            {
                current_machine_seq_num = 0;
                ++current_timestamp;
            }
        }

        return col_res;
    }

};

REGISTER_FUNCTION(GenerateSnowflakeID)
{
    factory.registerFunction<FunctionSnowflakeID>(FunctionDocumentation
    {
        .description=R"(
Generates a SnowflakeID -- unique identificators contains:
- The first 41 (+ 1 top zero bit) bits is the timestamp (millisecond since Unix epoch 1 Jan 1970)
- The middle 10 bits are the machine ID
- The last 12 bits are a counter to disambiguate multiple snowflakeIDs generated within the same millisecond by differen processes

In case the number of ids processed overflows, the timestamp field is incremented by 1 and the counter is reset to 0.
This function guarantees strict monotony on 1 machine and differences in values obtained on different machines.
)",
        .syntax = "generateSnowflakeID()",
        .arguments{},
        .returned_value = "Column of Int64",
        .examples{
            {"single call", "SELECT generateSnowflakeID();", R"(
┌─generateSnowflakeID()─┐
│   7195510166884597760 │
└───────────────────────┘)"},
            {"column call", "SELECT generateSnowflakeID() FROM numbers(10);", R"(
┌─generateSnowflakeID()─┐
│   7195516038159417344 │
│   7195516038159417345 │
│   7195516038159417346 │
│   7195516038159417347 │
│   7195516038159417348 │
│   7195516038159417349 │
│   7195516038159417350 │
│   7195516038159417351 │
│   7195516038159417352 │
│   7195516038159417353 │
└───────────────────────┘)"},
            },
        .categories{"Unique identifiers", "Snowflake ID"}
    });
}

}
