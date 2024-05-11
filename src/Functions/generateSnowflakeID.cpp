#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionsRandom.h>
#include <Core/ServerUUID.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/*
  Snowflake ID 
  https://en.wikipedia.org/wiki/Snowflake_ID

 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|0|                         timestamp                           |
├─┼                 ┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤
|                   |     machine_id    |    machine_seq_num    |
├─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┼─┤

- The first 41 (+ 1 top zero bit) bits is timestamp in Unix time milliseconds
- The middle 10 bits are the machine ID.
- The last 12 bits decode to number of ids processed by the machine at the given millisecond.
*/

constexpr auto timestamp_size = 41;
constexpr auto machine_id_size = 10;
constexpr auto machine_seq_num_size = 12;

constexpr int64_t timestamp_mask = ((1LL << timestamp_size) - 1) << (machine_id_size + machine_seq_num_size);
constexpr int64_t machine_id_mask = ((1LL << machine_id_size) - 1) << machine_seq_num_size;
constexpr int64_t machine_seq_num_mask = (1LL << machine_seq_num_size) - 1;

}

class FunctionSnowflakeID : public IFunction
{
private:
    mutable std::atomic<Int64> state{0};
    // previous snowflake id
    // state is 1 atomic value because we don't want use mutex

public:
    static constexpr auto name = "generateSnowflakeID";

    static FunctionPtr create(ContextPtr /*context*/)
    {
        return std::make_shared<FunctionSnowflakeID>();
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }

    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool isVariadic() const override { return true; }

    bool isStateful() const override { return true; }
    bool isDeterministic() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() > 1) {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 0 or 1.",
                getName(), arguments.size());
        }

        return std::make_shared<DataTypeInt64>();
    }


    ColumnPtr executeImpl(const ColumnsWithTypeAndName & /*arguments*/, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_res = ColumnVector<Int64>::create();
        typename ColumnVector<Int64>::Container & vec_to = col_res->getData();
        size_t size = input_rows_count;
        vec_to.resize(size);

        auto serverUUID = ServerUUID::get();

        // hash serverUUID into 32 bytes
        Int64 h = UUIDHelpers::getHighBytes(serverUUID);
        Int64 l = UUIDHelpers::getLowBytes(serverUUID);
        Int64 machine_id = ((h * 11) ^ (l * 17)) & machine_id_mask;

        for (Int64 & el : vec_to) {
            const auto tm_point = std::chrono::system_clock::now();
            Int64 current_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                   tm_point.time_since_epoch()).count() & ((1LL << timestamp_size) - 1);

            Int64 last_state, new_state;
            do {
                last_state = state.load();
                Int64 last_timestamp = (last_state & timestamp_mask) >> (machine_id_size + machine_seq_num_size);
                Int64 machine_seq_num = last_state & machine_seq_num_mask;

                if (current_timestamp == last_timestamp) {
                    ++machine_seq_num;
                }
                new_state = (current_timestamp << (machine_id_size + machine_seq_num_size)) | machine_id | machine_seq_num;
            } while (!state.compare_exchange_strong(last_state, new_state));
            // failed CAS     => another thread updated state
            // successful CAS => we have unique (timestamp, machine_seq_num) on this machine

            el = new_state;
        }

        return col_res;
    }

};

REGISTER_FUNCTION(GenerateSnowflakeID)
{
    factory.registerFunction<FunctionSnowflakeID>();
}

}
