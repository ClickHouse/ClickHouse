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

class FunctionSnowflakeID : public IFunction
{
private:
    mutable std::atomic<size_t> machine_sequence_number{0};
    mutable std::atomic<Int64> last_timestamp{0};

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
        Int64 machine_id = (h * 11) ^ (l * 17);

        for (Int64 & x : vec_to) {
            const auto tm_point = std::chrono::system_clock::now();
            Int64 current_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                   tm_point.time_since_epoch()).count();

            Int64 local_machine_sequence_number = 0;

            if (current_timestamp != last_timestamp.load()) {
                machine_sequence_number.store(0);
                last_timestamp.store(current_timestamp);
            } else {
                local_machine_sequence_number = machine_sequence_number.fetch_add(1) + 1;
            }

            x = (current_timestamp << 22) | (machine_id & 0x3ff000ull) | (local_machine_sequence_number & 0xfffull);
        }

        return col_res;
    }

};

REGISTER_FUNCTION(GenerateSnowflakeID)
{
    factory.registerFunction<FunctionSnowflakeID>();
}

}
