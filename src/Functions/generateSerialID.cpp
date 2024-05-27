#include "Common/Exception.h"
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int KEEPER_EXCEPTION;
}

constexpr auto function_node_name = "/serial_ids/";
constexpr size_t MAX_SERIES_NUMBER = 1000; // ?

class FunctionSerial : public IFunction
{
private:
    mutable zkutil::ZooKeeperPtr zk;
    ContextPtr context;

public:
    static constexpr auto name = "generateSerialID";

    explicit FunctionSerial(ContextPtr context_) : context(context_)
    {
        if (context->hasZooKeeper())
        {
            zk = context->getZooKeeper();
        }
    }

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionSerial>(std::move(context));
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isStateful() const override { return true; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isSuitableForConstantFolding() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForNothing() const override { return false; }
    bool canBeExecutedOnDefaultArguments() const override { return false; }
    bool isInjective(const ColumnsWithTypeAndName & /*sample_columns*/) const override { return true; }
    bool hasInformationAboutMonotonicity() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"series identifier", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"}
        };
        validateFunctionArgumentTypes(*this, arguments, mandatory_args);

        return std::make_shared<DataTypeInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (zk == nullptr)
            throw Exception(ErrorCodes::KEEPER_EXCEPTION,
            "ZooKeeper is not configured for function {}",
            getName());
        if (zk->expired())
            zk = context->getZooKeeper();

        Coordination::Stat stat;
        if (zk->exists(function_node_name, &stat) && stat.numChildren == MAX_SERIES_NUMBER)
        {
            throw Exception(ErrorCodes::KEEPER_EXCEPTION,
            "At most {} serial nodes can be created",
            MAX_SERIES_NUMBER);
        }

        auto col_res = ColumnVector<Int64>::create();
        typename ColumnVector<Int64>::Container & vec_to = col_res->getData();

        vec_to.resize(input_rows_count);

        const auto & serial_path = function_node_name + arguments[0].column->getDataAt(0).toString();

        /// CAS in ZooKeeper
        /// `get` value and version, `trySet` new with version check
        /// I didn't get how to do it with `multi`

        Int64 counter;
        std::string counter_path = serial_path + "/counter";

        // if serial name used first time
        zk->createAncestors(counter_path);
        zk->createIfNotExists(counter_path, "1");

        while (true)
        {
            const String counter_string = zk->get(counter_path, &stat);
            counter = std::stoll(counter_string);
            String updated_counter = std::to_string(counter + input_rows_count);
            const Coordination::Error err = zk->trySet(counter_path, updated_counter);
            if (err == Coordination::Error::ZOK)
            {
                // CAS is done
                break;
            }
            if (err != Coordination::Error::ZBADVERSION)
            {
                throw Exception(ErrorCodes::KEEPER_EXCEPTION,
                "ZooKeeper trySet operation failed with unexpected error = {} in function {}",
                err, getName());
            }
        }

        // Make a result
        for (auto & val : vec_to)
        {
            val = counter;
            ++counter;
        }

        return col_res;
    }

};

REGISTER_FUNCTION(Serial)
{
    factory.registerFunction<FunctionSerial>(FunctionDocumentation
    {
        .description=R"(
Generates and returns sequential numbers starting from the previous counter value.
This function takes a constant string argument - a series identifier.
The server should be configured with a ZooKeeper.
)",
        .syntax = "generateSerialID(identifier)",
        .arguments{
            {"series identifier", "Series identifier (String or FixedString)"}
        },
        .returned_value = "Sequential numbers of type Int64 starting from the previous counter value",
        .examples{
            {"first call", "SELECT generateSerialID('id1')", R"(
┌─generateSerialID('id1')──┐
│                        1 │
└──────────────────────────┘)"},
            {"second call", "SELECT generateSerialID('id1')", R"(
┌─generateSerialID('id1')──┐
│                        2 │
└──────────────────────────┘)"},
            {"column call", "SELECT *, generateSerialID('id1') FROM test_table", R"(
┌─CounterID─┬─UserID─┬─ver─┬─generateSerialID('id1')──┐
│         1 │      3 │   3 │                        3 │
│         1 │      1 │   1 │                        4 │
│         1 │      2 │   2 │                        5 │
│         1 │      5 │   5 │                        6 │
│         1 │      4 │   4 │                        7 │
└───────────┴────────┴─────┴──────────────────────────┘
                  )"}},
        .categories{"Unique identifiers"}
    });
}

}
