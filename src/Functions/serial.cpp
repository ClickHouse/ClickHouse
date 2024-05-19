#include <Common/ZooKeeper/ZooKeeper.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int KEEPER_EXCEPTION;
}

class FunctionSerial : public IFunction
{
private:
    mutable zkutil::ZooKeeperPtr zk;
    ContextPtr context;

public:
    static constexpr auto name = "serial";

    explicit FunctionSerial(ContextPtr context_) : context(context_)
    {
        if (context->hasZooKeeper()) {
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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 1.",
                getName(), arguments.size());
        if (!isStringOrFixedString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Type of argument for function {} doesn't match: passed {}, should be string",
                getName(), arguments[0]->getName());

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

        auto col_res = ColumnVector<Int64>::create();
        typename ColumnVector<Int64>::Container & vec_to = col_res->getData();

        vec_to.resize(input_rows_count);

        const auto & serial_path = "/serials/" + arguments[0].column->getDataAt(0).toString();

        /// CAS in ZooKeeper
        /// `get` value and version, `trySet` new with version check
        /// I didn't get how to do it with `multi`

        Int64 counter;
        std::string counter_path = serial_path + "/counter";

        // if serial name used first time
        zk->createAncestors(counter_path);
        zk->createIfNotExists(counter_path, "1");

        Coordination::Stat stat;
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
        .syntax = "serial(identifier)",
        .arguments{
            {"series identifier", "Series identifier (String)"}
        },
        .returned_value = "Sequential numbers of type Int64 starting from the previous counter value",
        .examples{
            {"first call", "SELECT serial('id1')", R"(
┌─serial('id1')──┐
│              1 │
└────────────────┘)"},
            {"second call", "SELECT serial('id1')", R"(
┌─serial('id1')──┐
│              2 │
└────────────────┘)"},
            {"column call", "SELECT *, serial('id1') FROM test_table", R"(
┌─CounterID─┬─UserID─┬─ver─┬─serial('id1')──┐
│         1 │      3 │   3 │              3 │
│         1 │      1 │   1 │              4 │
│         1 │      2 │   2 │              5 │
│         1 │      5 │   5 │              6 │
│         1 │      4 │   4 │              7 │
└───────────┴────────┴─────┴────────────────┘
                  )"}},
        .categories{"Unique identifiers"}
    });
}

}
