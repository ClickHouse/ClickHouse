#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/escapeForFileName.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>

#include <filesystem>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LIMIT_EXCEEDED;
}

namespace ServerSetting
{
    extern const ServerSettingsString series_keeper_path;
}

namespace Setting
{
    extern const SettingsUInt64 max_autoincrement_series;
}


namespace
{

class FunctionSerial : public IFunction
{
private:
    ContextPtr context;
    String keeper_path;
    size_t max_series = 0;
    bool at_capacity = false;

public:
    static constexpr auto name = "generateSerialID";

    explicit FunctionSerial(ContextPtr context_) : context(context_)
    {
        keeper_path = context->getServerSettings()[ServerSetting::series_keeper_path];
        zkutil::ZooKeeperPtr keeper = context->getZooKeeper();
        max_series = context->getSettingsRef()[Setting::max_autoincrement_series];

        Coordination::Stat stat;
        if (keeper->exists(keeper_path, &stat))
        {
            if (static_cast<size_t>(stat.numChildren) > max_series)
                throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                    "Too many series created by {} function, maximum: {}. This is controlled by the `max_autoincrement_series` setting.",
                    name, max_series);

            if (static_cast<size_t>(stat.numChildren) == max_series)
                at_capacity = true;
        }
        else
        {
            keeper->createAncestors(keeper_path);
            keeper->create(keeper_path, "", zkutil::CreateMode::Persistent);
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
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"series_identifier", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), &isColumnConst, "const String"}
        };
        validateFunctionArguments(*this, arguments, mandatory_args);

        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        String series_name = assert_cast<const ColumnConst &>(*arguments[0].column).getValue<String>();

        if (series_name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The first argument of function {} (the series name) cannot be empty", name);
        series_name = escapeForFileName(series_name);
        if (series_name.size() > 100) /// Arbitrary safety threshold
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Series name '{}' is too long", series_name);

        auto col_res = ColumnUInt64::create();
        typename ColumnUInt64::Container & vec_to = col_res->getData();

        vec_to.resize(input_rows_count);

        String serial_path = std::filesystem::path(keeper_path) / series_name;

        zkutil::ZooKeeperPtr keeper = context->getZooKeeper();
        if (at_capacity)
        {
            if (!keeper->exists(serial_path))
                throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                    "Too many series created by {} function, maximum: {}. This is controlled by the `max_autoincrement_series` setting.",
                    name, max_series);
        }
        else
            keeper->createIfNotExists(serial_path, "0");

        UInt64 counter = 0;
        Coordination::Stat stat;
        while (true)
        {
            String old_value = keeper->get(serial_path, &stat);
            counter = parse<UInt64>(old_value);
            String new_value = toString(counter + input_rows_count);
            auto code = keeper->trySet(serial_path, new_value, stat.version);

            if (code == Coordination::Error::ZOK)
                break;

            if (code == Coordination::Error::ZBADVERSION)
                continue;

            throw zkutil::KeeperException::fromPath(code, serial_path);
        }

        for (auto & val : vec_to)
        {
            val = counter;
            ++counter;
        }

        return col_res;
    }
};

}

REGISTER_FUNCTION(Serial)
{
    factory.registerFunction<FunctionSerial>(FunctionDocumentation
    {
        .description=R"(
Generates and returns sequential numbers starting from the previous counter value.
This function takes a constant string argument - a series identifier.

The server should be configured with Keeper.
The series are stored in Keeper nodes under the path, which can be configured in `series_keeper_path` in the server configuration.
)",
        .syntax = "generateSerialID('series_identifier')",
        .arguments{
            {"series_identifier", "Series identifier, (a short constant String)"}
        },
        .returned_value = "Sequential numbers starting from the previous counter value",
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
        .category{"Other"}
    });
}

}
