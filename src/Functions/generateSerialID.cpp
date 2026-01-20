#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/escapeForFileName.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
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

public:
    static constexpr auto name = "generateSerialID";

    explicit FunctionSerial(ContextPtr context_) : context(context_)
    {
        keeper_path = context->getServerSettings()[ServerSetting::series_keeper_path];
        max_series = context->getSettingsRef()[Setting::max_autoincrement_series];
    }

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionSerial>(std::move(context));
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    bool isStateful() const override { return true; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isSuitableForConstantFolding() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForNothing() const override { return false; }
    bool canBeExecutedOnDefaultArguments() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args
        {
            {"series_identifier", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}
        };

        FunctionArgumentDescriptors optional_args{
            {"start_value", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt), &isColumnConst, "const UInt*"}
        };

        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeUInt64>();
    }

    /// Increments the counter and returns the old counter.
    [[nodiscard]] UInt64 update(String series_name, UInt64 start_value, size_t & current_series, UInt64 increment, zkutil::ZooKeeperPtr & keeper) const
    {
        if (series_name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The first argument of function {} (the series name) cannot be empty", name);
        series_name = escapeForFileName(series_name);
        if (series_name.size() > 100) /// Arbitrary safety threshold
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Series name '{}' is too long", series_name);

        String serial_path = std::filesystem::path(keeper_path) / series_name;

        if (current_series < max_series)
        {
            const auto initial_value = toString(start_value);
            auto code = keeper->tryCreate(serial_path, initial_value, zkutil::CreateMode::Persistent);
            if (code == Coordination::Error::ZOK)
                ++current_series;
            else if (code != Coordination::Error::ZNODEEXISTS)
                throw zkutil::KeeperException::fromPath(code, serial_path);
        }
        else
        {
            if (!keeper->exists(serial_path))
                throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                    "Too many series created by {} function, maximum: {}. This is controlled by the `max_autoincrement_series` setting.",
                    name, max_series);
        }

        UInt64 counter = 0;
        Coordination::Stat stat;
        while (true)
        {
            String old_value = keeper->get(serial_path, &stat);
            counter = parse<UInt64>(old_value);
            String new_value = toString(counter + increment);
            auto code = keeper->trySet(serial_path, new_value, stat.version);

            if (code == Coordination::Error::ZOK)
                break;

            if (code == Coordination::Error::ZBADVERSION)
                continue;

            throw zkutil::KeeperException::fromPath(code, serial_path);
        }

        return counter;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_res = ColumnUInt64::create();
        typename ColumnUInt64::Container & vec_to = col_res->getData();
        vec_to.resize(input_rows_count);

        UInt64 start_value = 0;
        const auto has_start_value = arguments.size() >= 2;
        if (has_start_value)
            start_value = arguments[1].column->getUInt(0);

        zkutil::ZooKeeperPtr keeper = context->getZooKeeper();

        Coordination::Stat stat;
        if (!keeper->exists(keeper_path, &stat))
        {
            keeper->createAncestors(keeper_path);
            keeper->createIfNotExists(keeper_path, "");
        }
        size_t current_series = stat.numChildren;

        if (current_series > max_series)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                "Too many series created by {} function, maximum: {}. This is controlled by the `max_autoincrement_series` setting.",
                name, max_series);

        if (const auto * column_const = typeid_cast<const ColumnConst *>(arguments[0].column.get()))
        {
            String series_name = column_const->getValue<String>();
            UInt64 counter = update(series_name, start_value, current_series, input_rows_count, keeper);

            for (auto & val : vec_to)
            {
                val = counter;
                ++counter;
            }
        }
        else
        {
            struct Series
            {
                UInt64 num_rows = 0;
                UInt64 old_value = 0;
            };
            std::unordered_map<StringRef, Series, StringRefHash> series;

            /// Count the number of rows for each name:
            for (size_t i = 0; i < input_rows_count; ++i)
                ++series[arguments[0].column->getDataAt(i)].num_rows;

            /// Update counters in Keeper:
            for (auto & [series_name, values] : series)
                values.old_value = update(series_name.toString(), start_value, current_series, values.num_rows, keeper);

            /// Populate the result:
            for (size_t i = 0; i < input_rows_count; ++i)
                vec_to[i] = ++series[arguments[0].column->getDataAt(i)].old_value;
        }

        return col_res;
    }
};

}

REGISTER_FUNCTION(Serial)
{
    FunctionDocumentation::Description description = R"(
Generates and returns sequential numbers starting from the previous counter value.
This function takes a string argument - a series identifier, and an optional starting value.
The server should be configured with Keeper.
The series are stored in Keeper nodes under the path, which can be configured in [`series_keeper_path`](/operations/server-configuration-parameters/settings#series_keeper_path) in the server configuration.
    )";
    FunctionDocumentation::Syntax syntax = "generateSerialID(series_identifier[, start_value])";
    FunctionDocumentation::Arguments arguments = {
        {"series_identifier", "Series identifier", {"const String"}},
        {"start_value", "Optional. Starting value for the counter. Defaults to 0. Note: this value is only used when creating a new series and is ignored if the series already exists", {"UInt*"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns sequential numbers starting from the previous counter value.", {"UInt64"}};
    FunctionDocumentation::Examples examples =
    {
    {
        "first call",
        R"(
SELECT generateSerialID('id1')
        )",
        R"(
┌─generateSerialID('id1')──┐
│                        1 │
└──────────────────────────┘
        )"
    },
    {
        "second call",
        R"(
SELECT generateSerialID('id1')
        )",
        R"(
┌─generateSerialID('id1')──┐
│                        2 │
└──────────────────────────┘
        )"
    },
    {
        "column call",
        R"(
SELECT *, generateSerialID('id1') FROM test_table
        )",
        R"(
┌─CounterID─┬─UserID─┬─ver─┬─generateSerialID('id1')──┐
│         1 │      3 │   3 │                        3 │
│         1 │      1 │   1 │                        4 │
│         1 │      2 │   2 │                        5 │
│         1 │      5 │   5 │                        6 │
│         1 │      4 │   4 │                        7 │
└───────────┴────────┴─────┴──────────────────────────┘
        )"
    },
    {
        "with start value",
        R"(
SELECT generateSerialID('id2', 100)
        )",
        R"(
┌─generateSerialID('id2', 100)──┐
│                           100 │
└───────────────────────────────┘
        )"
    },
    {
        "with start value second call",
        R"(
SELECT generateSerialID('id2', 100)
        )",
        R"(
┌─generateSerialID('id2', 100)──┐
│                           101 │
└───────────────────────────────┘
        )"
}
};
FunctionDocumentation::IntroducedIn introduced_in = {25, 1};
FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

factory.registerFunction<FunctionSerial>(documentation);
}

}
