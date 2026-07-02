#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Common/ZooKeeper/ZooKeeper.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Get the keeper configuration by executing get('/keeper/config')
class FunctionGetKeeperConfig : public IFunction, WithContext
{
public:
    static constexpr auto name = "getKeeperConfig";

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionGetKeeperConfig>(context_); }
    explicit FunctionGetKeeperConfig(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }
    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() > 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Function {} accepts at most 1 argument (keeper_name), got {}",
                            String{name}, arguments.size());

        if (!arguments.empty() && !isString(arguments[0].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "The argument of function {} should be a string with the name of a keeper",
                            String{name});

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        String config_value = getConfigValue(arguments);
        return result_type->createColumnConst(input_rows_count, config_value);
    }

private:
    String getConfigValue(const ColumnsWithTypeAndName & arguments) const
    {
        String keeper_name = "default";

        // If argument is provided, use it as keeper name
        if (!arguments.empty())
        {
            if (!isString(arguments[0].type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "The argument of function {} should be a constant string with the name of a keeper",
                                String{name});

            const auto * column = arguments[0].column.get();
            if (!column || !checkAndGetColumnConstStringOrFixedString(column))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                                "The argument of function {} should be a constant string with the name of a keeper",
                                String{name});

            keeper_name = String{column->getDataAt(0).toView()};
        }

        // Get the keeper session from context
        zkutil::ZooKeeperPtr keeper;
        if (keeper_name == "default")
        {
            keeper = getContext()->getZooKeeper();
        }
        else
        {
            keeper = getContext()->getAuxiliaryZooKeeper(keeper_name);
        }

        if (!keeper)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot get keeper session for '{}'",
                keeper_name);

        // Execute get('/keeper/config')
        String config_value;
        try
        {
            config_value = keeper->get("/keeper/config");
        }
        catch (...)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Failed to get '/keeper/config' from keeper '{}': {}",
                keeper_name,
                getCurrentExceptionMessage(false));
        }

        return config_value;
    }};

}

REGISTER_FUNCTION(GetKeeperConfig)
{
    FunctionDocumentation::Description description = R"(
Returns the configuration from the specified keeper by executing get('/keeper/config').
If no keeper name is provided, uses the default keeper.
)";
    FunctionDocumentation::Syntax syntax = "getKeeperConfig([keeper_name])";
    FunctionDocumentation::Arguments arguments = {
        {"keeper_name", "The name of the keeper (e.g., 'default' or auxiliary keeper name). Optional, defaults to 'default'.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the keeper configuration as a string.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage with default keeper",
        R"(
SELECT getKeeperConfig();
        )",
        R"(
┌─getKeeperConfig()─┐
│ server_id=1       │
└───────────────────┘
        )"
    },
    {
        "Usage with specified keeper",
        R"(
SELECT getKeeperConfig('default');
        )",
        R"(
┌─getKeeperConfig('default')─┐
│ server_id=1                │
└────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {24, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionGetKeeperConfig>(documentation, FunctionFactory::Case::Sensitive);
}

}
