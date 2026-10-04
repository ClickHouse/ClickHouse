#include <Access/Common/AccessFlags.h>
#include <Access/Common/AccessType.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Common/Macros.h>
#include <Core/Field.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/** Get the value of macro from configuration file.
  * For example, it may be used as a sophisticated replacement for the function 'hostName' if servers have complicated hostnames
  *  but you still need to distinguish them by some convenient names.
  */
class FunctionGetMacro final : public IFunction
{
private:
    MultiVersion<Macros>::Version macros;
    bool is_distributed;

public:
    static constexpr auto name = "getMacro";
    /// The function is `SELECT substitution FROM system.macros WHERE macro = ...`, so it requires the
    /// grant that query needs, down to the columns it reads.
    static FunctionPtr create(ContextPtr context)
    {
        context->checkAccess(AccessType::SELECT, DatabaseCatalog::SYSTEM_DATABASE, "macros", Strings{"macro", "substitution"});
        return std::make_shared<FunctionGetMacro>(context->getMacros(), context->isDistributed());
    }

    explicit FunctionGetMacro(MultiVersion<Macros>::Version macros_, bool is_distributed_)
        : macros(std::move(macros_)), is_distributed(is_distributed_)
    {
    }

    String getName() const override
    {
        return name;
    }

    bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    /// getMacro may return different values on different shards/replicas, so it's not constant for distributed query
    bool isSuitableForConstantFolding() const override { return !is_distributed; }

    bool isServerConstant() const override { return true; }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The argument of function {} must have String type", getName());
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IColumn * arg_column = arguments[0].column.get();
        const ColumnString * arg_string = checkAndGetColumnConstData<ColumnString>(arg_column);

        if (!arg_string)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "The argument of function {} must be constant String", getName());

        return result_type->createColumnConst(input_rows_count, macros->getValue(arg_string->getDataAt(0)));
    }
};

}

REGISTER_FUNCTION(GetMacro)
{
    FunctionDocumentation::Description description = R"(
Returns the value of a macro from the server configuration file.
Macros are defined in the [`<macros>`](/reference/settings/server-settings/settings/other#macros) section of the configuration file and can be used to distinguish servers by convenient names even if they have complicated hostnames.
If the function is executed in the context of a distributed table, it generates a normal column with values relevant to each shard.

Requires the `SELECT` privilege on `system.macros`, just like reading that table.
)";
    FunctionDocumentation::Syntax syntax = "getMacro(name)";
    FunctionDocumentation::Arguments arguments = {
        {"name", "The name of the macro to retrieve.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the value of the specified macro.", {"String"}};
    FunctionDocumentation::Examples examples = {
        {
            "Basic usage",
            R"(
SELECT getMacro('test');
            )",
            R"(
┌─getMacro('test')─┐
│ Value            │
└──────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionGetMacro>(documentation);
}

}
