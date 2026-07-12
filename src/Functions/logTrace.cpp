#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
    class FunctionLogTrace final : public IFunction
    {
    public:
        static constexpr auto name = "logTrace";
        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionLogTrace>(); }

        String getName() const override { return name; }

        size_t getNumberOfArguments() const override { return 1; }

        /// Do not emit the log message during query analysis, and actually run the function for each block during execution.
        bool isSuitableForConstantFolding() const override { return false; }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            if (!isString(arguments[0]))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                    arguments[0]->getName(), getName());
            return std::make_shared<DataTypeUInt8>();
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
        {
            return execute(arguments, result_type, input_rows_count, /* dry_run= */ false);
        }

        /// Do not emit the log message during query analysis, e.g. while calculating the result header on an empty block.
        ColumnPtr executeImplDryRun(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
        {
            return execute(arguments, result_type, input_rows_count, /* dry_run= */ true);
        }

        ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count, bool dry_run) const
        {
            String message;
            if (const ColumnConst * col = checkAndGetColumnConst<ColumnString>(arguments[0].column.get()))
                message = col->getDataAt(0);
            else
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be Constant string",
                    getName());

            if (!dry_run)
            {
                static auto log = getLogger("FunctionLogTrace");
                LOG_TRACE(log, fmt::runtime(message));
            }

            return DataTypeUInt8().createColumnConst(input_rows_count, 0);
        }
    };

}

REGISTER_FUNCTION(LogTrace)
{
    FunctionDocumentation::Description description = R"(
Emits a trace log message to the server log for each [Block](/development/architecture/#block).
    )";
    FunctionDocumentation::Syntax syntax = "logTrace(message)";
    FunctionDocumentation::Arguments arguments = {
        {"message", "Message that is emitted to the server log.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `0` always.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Basic example",
        R"(
SELECT logTrace('logTrace message');
        )",
        R"(
┌─logTrace('logTrace message')─┐
│                            0 │
└──────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Introspection;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionLogTrace>(documentation);
}

}
