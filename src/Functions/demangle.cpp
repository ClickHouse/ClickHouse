#include <base/demangle.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <IO/WriteHelpers.h>
#include <Access/Common/AccessFlags.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

class FunctionDemangle : public IFunction
{
public:
    static constexpr auto name = "demangle";
    static FunctionPtr create(ContextPtr context)
    {
        context->checkAccess(AccessType::demangle);
        return std::make_shared<FunctionDemangle>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return true;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} needs exactly one argument; passed {}.",
                getName(), arguments.size());

        const auto & type = arguments[0].type;

        if (!WhichDataType(type.get()).isString())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The only argument for function {} must be String. "
                "Found {} instead.", getName(), type->getName());

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr & column = arguments[0].column;
        const ColumnString * column_concrete = checkAndGetColumn<ColumnString>(column.get());

        if (!column_concrete)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", column->getName(), getName());

        auto result_column = ColumnString::create();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            String source = column_concrete->getDataAt(i).toString();
            auto demangled = tryDemangle(source.c_str());
            if (demangled)
            {
                result_column->insertData(demangled.get(), strlen(demangled.get()));
            }
            else
            {
                result_column->insertData(source.data(), source.size());
            }
        }

        return result_column;
    }
};

}

REGISTER_FUNCTION(Demangle)
{
    FunctionDocumentation::Description description = R"(
Converts a symbol to a C++ function name.
The symbol is usually returned by function `addressToSymbol`.
    )";
    FunctionDocumentation::Syntax syntax = "demangle(symbol)";
    FunctionDocumentation::Arguments arguments = {
        {"symbol", "Symbol from an object file.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the name of the C++ function, or an empty string if the symbol is not valid.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Selecting the first string from the `trace_log` system table",
        R"(
SELECT * FROM system.trace_log LIMIT 1 \G;
        )",
        R"(
-- The `trace` field contains the stack trace at the moment of sampling.
Row 1:
──────
event_date:    2019-11-20
event_time:    2019-11-20 16:57:59
revision:      54429
timer_type:    Real
thread_number: 48
query_id:      724028bf-f550-45aa-910d-2af6212b94ac
trace:         [94138803686098,94138815010911,94138815096522,94138815101224,94138815102091,94138814222988,94138806823642,94138814457211,94138806823642,94138814457211,94138806823642,94138806795179,94138806796144,94138753770094,94138753771646,94138753760572,94138852407232,140399185266395,140399178045583]
        )"
    },
    {
        "Getting a function name for a single address",
        R"(
SET allow_introspection_functions=1;
SELECT demangle(addressToSymbol(94138803686098)) \G;
        )",
        R"(
Row 1:
──────
demangle(addressToSymbol(94138803686098)): DB::IAggregateFunctionHelper<DB::AggregateFunctionSum<unsigned long, unsigned long, DB::AggregateFunctionSumData<unsigned long> > >::addBatchSinglePlace(unsigned long, char*, DB::IColumn const**, DB::Arena*) const
        )"
    },
    {
        "Applying the function to the whole stack trace",
        R"(
SET allow_introspection_functions=1;

-- The arrayMap function allows to process each individual element of the trace array by the demangle function.
-- The result of this processing is shown in the trace_functions column of output.

SELECT
    arrayStringConcat(arrayMap(x -> demangle(addressToSymbol(x)), trace), '\n') AS trace_functions
FROM system.trace_log
LIMIT 1
\G
        )",
        R"(
Row 1:
──────
trace_functions: DB::IAggregateFunctionHelper<DB::AggregateFunctionSum<unsigned long, unsigned long, DB::AggregateFunctionSumData<unsigned long> > >::addBatchSinglePlace(unsigned long, char*, DB::IColumn const**, DB::Arena*) const
DB::Aggregator::executeWithoutKeyImpl(char*&, unsigned long, DB::Aggregator::AggregateFunctionInstruction*, DB::Arena*) const
DB::Aggregator::executeOnBlock(std::vector<COW<DB::IColumn>::immutable_ptr<DB::IColumn>, std::allocator<COW<DB::IColumn>::immutable_ptr<DB::IColumn> > >, unsigned long, DB::AggregatedDataVariants&, std::vector<DB::IColumn const*, std::allocator<DB::IColumn const*> >&, std::vector<std::vector<DB::IColumn const*, std::allocator<DB::IColumn const*> >, std::allocator<std::vector<DB::IColumn const*, std::allocator<DB::IColumn const*> > > >&, bool&)
DB::Aggregator::executeOnBlock(DB::Block const&, DB::AggregatedDataVariants&, std::vector<DB::IColumn const*, std::allocator<DB::IColumn const*> >&, std::vector<std::vector<DB::IColumn const*, std::allocator<DB::IColumn const*> >, std::allocator<std::vector<DB::IColumn const*, std::allocator<DB::IColumn const*> > > >&, bool&)
DB::Aggregator::execute(std::shared_ptr<DB::IBlockInputStream> const&, DB::AggregatedDataVariants&)
DB::AggregatingBlockInputStream::readImpl()
DB::IBlockInputStream::read()
DB::ExpressionBlockInputStream::readImpl()
DB::IBlockInputStream::read()
DB::ExpressionBlockInputStream::readImpl()
DB::IBlockInputStream::read()
DB::AsynchronousBlockInputStream::calculate()
std::_Function_handler<void (), DB::AsynchronousBlockInputStream::next()::{lambda()#1}>::_M_invoke(std::_Any_data const&)
ThreadPoolImpl<ThreadFromGlobalPool>::worker(std::_List_iterator<ThreadFromGlobalPool>)
ThreadFromGlobalPool::ThreadFromGlobalPool<ThreadPoolImpl<ThreadFromGlobalPool>::scheduleImpl<void>(std::function<void ()>, int, std::optional<unsigned long>)::{lambda()#3}>(ThreadPoolImpl<ThreadFromGlobalPool>::scheduleImpl<void>(std::function<void ()>, int, std::optional<unsigned long>)::{lambda()#3}&&)::{lambda()#1}::operator()() const
ThreadPoolImpl<std::thread>::worker(std::_List_iterator<std::thread>)
execute_native_thread_routine
start_thread
clone
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Introspection;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionDemangle>(documentation);
}

}
