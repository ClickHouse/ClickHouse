#include <memory>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/BloomFilter.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <IO/WriteHelpers.h>
#include <Common/CurrentThread.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
}

class FunctionFilterContains : public IFunction
{
public:
    static constexpr auto name = "__filterContains";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFilterContains>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return false; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return false; }
    bool isSuitableForConstantFolding() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                            "Number of arguments for function {} can't be {}, should be 2",
                            getName(), arguments.size());

        if (!WhichDataType(arguments[0]).isString())
            throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "First argument of function '{}' must be a String filter name",
                    getName());

        return std::make_shared<DataTypeUInt8>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * filter_name_column = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
        if (!filter_name_column)
            throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "First argument of function '{}' must be a String filter name",
                    getName());

        String filter_name = filter_name_column->getValue<String>();

        /// Query context contains filter lookup where per-query filters are stored
        auto query_context = CurrentThread::get().getQueryContext();
        auto filter_lookup = query_context->getRuntimeFilterLookup();
        auto filter = filter_lookup->find(filter_name);

        /// If filter is not present all rows pass
        if (!filter)
            return DataTypeUInt8().createColumnConst(input_rows_count, true);

        const auto & data_column = arguments[1];

        return filter->find(data_column);
    }
};

REGISTER_FUNCTION(FilterContains)
{
    FunctionDocumentation::Description description = R"(Special function for JOIN runtime filtering.)";
    FunctionDocumentation::Syntax syntax = "__filterContains(filter_name, key)";
    FunctionDocumentation::Arguments arguments = {
        {"filter_name", "Internal name of runtime filter. It is built by BuildRuntimeFilterStep.", {"String"}},
        {"key", "Value of any type that is checked to be present in the filter", {}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"True if the key was found in the filter", {"Bool"}};
    FunctionDocumentation::Examples examples = {{"Example", "This function is not supposed to be used in user queries. It might be added to query plan during optimization. ", ""}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;

    factory.registerFunction<FunctionFilterContains>({description, syntax, arguments, returned_value, examples, introduced_in, category}, FunctionFactory::Case::Sensitive);
}

}
