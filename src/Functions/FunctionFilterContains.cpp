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
    extern const int BAD_ARGUMENTS;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
}

class FunctionFilterContains : public IFunction
{
public:
    static constexpr auto name = "filterContains";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFilterContains>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                            "Number of arguments for function {} can't be {}, should be 2",
                            getName(), arguments.size());

        /// TODO: check that 1st argument is const string

        return std::make_shared<DataTypeUInt8>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto filter_name_column = arguments[0].column;
        if (/*!filter_name_column->isConst() || */filter_name_column->getDataType() != TypeIndex::String)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "First argument of function '{}' must be a String filter name",
                            getName());

        const auto filter_name = filter_name_column->getDataAt(0);
        /// Query context contains filter lookup where per-query filters are stored
        /// TODO: Is this the right way to get query context?
        auto query_context = CurrentThread::get().getQueryContext();
        auto filter_lookup = query_context->getRuntimeFilterLookup();
        auto filter = filter_lookup->find(filter_name.toString());

        /// FIXME: properly handle
//        if (!filter)
//            throw Exception(ErrorCodes::BAD_ARGUMENTS,
//                            "Filter '{}' not found",
//                            filter_name.toString());

        auto data_column = arguments[1].column;

        auto dst = ColumnVector<UInt8>::create();
        auto & dst_data = dst->getData();
        dst_data.resize(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            /// TODO: implement efficiently
            const auto & value = data_column->getDataAt(row);
            dst_data[row] = filter ? filter->find(value.data, value.size) : true;
        }

        return dst;
    }
};

REGISTER_FUNCTION(FilterContains)
{
    FunctionDocumentation::Description description = R"(Special function for JOIN runtime filtering.)";
    FunctionDocumentation::Syntax syntax = "filterContains(filter_name, key)";
    FunctionDocumentation::Arguments arguments = {
        {"filter_name", "Internal name of runtime filter. It is built by BuildRuntimeFilterStep.", {"String"}},
        {"key", "Value of any type that is checked to be present in the filter", {"Any type"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"True if the key was found in the filter", {"Bool"}};
    FunctionDocumentation::Examples examples = {{"Example", "This function is not supposed to be used in user queries. It might be added to query plan during optimization. ", ""}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;

    factory.registerFunction<FunctionFilterContains>({description, syntax, arguments, returned_value, examples, introduced_in, category}, FunctionFactory::Case::Sensitive);
}

}
