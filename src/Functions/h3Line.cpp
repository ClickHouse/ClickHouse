#include "config.h"

#if USE_H3

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>
#include <base/range.h>

#include <constants.h>
#include <h3api.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int INCORRECT_DATA;
}

namespace
{

class FunctionH3Line : public IFunction
{
public:
    static constexpr auto name = "h3Line";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionH3Line>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto * arg = arguments[0].get();
        if (!WhichDataType(arg).isUInt64())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arg->getName(), 1, getName());

        arg = arguments[1].get();
        if (!WhichDataType(arg).isUInt64())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arg->getName(), 2, getName());

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const auto * col_start_index = checkAndGetColumn<ColumnUInt64>(non_const_arguments[0].column.get());
        if (!col_start_index)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64.",
                arguments[0].type->getName(),
                1,
                getName());

        const auto & data_start_index = col_start_index->getData();

        const auto * col_end_index = checkAndGetColumn<ColumnUInt64>(non_const_arguments[1].column.get());
        if (!col_end_index)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64.",
                arguments[1].type->getName(),
                2,
                getName());

        const auto & data_end_index = col_end_index->getData();


        auto dst = ColumnArray::create(ColumnUInt64::create());
        auto & dst_data = typeid_cast<ColumnUInt64 &>(dst->getData());
        auto & dst_offsets = dst->getOffsets();
        dst_offsets.resize(input_rows_count);

        /// First calculate array sizes for all rows and save them in Offsets
        UInt64 current_offset = 0;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const UInt64 start = data_start_index[row];
            const UInt64 end = data_end_index[row];

            auto size = gridPathCellsSize(start, end);
            if (size < 0)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Line cannot be computed between start H3 index {} and end H3 index {}",
                    start, end);

            current_offset += size;
            dst_offsets[row] = current_offset;
        }

        /// Allocate based on total size of arrays for all rows
        dst_data.getData().resize(current_offset);

        /// Fill the array for each row with known size
        auto* ptr = dst_data.getData().data();
        current_offset = 0;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const UInt64 start = data_start_index[row];
            const UInt64 end = data_end_index[row];
            const auto size = dst_offsets[row] - current_offset;
            gridPathCells(start, end, ptr + current_offset);
            current_offset += size;
        }

        return dst;
    }
};

}

REGISTER_FUNCTION(H3Line)
{
    FunctionDocumentation::Description description = R"(
Returns the line of [H3](#h3-index) indices between the two provided indices.
    )";
    FunctionDocumentation::Syntax syntax = "h3Line(start, end)";
    FunctionDocumentation::Arguments arguments = {
        {"start", "Hexagon index number that represents the starting point.", {"UInt64"}},
        {"end", "Hexagon index number that represents the ending point.", {"UInt64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns an array of H3 indices representing the line between the start and end indices.",
        {"Array(UInt64)"}
    };
    FunctionDocumentation::Examples examples = {
        {
            "Get line of H3 indices between two points",
            "SELECT h3Line(590080540275638271, 590103561300344831) AS indexes",
            R"(
┌─indexes────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ [590080540275638271,590080471556161535,590080883873021951,590106516237844479,590104385934065663,590103630019821567,590103561300344831] │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionH3Line>(documentation);
}

}

#endif
