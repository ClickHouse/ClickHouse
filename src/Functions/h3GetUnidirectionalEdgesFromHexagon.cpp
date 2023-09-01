#include "config_functions.h"

#if USE_H3

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>
#include <IO/WriteHelpers.h>
#include <h3api.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

class FunctionH3GetUnidirectionalEdgesFromHexagon : public IFunction
{
public:
    static constexpr auto name = "h3GetUnidirectionalEdgesFromHexagon";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionH3GetUnidirectionalEdgesFromHexagon>(); }

    std::string getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
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

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const auto * col_hindex_edge = checkAndGetColumn<ColumnUInt64>(non_const_arguments[0].column.get());
        if (!col_hindex_edge)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64.",
                arguments[0].type->getName(),
                1,
                getName());

        const auto & data_hindex_edge = col_hindex_edge->getData();

        auto result_column_data = ColumnUInt64::create();
        auto & result_data = result_column_data->getData();

        auto result_column_offsets = ColumnArray::ColumnOffsets::create();
        auto & result_offsets = result_column_offsets->getData();
        result_offsets.resize(input_rows_count);

        auto current_offset = 0;
        result_data.reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            // allocate array of size 6
            // originToDirectedEdges places 6 edges into
            // array that's passed to it
            std::array<H3Index, 6> res;

            const UInt64 edge = data_hindex_edge[row];
            originToDirectedEdges(edge, res.data());

            for (auto & i : res)
            {
                ++current_offset;
                result_data.emplace_back(i);
            }

            result_offsets[row] = current_offset;
        }
        return ColumnArray::create(std::move(result_column_data), std::move(result_column_offsets));
    }
};

}

REGISTER_FUNCTION(H3GetUnidirectionalEdgesFromHexagon)
{
    factory.registerFunction<FunctionH3GetUnidirectionalEdgesFromHexagon>();
}

}

#endif
