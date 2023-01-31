#include "config_functions.h"

#if USE_H3

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypesNumber.h>
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

class FunctionH3GetIndexesFromUnidirectionalEdge : public IFunction
{
public:
    static constexpr auto name = "h3GetIndexesFromUnidirectionalEdge";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionH3GetIndexesFromUnidirectionalEdge>(); }

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

        return std::make_shared<DataTypeTuple>(
                DataTypes{std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeUInt64>()},
                Strings{"origin", "destination"});
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

        auto origin = ColumnUInt64::create(input_rows_count);
        auto destination = ColumnUInt64::create(input_rows_count);

        ColumnUInt64::Container & origin_data = origin->getData();
        ColumnUInt64::Container & destination_data = destination->getData();


        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const UInt64 edge = data_hindex_edge[row];
            // allocate array of size 2
            // directedEdgeToCells func sets the origin and
            // destination at [0] and [1] of the input vector
            std::array<H3Index, 2> res;

            directedEdgeToCells(edge, res.data());

            origin_data[row] = res[0];
            destination_data[row] = res[1];
        }

        MutableColumns columns;
        columns.emplace_back(std::move(origin));
        columns.emplace_back(std::move(destination));

        return ColumnTuple::create(std::move(columns));
    }
};

}

REGISTER_FUNCTION(H3GetIndexesFromUnidirectionalEdge)
{
    factory.registerFunction<FunctionH3GetIndexesFromUnidirectionalEdge>();
}

}

#endif
