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

class FunctionH3GetUnidirectionalEdgeBoundary : public IFunction
{
public:
    static constexpr auto name = "h3GetUnidirectionalEdgeBoundary";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionH3GetUnidirectionalEdgeBoundary>(); }

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

        return std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeTuple>(
                DataTypes{std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()}));
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

        auto latitude = ColumnFloat64::create();
        auto longitude = ColumnFloat64::create();
        auto offsets = DataTypeNumber<IColumn::Offset>().createColumn();
        offsets->reserve(input_rows_count);
        IColumn::Offset current_offset = 0;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            H3Index edge = data_hindex_edge[row];
            CellBoundary boundary{};

            directedEdgeToBoundary(edge, &boundary);

            for (int vert = 0; vert < boundary.numVerts; ++vert)
            {
                latitude->getData().push_back(radsToDegs(boundary.verts[vert].lat));
                longitude->getData().push_back(radsToDegs(boundary.verts[vert].lng));
            }

            current_offset += boundary.numVerts;
            offsets->insert(current_offset);
        }

        return ColumnArray::create(
            ColumnTuple::create(Columns{std::move(latitude), std::move(longitude)}),
            std::move(offsets));
    }
};

}

void registerFunctionH3GetUnidirectionalEdgeBoundary(FunctionFactory & factory)
{
    factory.registerFunction<FunctionH3GetUnidirectionalEdgeBoundary>();
}

}

#endif
