#include "config.h"

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

REGISTER_FUNCTION(H3GetUnidirectionalEdgeBoundary)
{
    FunctionDocumentation::Description description = R"(
Returns the coordinates defining the unidirectional edge [H3](#h3-index).
    )";
    FunctionDocumentation::Syntax syntax = "h3GetUnidirectionalEdgeBoundary(index)";
    FunctionDocumentation::Arguments arguments = {
        {"index", "Hexagon index number that represents a unidirectional edge.", {"UInt64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns an array of (longitude, latitude) pairs defining a unidirectional edge.",
        {"Array(Float64, Float64)"}
    };
    FunctionDocumentation::Examples examples = {
        {
            "Get boundary coordinates of a unidirectional edge",
            "SELECT h3GetUnidirectionalEdgeBoundary(1248204388774707199) AS boundary",
            R"(
┌─boundary────────────────────────────────────────────────────────────────────────┐
│ [(37.42012867767779,-122.03773496427027),(37.33755608435299,-122.090428929044)] │
└─────────────────────────────────────────────────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionH3GetUnidirectionalEdgeBoundary>(documentation);
}

}

#endif
