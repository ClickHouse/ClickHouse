#include "config.h"

#if USE_H3

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>

#include <h3api.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INCORRECT_DATA;
    extern const int ILLEGAL_COLUMN;
}

class FunctionH3ToGeoBoundary : public IFunction
{
public:
    static constexpr auto name = "h3ToGeoBoundary";
    String getName() const override { return name; }
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionH3ToGeoBoundary>(); }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto & arg = arguments[0];
        if (!isUInt64(arg))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument {} of function {}. Must be UInt64",
                arg->getName(), 1, getName());
        }

        return std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeTuple>(
                DataTypes{std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()}));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto non_const_arguments = arguments;
        for (auto & argument : non_const_arguments)
            argument.column = argument.column->convertToFullColumnIfConst();

        const auto * column = checkAndGetColumn<ColumnUInt64>(non_const_arguments[0].column.get());
        if (!column)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal type {} of argument {} of function {}. Must be UInt64.",
                arguments[0].type->getName(),
                1,
                getName());

        const auto & data = column->getData();

        auto latitude = ColumnFloat64::create();
        auto longitude = ColumnFloat64::create();
        auto offsets = DataTypeNumber<IColumn::Offset>().createColumn();
        offsets->reserve(input_rows_count);
        IColumn::Offset current_offset = 0;

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            H3Index h3index = data[row];
            CellBoundary boundary{};

            auto err = cellToBoundary(h3index, &boundary);
            if (err)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect H3 index: {}, error: {}", h3index, err);

            for (int vert = 0; vert < boundary.numVerts; ++vert)
            {
                latitude->insert(radsToDegs(boundary.verts[vert].lat));
                longitude->insert(radsToDegs(boundary.verts[vert].lng));
            }

            current_offset += boundary.numVerts;
            offsets->insert(current_offset);
        }

        return ColumnArray::create(
            ColumnTuple::create(Columns{std::move(latitude), std::move(longitude)}),
            std::move(offsets));
    }
};

REGISTER_FUNCTION(H3ToGeoBoundary)
{
    FunctionDocumentation::Description description = R"(
Returns array of pairs `(lat, lon)`, which corresponds to the boundary of the provided H3 index.
    )";
    FunctionDocumentation::Syntax syntax = "h3ToGeoBoundary(h3Index)";
    FunctionDocumentation::Arguments arguments = {
        {"h3Index", "H3 index.", {"UInt64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns an array of coordinate pairs `(lat, lon)` that define the boundary of the H3 hexagon.",
        {"Array(Tuple(Float64, Float64))"}
    };
    FunctionDocumentation::Examples examples = {
        {
            "Get boundary coordinates for an H3 index",
            "SELECT h3ToGeoBoundary(644325524701193974) AS coordinates",
            R"(
┌─h3ToGeoBoundary(599686042433355775)────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ [(37.2713558667319,-121.91508032705622),(37.353926450852256,-121.8622232890249),(37.42834118609435,-121.92354999630156),(37.42012867767779,-122.03773496427027),(37.33755608435299,-122.090428929044),(37.26319797461824,-122.02910130919001)] │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionH3ToGeoBoundary>(documentation);
}

}

#endif
