#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/geometry.h>
#include <Functions/geometryConverters.h>
#include <base/EnumReflection.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionFlipCoordinates : public IFunction
{
public:
    static constexpr auto name = "flipCoordinates";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFlipCoordinates>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes exactly one argument, got {}",
                getName(),
                arguments.size());

        return arguments[0];
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const ColumnWithTypeAndName & arg = arguments[0];

        ColumnPtr column = arg.column;
        bool is_const = false;
        size_t const_size = 0;

        if (const auto * const_column = checkAndGetColumn<ColumnConst>(column.get()))
        {
            column = const_column->getDataColumnPtr();
            is_const = true;
            const_size = const_column->size();
        }

        ColumnPtr result;

        // Check if input is Geometry variant type
        if (const auto * variant_type = checkAndGetDataType<DataTypeVariant>(arg.type.get()))
        {
            result = executeForGeometry(column, variant_type);
        }
        else if (checkAndGetDataType<DataTypeTuple>(arg.type.get()))
        {
            result = executeForPoint(column);
        }
        else if (const auto * array_type = checkAndGetDataType<DataTypeArray>(arg.type.get()))
        {
            result = executeForArray(column, array_type);
        }
        else
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}. Expected Point (Tuple), LineString (Array(Point)), Ring (Array(Point)), Polygon (Array(Ring)), MultiLineString (Array(LineString)), MultiPolygon (Array(Polygon)), or Geometry (Variant)",
                arg.type->getName(),
                getName());
        }

        if (is_const)
            return ColumnConst::create(result, const_size);

        return result;
    }

private:
    static CartesianPoint flipPoint(const CartesianPoint & point)
    {
        return CartesianPoint(point.get<1>(), point.get<0>());
    }

    static CartesianLineString flipLineString(const CartesianLineString & linestring)
    {
        CartesianLineString result;
        result.reserve(linestring.size());
        for (const auto & point : linestring)
            result.push_back(flipPoint(point));
        return result;
    }

    static CartesianRing flipRing(const CartesianRing & ring)
    {
        CartesianRing result;
        result.reserve(ring.size());
        for (const auto & point : ring)
            result.push_back(flipPoint(point));
        return result;
    }

    static CartesianPolygon flipPolygon(const CartesianPolygon & polygon)
    {
        CartesianPolygon result;
        result.outer() = flipRing(polygon.outer());
        result.inners().reserve(polygon.inners().size());
        for (const auto & inner : polygon.inners())
            result.inners().push_back(flipRing(inner));
        return result;
    }

    static CartesianMultiLineString flipMultiLineString(const CartesianMultiLineString & multilinestring)
    {
        CartesianMultiLineString result;
        result.reserve(multilinestring.size());
        for (const auto & linestring : multilinestring)
            result.push_back(flipLineString(linestring));
        return result;
    }

    static CartesianMultiPolygon flipMultiPolygon(const CartesianMultiPolygon & multipolygon)
    {
        CartesianMultiPolygon result;
        result.reserve(multipolygon.size());
        for (const auto & polygon : multipolygon)
            result.push_back(flipPolygon(polygon));
        return result;
    }

    ColumnPtr executeForGeometry(const ColumnPtr & column, const DataTypeVariant * /*variant_type*/) const
    {
        const auto & column_variant = assert_cast<const ColumnVariant &>(*column);
        const size_t rows = column_variant.size();

        // Create serializers matching GeometryColumnType enum:
        // Linestring=0, MultiLinestring=1, MultiPolygon=2, Point=3, Polygon=4, Ring=5
        PointSerializer<CartesianPoint> point_serializer;
        LineStringSerializer<CartesianPoint> linestring_serializer;
        PolygonSerializer<CartesianPoint> polygon_serializer;
        MultiLineStringSerializer<CartesianPoint> multilinestring_serializer;
        MultiPolygonSerializer<CartesianPoint> multipolygon_serializer;
        RingSerializer<CartesianPoint> ring_serializer;

        auto discriminators_column = ColumnVariant::ColumnDiscriminators::create();
        discriminators_column->reserve(rows);

        Field field;
        const auto & descriptors = column_variant.getLocalDiscriminators();

        // Process each row using the discriminator to determine geometry type
        for (size_t i = 0; i < rows; ++i)
        {
            const auto discr = descriptors[i];
            discriminators_column->insertValue(discr);

            column_variant.get(i, field);
            auto type = magic_enum::enum_cast<GeometryColumnType>(discr);
            if (!type)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unknown geometry type with discriminator {}", static_cast<Int32>(discr));

            // Process based on GeometryColumnType enum
            switch (*type)
            {
                case GeometryColumnType::Point: {
                    auto point = getPointFromField<CartesianPoint>(field);
                    auto flipped = flipPoint(point);
                    point_serializer.add(flipped);
                    break;
                }
                case GeometryColumnType::Linestring: {
                    auto linestring = getLineStringFromField<CartesianPoint>(field);
                    auto flipped = flipLineString(linestring);
                    linestring_serializer.add(flipped);
                    break;
                }
                case GeometryColumnType::Ring: {
                    auto ring = getRingFromField<CartesianPoint>(field);
                    auto flipped = flipRing(ring);
                    ring_serializer.add(flipped);
                    break;
                }
                case GeometryColumnType::Polygon: {
                    auto polygon = getPolygonFromField<CartesianPoint>(field);
                    auto flipped = flipPolygon(polygon);
                    polygon_serializer.add(flipped);
                    break;
                }
                case GeometryColumnType::MultiLinestring: {
                    auto multilinestring = getMultiLineStringFromField<CartesianPoint>(field);
                    auto flipped = flipMultiLineString(multilinestring);
                    multilinestring_serializer.add(flipped);
                    break;
                }
                case GeometryColumnType::MultiPolygon: {
                    auto multipolygon = getMultiPolygonFromField<CartesianPoint>(field);
                    auto flipped = flipMultiPolygon(multipolygon);
                    multipolygon_serializer.add(flipped);
                    break;
                }
                case GeometryColumnType::Null: {
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot flip coordinates of NULL geometry");
                }
            }
        }

        // Build result columns in GeometryColumnType enum order
        Columns result_columns;
        result_columns.push_back(linestring_serializer.finalize()); // 0 = Linestring
        result_columns.push_back(multilinestring_serializer.finalize()); // 1 = MultiLinestring
        result_columns.push_back(multipolygon_serializer.finalize()); // 2 = MultiPolygon
        result_columns.push_back(point_serializer.finalize()); // 3 = Point
        result_columns.push_back(polygon_serializer.finalize()); // 4 = Polygon
        result_columns.push_back(ring_serializer.finalize()); // 5 = Ring

        return ColumnVariant::create(std::move(discriminators_column), result_columns);
    }

    ColumnPtr executeForPoint(const ColumnPtr & column) const
    {
        const auto * column_tuple = checkAndGetColumn<ColumnTuple>(column.get());
        if (!column_tuple)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}", column->getName(), getName());

        const auto & tuple_columns = column_tuple->getColumns();

        if (tuple_columns.size() != 2)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} expects all Tuple elements to have exactly 2 values (x, y), but found a Tuple with {} elements",
                getName(),
                tuple_columns.size());

        for (size_t i = 0; i < 2; ++i)
        {
            const auto * float_col = checkAndGetColumn<ColumnFloat64>(tuple_columns[i].get());
            const auto * const_float_col = checkAndGetColumnConstData<ColumnFloat64>(tuple_columns[i].get());

            if (!float_col && !const_float_col)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function {} expects tuple elements to be Float64, but element {} has type {}",
                    getName(),
                    i + 1,
                    tuple_columns[i]->getName());
        }

        Columns new_columns = {tuple_columns[1], tuple_columns[0]};
        return ColumnTuple::create(new_columns);
    }

    ColumnPtr executeForArray(const ColumnPtr & column, const DataTypeArray * array_type) const
    {
        const auto * column_array = checkAndGetColumn<ColumnArray>(column.get());
        if (!column_array)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}", column->getName(), getName());

        const auto & nested_type = array_type->getNestedType();
        const auto & nested_column = column_array->getDataPtr();

        ColumnPtr result_nested;

        if (checkAndGetDataType<DataTypeTuple>(nested_type.get()))
        {
            result_nested = executeForPoint(nested_column);
        }
        else if (const auto * nested_array = checkAndGetDataType<DataTypeArray>(nested_type.get()))
        {
            result_nested = executeForArray(nested_column, nested_array);
        }
        else
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal nested type {} of argument of function {}",
                nested_type->getName(),
                getName());
        }

        auto offsets_column = column_array->getOffsetsPtr();
        auto result = ColumnArray::create(result_nested, offsets_column);
        return result;
    }
};

REGISTER_FUNCTION(FlipCoordinates)
{
    FunctionDocumentation::Description description = "Flips the coordinates of a Point, LineString, Ring, Polygon, MultiLineString, or MultiPolygon. For a Point, it swaps the coordinates. For arrays, it recursively applies the same transformation for each coordinate pair.";
    FunctionDocumentation::Syntax syntax = "flipCoordinates(geometry)";
    FunctionDocumentation::Arguments arguments = {
        {"geometry", "The geometry to transform. Supported types: Point (Tuple(Float64, Float64)), LineString (Array(Point)), Ring (Array(Point)), Polygon (Array(Ring)), MultiLineString (Array(LineString)), MultiPolygon (Array(Polygon)), Geometry (Variant)."}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"The geometry with flipped coordinates. The type is the same as the input.", {"Point", "LineString", "Ring", "Polygon", "MultiLineString", "MultiPolygon", "Geometry"}};
    FunctionDocumentation::Examples examples = {
        {"basic_point",
         "SELECT flipCoordinates((1.0, 2.0));",
         "(2.0, 1.0)"},
        {"ring",
         "SELECT flipCoordinates([(1.0, 2.0), (3.0, 4.0)]);",
         "[(2.0, 1.0), (4.0, 3.0)]"},
        {"polygon",
         "SELECT flipCoordinates([[(1.0, 2.0), (3.0, 4.0)], [(5.0, 6.0), (7.0, 8.0)]]);",
         "[[(2.0, 1.0), (4.0, 3.0)], [(6.0, 5.0), (8.0, 7.0)]]"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;

    FunctionDocumentation function_documentation = {
        .description = description,
        .syntax = syntax,
        .arguments = arguments,
        .returned_value = returned_value,
        .examples = examples,
        .introduced_in = introduced_in,
        .category = category
    };

    factory.registerFunction<FunctionFlipCoordinates>(function_documentation);
}

}
