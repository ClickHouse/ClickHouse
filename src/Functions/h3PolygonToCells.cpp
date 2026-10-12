#include "config.h"

#if USE_H3

#include <Columns/ColumnArray.h>

#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>
#include <Functions/h3GeometryToCells.h>
#include <Functions/IFunction.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/CancellationBudget.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>

#include <constants.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_COLUMN;
}

/// Takes a geometry (Ring, Polygon or MultiPolygon) and returns an array of H3 hexagons that cover this geometry.
/// The geometry should be in spherical coordinates as it is in GeoJSON.
class FunctionH3PolygonToCells final : public IFunction
{
public:
    static constexpr auto name = "h3PolygonToCells";
    String getName() const override { return name; }
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionH3PolygonToCells>(); }

    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        /// One polygon can expand to hundreds of millions of cells, so the executor's between-blocks check
        /// cannot bound this call. Resolved from the executing thread rather than captured: this instance
        /// can be stored in table metadata and then run by any later query.
        const std::function<void()> check_cancellation = makeCancellationCheck(name);
        CancellationBudget budget(check_cancellation);

        const bool is_const_geometry = isColumnConst(*arguments[0].column);

        /// Avoid materializing const geometry to full column — extract the inner data column instead,
        /// so that `Converter::convert` processes only one row for the const case.
        ColumnPtr col_array_holder;
        if (is_const_geometry)
            col_array_holder = assert_cast<const ColumnConst &>(*arguments[0].column).getDataColumnPtr();
        else
            col_array_holder = arguments[0].column;

        const auto * col_array = checkAndGetColumn<ColumnArray>(col_array_holder.get());
        if (!col_array)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column type {} of argument 1 of function {}. Must be Array",
                arguments[0].column->getName(), getName());

        auto col_resolution_materialized = arguments[1].column->convertToFullColumnIfConst();
        const auto * col_resolution = checkAndGetColumn<ColumnUInt8>(col_resolution_materialized.get());
        if (!col_resolution)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column type {} of argument 2 of function {}. Must be UInt8",
                arguments[1].column->getName(), getName());

        const auto & data_resolution = col_resolution->getData();
        const String function_name = getName();

        auto dst_data_column = ColumnUInt64::create();
        auto dst_offsets_column = ColumnArray::ColumnOffsets::create(input_rows_count);
        auto & dst_data = *dst_data_column;
        auto & dst_offsets = dst_offsets_column->getData();

        callOnGeometryDataType<SphericalPoint>(arguments[0].type, [&] (const auto & type)
        {
            using TypeConverter = std::decay_t<decltype(type)>;
            using Converter = typename TypeConverter::Type;

            // polygonToCells does not work for points and lines
            if constexpr (std::is_same_v<ColumnToPointsConverter<SphericalPoint>, Converter>)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The first argument of function {} must not be Point", getName());
            if constexpr (std::is_same_v<ColumnToLineStringsConverter<SphericalPoint>, Converter>)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The first argument of function {} must not be LineString", getName());
            if constexpr (std::is_same_v<ColumnToMultiLineStringsConverter<SphericalPoint>, Converter>)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The first argument of function {} must not be MultiLineString", getName());
            if constexpr (std::is_same_v<ColumnToMultiPointsConverter<SphericalPoint>, Converter>)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The first argument of function {} must not be MultiPoint", getName());

            if (input_rows_count == 0)
                return;

            /// All geometries will be of same kind
            auto geometries = Converter::convert(col_array->getPtr());

            auto to_multi_polygon = [](auto && geom) -> SphericalMultiPolygon
            {
                boost::geometry::correct(geom);

                if constexpr (std::is_same_v<ColumnToMultiPolygonsConverter<SphericalPoint>, Converter>)
                    return std::forward<decltype(geom)>(geom);
                else if constexpr (std::is_same_v<ColumnToPolygonsConverter<SphericalPoint>, Converter>)
                    return SphericalMultiPolygon({std::forward<decltype(geom)>(geom)});
                else if constexpr (std::is_same_v<ColumnToRingsConverter<SphericalPoint>, Converter>)
                    return SphericalMultiPolygon({SphericalPolygon({std::forward<decltype(geom)>(geom)})});

                return {};
            };

            /// When the geometry argument is const, correct and wrap it once and reuse across rows.
            SphericalMultiPolygon const_multi_polygon;
            if (is_const_geometry)
                const_multi_polygon = to_multi_polygon(std::move(geometries[0]));

            for (size_t row = 0; row < input_rows_count; ++row)
            {
                UInt8 resolution = data_resolution[row];

                if (resolution > MAX_H3_RES)
                    throw Exception(
                        ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                        "The argument 'resolution' ({}) of function {} is out of bounds because the maximum resolution in H3 library is {}",
                        toString(resolution), getName(), toString(MAX_H3_RES));

                SphericalMultiPolygon row_multi_polygon;
                if (!is_const_geometry)
                    row_multi_polygon = to_multi_polygon(std::move(geometries[row]));
                const SphericalMultiPolygon & multi_polygon = is_const_geometry ? const_multi_polygon : row_multi_polygon;

                /// CONTAINMENT_CENTER: the rule `polygonToCells` documents for itself.
                appendH3Cells(multi_polygon, resolution, 0, function_name, budget, dst_data);
                dst_offsets[row] = dst_data.size();
            }
        });

        return ColumnArray::create(std::move(dst_data_column), std::move(dst_offsets_column));
    }
};

REGISTER_FUNCTION(H3PolygonToCells)
{
    factory.registerFunction<FunctionH3PolygonToCells>(FunctionDocumentation{
        .description="Returns the hexagons (at specified resolution) contained by the provided geometry, either ring or (multi-)polygon. Every vertex must be on the sphere: longitude in -180..180 and latitude in -90..90 degrees. The order of the returned cells is not guaranteed.",
        .syntax = "h3PolygonToCells(geometry, resolution)",
        .introduced_in = {25, 11},
        .category = FunctionDocumentation::Category::Geo});
}

}

#endif
