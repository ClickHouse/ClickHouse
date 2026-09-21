#include "config.h"

#if USE_H3

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>

#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>
#include <Functions/h3GeometryToCells.h>
#include <Functions/IFunction.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/CancellationBudget.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/castColumn.h>

#include <constants.h>
#include <h3api.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

[[noreturn]] void throwInvalidContainmentFlag(Int64 flags, std::string_view function_name)
{
    throw Exception(
        ErrorCodes::ARGUMENT_OUT_OF_BOUND,
        "The argument 'flags' ({}) of function {} is invalid (must be 0..3: "
        "0=CONTAINMENT_CENTER, 1=CONTAINMENT_FULL, 2=CONTAINMENT_OVERLAPPING, "
        "3=CONTAINMENT_OVERLAPPING_BBOX)",
        toString(flags),
        function_name);
}

void validateContainmentFlag(Int64 flags, std::string_view function_name)
{
    if (flags < 0 || flags >= static_cast<Int64>(CONTAINMENT_INVALID))
        throwInvalidContainmentFlag(flags, function_name);
}

}

class Functionh3PolygonToCellsWithContainment : public IFunction
{
public:
    static constexpr auto name = "h3PolygonToCellsWithContainment";
    String getName() const override { return name; }
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<Functionh3PolygonToCellsWithContainment>();
    }

    size_t getNumberOfArguments() const override { return 3; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!WhichDataType(arguments[2].get()).isNativeInteger())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument 3 of function {}. Must be integer (values are converted to UInt32 for the H3 API)",
                arguments[2]->getName(), getName());

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

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const UInt8 resolution = data_resolution[row];
            if (resolution > MAX_H3_RES)
                throw Exception(
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "The argument 'resolution' ({}) of function {} is out of bounds (max {})",
                    toString(resolution),
                    function_name,
                    toString(MAX_H3_RES));
        }

        /// H3's containment mode is a `uint32_t` flag mask.
        /// Fast path: UInt8 literals (0..3) and UInt32 columns; otherwise cast to Int64 and validate 0..3.
        /// All flag values are validated before geometry conversion.
        auto col_flags_materialized = arguments[2].column->convertToFullColumnIfConst();

        const ColumnUInt8::Container * flags_data_u8 = nullptr;
        const ColumnUInt32::Container * flags_data_u32 = nullptr;
        const ColumnInt64::Container * flags_data_i64 = nullptr;
        ColumnPtr flags_column_holder;

        if (const auto * col_flags_u8 = checkAndGetColumn<ColumnUInt8>(col_flags_materialized.get()))
        {
            flags_data_u8 = &col_flags_u8->getData();
            for (size_t row = 0; row < input_rows_count; ++row)
                validateContainmentFlag(static_cast<Int64>((*flags_data_u8)[row]), function_name);
        }
        else if (const auto * col_flags_u32 = checkAndGetColumn<ColumnUInt32>(col_flags_materialized.get()))
        {
            flags_data_u32 = &col_flags_u32->getData();
            for (size_t row = 0; row < input_rows_count; ++row)
                validateContainmentFlag(static_cast<Int64>((*flags_data_u32)[row]), function_name);
        }
        else
        {
            flags_column_holder = castColumnAccurate(
                {col_flags_materialized, arguments[2].type, {}},
                std::make_shared<DataTypeInt64>());
            const auto * col_flags_i64 = checkAndGetColumn<ColumnInt64>(flags_column_holder.get());
            if (!col_flags_i64)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column type {} of argument 3 of function {}. Must be integer",
                    arguments[2].column->getName(),
                    function_name);

            flags_data_i64 = &col_flags_i64->getData();
            for (size_t row = 0; row < input_rows_count; ++row)
                validateContainmentFlag((*flags_data_i64)[row], function_name);
        }

        auto dst_data_column = ColumnUInt64::create();
        auto dst_offsets_column = ColumnArray::ColumnOffsets::create(input_rows_count);
        auto & dst_data = *dst_data_column;
        auto & dst_offsets = dst_offsets_column->getData();

        callOnGeometryDataType<SphericalPoint>(arguments[0].type, [&] (const auto & type)
        {
            using TypeConverter = std::decay_t<decltype(type)>;
            using Converter = typename TypeConverter::Type;

            if constexpr (std::is_same_v<ColumnToPointsConverter<SphericalPoint>, Converter>)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The first argument of function {} must not be Point", getName());
            if constexpr (std::is_same_v<ColumnToLineStringsConverter<SphericalPoint>, Converter>)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The first argument of function {} must not be LineString", getName());
            if constexpr (std::is_same_v<ColumnToMultiLineStringsConverter<SphericalPoint>, Converter>)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The first argument of function {} must not be MultiLineString", getName());
            if constexpr (std::is_same_v<ColumnToMultiPointsConverter<SphericalPoint>, Converter>)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The first argument of function {} must not be MultiPoint", getName());
            if (input_rows_count == 0)
                return;

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

            SphericalMultiPolygon const_multi_polygon;
            if (is_const_geometry)
                const_multi_polygon = to_multi_polygon(std::move(geometries[0]));

            for (size_t row = 0; row < input_rows_count; ++row)
            {
                const UInt8 resolution = data_resolution[row];
                const UInt32 flags = flags_data_u8
                    ? static_cast<UInt32>((*flags_data_u8)[row])
                    : flags_data_u32
                        ? (*flags_data_u32)[row]
                        : static_cast<UInt32>((*flags_data_i64)[row]);

                SphericalMultiPolygon row_multi_polygon;
                if (!is_const_geometry)
                    row_multi_polygon = to_multi_polygon(std::move(geometries[row]));
                const SphericalMultiPolygon & multi_polygon =
                    is_const_geometry ? const_multi_polygon : row_multi_polygon;

                appendH3Cells(multi_polygon, resolution, flags, function_name, budget, dst_data);
                dst_offsets[row] = dst_data.size();
            }
        });

        return ColumnArray::create(std::move(dst_data_column), std::move(dst_offsets_column));
    }
};

REGISTER_FUNCTION(h3PolygonToCellsWithContainment)
{
    factory.registerFunction<Functionh3PolygonToCellsWithContainment>(FunctionDocumentation{
        .description =
            "Returns the hexagons (at specified resolution) covering the provided geometry, "
            "using H3's experimental algorithm with selectable containment mode. "
            "Flags: 0=CONTAINMENT_CENTER, 1=CONTAINMENT_FULL, 2=CONTAINMENT_OVERLAPPING, "
            "3=CONTAINMENT_OVERLAPPING_BBOX. The flags argument is passed to the H3 API as UInt32; "
            "use integer literals (0..3) or toUInt32. Other native integer types are converted with an accurate cast. "
            "Every vertex must be on the sphere: longitude in -180..180 and latitude in -90..90 degrees. "
            "The order of the returned cells is not guaranteed. See H3 docs.",
        .syntax = "h3PolygonToCellsWithContainment(geometry, resolution, flags)",
        .introduced_in = {26, 6},
        .category = FunctionDocumentation::Category::Geo});
}

}

#endif
