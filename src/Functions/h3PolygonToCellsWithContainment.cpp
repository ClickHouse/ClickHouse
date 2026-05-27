#include "config.h"

#if USE_H3

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>

#include <Functions/FunctionFactory.h>
#include <Functions/geometryConverters.h>
#include <Functions/IFunction.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/castColumn.h>

#include <Common/VectorWithMemoryTracking.h>
#include <constants.h>
#include <h3api.h>

static constexpr size_t MAX_ARRAY_SIZE = 1 << 30;

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int ILLEGAL_COLUMN;
    extern const int QUERY_WAS_CANCELLED;
    extern const int INCORRECT_DATA;
}

namespace
{

SphericalPointInRadians toRadianPoint(const SphericalPoint & degree_point)
{
    Float64 lon = get<0>(degree_point) * M_PI / 180.0;
    Float64 lat = get<1>(degree_point) * M_PI / 180.0;
    return SphericalPointInRadians(lon, lat);
}

LatLng toH3LatLng(const SphericalPointInRadians & point)
{
    LatLng result;
    result.lat = point.get<1>();
    result.lng = point.get<0>();
    return result;
}

}

class Functionh3PolygonToCellsWithContainment : public IFunction
{
public:
    static constexpr auto name = "h3PolygonToCellsWithContainment";
    String getName() const override { return name; }
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<Functionh3PolygonToCellsWithContainment>(context);
    }

    explicit Functionh3PolygonToCellsWithContainment(ContextPtr context)
        : process_list_element(context ? context->getProcessListElement() : nullptr)
    {
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

        /// H3 polygonToCellsExperimental expects uint32_t flags.
        /// Fast path: UInt8 literals (0..3) and UInt32 columns (toUInt32); otherwise accurate cast to UInt32.
        auto col_flags_materialized = arguments[2].column->convertToFullColumnIfConst();

        const ColumnUInt8::Container * flags_data_u8 = nullptr;
        const ColumnUInt32::Container * flags_data_u32 = nullptr;
        ColumnPtr flags_column_casted;

        if (const auto * col_flags_u8 = checkAndGetColumn<ColumnUInt8>(col_flags_materialized.get()))
            flags_data_u8 = &col_flags_u8->getData();
        else if (const auto * col_flags_u32 = checkAndGetColumn<ColumnUInt32>(col_flags_materialized.get()))
            flags_data_u32 = &col_flags_u32->getData();
        else
        {
            flags_column_casted = castColumnAccurate(
                {col_flags_materialized, arguments[2].type, {}},
                std::make_shared<DataTypeUInt32>());
            const auto * col_flags_u32_casted = checkAndGetColumn<ColumnUInt32>(flags_column_casted.get());
            if (!col_flags_u32_casted)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal column type {} of argument 3 of function {}. Must be integer",
                    arguments[2].column->getName(), getName());
            flags_data_u32 = &col_flags_u32_casted->getData();
        }

        auto dst = ColumnArray::create(ColumnUInt64::create());
        auto & dst_data = dst->getData();
        auto & dst_offsets = dst->getOffsets();
        dst_offsets.resize(input_rows_count);

        size_t current_offset = 0;


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

            VectorWithMemoryTracking<H3Index> hindex_vec;

            for (size_t row = 0; row < input_rows_count; ++row)
            {
                UInt8 resolution = data_resolution[row];
                if (resolution > MAX_H3_RES)
                    throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                        "The argument 'resolution' ({}) of function {} is out of bounds (max {})",
                        toString(resolution), getName(), toString(MAX_H3_RES));

                UInt32 flags = flags_data_u8
                    ? static_cast<UInt32>((*flags_data_u8)[row])
                    : (*flags_data_u32)[row];
                if (flags >= CONTAINMENT_INVALID)
                    throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                        "The argument 'flags' ({}) of function {} is invalid (must be 0..3: "
                        "0=CONTAINMENT_CENTER, 1=CONTAINMENT_FULL, 2=CONTAINMENT_OVERLAPPING, "
                        "3=CONTAINMENT_OVERLAPPING_BBOX)",
                        toString(flags), getName());

                SphericalMultiPolygon row_multi_polygon;
                if (!is_const_geometry)
                    row_multi_polygon = to_multi_polygon(std::move(geometries[row]));
                const SphericalMultiPolygon & multi_polygon =
                    is_const_geometry ? const_multi_polygon : row_multi_polygon;

                if (process_list_element && process_list_element->isKilled())
                    throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");

                const size_t row_start_offset = current_offset;
                for (const auto & polygon : multi_polygon)
                {
                    VectorWithMemoryTracking<LatLng> exterior;
                    exterior.reserve(polygon.outer().size());
                    for (const auto & point : polygon.outer())
                        exterior.push_back(toH3LatLng(toRadianPoint(point)));

                    VectorWithMemoryTracking<VectorWithMemoryTracking<LatLng>> holes;
                    holes.reserve(polygon.inners().size());
                    for (const auto & inner : polygon.inners())
                    {
                        VectorWithMemoryTracking<LatLng> hole;
                        hole.reserve(inner.size());
                        for (const auto & point : inner)
                            hole.push_back(toH3LatLng(toRadianPoint(point)));
                        holes.emplace_back(std::move(hole));
                    }

                    GeoPolygonContainer polygon_wrapper(std::move(exterior), std::move(holes));

                    int64_t polygon_size = 0;
                    H3Error size_err = maxPolygonToCellsSizeExperimental(
                        polygon_wrapper.unwrap(), resolution, flags, &polygon_size);
                    if (size_err != E_SUCCESS)
                        throw Exception(ErrorCodes::INCORRECT_DATA,
                            "Failed to estimate H3 polygon to cells size in function {}: {}",
                            getName(), describeH3Error(size_err));

                    const size_t vec_size = static_cast<size_t>(polygon_size);
                    const size_t row_size_so_far = current_offset - row_start_offset;
                    if (vec_size > MAX_ARRAY_SIZE || row_size_so_far + vec_size > MAX_ARRAY_SIZE)
                        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                            "The result of function {} (array of {} elements) will be too large with resolution = {}",
                            getName(), row_size_so_far + vec_size, toString(resolution));

                    hindex_vec.assign(vec_size, 0);
                    H3Error fill_err = polygonToCellsExperimental(
                        polygon_wrapper.unwrap(),
                        resolution,
                        flags,
                        static_cast<int64_t>(vec_size),
                        hindex_vec.data());
                    if (fill_err != E_SUCCESS)
                        throw Exception(ErrorCodes::INCORRECT_DATA,
                            "Failed to compute H3 polygon to cells in function {}: {}",
                            getName(), describeH3Error(fill_err));

                    dst_data.reserve(dst_data.size() + vec_size);
                    for (auto hindex : hindex_vec)
                    {
                        if (hindex != 0)
                        {
                            ++current_offset;
                            dst_data.insert(hindex);
                        }
                    }
                }

                if (current_offset - row_start_offset > MAX_ARRAY_SIZE)
                    throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                        "The result of function {} (array of {} elements) will be too large with resolution = {}",
                        getName(), current_offset - row_start_offset, toString(resolution));

                dst_offsets[row] = current_offset;
            }
        });

        return dst;
    }

private:
    QueryStatusPtr process_list_element;

    class GeoPolygonContainer
    {
    private:
        VectorWithMemoryTracking<LatLng> mainLoopVerts;
        VectorWithMemoryTracking<VectorWithMemoryTracking<LatLng>> holeVerts;

        mutable GeoLoop mutableMainLoop;
        mutable GeoPolygon mutablePolygon;
        mutable VectorWithMemoryTracking<GeoLoop> mutableHoles;

    public:
        explicit GeoPolygonContainer(VectorWithMemoryTracking<LatLng> && mainLoop,
                                     VectorWithMemoryTracking<VectorWithMemoryTracking<LatLng>> && holes = {})
            : mainLoopVerts(std::move(mainLoop)), holeVerts(std::move(holes)) {}

        const GeoPolygon * unwrap() const
        {
            mutableMainLoop = {static_cast<int>(mainLoopVerts.size()),
                               const_cast<LatLng*>(mainLoopVerts.data())};

            mutableHoles.clear();
            mutableHoles.reserve(holeVerts.size());
            for (const auto & hole : holeVerts)
                mutableHoles.push_back({static_cast<int>(hole.size()),
                                        const_cast<LatLng*>(hole.data())});

            mutablePolygon = {mutableMainLoop,
                              static_cast<int>(mutableHoles.size()),
                              mutableHoles.data()};
            return &mutablePolygon;
        }
    };
};

REGISTER_FUNCTION(h3PolygonToCellsWithContainment)
{
    factory.registerFunction<Functionh3PolygonToCellsWithContainment>(FunctionDocumentation{
        .description =
            "Returns the hexagons (at specified resolution) covering the provided geometry, "
            "using H3's experimental algorithm with selectable containment mode. "
            "Flags: 0=CONTAINMENT_CENTER, 1=CONTAINMENT_FULL, 2=CONTAINMENT_OVERLAPPING, "
            "3=CONTAINMENT_OVERLAPPING_BBOX. The flags argument is passed to the H3 API as UInt32; "
            "use integer literals (0..3) or toUInt32. Other native integer types are converted with an accurate cast. See H3 docs.",
        .syntax = "h3PolygonToCellsWithContainment(geometry, resolution, flags)",
        .introduced_in = {26, 6},
        .category = FunctionDocumentation::Category::Geo});
}

}

#endif
