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
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>

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

class FunctionH3PolygonToCellsExperimental : public IFunction
{
public:
    static constexpr auto name = "h3PolygonToCellsExperimental";
    String getName() const override { return name; }
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionH3PolygonToCellsExperimental>(context);
    }

    explicit FunctionH3PolygonToCellsExperimental(ContextPtr context)
        : process_list_element(context ? context->getProcessListElement() : nullptr)
    {
    }

    size_t getNumberOfArguments() const override { return 3; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
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

        auto col_flags_full = arguments[2].column->convertToFullColumnIfConst();

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

            std::vector<H3Index> hindex_vec;

            for (size_t row = 0; row < input_rows_count; ++row)
            {
                UInt8 resolution = data_resolution[row];
                if (resolution > MAX_H3_RES)
                    throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                        "The argument 'resolution' ({}) of function {} is out of bounds (max {})",
                        toString(resolution), getName(), toString(MAX_H3_RES));

                UInt32 flags = static_cast<UInt32>(col_flags_full->getUInt(row));
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

                for (const auto & polygon : multi_polygon)
                {
                    std::vector<LatLng> exterior;
                    exterior.reserve(polygon.outer().size());
                    for (const auto & point : polygon.outer())
                        exterior.push_back(toH3LatLng(toRadianPoint(point)));

                    std::vector<std::vector<LatLng>> holes;
                    holes.reserve(polygon.inners().size());
                    for (const auto & inner : polygon.inners())
                    {
                        std::vector<LatLng> hole;
                        hole.reserve(inner.size());
                        for (const auto & point : inner)
                            hole.push_back(toH3LatLng(toRadianPoint(point)));
                        holes.emplace_back(std::move(hole));
                    }

                    GeoPolygonContainer polygon_wrapper(std::move(exterior), std::move(holes));

                    int64_t polygon_size = 0;
                    // Experimental size also takes flags
                    maxPolygonToCellsSizeExperimental(polygon_wrapper.unwrap(), resolution, flags, &polygon_size);

                    const size_t vec_size = static_cast<size_t>(polygon_size);
                    if (vec_size > MAX_ARRAY_SIZE)
                        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                            "The result of function {} (array of {} elements) will be too large with resolution = {}",
                            getName(), vec_size, toString(resolution));

                    hindex_vec.assign(vec_size, 0);
                    polygonToCellsExperimental(polygon_wrapper.unwrap(),
                                               resolution,
                                               flags,
                                               static_cast<int64_t>(vec_size),
                                               hindex_vec.data());

                    dst_data.reserve(dst_data.size() + vec_size);
                    for (auto hindex : hindex_vec)
                    {
                        if (hindex != 0)
                        {
                            ++current_offset;
                            dst_data.insert(hindex);
                        }
                    }
                    dst_offsets[row] = current_offset;
                }
            }
        });

        return dst;
    }

private:
    QueryStatusPtr process_list_element;

    class GeoPolygonContainer
    {
    private:
        std::vector<LatLng> mainLoopVerts;
        std::vector<std::vector<LatLng>> holeVerts;

        mutable GeoLoop mutableMainLoop;
        mutable GeoPolygon mutablePolygon;
        mutable std::vector<GeoLoop> mutableHoles;

    public:
        explicit GeoPolygonContainer(std::vector<LatLng> && mainLoop,
                                     std::vector<std::vector<LatLng>> && holes = {})
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

REGISTER_FUNCTION(H3PolygonToCellsExperimental)
{
    factory.registerFunction<FunctionH3PolygonToCellsExperimental>(FunctionDocumentation{
        .description =
            "Returns the hexagons (at specified resolution) covering the provided geometry, "
            "using H3's experimental algorithm with selectable containment mode. "
            "Flags: 0=CONTAINMENT_CENTER, 1=CONTAINMENT_FULL, 2=CONTAINMENT_OVERLAPPING, "
            "3=CONTAINMENT_OVERLAPPING_BBOX. See H3 docs.",
        .syntax = "h3PolygonToCellsExperimental(geometry, resolution, flags)",
        .introduced_in = {26, 6},
        .category = FunctionDocumentation::Category::Geo});
}

}

#endif
