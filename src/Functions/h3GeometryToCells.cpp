#include <Functions/h3GeometryToCells.h>

#if USE_H3

#include <Columns/ColumnsNumber.h>
#include <Common/VectorWithMemoryTracking.h>
#include <IO/WriteHelpers.h>

#include <h3api.h>
/// H3's cell iterator: private to the library, and not declared for C++.
extern "C"
{
#include <polyfill.h>
}

static constexpr size_t MAX_ARRAY_SIZE = 1 << 30;

namespace DB
{
namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int TOO_LARGE_ARRAY_SIZE;
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

/// A vertex off the sphere makes H3's bounding box, and the area it derives from it, meaningless: the size
/// estimate then counts every cell of a polygon covering much of the globe, at the requested resolution.
/// NaN and infinity are already rejected when the geometry is converted; this is the range they leave open.
void validateCoordinates(const SphericalPoint & degree_point, std::string_view function_name)
{
    const Float64 lon = get<0>(degree_point);
    const Float64 lat = get<1>(degree_point);

    if (!std::isfinite(lon) || !std::isfinite(lat) || std::abs(lon) > 180.0 || std::abs(lat) > 90.0)
        throw Exception(
            ErrorCodes::ARGUMENT_OUT_OF_BOUND,
            "The vertex ({}, {}) of the geometry passed to function {} is out of bounds "
            "(longitude must be -180..180 and latitude -90..90 degrees)",
            toString(lon),
            toString(lat),
            function_name);
}

/// The cells of one polygon, one at a time. H3's own polygon-to-cells entry points are this loop wrapped in
/// a single call, which leaves no place to check for a timeout or `KILL QUERY`.
class CellIterator
{
public:
    CellIterator(const GeoPolygon * polygon, int resolution, UInt32 flags)
        : iter(iterInitPolygon(polygon, resolution, flags)) {}

    ~CellIterator() { iterDestroyPolygon(&iter); }

    CellIterator(const CellIterator &) = delete;
    CellIterator & operator=(const CellIterator &) = delete;

    /// `H3_NULL` once the polygon is exhausted, and on an error.
    H3Index cell() const { return iter.cell; }
    void step() { iterStepPolygon(&iter); }
    H3Error error() const { return iter.error; }

private:
    IterCellsPolygon iter;
};

LatLng toH3LatLng(const SphericalPointInRadians & point)
{
    LatLng result;
    result.lat = point.get<1>();
    result.lng = point.get<0>();
    return result;
}

class GeoPolygonContainer
{
private:
    VectorWithMemoryTracking<LatLng> mainLoopVerts;
    VectorWithMemoryTracking<VectorWithMemoryTracking<LatLng>> holeVerts;

    mutable GeoLoop mutableMainLoop{};
    mutable GeoPolygon mutablePolygon{};
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

}

void appendH3Cells(
    const SphericalMultiPolygon & multi_polygon,
    UInt8 resolution,
    UInt32 flags,
    std::string_view function_name,
    CancellationBudget & budget,
    ColumnUInt64 & dst_data)
{
    const size_t row_start_offset = dst_data.size();
    for (const auto & polygon : multi_polygon)
    {
        VectorWithMemoryTracking<LatLng> exterior;
        exterior.reserve(polygon.outer().size());
        for (const auto & point : polygon.outer())
        {
            validateCoordinates(point, function_name);
            exterior.push_back(toH3LatLng(toRadianPoint(point)));
        }

        size_t vertex_count = exterior.size();

        VectorWithMemoryTracking<VectorWithMemoryTracking<LatLng>> holes;
        holes.reserve(polygon.inners().size());
        for (const auto & inner : polygon.inners())
        {
            VectorWithMemoryTracking<LatLng> hole;
            hole.reserve(inner.size());
            for (const auto & point : inner)
            {
                validateCoordinates(point, function_name);
                hole.push_back(toH3LatLng(toRadianPoint(point)));
            }
            vertex_count += hole.size();
            holes.emplace_back(std::move(hole));
        }

        GeoPolygonContainer polygon_wrapper(std::move(exterior), std::move(holes));

        /// Cheap bbox-based estimate: rejects a polygon that cannot fit before enumerating it. Rough in
        /// both directions, hence the exact check per cell below.
        int64_t estimated_size = 0;
        H3Error size_err = maxPolygonToCellsSizeExperimental(
            polygon_wrapper.unwrap(), resolution, flags, &estimated_size);
        if (size_err != E_SUCCESS)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Failed to estimate H3 polygon to cells size in function {}: {}",
                function_name, describeH3Error(size_err));

        const size_t row_size_so_far = dst_data.size() - row_start_offset;
        const size_t estimate = static_cast<size_t>(estimated_size);
        if (estimate > MAX_ARRAY_SIZE || row_size_so_far + estimate > MAX_ARRAY_SIZE)
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                "The result of function {} (array of {} elements) will be too large with resolution = {}",
                function_name, row_size_so_far + estimate, toString(resolution));

        /// A step tests the candidate cell against every vertex, so charge by vertex count.
        const size_t units_per_cell = 1 + vertex_count;

        CellIterator cells(polygon_wrapper.unwrap(), resolution, flags);

        for (; cells.cell(); cells.step())
        {
            budget.chargeUnits(units_per_cell);

            if (dst_data.size() - row_start_offset >= MAX_ARRAY_SIZE)
                throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                    "The result of function {} (array of {} elements) will be too large with resolution = {}",
                    function_name, dst_data.size() - row_start_offset + 1, toString(resolution));

            /// `PODArray::push_back` grows geometrically; `IColumn::reserve` would size it exactly.
            dst_data.getData().push_back(cells.cell());
        }

        if (cells.error() != E_SUCCESS)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Failed to compute H3 polygon to cells in function {}: {}",
                function_name, describeH3Error(cells.error()));
    }
}

}

#endif
