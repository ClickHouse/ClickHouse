#include <Functions/h3GeometryToCells.h>

#if USE_H3

#include <Columns/ColumnsNumber.h>
#include <Common/VectorWithMemoryTracking.h>
#include <IO/WriteHelpers.h>

#include <algorithm>
#include <array>

#include <constants.h>
#include <h3api.h>
/// H3's polygon internals: private to the library, and not declared for C++. The candidate walk below
/// is H3's own, so it needs the same primitives.
extern "C"
{
#include <bbox.h>
#include <coordijk.h>
#include <h3Index.h>
#include <iterators.h>
#include <polyfill.h>
#include <polygon.h>
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

/// Number of cells the size estimate aims to fit in the polygon's bounding box, `MAX_SIZE_CELL_THRESHOLD`
/// in H3's `polyfill.c`.
constexpr double MAX_SIZE_CELL_THRESHOLD = 10;
/// The coordinate range `latLngToCell` accepts, `VALID_RANGE_BBOX` in H3's `polyfill.c`.
constexpr BBox VALID_RANGE_BBOX = {M_PI_2, -M_PI_2, M_PI, -M_PI};
/// Closer to the poles or at coarser resolutions, the bounding boxes of cells reach too far for `findSubtreeRoots`.
constexpr int MIN_ROOT_RESOLUTION = 2;
constexpr double MAX_ROOT_LATITUDE = 80 * M_PI / 180;

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

void checkH3Error(H3Error error, std::string_view function_name)
{
    if (error != E_SUCCESS)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Failed to compute H3 polygon to cells in function {}: {}",
            function_name, describeH3Error(error));
}

/// The next cell in the sequence of all cells to check, `nextCell` in H3's `polyfill.c`.
H3Index nextCellInSequence(H3Index cell)
{
    int res = H3_GET_RESOLUTION(cell);
    while (true)
    {
        /// A base cell has no parent, so continue with the next one.
        if (res == 0)
            return baseCellNumToCell(H3_GET_BASE_CELL(cell) + 1);

        H3Index parent = cell;
        H3_SET_RESOLUTION(parent, res - 1);
        H3_SET_INDEX_DIGIT(parent, res, H3_DIGIT_MASK);

        /// Not the last sibling: continue with the next one, skipping the missing child of a pentagon.
        const int digit = static_cast<int>(H3_GET_INDEX_DIGIT(cell, res));
        if (digit < INVALID_DIGIT - 1)
        {
            H3_SET_INDEX_DIGIT(cell, res, digit + ((isPentagon(parent) && digit == CENTER_DIGIT) ? 2 : 1));
            return cell;
        }

        --res;
        cell = parent;
    }
}

/// The candidate walk of `iterStepPolygonCompact` in H3's `polyfill.c`, over the same H3 primitives, with a
/// cancellation checkpoint per candidate. H3 wraps the walk in one call, which can reject millions of
/// candidates before it returns a cell, leaving no place to observe `max_execution_time` or `KILL QUERY`.
/// Starting at `root` confines the walk to that cell's subtree. Keep in sync with H3 when the submodule is bumped.
class CompactCellScan
{
public:
    CompactCellScan(
        const GeoPolygon * polygon_,
        const BBox * bboxes_,
        int res_,
        UInt32 flags_,
        std::string_view function_name_,
        CancellationBudget & budget_,
        size_t units_per_candidate_,
        H3Index root_ = H3_NULL)
        : polygon(polygon_)
        , bboxes(bboxes_)
        , res(res_)
        , mode(static_cast<ContainmentMode>(FLAG_GET_CONTAINMENT_MODE(flags_)))
        , function_name(function_name_)
        , budget(budget_)
        , units_per_candidate(units_per_candidate_)
        , root_res(root_ == H3_NULL ? -1 : H3_GET_RESOLUTION(root_))
        , cell(root_ == H3_NULL ? baseCellNumToCell(0) : root_)
    {
    }

    /// The next cell covering the polygon, coarser than the target resolution when all of its children are
    /// covered, or `H3_NULL` once the polygon is exhausted.
    H3Index next()
    {
        if (cell == H3_NULL)
            return H3_NULL;

        /// The first candidate is the cell the walk starts at; after that, the one after the last output.
        if (started)
            cell = nextCandidate(cell);
        else
            started = true;

        /// Nothing covers a polygon without vertices.
        if (polygon->geoloop.numVerts == 0)
            cell = H3_NULL;

        while (cell)
        {
            budget.chargeUnits(units_per_candidate);

            const int cell_res = H3_GET_RESOLUTION(cell);

            /// Target resolution: a fine-grained check.
            if (cell_res == res && matchesContainmentMode(cell, cell_res))
                return cell;

            /// Coarser cell: check the bounding box of all of its children.
            if (cell_res < res)
            {
                BBox bbox;
                checkH3Error(cellToBBox(cell, &bbox, true), function_name);

                if (bboxOverlapsBBox(&bboxes[0], &bbox))
                {
                    /// Quick check for possible containment, then the expensive one on the polygon.
                    if (bboxContainsBBox(&bboxes[0], &bbox))
                    {
                        CellBoundary bbox_boundary = bboxToCellBoundary(&bbox);
                        if (cellBoundaryInsidePolygon(polygon, bboxes, &bbox_boundary, &bbox))
                            return cell;
                    }

                    /// An intersecting bounding box means every child has to be tested.
                    H3Index child = H3_NULL;
                    checkH3Error(cellToCenterChild(cell, cell_res + 1, &child), function_name);
                    cell = child;
                    continue;
                }
            }

            cell = nextCandidate(cell);
        }

        return H3_NULL;
    }

private:
    /// Leaving the subtree of `root` yields a cell at its resolution or coarser.
    H3Index nextCandidate(H3Index candidate) const
    {
        const H3Index next_cell = nextCellInSequence(candidate);
        return H3_GET_RESOLUTION(next_cell) <= root_res ? H3_NULL : next_cell;
    }

    bool matchesContainmentMode(H3Index candidate, int candidate_res)
    {
        if (mode == CONTAINMENT_CENTER || mode == CONTAINMENT_OVERLAPPING || mode == CONTAINMENT_OVERLAPPING_BBOX)
        {
            LatLng center;
            checkH3Error(cellToLatLng(candidate, &center), function_name);
            if (pointInsidePolygon(polygon, bboxes, &center))
                return true;
        }

        if (mode == CONTAINMENT_OVERLAPPING || mode == CONTAINMENT_OVERLAPPING_BBOX)
        {
            /// The polygon may be wholly contained by the cell, which its first vertex tells us. That
            /// vertex exists because the caller rejects a polygon without vertices.
            const LatLng first_vertex = polygon->geoloop.verts[0];
            /// Out-of-range coordinates yield false positives with `latLngToCell`.
            if (bboxContains(&VALID_RANGE_BBOX, &first_vertex))
            {
                H3Index polygon_cell = H3_NULL;
                checkH3Error(latLngToCell(&first_vertex, candidate_res, &polygon_cell), function_name);
                if (polygon_cell == candidate)
                    return true;
            }
        }

        if (mode == CONTAINMENT_FULL || mode == CONTAINMENT_OVERLAPPING || mode == CONTAINMENT_OVERLAPPING_BBOX)
        {
            CellBoundary boundary;
            checkH3Error(cellToBoundary(candidate, &boundary), function_name);
            BBox bbox;
            checkH3Error(cellToBBox(candidate, &bbox, false), function_name);

            if ((mode == CONTAINMENT_FULL || mode == CONTAINMENT_OVERLAPPING_BBOX)
                && cellBoundaryInsidePolygon(polygon, bboxes, &boundary, &bbox))
                return true;

            /// Center point inclusion was checked above, so overlap only needs line intersection.
            if ((mode == CONTAINMENT_OVERLAPPING || mode == CONTAINMENT_OVERLAPPING_BBOX)
                && cellBoundaryCrossesPolygon(polygon, bboxes, &boundary, &bbox))
                return true;
        }

        if (mode == CONTAINMENT_OVERLAPPING_BBOX)
        {
            /// A bounding box containing all the cell's children, so that this works for the size estimate.
            BBox bbox;
            checkH3Error(cellToBBox(candidate, &bbox, true), function_name);

            if (bboxOverlapsBBox(&bboxes[0], &bbox))
            {
                CellBoundary bbox_boundary = bboxToCellBoundary(&bbox);
                if (bboxContainsBBox(&bbox, &bboxes[0])
                    || pointInsidePolygon(polygon, bboxes, &bbox_boundary.verts[0])
                    || cellBoundaryCrossesPolygon(polygon, bboxes, &bbox_boundary, &bbox))
                    return true;
            }
        }

        return false;
    }

    const GeoPolygon * polygon;
    const BBox * bboxes;
    const int res;
    const ContainmentMode mode;
    const std::string_view function_name;
    CancellationBudget & budget;
    const size_t units_per_candidate;
    /// The walk covers the subtree of its root, or every cell when it has none.
    const int root_res;

    H3Index cell;
    bool started = false;
};

/// `maxPolygonToCellsSizeExperimental` in H3's `polyfill.c`, over the walk above so that it is checkpointed
/// too: a rough upper bound from the polygon's bounding box, counted at a resolution coarse enough for the
/// box to hold about `MAX_SIZE_CELL_THRESHOLD` cells.
Int64 estimateCellCount(
    const GeoPolygon * polygon,
    const BBox * bboxes,
    int res,
    std::string_view function_name,
    CancellationBudget & budget,
    size_t units_per_candidate)
{
    if (polygon->geoloop.numVerts == 0)
        return 0;

    /// A (very) rough area of the polygon's bounding box.
    const BBox & polygon_bbox = bboxes[0];
    const double polygon_bbox_area_km2 = bboxHeightRads(&polygon_bbox) * bboxWidthRads(&polygon_bbox)
        / cos(fmin(fabs(polygon_bbox.north), fabs(polygon_bbox.south))) * EARTH_RADIUS_KM * EARTH_RADIUS_KM;

    /// All this needs is a general order of magnitude of the number of cells that would fit.
    int estimate_res = res;
    while (estimate_res > 0)
    {
        double average_cell_area_km2 = 0;
        checkH3Error(getHexagonAreaAvgKm2(estimate_res - 1, &average_cell_area_km2), function_name);
        if (polygon_bbox_area_km2 / average_cell_area_km2 <= MAX_SIZE_CELL_THRESHOLD)
            break;
        --estimate_res;
    }

    /// Ignore the requested containment mode and use the faster overlapping-bbox one.
    CompactCellScan scan(
        polygon, bboxes, estimate_res, CONTAINMENT_OVERLAPPING_BBOX, function_name, budget, units_per_candidate);

    Int64 estimate = 0;
    while (H3Index cell = scan.next())
    {
        Int64 children_size = 0;
        checkH3Error(cellToChildrenSize(cell, res, &children_size), function_name);
        estimate += children_size;
    }

    return estimate;
}

/// The cells whose subtrees hold everything the walk descends into, in ascending order, at a resolution whose cells
/// are at least twice as large as the polygon; none when the walk has to start from the base cells. The walk only
/// descends into a cell whose bounding box of children overlaps the polygon's. Those cells are connected and include
/// the cell of the first vertex, so none is missed unless one is three steps away from it.
VectorWithMemoryTracking<H3Index> findSubtreeRoots(
    const GeoPolygon * polygon, const BBox & bbox, int res, std::string_view function_name)
{
    if (polygon->geoloop.numVerts == 0 || bboxIsTransmeridian(&bbox) || bbox.north > MAX_ROOT_LATITUDE
        || bbox.south < -MAX_ROOT_LATITUDE)
        return {};

    /// Bounds the distance between any two points of the bounding box.
    const double min_abs_latitude = bbox.south > 0 ? bbox.south : (bbox.north < 0 ? -bbox.north : 0);
    const double diameter = bboxHeightRads(&bbox) + bboxWidthRads(&bbox) * cos(min_abs_latitude);

    int root_res = res - 1;
    for (; root_res >= MIN_ROOT_RESOLUTION; --root_res)
    {
        double edge_km = 0;
        checkH3Error(getHexagonEdgeLengthAvgKm(root_res, &edge_km), function_name);
        if (diameter <= 0.5 * edge_km / EARTH_RADIUS_KM)
            break;
    }
    if (root_res < MIN_ROOT_RESOLUTION)
        return {};

    H3Index origin = H3_NULL;
    checkH3Error(latLngToCell(&polygon->geoloop.verts[0], root_res, &origin), function_name);

    constexpr int disk_radius = 3;
    constexpr size_t disk_size = 1 + 3 * disk_radius * (disk_radius + 1);
    std::array<H3Index, disk_size> disk{};
    std::array<int, disk_size> distances{};
    checkH3Error(gridDiskDistances(origin, disk_radius, disk.data(), distances.data()), function_name);

    VectorWithMemoryTracking<H3Index> roots;
    for (size_t i = 0; i < disk.size(); ++i)
    {
        if (disk[i] == H3_NULL)
            continue;

        BBox children_bbox;
        checkH3Error(cellToBBox(disk[i], &children_bbox, true), function_name);
        if (!bboxOverlapsBBox(&bbox, &children_bbox))
            continue;

        if (distances[i] == disk_radius)
            return {};
        roots.push_back(disk[i]);
    }

    std::sort(roots.begin(), roots.end());
    return roots;
}

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
        const GeoPolygon * geo_polygon = polygon_wrapper.unwrap();

        checkH3Error(validatePolygonFlags(flags), function_name);

        /// Bounding boxes of the polygon and of each of its holes, which every candidate check needs.
        VectorWithMemoryTracking<BBox> bboxes(geo_polygon->numHoles + 1);
        bboxesFromGeoPolygon(geo_polygon, bboxes.data());

        /// A candidate at the target resolution is tested against every vertex of the polygon.
        const size_t units_per_candidate = 1 + vertex_count;

        const VectorWithMemoryTracking<H3Index> roots = findSubtreeRoots(geo_polygon, bboxes[0], resolution, function_name);

        /// Every covering cell descends from a root, so this bounds the result and the estimate below.
        Int64 roots_children = 0;
        for (const H3Index root : roots)
        {
            Int64 children_size = 0;
            checkH3Error(cellToChildrenSize(root, resolution, &children_size), function_name);
            roots_children += children_size;
        }

        /// Cheap bbox-based estimate: rejects a polygon that cannot fit before enumerating it. Rough in
        /// both directions, hence the exact check per cell below.
        const size_t row_size_so_far = dst_data.size() - row_start_offset;
        if (roots.empty() || row_size_so_far + static_cast<size_t>(roots_children) > MAX_ARRAY_SIZE)
        {
            const size_t estimate = static_cast<size_t>(
                estimateCellCount(geo_polygon, bboxes.data(), resolution, function_name, budget, units_per_candidate));
            if (estimate > MAX_ARRAY_SIZE || row_size_so_far + estimate > MAX_ARRAY_SIZE)
                throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                    "The result of function {} (array of {} elements) will be too large with resolution = {}",
                    function_name, row_size_so_far + estimate, toString(resolution));
        }

        /// The cells of the walk from `root`, or from the base cells when it is `H3_NULL`.
        const auto append_walk = [&](H3Index root)
        {
            CompactCellScan scan(
                geo_polygon, bboxes.data(), resolution, flags, function_name, budget, units_per_candidate, root);

            while (H3Index compact_cell = scan.next())
            {
                /// A cell coarser than the requested resolution stands for all of its children.
                IterCellsChildren children = iterInitParent(compact_cell, resolution);
                for (; children.h; iterStepChild(&children))
                {
                    /// The candidates rejected on the way here are charged by the walk; a child of a covered
                    /// cell only costs the index arithmetic that produced it.
                    budget.charge(sizeof(H3Index));

                    if (dst_data.size() - row_start_offset >= MAX_ARRAY_SIZE)
                        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                            "The result of function {} (array of {} elements) will be too large with resolution = {}",
                            function_name, dst_data.size() - row_start_offset + 1, toString(resolution));

                    /// `PODArray::push_back` grows geometrically; `IColumn::reserve` would size it exactly.
                    dst_data.getData().push_back(children.h);
                }
            }
        };

        /// Subtrees of distinct cells of one resolution are disjoint, ascending ranges of the walk.
        if (roots.empty())
            append_walk(H3_NULL);
        for (const H3Index root : roots)
            append_walk(root);
    }
}

}

#endif
