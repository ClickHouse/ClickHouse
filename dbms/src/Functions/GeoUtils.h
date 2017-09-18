#pragma once
#include <Core/Types.h>
#include <Core/Defines.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <Core/TypeListNumber.h>
#include <Common/typeid_cast.h>
#include <ext/range.h>

/// Warning in boost::geometry during template strategy substitution.
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

#include <boost/geometry.hpp>

#if !__clang__
#pragma GCC diagnostic pop
#endif

#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>
#include <boost/geometry/geometries/multi_polygon.hpp>
#include <boost/geometry/geometries/segment.hpp>
#include <boost/geometry/algorithms/comparable_distance.hpp>
#include <boost/geometry/strategies/cartesian/distance_pythagoras.hpp>

#include <array>
#include <vector>
#include <iterator>
#include <cmath>
#include <algorithm>
#include <IO/WriteBufferFromString.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}


namespace GeoUtils
{


template <typename CoordinateType = Float32, UInt16 gridHeight = 8, UInt16 gridWidth = 8>
class PointInPolygonWithGrid
{
public:
    using Point = boost::geometry::model::d2::point_xy<CoordinateType>;
    /// Counter-Clockwise ordering.
    using Polygon = boost::geometry::model::polygon<Point, false>;
    using MultiPolygon = boost::geometry::model::multi_polygon<Polygon>;
    using Box = boost::geometry::model::box<Point>;
    using Segment = boost::geometry::model::segment<Point>;

    explicit PointInPolygonWithGrid(const Polygon & polygon) : polygon(polygon) {}

    inline void init();

    bool hasEmptyBound() const { return has_empty_bound; }

    inline bool ALWAYS_INLINE contains(CoordinateType x, CoordinateType y);

private:
    enum class CellType
    {
        inner,
        outer,
        singleLine,
        pairOfLinesSinglePolygon,
        pairOfLinesDifferentPolygons,
        complexPolygon
    };

    struct HalfPlane
    {
        /// Line
        CoordinateType a;
        CoordinateType b;
        CoordinateType c;

        void fill(const Point & from, const Point & to)
        {
            a = -(to.y() - from.y());
            b = to.x() - from.x();
            c = -from.x() * a - from.y() * b;
        }

        /// Inner part of the HalfPlane is the left side of initialized vector.
        bool ALWAYS_INLINE contains(CoordinateType x, CoordinateType y) const { return a * x + b * y + c >= 0; }
    };

    struct Cell
    {
        static const int maxStoredHalfPlanes = 2;

        HalfPlane half_planes[maxStoredHalfPlanes];
        size_t index_of_inner_polygon;
        CellType type;
    };

    Polygon polygon;
    std::array<Cell, gridHeight * gridWidth> cells;
    std::vector<MultiPolygon> polygons;

    CoordinateType cell_width;
    CoordinateType cell_height;

    CoordinateType x_shift;
    CoordinateType y_shift;
    CoordinateType x_scale;
    CoordinateType y_scale;

    bool has_empty_bound = false;
    bool was_grid_built = false;

    void buildGrid();

    void calcGridAttributes(Box & box);

    template <typename T>
    T ALWAYS_INLINE getCellIndex(T row, T col) const { return row * gridWidth + col; }

    /// Complex case. Will check intersection directly.
    inline void addComplexPolygonCell(size_t index, const Box & box);

    /// Empty intersection or intersection == box.
    inline void addCell(size_t index, const Box & empty_box);

    /// Intersection is a single polygon.
    inline void addCell(size_t index, const Box & box, const Polygon & intersection);

    /// Intersection is a pair of polygons.
    inline void addCell(size_t index, const Box & box, const Polygon & first, const Polygon & second);

    /// Returns a list of half-planes were formed from intersection edges without box edges.
    inline std::vector<HalfPlane> findHalfPlanes(const Box & box, const Polygon & intersection);

    using Distance = typename boost::geometry::default_comparable_distance_result<Point, Segment>::type;

    /// min(distance(point, edge) : edge in polygon)
    inline Distance distance(const Point & point, const Polygon & polygon);
};

template <typename CoordinateType, UInt16 gridHeight, UInt16 gridWidth>
void PointInPolygonWithGrid<CoordinateType, gridHeight, gridWidth>::init()
{
    if (!was_grid_built)
        buildGrid();

    was_grid_built = true;
}

template <typename CoordinateType, UInt16 gridHeight, UInt16 gridWidth>
void PointInPolygonWithGrid<CoordinateType, gridHeight, gridWidth>::calcGridAttributes(
        PointInPolygonWithGrid<CoordinateType, gridHeight, gridWidth>::Box & box)
{
    boost::geometry::envelope(polygon, box);

    const Point & min_corner = box.min_corner();
    const Point & max_corner = box.max_corner();

    cell_width = (max_corner.x() - min_corner.x()) / gridWidth;
    cell_height = (max_corner.y() - min_corner.y()) / gridHeight;

    if (cell_width == 0 || cell_height == 0)
    {
        has_empty_bound = true;
        return;
    }

    x_scale = 1 / cell_width;
    y_scale = 1 / cell_height;
    x_shift = -min_corner.x();
    y_shift = -min_corner.y();
}

template <typename CoordinateType, UInt16 gridHeight, UInt16 gridWidth>
void PointInPolygonWithGrid<CoordinateType, gridHeight, gridWidth>::buildGrid()
{
    Box box;
    calcGridAttributes(box);

    const Point & min_corner = box.min_corner();
    const Point & max_corner = box.max_corner();

    for (size_t row = 0; row < gridHeight; ++row)
    {
        CoordinateType y_min = min_corner.y() + row * cell_height;
        CoordinateType y_max = min_corner.y() + (row + 1) * cell_height;

        for (size_t col = 0; col < gridWidth; ++col)
        {
            CoordinateType x_min = min_corner.x() + col * cell_width;
            CoordinateType x_max = min_corner.x() + (col + 1) * cell_width;
            Box cell_box(Point(x_min, y_min), Point(x_max, y_max));

            Polygon cell_bound;
            boost::geometry::convert(cell_box, cell_bound);

            MultiPolygon intersection;
            boost::geometry::intersection(polygon, cell_bound, intersection);

            size_t cellIndex = getCellIndex(row, col);

            if (intersection.empty())
                addCell(cellIndex, cell_box);
            else if (intersection.size() == 1)
                addCell(cellIndex, cell_box, intersection.front());
            else if (intersection.size() == 2)
                addCell(cellIndex, cell_box, intersection.front(), intersection.back());
            else
                addComplexPolygonCell(cellIndex, cell_box);
        }
    }
}

template <typename CoordinateType, UInt16 gridHeight, UInt16 gridWidth>
bool PointInPolygonWithGrid<CoordinateType, gridHeight, gridWidth>::contains(CoordinateType x, CoordinateType y)
{
    CoordinateType float_row = (y + y_shift) * y_scale;
    CoordinateType float_col = (x + x_shift) * x_scale;

    if (float_row < 0 || float_row > gridHeight)
        return false;
    if (float_col < 0 || float_col > gridWidth)
        return false;

    int row = std::min<int>(float_row, gridHeight - 1);
    int col = std::min<int>(float_col, gridWidth - 1);

    int index = getCellIndex(row, col);
    const auto & cell = cells[index];

    switch (cell.type)
    {
        case CellType::inner:
            return true;
        case CellType::outer:
            return false;
        case CellType::singleLine:
            return cell.half_planes[0].contains(x, y);
        case CellType::pairOfLinesSinglePolygon:
            return cell.half_planes[0].contains(x, y) && cell.half_planes[1].contains(x, y);
        case CellType::pairOfLinesDifferentPolygons:
            return cell.half_planes[0].contains(x, y) || cell.half_planes[1].contains(x, y);
        case CellType::complexPolygon:
            return boost::geometry::within(Point(x, y), polygons[cell.index_of_inner_polygon]);
        default:
            return false;

    }
}

template <typename CoordinateType, UInt16 gridHeight, UInt16 gridWidth>
typename PointInPolygonWithGrid<CoordinateType, gridHeight, gridWidth>::Distance
PointInPolygonWithGrid<CoordinateType, gridHeight, gridWidth>::distance(
        const PointInPolygonWithGrid<CoordinateType, gridHeight, gridWidth>::Point & point,
        const PointInPolygonWithGrid<CoordinateType, gridHeight, gridWidth>::Polygon & polygon)
{
    const auto & outer = polygon.outer();
    Distance distance = 0;
    for (auto i : ext::range(0, outer.size() - 1))
    {
        Segment segment(outer[i], outer[i + 1]);
        Distance current = boost::geometry::comparable_distance(point, segment);
        distance = i ? std::min(current, distance) : current;
    }
    return distance;
}

template <typename CoordinateType, UInt16 gridHeight, UInt16 gridWidth>
std::vector<typename PointInPolygonWithGrid<CoordinateType, gridHeight, gridWidth>::HalfPlane>
PointInPolygonWithGrid<CoordinateType, gridHeight, gridWidth>::findHalfPlanes(
        const PointInPolygonWithGrid<CoordinateType, gridHeight, gridWidth>::Box & box,
        const PointInPolygonWithGrid<CoordinateType, gridHeight, gridWidth>::Polygon & intersection)
{
    std::vector<HalfPlane> half_planes;
    Polygon bound;
    boost::geometry::convert(box, bound);
    const auto & outer = intersection.outer();

    for (auto i : ext::range(0, outer.size() - 1))
    {
        /// Want to detect is intersection edge was formed from box edge or from polygon edge.
        /// If center of the edge closer to box, than don't form the half-plane.
        Segment segment(outer[i], outer[i + 1]);
        Point center((segment.first.x() + segment.second.x()) / 2, (segment.first.y() + segment.second.y()) / 2);
        if (distance(center, polygon) < distance(center, bound))
        {
            half_planes.push_back({});
            half_planes.back().fill(segment.first, segment.second);
        }
    }

    return half_planes;
}

template <typename CoordinateType, UInt16 gridHeight, UInt16 gridWidth>
void PointInPolygonWithGrid<CoordinateType, gridHeight, gridWidth>::addComplexPolygonCell(
        size_t index, const PointInPolygonWithGrid<CoordinateType, gridHeight, gridWidth>::Box & box)
{
    cells[index].type = CellType::complexPolygon;
    cells[index].index_of_inner_polygon = polygons.size();

    /// Expand box in (1 + eps_factor) times to eliminate errors for points on box bound.
    static constexpr float eps_factor = 0.01;
    float x_eps = eps_factor * (box.max_corner().x() - box.min_corner().x());
    float y_eps = eps_factor * (box.max_corner().y() - box.min_corner().y());

    Point min_corner(box.min_corner().x() - x_eps, box.min_corner().y() - y_eps);
    Point max_corner(box.max_corner().x() + x_eps, box.max_corner().y() + y_eps);
    Box box_with_eps_bound(min_corner, max_corner);

    Polygon bound;
    boost::geometry::convert(box_with_eps_bound, bound);

    MultiPolygon intersection;
    boost::geometry::intersection(polygon, bound, intersection);

    polygons.push_back(intersection);
}

template <typename CoordinateType, UInt16 gridHeight, UInt16 gridWidth>
void PointInPolygonWithGrid<CoordinateType, gridHeight, gridWidth>::addCell(
        size_t index, const PointInPolygonWithGrid<CoordinateType, gridHeight, gridWidth>::Box & empty_box)
{
    const auto & min_corner = empty_box.min_corner();
    const auto & max_corner = empty_box.max_corner();

    Point center((min_corner.x() + max_corner.x()) / 2, (min_corner.y() + max_corner.y()) / 2);

    if (boost::geometry::within(center, polygon))
        cells[index].type = CellType::inner;
    else
        cells[index].type = CellType::outer;

}

template <typename CoordinateType, UInt16 gridHeight, UInt16 gridWidth>
void PointInPolygonWithGrid<CoordinateType, gridHeight, gridWidth>::addCell(
        size_t index,
        const PointInPolygonWithGrid<CoordinateType, gridHeight, gridWidth>::Box & box,
        const PointInPolygonWithGrid<CoordinateType, gridHeight, gridWidth>::Polygon & intersection)
{
    if (!intersection.inners().empty())
        addComplexPolygonCell(index, box);

    auto half_planes = findHalfPlanes(box, intersection);

    if (half_planes.empty())
        addCell(index, box);
    else if (half_planes.size() == 1)
    {
        cells[index].type = CellType::singleLine;
        cells[index].half_planes[0] = half_planes[0];
    }
    else if (half_planes.size() == 2)
    {
        cells[index].type = CellType::pairOfLinesSinglePolygon;
        cells[index].half_planes[0] = half_planes[0];
        cells[index].half_planes[1] = half_planes[1];
    }
    else
        addComplexPolygonCell(index, box);
}

template <typename CoordinateType, UInt16 gridHeight, UInt16 gridWidth>
void PointInPolygonWithGrid<CoordinateType, gridHeight, gridWidth>::addCell(
        size_t index,
        const PointInPolygonWithGrid<CoordinateType, gridHeight, gridWidth>::Box & box,
        const PointInPolygonWithGrid<CoordinateType, gridHeight, gridWidth>::Polygon & first,
        const PointInPolygonWithGrid<CoordinateType, gridHeight, gridWidth>::Polygon & second)
{
    if (!first.inners().empty() || !second.inners().empty())
        addComplexPolygonCell(index, box);

    auto first_half_planes = findHalfPlanes(box, first);
    auto second_half_planes = findHalfPlanes(box, second);

    if (first_half_planes.empty())
        addCell(index, box, first);
    else if (second_half_planes.empty())
        addCell(index, box, second);
    else if (first_half_planes.size() == 1 && second_half_planes.size() == 1)
    {
        cells[index].type = CellType::pairOfLinesDifferentPolygons;
        cells[index].half_planes[0] = first_half_planes[0];
        cells[index].half_planes[1] = second_half_planes[0];
    }
    else
        addComplexPolygonCell(index, box);
}


/// Algorithms.

template <typename PointInPolygonImpl, typename T, typename U>
ColumnPtr pointInPolygon(const ColumnVector<T> & x, const ColumnVector<U> & y, PointInPolygonImpl && impl)
{
    auto size = x.size();

    impl.init();

    if (impl.hasEmptyBound())
    {
        return std::make_shared<ColumnVector<UInt8>>(size, 0);
    }

    auto result = std::make_shared<ColumnVector<UInt8>>(size);
    auto & data = result->getData();

    const auto & x_data = x.getData();
    const auto & y_data = y.getData();

    for (auto i : ext::range(0, size))
    {
        data[i] = static_cast<UInt8>(impl.contains(x_data[i], y_data[i]));
    }

    return result;
}

template <typename PointInPolygonImpl, typename ... Types>
struct CallPointInPolygon;

template <typename PointInPolygonImpl, typename Type, typename ... Types>
struct CallPointInPolygon<PointInPolygonImpl, Type, Types ...>
{
    using Polygon = typename PointInPolygonImpl::Polygon;

    template <typename T>
    static ColumnPtr call(const ColumnVector<T> & x, const IColumn & y, PointInPolygonImpl & impl)
    {
        if (auto column = typeid_cast<const ColumnVector<Type> *>(&y))
            return pointInPolygon(x, *column, impl);
        return CallPointInPolygon<PointInPolygonImpl, Types ...>::template call<T>(x, y, impl);
    }

    static ColumnPtr call(const IColumn & x, const IColumn & y, PointInPolygonImpl & impl)
    {
        using List = typename PrependToTypeList<PointInPolygonImpl, TypeListNumbers>::Type;
        using Impl = typename ApplyTypeListForClass<CallPointInPolygon, List>::Type;
        if (auto column = typeid_cast<const ColumnVector<Type> *>(&x))
            return Impl::template call<Type>(*column, y, impl);
        return CallPointInPolygon<PointInPolygonImpl, Types ...>::call(x, y, impl);
    }
};

template <typename PointInPolygonImpl>
struct CallPointInPolygon<PointInPolygonImpl>
{
    template <typename T>
    static ColumnPtr call(const ColumnVector<T> & x, const IColumn & y, PointInPolygonImpl & impl)
    {
        throw Exception(std::string("Unknown numeric column type: ") + typeid(y).name(), ErrorCodes::LOGICAL_ERROR);
    }

    static ColumnPtr call(const IColumn & x, const IColumn & y, PointInPolygonImpl & impl)
    {
        throw Exception(std::string("Unknown numeric column type: ") + typeid(x).name(), ErrorCodes::LOGICAL_ERROR);
    }
};

template <typename PointInPolygonImpl>
ColumnPtr pointInPolygon(const IColumn & x, const IColumn & y, PointInPolygonImpl && impl)
{
    using List = typename PrependToTypeList<PointInPolygonImpl, TypeListNumbers>::Type;
    using Impl = typename ApplyTypeListForClass<CallPointInPolygon, List>::Type;
    return Impl::call(x, y, impl);
}

/// Total angle (signed) between neighbor vectors in linestring. Zero if linestring.size() < 2.
template <typename Linestring>
float calcLinestringRotation(const Linestring & points)
{
    using Point = boost::geometry::model::d2::point_xy<float>;
    float rotation = 0;

    auto sqrLength = [](const Point & point) { return point.x() * point.x() + point.y() * point.y(); };
    auto vecProduct = [](const Point & from, const Point & to) { return from.x() * to.y() - from.y() * to.x(); };
    auto getVector = [](const Point & from, const Point & to) -> Point
    {
        return Point(to.x() - from.x(), to.y() - from.y());
    };

    for (auto it = points.begin(); std::next(it) != points.end(); ++it)
    {
        if (it != points.begin())
        {
            auto prev = std::prev(it);
            auto next = std::next(it);
            Point from = getVector(*prev, *it);
            Point to = getVector(*it, *next);
            float sqr_from_len = sqrLength(from);
            float sqr_to_len = sqrLength(to);
            float sqr_len_product = (sqr_from_len * sqr_to_len);
            if (std::isfinite(sqr_len_product))
            {
                float vec_prod = vecProduct(from, to);
                float sin_ang = vec_prod * std::fabs(vec_prod) / sqr_len_product;
                sin_ang = std::max(-1.f, std::min(1.f, sin_ang));
                rotation += std::asin(sin_ang);
            }
        }
    }

    return rotation;
}

/// Make inner linestring counter-clockwise and outers clockwise oriented.
template <typename Polygon>
void normalizePolygon(Polygon && polygon)
{
    auto & outer = polygon.outer();
    if (calcLinestringRotation(outer) < 0)
        std::reverse(outer.begin(), outer.end());

    auto & inners = polygon.inners();
    for (auto & inner : inners)
        if (calcLinestringRotation(inner) > 0)
            std::reverse(inner.begin(), inner.end());
}


template <typename Polygon>
std::string serialize(Polygon && polygon)
{
    std::string result;

    {
        WriteBufferFromString buffer(result);

        using RingType = typename std::decay<Polygon>::type::ring_type;

        auto serializeFloat = [&buffer](float value) { buffer.write(static_cast<char *>(&value), sizeof(value)); };
        auto serializeSize = [&buffer](size_t size) { buffer.write(static_cast<char *>(&size), sizeof(size)); };

        auto serializeRing = [& buffer, & serializeFloat, & serializeSize](const RingType & ring)
        {
            serializeSize(ring.size());
            for (const auto & point : ring)
            {
                serializeFloat(point.x());
                serializeFloat(point.y());
            }
        };

        serializeRing(polygon.outer());

        const auto & inners = polygon.inners();
        serializeSize(inners.size());
        for (auto & inner : inners)
            serializeRing(inner);
    }

    return result;
}


} /// GeoUtils

} /// DB
