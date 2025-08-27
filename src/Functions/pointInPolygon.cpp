#include <Functions/FunctionFactory.h>
#include <Functions/PolygonUtils.h>
#include <Functions/FunctionHelpers.h>

#include <boost/geometry/core/tag.hpp>
#include <boost/geometry/core/tags.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ObjectPool.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Core/Settings.h>
#include <base/arithmeticOverflow.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/castColumn.h>

#include <string>
#include <memory>


namespace ProfileEvents
{
    extern const Event PolygonsAddedToPool;
    extern const Event PolygonsInPoolAllocatedBytes;
}

namespace DB
{
namespace Setting
{
    extern const SettingsBool validate_polygons;
}

namespace ErrorCodes
{
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

namespace bg = boost::geometry;

using CoordinateType = Float64;
using Point = bg::model::d2::point_xy<CoordinateType>;
using Polygon = bg::model::polygon<Point, false>;
using MultiPolygon = bg::model::multi_polygon<Polygon>;
using Box = bg::model::box<Point>;

template <typename G>
concept PolygonGeometry = std::is_same_v<typename bg::traits::tag<G>::type, bg::polygon_tag>;

template <typename G>
concept MultiPolygonGeometry = std::is_same_v<typename bg::traits::tag<G>::type, bg::multi_polygon_tag>;

template <class Ring>
inline void sipHashRing(SipHash & hash, const Ring & ring)
{
    static_assert(std::contiguous_iterator<decltype(ring.data())>, "sipHashRing expects a container with contiguous storage (e.g. std::vector).");

    UInt32 size = static_cast<UInt32>(ring.size());
    hash.update(size);
    hash.update(reinterpret_cast<const char *>(ring.data()), size * sizeof(ring[0]));
}

template <PolygonGeometry Polygon>
UInt128 sipHash128(const Polygon & polygon)
{
    SipHash hash;

    sipHashRing(hash, polygon.outer());

    const auto & inners = polygon.inners();
    hash.update(static_cast<UInt32>(inners.size()));
    for (const auto & inner_ring : inners)
        sipHashRing(hash, inner_ring);

    return hash.get128();
}

template <MultiPolygonGeometry MultiPolygon>
UInt128 sipHash128(const MultiPolygon & multi_polygon)
{
    SipHash hash;

    hash.update(static_cast<UInt32>(multi_polygon.size()));

    for (const auto & component : multi_polygon)
    {
        UInt128 component_hash = sipHash128(component);
        hash.update(component_hash);
    }

    return hash.get128();
}

template <typename PointInConstPolygonImpl, typename PointInConstMultiPolygonImpl>
class FunctionPointInPolygon : public IFunction
{
public:
    static inline const char * name = "pointInPolygon";

    explicit FunctionPointInPolygon(bool validate_) : validate(validate_) {}

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionPointInPolygon<PointInConstPolygonImpl, PointInConstMultiPolygonImpl>>(
            context->getSettingsRef()[Setting::validate_polygons]);
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
        {
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Function {} requires at least 2 arguments", getName());
        }

        /** We allow function invocation in one of the following forms:
          *
          * pointInPolygon((x, y), [(x1, y1), (x2, y2), ...])
          * - simple polygon
          * pointInPolygon((x, y), [(x1, y1), (x2, y2), ...], [(x21, y21), (x22, y22), ...], ...)
          * - polygon with a number of holes, each hole as a subsequent argument.
          * pointInPolygon((x, y), [[(x1, y1), (x2, y2), ...], [(x21, y21), (x22, y22), ...], ...])
          * - polygon with a number of holes, all as multidimensional array
          * pointInPolygon((x, y), [[[(x1, y1), (x2, y2), ...], [(x21, y21), (x22, y22), ...], ...]])
          * - multi polygon
          * pointInPolygon((x, y), [[(x1, y1), (x2, y2), ...], [(x21, y21), (x22, y22), ...]], [[(x1, y1), (x2, y2), ...], [(x21, y21), (x22, y22), ...], ...])
          * - multi polygon, each polygon as a subsequent argument.
          */

        auto validate_tuple = [this](size_t i, const DataTypeTuple * tuple)
        {
            if (tuple == nullptr)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "{} must contain a tuple", getMessagePrefix(i));

            const DataTypes & elements = tuple->getElements();

            if (elements.size() != 2)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} must have exactly two elements", getMessagePrefix(i));

            for (auto j : collections::range(0, elements.size()))
            {
                if (!isNativeNumber(elements[j]))
                {
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "{} must contain numeric tuple at position {}",
                                    getMessagePrefix(i), j + 1);
                }
            }
        };


        /// Validate the given first argument point (x, y) tuple.
        validate_tuple(0, checkAndGetDataType<DataTypeTuple>(arguments[0].get()));

        auto getArrayDepthAndInnermostTuple = [this](const IDataType & type, size_t arg_pos) -> std::pair<size_t, const DataTypeTuple *>
        {
            const IDataType * current_type = &type;
            size_t array_depth = 0;

            while (WhichDataType(*current_type).isArray())
            {
                ++array_depth;
                current_type = static_cast<const DataTypeArray *>(current_type)->getNestedType().get();
            }

            if (array_depth == 0 || array_depth > 3)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "{} must contain an array of tuples or an array of arrays of tuples or an array of arrays of arrays of tuples.",
                    getMessagePrefix(arg_pos));

            return {array_depth, checkAndGetDataType<DataTypeTuple>(current_type)};
        };

        auto [depth_first_polygon_argument, tuple_first_polygon_argument] = getArrayDepthAndInnermostTuple(*arguments[1], 1);

        validate_tuple(1, tuple_first_polygon_argument); /// verify its innermost tuple

        if (arguments.size() == 2)
        {
            /// depth 1  -> polygon without holes
            /// depth 2  -> polygon with holes
            /// depth 3  -> multi polygon
            return std::make_shared<DataTypeUInt8>();
        }

        if (depth_first_polygon_argument == 3)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "{}: an array of arrays of arrays of tuples can be used only "
                "when it is the sole polygon argument.",
                getMessagePrefix(1));

        for (size_t i = 2; i < arguments.size(); ++i)
        {
            auto [depth_current_argument, tuple_current_argument] = getArrayDepthAndInnermostTuple(*arguments[i], i);

            validate_tuple(i, tuple_current_argument);

            if (depth_first_polygon_argument == 2) /// Variadic multi polygon: first polygon given as 2-array
            {
                /// Every subsequent polygon argument must also be 2-array.
                if (depth_current_argument != 2)
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "{} must be a array of arrays of tuples because an array of arrays of"
                        " tuples of first polygon indicates that it is part of MultiPolygon.",
                        getMessagePrefix(i));
            }
            else /// Polygon with holes case
            {
                if (depth_current_argument != 1)
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "{} must be a array of tuples.", getMessagePrefix(i));
            }
        }

        return std::make_shared<DataTypeUInt8>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const IColumn * point_col = arguments[0].column.get();
        const auto * const_tuple_col = checkAndGetColumn<ColumnConst>(point_col);
        if (const_tuple_col)
            point_col = &const_tuple_col->getDataColumn();

        const auto * tuple_col = checkAndGetColumn<ColumnTuple>(point_col);
        if (!tuple_col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument for function {} must be constant array of tuples.",
                            getName());

        const auto & tuple_columns = tuple_col->getColumns();

        bool point_is_const = const_tuple_col != nullptr;
        bool poly_is_const = true;

        for (size_t i = 1; i < arguments.size(); ++i)
        {
            const IColumn * poly_col = arguments[i].column.get();
            const auto * const_poly_col = checkAndGetColumn<ColumnConst>(poly_col);
            if (const_poly_col == nullptr)
            {
                poly_is_const = false;
                break;
            }
        }

        /// Two different algorithms are used for constant and non constant polygons.
        /// Constant polygons are preprocessed to speed up matching.
        /// For non-constant polygons, we cannot spend time for preprocessing
        ///  and have to quickly match on the fly without creating temporary data structures.

        if (poly_is_const)
        {
            const ColumnWithTypeAndName & first_poly_col = arguments[1];
            bool is_const_multi_polygon = (arguments.size() == 2 && isThreeDimensionalArray(*first_poly_col.type))
                || (arguments.size() > 2 && isTwoDimensionalArray(*first_poly_col.type));

            if (is_const_multi_polygon)
            {
                MultiPolygon multi_polygon;
                parseConstMultiPolygon(arguments, multi_polygon);

                /// Polygons are preprocessed and saved in cache.
                /// Preprocessing can be computationally heavy but dramatically speeds up matching.

                using Pool = ObjectPoolMap<PointInConstMultiPolygonImpl, UInt128>;

                /// C++11 has thread-safe function-local static.
                static Pool known_multi_polygons;

                auto factory = [&multi_polygon]()
                {
                    auto ptr = std::make_unique<PointInConstMultiPolygonImpl>(multi_polygon);

                    ProfileEvents::increment(ProfileEvents::PolygonsAddedToPool);
                    ProfileEvents::increment(ProfileEvents::PolygonsInPoolAllocatedBytes, ptr->getAllocatedBytes());

                    return ptr.release();
                };

                auto impl = known_multi_polygons.get(sipHash128(multi_polygon), factory);

                if (point_is_const)
                {
                    bool is_in = impl->contains(tuple_columns[0]->getFloat64(0), tuple_columns[1]->getFloat64(0));
                    return result_type->createColumnConst(input_rows_count, is_in);
                }

                return pointInPolygon(*tuple_columns[0], *tuple_columns[1], *impl);
            }
            else // Kept for easier readability
            {
                Polygon polygon;
                parseConstPolygon(arguments, polygon);

                using Pool = ObjectPoolMap<PointInConstPolygonImpl, UInt128>;
                static Pool known_polygons;

                auto factory = [&polygon]()
                {
                    auto ptr = std::make_unique<PointInConstPolygonImpl>(polygon);

                    ProfileEvents::increment(ProfileEvents::PolygonsAddedToPool);
                    ProfileEvents::increment(ProfileEvents::PolygonsInPoolAllocatedBytes, ptr->getAllocatedBytes());

                    return ptr.release();
                };

                auto impl = known_polygons.get(sipHash128(polygon), factory);

                if (point_is_const)
                {
                    bool is_in = impl->contains(tuple_columns[0]->getFloat64(0), tuple_columns[1]->getFloat64(0));
                    return result_type->createColumnConst(input_rows_count, is_in);
                }

                return pointInPolygon(*tuple_columns[0], *tuple_columns[1], *impl);
            }
        }

        if (arguments.size() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multi-argument version of function {} works only with const Polygon/MultiPolygon", getName());

        auto res_column = ColumnVector<UInt8>::create(input_rows_count);
        auto & data = res_column->getData();

        /// A polygon, possibly with holes, is represented by 2d array:
        /// [[(outer_x_1, outer_y_1, ...)], [(hole1_x_1, hole1_y_1), ...], ...]
        ///
        /// Or, a polygon without holes can be represented by 1d array:
        /// [(outer_x_1, outer_y_1, ...)]
        ///
        /// A multi-polygon is represented by 3d array:
        /// [[[(outer_x_1, outer_y_1, ...)], [(hole1_x_1, hole1_y_1), ...], ...], ...]

        if (isThreeDimensionalArray(*arguments[1].type))
        {
            ColumnPtr multi_polygon_column_float64 = castColumn(
                arguments[1],
                std::make_shared<DataTypeArray>( // depth-1
                    std::make_shared<DataTypeArray>( // depth-2
                        std::make_shared<DataTypeArray>( // depth-3
                            std::make_shared<DataTypeTuple>(
                                DataTypes{std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()})))));

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                size_t point_index = point_is_const ? 0 : i;
                data[i] = isInsideMultiPolygon(
                    tuple_columns[0]->getFloat64(point_index), tuple_columns[1]->getFloat64(point_index), *multi_polygon_column_float64, i);
            }
        }
        else if (isTwoDimensionalArray(*arguments[1].type))
        {
            /// We cast everything to Float64 in advance (in batch fashion)
            ///  to avoid casting with virtual calls in a loop.
            /// Note that if the type is already Float64, the operation in noop.

            ColumnPtr polygon_column_float64 = castColumn(
                arguments[1],
                std::make_shared<DataTypeArray>( // depth-1
                  std::make_shared<DataTypeArray>( // depth-2
                    std::make_shared<DataTypeTuple>(
                      DataTypes{std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()}))));

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                size_t point_index = point_is_const ? 0 : i;
                data[i] = isInsidePolygonWithHoles(
                    tuple_columns[0]->getFloat64(point_index), tuple_columns[1]->getFloat64(point_index), *polygon_column_float64, i);
            }
        }
        else
        {
            ColumnPtr polygon_column_float64 = castColumn(
                arguments[1],
                std::make_shared<DataTypeArray>(
                    std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()})));

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                size_t point_index = point_is_const ? 0 : i;
                data[i] = isInsidePolygonWithoutHoles(
                    tuple_columns[0]->getFloat64(point_index), tuple_columns[1]->getFloat64(point_index), *polygon_column_float64, i);
            }
        }

        return res_column;
    }

private:
    bool validate;

    std::string getMessagePrefix(size_t i) const
    {
        return "Argument " + toString(i + 1) + " for function " + getName();
    }

    bool isTwoDimensionalArray(const IDataType & type) const
    {
        return WhichDataType(type).isArray()
            && WhichDataType(static_cast<const DataTypeArray &>(type).getNestedType()).isArray();
    }

    bool isThreeDimensionalArray(const IDataType & type) const
    {
        const auto * level1 = checkAndGetDataType<DataTypeArray>(&type);
        if (!level1)
            return false;

        const auto * level2 = checkAndGetDataType<DataTypeArray>(level1->getNestedType().get());
        if (!level2)
            return false;

        const auto * level3 = checkAndGetDataType<DataTypeArray>(level2->getNestedType().get());
        return level3 != nullptr;
    }

    /// Implementation methods to check point-in-polygon on the fly (for non-const polygons).

    bool isInsideRing(
        Float64 point_x,
        Float64 point_y,
        const Float64 * ring_x_data,
        const Float64 * ring_y_data,
        size_t ring_begin,
        size_t ring_end) const
    {
        size_t size = ring_end - ring_begin;

        if (size < 2)
            return false;

        /** This is the algorithm by W. Randolph Franklin
          * https://wrf.ecse.rpi.edu//Research/Short_Notes/pnpoly.html
          *
          * Basically it works like this:
          * From the point, cast a horizontal ray to the right
          *  and count the number of intersections with polygon edges
          *  (every edge is considered semi-closed, e.g. includes the first vertex and does not include the last)
          *
          * Advantages:
          * - works regardless to the orientation;
          * - for polygon without holes:
          *   works regardless to whether the polygon is closed by last vertex equals to first vertex or not;
          *   (no need to preprocess polygon in any way)
          * - easy to apply for polygons with holes and for multi-polygons;
          * - it even works for polygons with self-intersections in a reasonable way;
          * - simplicity and performance;
          * - can be additionally speed up with loop unrolling and/or binary search for possible intersecting edges.
          *
          * Drawbacks:
          * - it's unspecified whether a point of the edge is inside or outside of a polygon
          *   (looks like it's inside for "left" edges and outside for "right" edges)
          *
          * Why not to apply the same algorithm available in boost::geometry?
          * It will require to move data from columns to temporary containers.
          * Despite the fact that the boost library is template based and allows arbitrary containers and points,
          *  it's diffucult to use without data movement because
          *  we use structure-of-arrays for coordinates instead of arrays-of-structures.
          */

        size_t vertex1_idx = ring_begin;
        size_t vertex2_idx = ring_end - 1;
        bool res = false;

        while (vertex1_idx < ring_end)
        {
            /// First condition checks that the point is inside horizontal row between edge top and bottom y-coordinate.
            /// Second condition checks for intersection with the edge.

            if (((ring_y_data[vertex1_idx] > point_y) != (ring_y_data[vertex2_idx] > point_y))
                && (point_x < (ring_x_data[vertex2_idx] - ring_x_data[vertex1_idx])
                    * (point_y - ring_y_data[vertex1_idx]) / (ring_y_data[vertex2_idx] - ring_y_data[vertex1_idx])
                    + ring_x_data[vertex1_idx]))
            {
                res = !res;
            }

            vertex2_idx = vertex1_idx;
            ++vertex1_idx;
        }

        return res;
    }

    bool isInsidePolygonWithoutHoles(
        Float64 point_x,
        Float64 point_y,
        const IColumn & polygon_column,
        size_t i) const
    {
        const auto & array_col = static_cast<const ColumnArray &>(polygon_column);

        size_t begin = array_col.getOffsets()[i - 1];
        size_t end = array_col.getOffsets()[i];
        size_t size = end - begin;

        if (size < 2)
            return false;

        const auto & tuple_columns = static_cast<const ColumnTuple &>(array_col.getData()).getColumns();
        const auto * x_data = static_cast<const ColumnFloat64 &>(*tuple_columns[0]).getData().data();
        const auto * y_data = static_cast<const ColumnFloat64 &>(*tuple_columns[1]).getData().data();

        return isInsideRing(point_x, point_y, x_data, y_data, begin, end);
    }

    bool isInsidePolygonWithHoles(
        Float64 point_x,
        Float64 point_y,
        const IColumn & polygon_column,
        size_t i) const
    {
        const auto & array_col = static_cast<const ColumnArray &>(polygon_column);
        size_t rings_begin = array_col.getOffsets()[i - 1];
        size_t rings_end = array_col.getOffsets()[i];

        const auto & nested_array_col = static_cast<const ColumnArray &>(array_col.getData());
        const auto & tuple_columns = static_cast<const ColumnTuple &>(nested_array_col.getData()).getColumns();
        const auto * x_data = static_cast<const ColumnFloat64 &>(*tuple_columns[0]).getData().data();
        const auto * y_data = static_cast<const ColumnFloat64 &>(*tuple_columns[1]).getData().data();

        for (size_t j = rings_begin; j < rings_end; ++j)
        {
            size_t begin = nested_array_col.getOffsets()[j - 1];
            size_t end = nested_array_col.getOffsets()[j];

            if (j == rings_begin)
            {
                if (!isInsideRing(point_x, point_y, x_data, y_data, begin, end))
                    return false;
            }
            else
            {
                if (isInsideRing(point_x, point_y, x_data, y_data, begin, end))
                    return false;
            }
        }

        return true;
    }

    bool isInsideMultiPolygon(Float64 point_x, Float64 point_y, const IColumn & multi_polygon_column, size_t i) const
    {
        const auto & array_col = static_cast<const ColumnArray &>(multi_polygon_column);
        size_t polys_begin = array_col.getOffsets()[i - 1];
        size_t polys_end = array_col.getOffsets()[i];

        const auto & nested_array_col = static_cast<const ColumnArray &>(array_col.getData());

        for (size_t j = polys_begin; j < polys_end; ++j)
        {
            if (isInsidePolygonWithHoles(point_x, point_y, nested_array_col, j))
                return true;
        }

        return false;
    }


    /// Implementation methods to create bg::polygon for subsequent preprocessing.
    /// They are used to optimize matching for constant polygons. Preprocessing may take significant amount of time.

    template <typename T>
    void parseRing(
        const Float64 * x_data,
        const Float64 * y_data,
        size_t begin,
        size_t end,
        T & out_container) const
    {
        out_container.reserve(end - begin);
        for (size_t i = begin; i < end; ++i)
        {
            Int64 result = 0;
            if (common::mulOverflow(static_cast<Int64>(x_data[i]), static_cast<Int64>(y_data[i]), result))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The coordinates of the point are such that "
                                "subsequent calculations cannot be performed correctly. "
                                "Most likely they are very large in modulus.");

            out_container.emplace_back(x_data[i], y_data[i]);
        }
    }

    void parseConstPolygonWithoutHolesFromSingleColumn(const IColumn & column, size_t i, Polygon & out_polygon) const
    {
        const auto & array_col = static_cast<const ColumnArray &>(column);
        size_t begin = array_col.getOffsets()[i - 1];
        size_t end = array_col.getOffsets()[i];

        const auto & tuple_columns = static_cast<const ColumnTuple &>(array_col.getData()).getColumns();
        const auto * x_data = static_cast<const ColumnFloat64 &>(*tuple_columns[0]).getData().data();
        const auto * y_data = static_cast<const ColumnFloat64 &>(*tuple_columns[1]).getData().data();

        parseRing(x_data, y_data, begin, end, out_polygon.outer());
    }

    void parseConstPolygonWithHolesFromSingleColumn(const IColumn & column, size_t i, Polygon & out_polygon) const
    {
        const auto & array_col = static_cast<const ColumnArray &>(column);
        size_t rings_begin = array_col.getOffsets()[i - 1];
        size_t rings_end = array_col.getOffsets()[i];

        const auto & nested_array_col = static_cast<const ColumnArray &>(array_col.getData());
        const auto & tuple_columns = static_cast<const ColumnTuple &>(nested_array_col.getData()).getColumns();
        const auto * x_data = static_cast<const ColumnFloat64 &>(*tuple_columns[0]).getData().data();
        const auto * y_data = static_cast<const ColumnFloat64 &>(*tuple_columns[1]).getData().data();

        for (size_t j = rings_begin; j < rings_end; ++j)
        {
            size_t begin = nested_array_col.getOffsets()[j - 1];
            size_t end = nested_array_col.getOffsets()[j];

            if (out_polygon.outer().empty())
            {
                parseRing(x_data, y_data, begin, end, out_polygon.outer());
            }
            else
            {
                out_polygon.inners().emplace_back();
                parseRing(x_data, y_data, begin, end, out_polygon.inners().back());
            }
        }
    }

    void parseConstPolygonWithHolesFromMultipleColumns(const ColumnsWithTypeAndName & arguments, Polygon & out_polygon) const
    {
        for (size_t i = 1; i < arguments.size(); ++i)
        {
            const auto * const_col = checkAndGetColumn<ColumnConst>(arguments[i].column.get());
            if (!const_col)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multi-argument version of function {} works only with const polygon",
                    getName());

            const auto * array_col = checkAndGetColumn<ColumnArray>(&const_col->getDataColumn());
            const auto * tuple_col = array_col ? checkAndGetColumn<ColumnTuple>(&array_col->getData()) : nullptr;

            if (!tuple_col)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "{} must be constant array of tuples", getMessagePrefix(i));

            const auto & tuple_columns = tuple_col->getColumns();
            const auto & column_x = tuple_columns[0];
            const auto & column_y = tuple_columns[1];

            if (!out_polygon.outer().empty())
                out_polygon.inners().emplace_back();

            auto & container = out_polygon.outer().empty() ? out_polygon.outer() : out_polygon.inners().back();

            auto size = column_x->size();

            if (size == 0)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "{} shouldn't be empty.", getMessagePrefix(i));

            for (auto j : collections::range(0, size))
            {
                CoordinateType x_coord = column_x->getFloat64(j);
                CoordinateType y_coord = column_y->getFloat64(j);
                container.push_back(Point(x_coord, y_coord));
            }
        }
    }

    void parseConstPolygonFromSingleColumn(const ColumnWithTypeAndName & argument, Polygon & out_polygon) const
    {
        if (isTwoDimensionalArray(*argument.type))
        {
            ColumnPtr polygon_column_float64 = castColumn(
                argument,
                std::make_shared<DataTypeArray>(
                    std::make_shared<DataTypeArray>(
                        std::make_shared<DataTypeTuple>(DataTypes{
                            std::make_shared<DataTypeFloat64>(),
                            std::make_shared<DataTypeFloat64>()}))));

            const ColumnConst & column_const = typeid_cast<const ColumnConst &>(*polygon_column_float64);
            const IColumn & column_const_data = column_const.getDataColumn();

            parseConstPolygonWithHolesFromSingleColumn(column_const_data, 0, out_polygon);
        }
        else
        {
            ColumnPtr polygon_column_float64 = castColumn(
                argument,
                std::make_shared<DataTypeArray>(
                    std::make_shared<DataTypeTuple>(DataTypes{
                        std::make_shared<DataTypeFloat64>(),
                        std::make_shared<DataTypeFloat64>()})));

            const ColumnConst & column_const = typeid_cast<const ColumnConst &>(*polygon_column_float64);
            const IColumn & column_const_data = column_const.getDataColumn();

            parseConstPolygonWithoutHolesFromSingleColumn(column_const_data, 0, out_polygon);
        }
    }

    void NO_SANITIZE_UNDEFINED parseConstPolygon(const ColumnsWithTypeAndName & arguments, Polygon & out_polygon) const
    {
        if (arguments.size() == 2)
            parseConstPolygonFromSingleColumn(arguments[1], out_polygon);
        else
            parseConstPolygonWithHolesFromMultipleColumns(arguments, out_polygon);

        /// Fix orientation and close rings. It's required for subsequent processing.
        bg::correct(out_polygon);

#if !defined(__clang_analyzer__) /// It does not like boost.
        if (validate)
        {
            std::string failure_message;
            auto is_valid = bg::is_valid(out_polygon, failure_message);
            if (!is_valid)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Polygon is not valid: {}", failure_message);
        }
#endif
    }

    void parseConstMultiPolygonFromSingleColumn(const ColumnWithTypeAndName & argument, MultiPolygon & out_multi_polygon) const
    {
        ColumnPtr multi_polygon_column_float64 = castColumn(
            argument,
            std::make_shared<DataTypeArray>(
              std::make_shared<DataTypeArray>(
                std::make_shared<DataTypeArray>(
                std::make_shared<DataTypeTuple>(
                  DataTypes{std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()})))));

        const ColumnConst & column_const = typeid_cast<const ColumnConst &>(*multi_polygon_column_float64);
        const auto & array_col = static_cast<const ColumnArray &>(column_const.getDataColumn()); // depth-1 (polygons)
        const auto & nested_array_col = static_cast<const ColumnArray &>(array_col.getData());

        size_t polygons_count = nested_array_col.size();
        for (size_t i = 0; i < polygons_count; ++i)
        {
            out_multi_polygon.emplace_back();
            parseConstPolygonWithHolesFromSingleColumn(nested_array_col, i, out_multi_polygon.back());
        }
    }

    void parseConstMultiPolygonFromMultipleColumns(const ColumnsWithTypeAndName & arguments, MultiPolygon & out_multi_polygon) const
    {
        for (size_t arg_pos = 1; arg_pos < arguments.size(); ++arg_pos)
        {
            out_multi_polygon.emplace_back();
            parseConstPolygonFromSingleColumn(arguments[arg_pos], out_multi_polygon.back());
        }
    }

    void NO_SANITIZE_UNDEFINED parseConstMultiPolygon(const ColumnsWithTypeAndName & arguments, MultiPolygon & out_multi_polygon) const
    {
        if (arguments.size() == 2)
            parseConstMultiPolygonFromSingleColumn(arguments[1], out_multi_polygon);
        else
            parseConstMultiPolygonFromMultipleColumns(arguments, out_multi_polygon);


        /// Fix orientation and close rings. It's required for subsequent processing.
        bg::correct(out_multi_polygon);

#if !defined(__clang_analyzer__)
        if (validate)
        {
            std::string failure_message;
            if (!bg::is_valid(out_multi_polygon, failure_message))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "MultiPolygon is not valid: {}", failure_message);
        }
#endif
    }
};

}

REGISTER_FUNCTION(PointInPolygon)
{
    using PointInPolygonWithGridF64 = PointInPolygonWithGrid<Float64>;
    using PointInMultiPolygonRTreeWithGrid = PointInMultiPolygonRTree<PointInPolygonWithGridF64>;

    factory.registerFunction<FunctionPointInPolygon<PointInPolygonWithGridF64, PointInMultiPolygonRTreeWithGrid>>();
}

}
