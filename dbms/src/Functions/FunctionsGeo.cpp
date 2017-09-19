#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsGeo.h>
#include <Functions/GeoUtils.h>
#include <Functions/ObjectPool.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>

#include <Interpreters/ExpressionActions.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnTuple.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace FunctionPointInPolygonDetail
{

template <typename Polygon, typename PointInPolygonImpl>
ColumnPtr callPointInPolygonImplWithPool(const IColumn & x, const IColumn & y, Polygon & polygon)
{
    using Pool = ObjectPoolMap<PointInPolygonImpl, std::string>;
    /// C++11 has thread-safe function-local statics on most modern compilers.
    static Pool known_polygons;

    auto factory = [& polygon]()
    {
        GeoUtils::normalizePolygon(polygon);
        return new PointInPolygonImpl(polygon);
    };

    std::string serialized_polygon = GeoUtils::serialize(polygon);
    auto impl = known_polygons.get(serialized_polygon, factory);

    return GeoUtils::pointInPolygon(x, y, *impl);
}

template <typename Polygon, typename PointInPolygonImpl>
ColumnPtr callPointInPolygonImpl(const IColumn & x, const IColumn & y, Polygon & polygon)
{
    PointInPolygonImpl impl(polygon);
    return GeoUtils::pointInPolygon(x, y, impl);
}

}

template <typename PointInPolygonImpl, bool useObjectPool = false>
class FunctionPointInPolygon : public IFunction
{
public:

    using Point = boost::geometry::model::d2::point_xy<Float32>;
    using Polygon = boost::geometry::model::polygon<Point, false>;
    using Box = boost::geometry::model::box<Point>;

    static const char * name;

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionPointInPolygon<PointInPolygonImpl>>();
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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
        {
            throw Exception("Too few arguments", ErrorCodes::TOO_LESS_ARGUMENTS_FOR_FUNCTION);
        }

        for (size_t i = 1; i < arguments.size(); ++i)
        {
            auto * array = checkAndGetDataType<DataTypeArray>(arguments[i].get());
            if (array == nullptr && i != 1)
            {
                throw Exception("Argument " + toString(i + 1) + " for function " + getName() + " must be array of tuples.",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }

            auto * tuple = checkAndGetDataType<DataTypeTuple>(array ? array->getNestedType().get() : arguments[i].get());
            if (tuple == nullptr)
            {
                throw Exception("Argument " + toString(i + 1) + " for function " + getName() + " must contains tuple.",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }

            const DataTypes & elements = tuple->getElements();

            if (elements.size() != 2)
            {
                throw Exception("Tuples in argument " + toString(i + 1) + " must have exactly two elements.",
                                ErrorCodes::BAD_ARGUMENTS);
            }

            for (auto j : ext::range(0, elements.size()))
            {
                if (!checkDataType<DataTypeFloat32>(elements[j].get()) && !checkDataType<DataTypeFloat64>(elements[j].get()))
                {
                    throw Exception("Tuple element " + toString(j + 1) + " in argument " + toString(i + 1) + " must be float.",
                                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                }
            }
        }

        return std::make_shared<DataTypeUInt8>();
    }


    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        Polygon polygon;

        for (size_t i = 1; i < arguments.size(); ++i)
        {
            auto const_array_col = checkAndGetColumn<ColumnConst>(block.getByPosition(arguments[i]).column.get());
            auto array_col = const_array_col ? checkAndGetColumn<ColumnArray>(&const_array_col->getDataColumn()) : nullptr;
            auto tuple_col = array_col ? checkAndGetColumn<ColumnTuple>(&array_col->getData()) : nullptr;

            if (!tuple_col)
            {
                throw Exception("Argument " + toString(i + 1) + " for function " + getName() + " must be constant array of tuples.",
                                ErrorCodes::ILLEGAL_COLUMN);
            }

            const auto & tuple_block = tuple_col->getData();
            const auto & column_x = tuple_block.safeGetByPosition(0).column;
            const auto & column_y = tuple_block.safeGetByPosition(1).column;

            if (!polygon.outer().empty())
                polygon.inners().emplace_back();

            auto & container = polygon.outer().empty() ? polygon.outer() : polygon.inners().back();

            auto size = column_x->size();

            if (size == 0)
            {
                throw Exception("Argument " + toString(i + 1) + " for function " + getName() + " shouldn't be empty.",
                                ErrorCodes::ILLEGAL_COLUMN);
            }

            for (auto j : ext::range(0, size))
            {
                auto x = static_cast<Float32>((*column_x)[j].get<Float64>());
                auto y = static_cast<Float32>((*column_y)[j].get<Float64>());
                container.push_back(Point(x, y));
            }

            /// Polygon assumed to be closed. Allow user to escape repeating of first point.
            if (!boost::geometry::equals(container.front(), container.back()))
                container.push_back(container.front());
        }

        const IColumn * point_col = block.getByPosition(arguments[0]).column.get();
        auto const_tuple_col = checkAndGetColumn<ColumnConst>(point_col);
        if (const_tuple_col)
            point_col = &const_tuple_col->getDataColumn();
        auto tuple_col = checkAndGetColumn<ColumnTuple>(point_col);

        if (!tuple_col)
        {
            throw Exception("First argument for function " + getName() + " must be constant array of tuples.",
                            ErrorCodes::ILLEGAL_COLUMN);
        }

        const auto & tuple_block = tuple_col->getData();
        const auto & column_x = tuple_block.safeGetByPosition(0).column;
        const auto & column_y = tuple_block.safeGetByPosition(1).column;

        auto & result_column = block.safeGetByPosition(result).column;

        auto callImpl = useObjectPool
                        ? FunctionPointInPolygonDetail::callPointInPolygonImplWithPool<Polygon, PointInPolygonImpl>
                        : FunctionPointInPolygonDetail::callPointInPolygonImpl<Polygon, PointInPolygonImpl>;

        result_column = callImpl(*column_x, *column_y, polygon);

        if (const_tuple_col)
            result_column = std::make_shared<ColumnConst>(result_column, const_tuple_col->size());
    }


};


using Point = boost::geometry::model::d2::point_xy<Float32>;

using PointInPolygonCrossingStrategy = boost::geometry::strategy::within::crossings_multiply<Point>;
using PointInPolygonWindingStrategy = boost::geometry::strategy::within::winding<Point>;
using PointInPolygonFranklinStrategy = boost::geometry::strategy::within::franklin<Point>;

using PointInPolygonCrossing = GeoUtils::PointInPolygon<PointInPolygonCrossingStrategy>;
using PointInPolygonWinding = GeoUtils::PointInPolygon<PointInPolygonWindingStrategy>;
using PointInPolygonFranklin = GeoUtils::PointInPolygon<PointInPolygonFranklinStrategy>;
using PointInPolygonWithGrid = GeoUtils::PointInPolygonWithGrid<>;

template <>
const char * FunctionPointInPolygon<PointInPolygonCrossing>::name = "pointInPolygon";
template <>
const char * FunctionPointInPolygon<PointInPolygonWinding>::name = "pointInPolygonWinding";
template <>
const char * FunctionPointInPolygon<PointInPolygonFranklin>::name = "pointInPolygonFranklin";
template <>
const char * FunctionPointInPolygon<PointInPolygonWithGrid, false>::name = "pointInPolygonWithGrid";
template <>
const char * FunctionPointInPolygon<PointInPolygonWithGrid, true>::name = "pointInPolygonWithGridAndPool";


void registerFunctionsGeo(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGreatCircleDistance>();
    factory.registerFunction<FunctionPointInEllipses>();

    factory.registerFunction<FunctionPointInPolygon<PointInPolygonFranklin>>();
    factory.registerFunction<FunctionPointInPolygon<PointInPolygonWinding>>();
    factory.registerFunction<FunctionPointInPolygon<PointInPolygonCrossing>>();
    factory.registerFunction<FunctionPointInPolygon<PointInPolygonWithGrid, true>>();
    factory.registerFunction<FunctionPointInPolygon<PointInPolygonWithGrid, false>>();
}
}
