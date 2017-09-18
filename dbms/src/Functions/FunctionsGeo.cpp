#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsGeo.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>

#include <Interpreters/ExpressionActions.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnTuple.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
    extern const int BAD_ARGUMENTS;
}


template <typename... Args>
using PointInPolygonCrossing = boost::geometry::strategy::within::crossings_multiply<Args...>;
template <typename... Args>
using PointInPolygonWinding = boost::geometry::strategy::within::winding<Args...>;
template <typename... Args>
using PointInPolygonFranklin = boost::geometry::strategy::within::franklin<Args...>;

template <template <typename...> typename Strategy>
class FunctionPointInPolygon : public IFunction
{
private:
    using CoordinateType = Float64;

public:
    static const char * name;

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionPointInPolygon<Strategy>>();
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


    void getReturnTypeAndPrerequisitesImpl(
        const ColumnsWithTypeAndName & arguments, DataTypePtr & out_return_type, ExpressionActions::Actions & out_prerequisites) override
    {
        if (arguments.size() < 2)
        {
            throw Exception("Too few arguments", ErrorCodes::TOO_LESS_ARGUMENTS_FOR_FUNCTION);
        }

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const DataTypeTuple * tuple = checkAndGetDataType<DataTypeTuple>(&*arguments[i].type);
            if (tuple == nullptr)
            {
                throw Exception("Argument " + toString(i + 1) + " for function " + getName() + " must be tuple.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }

            const DataTypes & elems = tuple->getElements();

            if (elems.size() != 2)
            {
                throw Exception("Tuple in argument " + toString(i + 1) + " must have exactly two elements.", ErrorCodes::BAD_ARGUMENTS);
            }

            for (size_t elems_index = 0; elems_index < elems.size(); ++elems_index)
            {
                if (!checkDataType<DataTypeNumber<CoordinateType>>(&*elems[elems_index]))
                {
                    throw Exception("Tuple element " + toString(elems_index + 1) + " in argument " + toString(i + 1)
                        + " must be " + TypeName<CoordinateType>::get() + ".", ErrorCodes::BAD_ARGUMENTS);
                }
            }
        }

        out_return_type = std::make_shared<DataTypeUInt8>();
    }


    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        using PointType = boost::geometry::model::d2::point_xy<CoordinateType>;
        using PolygonType = boost::geometry::model::polygon<PointType>;
        std::pair<CoordinateType, CoordinateType> min, max;

        std::vector<PointType> polygon_points(arguments.size() - 1);

        for (size_t i = 1; i < arguments.size(); ++i)
        {
            auto const_tuple_col = checkAndGetColumnConst<ColumnTuple>(block.getByPosition(arguments[i]).column.get());
            if (!const_tuple_col)
            {
                throw Exception("Argument " + toString(i + 1) + " for function " + getName() + " must be constant tuple.", ErrorCodes::ILLEGAL_COLUMN);
            }

            TupleBackend data = const_tuple_col->getValue<Tuple>();
            const CoordinateType x = data[0].get<Float64>();
            const CoordinateType y = data[1].get<Float64>();
            polygon_points[i - 1] = PointType(x, y);

            if (i == 1)
            {
                min.first = x;
                min.second = y;
                max.first = x;
                max.second = y;
            }
            else
            {
                min.first = std::min(min.first, x);
                max.first = std::max(max.first, x);
                min.second = std::min(min.second, y);
                max.second = std::max(max.second, y);
            }
        }

        PolygonType polygon;
        boost::geometry::assign_points(polygon, polygon_points);

        Strategy<PointType> strategy;

        auto point_checker = [&](CoordinateType x, CoordinateType y) -> bool
        {
            if (x < min.first || x > max.first || y < min.second || y > max.second)
                return false;

            PointType point(x, y);
            return boost::geometry::covered_by(point, polygon, strategy);
        };

        size_t rows = block.rows();

        auto point_column_const = checkAndGetColumnConst<ColumnTuple>(block.getByPosition(arguments[0]).column.get());
        if (point_column_const)
        {
            TupleBackend data = point_column_const->getValue<Tuple>();
            const CoordinateType point_x = data[0].get<Float64>();
            const CoordinateType point_y = data[1].get<Float64>();
            UInt8 value = point_checker(point_x, point_y);
            block.getByPosition(result).column = DataTypeUInt8().createConstColumn(rows, UInt64(value));
            return;
        }

        auto & res = block.getByPosition(result);
        res.column = std::make_shared<ColumnUInt8>(rows);
        IColumn & result_column = *res.column;
        auto & result_data = static_cast<ColumnUInt8 &>(result_column).getData();

        auto point_column = checkAndGetColumn<ColumnTuple>(block.getByPosition(arguments[0]).column.get());
        auto column_x = checkAndGetColumn<ColumnVector<CoordinateType>>(point_column->getData().getByPosition(0).column.get());
        auto column_y = checkAndGetColumn<ColumnVector<CoordinateType>>(point_column->getData().getByPosition(1).column.get());

        for (size_t i = 0; i < rows; ++i)
        {
            const CoordinateType point_x = column_x->getElement(i);
            const CoordinateType point_y = column_y->getElement(i);
            result_data[i] = point_checker(point_x, point_y);
        }
    }
};

template <>
const char * FunctionPointInPolygon<PointInPolygonCrossing>::name = "pointInPolygon";
template <>
const char * FunctionPointInPolygon<PointInPolygonWinding>::name = "pointInPolygonWinding";
template <>
const char * FunctionPointInPolygon<PointInPolygonFranklin>::name = "pointInPolygonFranklin";


void registerFunctionsGeo(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGreatCircleDistance>();
    factory.registerFunction<FunctionPointInEllipses>();

    factory.registerFunction<FunctionPointInPolygon<PointInPolygonFranklin>>();
    factory.registerFunction<FunctionPointInPolygon<PointInPolygonWinding>>();
    factory.registerFunction<FunctionPointInPolygon<PointInPolygonCrossing>>();
}
}
