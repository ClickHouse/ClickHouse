#include <Functions/FunctionFactory.h>
#include <Functions/PolygonUtils.h>
#include <Functions/FunctionHelpers.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Common/ObjectPool.h>
#include <Common/ProfileEvents.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ExpressionActions.h>

#include <string>
#include <memory>


namespace ProfileEvents
{
    extern const Event PolygonsAddedToPool;
    extern const Event PolygonsInPoolAllocatedBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

template <typename Polygon, typename PointInPolygonImpl>
UInt8 callPointInPolygonImplWithPool(Float64 x, Float64 y, Polygon & polygon)
{
    using Pool = ObjectPoolMap<PointInPolygonImpl, std::string>;
    /// C++11 has thread-safe function-local statics on most modern compilers.
    static Pool known_polygons;

    auto factory = [& polygon]()
    {
        auto ptr = std::make_unique<PointInPolygonImpl>(polygon);

        ProfileEvents::increment(ProfileEvents::PolygonsAddedToPool);
        ProfileEvents::increment(ProfileEvents::PolygonsInPoolAllocatedBytes, ptr->getAllocatedBytes());

        return ptr.release();
    };

    std::string serialized_polygon = serialize(polygon);
    auto impl = known_polygons.get(serialized_polygon, factory);

    return impl->contains(x, y);
}

template <typename Polygon, typename PointInPolygonImpl>
UInt8 callPointInPolygonImpl(Float64 x, Float64 y, Polygon & polygon)
{
    PointInPolygonImpl impl(polygon);
    return impl.contains(x, y);
}

}

template <typename PointInConstPolygonImpl, typename PointInNonConstPolygonImpl>
class FunctionPointInPolygon : public IFunction
{
public:
    using CoordinateType = Float64;

    using Point = boost::geometry::model::d2::point_xy<CoordinateType>;
    using Polygon = boost::geometry::model::polygon<Point, false>;
    using Box = boost::geometry::model::box<Point>;

    static inline const char * name = "pointInPolygon";

    explicit FunctionPointInPolygon(bool validate_) : validate(validate_) {}

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionPointInPolygon<PointInConstPolygonImpl, PointInNonConstPolygonImpl>>(
            context.getSettingsRef().validate_polygons);
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
            throw Exception("Too few arguments", ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION);
        }

        auto validate_tuple = [this](size_t i, const DataTypeTuple * tuple)
        {
            if (tuple == nullptr)
                throw Exception(getMessagePrefix(i) + " must contain a tuple", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const DataTypes & elements = tuple->getElements();

            if (elements.size() != 2)
                throw Exception(getMessagePrefix(i) + " must have exactly two elements", ErrorCodes::BAD_ARGUMENTS);

            for (auto j : ext::range(0, elements.size()))
            {
                if (!isNativeNumber(elements[j]))
                {
                    throw Exception(getMessagePrefix(i) + " must contain numeric tuple at position " + toString(j + 1),
                                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                }
            }
        };

        validate_tuple(0, checkAndGetDataType<DataTypeTuple>(arguments[0].get()));

        if (arguments.size() == 2)
        {
            const auto * array = checkAndGetDataType<DataTypeArray>(arguments[1].get());
            if (array == nullptr)
                throw Exception(getMessagePrefix(1) + " must contain an array of tuples or an array of arrays of tuples.",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const auto * nested_array = checkAndGetDataType<DataTypeArray>(array->getNestedType().get());
            if (nested_array != nullptr)
            {
                array = nested_array;
            }

            validate_tuple(1, checkAndGetDataType<DataTypeTuple>(array->getNestedType().get()));
        }
        else
        {
            for (size_t i = 1; i < arguments.size(); i++)
            {
                const auto * array = checkAndGetDataType<DataTypeArray>(arguments[i].get());
                if (array == nullptr)
                    throw Exception(getMessagePrefix(i) + " must contain an array of tuples",
                                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

                validate_tuple(i, checkAndGetDataType<DataTypeTuple>(array->getNestedType().get()));
            }
        }

        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const IColumn * point_col = block.getByPosition(arguments[0]).column.get();
        const auto * const_tuple_col = checkAndGetColumn<ColumnConst>(point_col);
        if (const_tuple_col)
            point_col = &const_tuple_col->getDataColumn();

        const auto * tuple_col = checkAndGetColumn<ColumnTuple>(point_col);
        if (!tuple_col)
            throw Exception("First argument for function " + getName() + " must be constant array of tuples.",
                            ErrorCodes::ILLEGAL_COLUMN);

        const auto & tuple_columns = tuple_col->getColumns();

        const IColumn * poly_col = block.getByPosition(arguments[1]).column.get();
        const auto * const_poly_col = checkAndGetColumn<ColumnConst>(poly_col);

        bool point_is_const = const_tuple_col != nullptr;
        bool poly_is_const = const_poly_col != nullptr;

        auto call_impl = poly_is_const
            ? callPointInPolygonImplWithPool<Polygon, PointInConstPolygonImpl>
            : callPointInPolygonImpl<Polygon, PointInNonConstPolygonImpl>;

        size_t size = point_is_const && poly_is_const ? 1 : input_rows_count;
        auto execution_result = ColumnVector<UInt8>::create(size);
        auto & data = execution_result->getData();

        Polygon polygon;
        for (auto i : ext::range(0, size))
        {
            if (!poly_is_const || i == 0)
            {
                polygon = parsePolygon(block, arguments, i);
            }

            size_t point_index = point_is_const ? 0 : i;
            data[i] = call_impl(tuple_columns[0]->getFloat64(point_index), tuple_columns[1]->getFloat64(point_index), polygon);
        }

        auto & result_column = block.safeGetByPosition(result).column;
        result_column = std::move(execution_result);
        if (point_is_const && poly_is_const)
            result_column = ColumnConst::create(result_column, const_tuple_col->size());
    }


private:
    bool validate;

    std::string getMessagePrefix(size_t i) const
    {
        return "Argument " + toString(i + 1) + " for function " + getName();
    }

    Polygon parsePolygonFromSingleColumn(Block & block, const ColumnNumbers & arguments, size_t i) const
    {
        const auto & poly = block.getByPosition(arguments[1]).column.get();
        const auto * column_const = checkAndGetColumn<ColumnConst>(poly);
        const auto * array_col =
            column_const ? checkAndGetColumn<ColumnArray>(column_const->getDataColumn()) : checkAndGetColumn<ColumnArray>(poly);

        if (!array_col)
            throw Exception(getMessagePrefix(1) + " must contain an array of tuples or an array of arrays of tuples",
                            ErrorCodes::ILLEGAL_COLUMN);

        const auto * nested_array_col = checkAndGetColumn<ColumnArray>(array_col->getData());
        const auto & tuple_data = nested_array_col ? nested_array_col->getData() : array_col->getData();
        const auto & tuple_col = checkAndGetColumn<ColumnTuple>(tuple_data);
        if (!tuple_col)
            throw Exception(getMessagePrefix(1) + " must contain an array of tuples or an array of arrays of tuples",
                            ErrorCodes::ILLEGAL_COLUMN);

        const auto & tuple_columns = tuple_col->getColumns();
        const auto & x_column = tuple_columns[0];
        const auto & y_column = tuple_columns[1];

        auto parse_polygon_part = [&x_column, &y_column](auto & container, size_t l, size_t r)
        {
            for (auto j : ext::range(l, r))
            {
                CoordinateType x_coord = x_column->getFloat64(j);
                CoordinateType y_coord = y_column->getFloat64(j);

                container.push_back(Point(x_coord, y_coord));
            }
        };

        Polygon polygon;
        if (nested_array_col)
        {
            for (auto j : ext::range(array_col->getOffsets()[i - 1], array_col->getOffsets()[i]))
            {
                size_t l = nested_array_col->getOffsets()[j - 1];
                size_t r = nested_array_col->getOffsets()[j];
                if (polygon.outer().empty())
                {
                    parse_polygon_part(polygon.outer(), l, r);
                }
                else
                {
                    polygon.inners().emplace_back();
                    parse_polygon_part(polygon.inners().back(), l, r);
                }
            }
        }
        else
        {
            size_t l = array_col->getOffsets()[i - 1];
            size_t r = array_col->getOffsets()[i];

            parse_polygon_part(polygon.outer(), l, r);
        }

        return polygon;
    }

    Polygon parsePolygonFromMultipleColumns(Block & block, const ColumnNumbers & arguments, size_t) const
    {
        Polygon polygon;

        for (size_t i = 1; i < arguments.size(); ++i)
        {
            const auto * const_col = checkAndGetColumn<ColumnConst>(block.getByPosition(arguments[i]).column.get());
            if (!const_col)
                throw Exception("Multi-argument version of function " + getName() + " works only with const polygon",
                            ErrorCodes::BAD_ARGUMENTS);

            const auto * array_col = checkAndGetColumn<ColumnArray>(&const_col->getDataColumn());
            const auto * tuple_col = array_col ? checkAndGetColumn<ColumnTuple>(&array_col->getData()) : nullptr;

            if (!tuple_col)
                throw Exception(getMessagePrefix(i) + " must be constant array of tuples", ErrorCodes::ILLEGAL_COLUMN);

            const auto & tuple_columns = tuple_col->getColumns();
            const auto & column_x = tuple_columns[0];
            const auto & column_y = tuple_columns[1];

            if (!polygon.outer().empty())
                polygon.inners().emplace_back();

            auto & container = polygon.outer().empty() ? polygon.outer() : polygon.inners().back();

            auto size = column_x->size();

            if (size == 0)
                throw Exception(getMessagePrefix(i) + " shouldn't be empty.", ErrorCodes::ILLEGAL_COLUMN);

            for (auto j : ext::range(0, size))
            {
                CoordinateType x_coord = column_x->getFloat64(j);
                CoordinateType y_coord = column_y->getFloat64(j);
                container.push_back(Point(x_coord, y_coord));
            }
        }

        return polygon;
    }

    Polygon parsePolygon(Block & block, const ColumnNumbers & arguments, size_t i) const
    {
        Polygon polygon;
        if (arguments.size() == 2)
        {
            polygon = parsePolygonFromSingleColumn(block, arguments, i);
        }
        else
        {
            polygon = parsePolygonFromMultipleColumns(block, arguments, i);
        }

        boost::geometry::correct(polygon);

#if !defined(__clang_analyzer__) /// It does not like boost.
        if (validate)
        {
            std::string failure_message;
            auto is_valid = boost::geometry::is_valid(polygon, failure_message);
            if (!is_valid)
                throw Exception("Polygon is not valid: " + failure_message, ErrorCodes::BAD_ARGUMENTS);
        }
#endif
        return polygon;
    }
};


void registerFunctionPointInPolygon(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPointInPolygon<PointInPolygonWithGrid<Float64>, PointInPolygonTrivial<Float64>>>();
}

}
