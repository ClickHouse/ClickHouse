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
ColumnPtr callPointInPolygonImplWithPool(const IColumn & x, const IColumn & y, Polygon & polygon)
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

    return pointInPolygon(x, y, *impl);
}

template <typename Polygon, typename PointInPolygonImpl>
ColumnPtr callPointInPolygonImpl(const IColumn & x, const IColumn & y, Polygon & polygon)
{
    PointInPolygonImpl impl(polygon);
    return pointInPolygon(x, y, impl);
}

}

template <typename PointInPolygonImpl, bool use_object_pool>
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
        return std::make_shared<FunctionPointInPolygon<PointInPolygonImpl, use_object_pool>>(context.getSettingsRef().validate_polygons);
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

        auto get_message_prefix = [this](size_t i) { return "Argument " + toString(i + 1) + " for function " + getName(); };

        for (size_t i = 1; i < arguments.size(); ++i)
        {
            const auto * array = checkAndGetDataType<DataTypeArray>(arguments[i].get());
            if (array == nullptr && i != 1)
                throw Exception(get_message_prefix(i) + " must be array of tuples.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const auto * tuple = checkAndGetDataType<DataTypeTuple>(array ? array->getNestedType().get() : arguments[i].get());
            if (tuple == nullptr)
                throw Exception(get_message_prefix(i) + " must contains tuple.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const DataTypes & elements = tuple->getElements();

            if (elements.size() != 2)
                throw Exception(get_message_prefix(i) + " must have exactly two elements.", ErrorCodes::BAD_ARGUMENTS);

            for (auto j : ext::range(0, elements.size()))
            {
                if (!isNativeNumber(elements[j]))
                {
                    throw Exception(get_message_prefix(i) + " must contains numeric tuple at position " + toString(j + 1),
                                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                }
            }
        }

        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const IColumn * point_col = block.getByPosition(arguments[0]).column.get();
        const auto * const_tuple_col = checkAndGetColumn<ColumnConst>(point_col);
        if (const_tuple_col)
            point_col = &const_tuple_col->getDataColumn();
        const auto * tuple_col = checkAndGetColumn<ColumnTuple>(point_col);

        if (!tuple_col)
            throw Exception("First argument for function " + getName() + " must be constant array of tuples.",
                            ErrorCodes::ILLEGAL_COLUMN);

        auto & result_column = block.safeGetByPosition(result).column;

        const auto & tuple_columns = tuple_col->getColumns();
        result_column = executeForType(*tuple_columns[0], *tuple_columns[1], block, arguments);

        if (const_tuple_col)
            result_column = ColumnConst::create(result_column, const_tuple_col->size());
    }

private:
    bool validate;

    ColumnPtr executeForType(const IColumn & x, const IColumn & y, Block & block, const ColumnNumbers & arguments)
    {
        Polygon polygon;

        auto get_message_prefix = [this](size_t i) { return "Argument " + toString(i + 1) + " for function " + getName(); };

        for (size_t i = 1; i < arguments.size(); ++i)
        {
            const auto * const_col = checkAndGetColumn<ColumnConst>(block.getByPosition(arguments[i]).column.get());
            const auto * array_col = const_col ? checkAndGetColumn<ColumnArray>(&const_col->getDataColumn()) : nullptr;
            const auto * tuple_col = array_col ? checkAndGetColumn<ColumnTuple>(&array_col->getData()) : nullptr;

            if (!tuple_col)
                throw Exception(get_message_prefix(i) + " must be constant array of tuples.", ErrorCodes::ILLEGAL_COLUMN);

            const auto & tuple_columns = tuple_col->getColumns();
            const auto & column_x = tuple_columns[0];
            const auto & column_y = tuple_columns[1];

            if (!polygon.outer().empty())
                polygon.inners().emplace_back();

            auto & container = polygon.outer().empty() ? polygon.outer() : polygon.inners().back();

            auto size = column_x->size();

            if (size == 0)
                throw Exception(get_message_prefix(i) + " shouldn't be empty.", ErrorCodes::ILLEGAL_COLUMN);

            for (auto j : ext::range(0, size))
            {
                CoordinateType x_coord = column_x->getFloat64(j);
                CoordinateType y_coord = column_y->getFloat64(j);
                container.push_back(Point(x_coord, y_coord));
            }
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

        auto call_impl = use_object_pool
            ? callPointInPolygonImplWithPool<Polygon, PointInPolygonImpl>
            : callPointInPolygonImpl<Polygon, PointInPolygonImpl>;

        return call_impl(x, y, polygon);
    }
};


void registerFunctionPointInPolygon(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPointInPolygon<PointInPolygonWithGrid<Float64>, true>>();
}

}
