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
#include <Interpreters/Context.h>

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

        /** We allow function invocation in one of the following forms:
          *
          * pointInPolygon((x, y), [(x1, y1), (x2, y2), ...])
          * - simple polygon
          * pointInPolygon((x, y), [(x1, y1), (x2, y2), ...], [(x21, y21), (x22, y22), ...], ...)
          * - polygon with a number of holes, each hole as a subsequent argument.
          * pointInPolygon((x, y), [[(x1, y1), (x2, y2), ...], [(x21, y21), (x22, y22), ...], ...])
          * - polygon with a number of holes, all as multidimensional array
          */

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

        const ColumnWithTypeAndName poly = block.getByPosition(arguments[1]);
        const IColumn * poly_col = poly.column.get();
        const ColumnConst * const_poly_col = checkAndGetColumn<ColumnConst>(poly_col);

        bool point_is_const = const_tuple_col != nullptr;
        bool poly_is_const = const_poly_col != nullptr;

        if (poly_is_const)
        {
            Polygon polygon;
            parseConstPolygon(block, arguments, polygon);

            using Pool = ObjectPoolMap<PointInConstPolygonImpl, UInt128>;
            /// C++11 has thread-safe function-local statics.
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
                block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(input_rows_count, is_in);
            }
            else
            {
                block.getByPosition(result).column = pointInPolygon(*tuple_columns[0], *tuple_columns[1], *impl);
            }
        }
        else
        {
            if (arguments.size() != 2)
                throw Exception("Multi-argument version of function " + getName() + " works only with const polygon",
                    ErrorCodes::BAD_ARGUMENTS);

            auto res_column = ColumnVector<UInt8>::create(input_rows_count);
            auto & data = res_column->getData();

            Polygon polygon;

            if (isTwoDimensionalArray(*block.getByPosition(arguments[1]).type))
            {
                std::cerr << "2d\n";
                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    polygon.clear();
                    parsePolygonFromSingleColumn2D(*poly_col, i, polygon);

                    PointInNonConstPolygonImpl impl(polygon);
                    size_t point_index = point_is_const ? 0 : i;
                    data[i] = impl.contains(tuple_columns[0]->getFloat64(point_index), tuple_columns[1]->getFloat64(point_index));
                }
            }
            else
            {
                std::cerr << "1d\n";
                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    polygon.clear();
                    parsePolygonFromSingleColumn1D(*poly_col, i, polygon);

                    PointInNonConstPolygonImpl impl(polygon);
                    size_t point_index = point_is_const ? 0 : i;
                    data[i] = impl.contains(tuple_columns[0]->getFloat64(point_index), tuple_columns[1]->getFloat64(point_index));
                }
            }

            block.getByPosition(result).column = std::move(res_column);
        }
    }

private:
    bool validate;

    std::string getMessagePrefix(size_t i) const
    {
        return "Argument " + toString(i + 1) + " for function " + getName();
    }

    void parseConstPolygonFromSingleColumn(Block & block, const ColumnNumbers & arguments, Polygon & out_polygon) const
    {
        const auto & poly = *block.getByPosition(arguments[1]).column;
        const ColumnConst & column_const = typeid_cast<const ColumnConst &>(poly);
        const IColumn & column_const_data = column_const.getDataColumn();

        if (isTwoDimensionalArray(*block.getByPosition(arguments[1]).type))
            parsePolygonFromSingleColumn2D(column_const_data, 0, out_polygon);
        else
            parsePolygonFromSingleColumn1D(column_const_data, 0, out_polygon);
    }

    template <typename T>
    void parsePolygonPart(const IColumn & x_column, const IColumn & y_column, size_t begin, size_t end, T & out_container) const
    {
        for (size_t i = begin; i < end; ++i)
            out_container.emplace_back(x_column.getFloat64(i), y_column.getFloat64(i));
    }

    void parsePolygonFromSingleColumn1D(const IColumn & column, size_t i, Polygon & out_polygon) const
    {
        const auto & array_col = static_cast<const ColumnArray &>(column);
        const auto & tuple_columns = static_cast<const ColumnTuple &>(array_col.getData()).getColumns();

        size_t begin = array_col.getOffsets()[i - 1];
        size_t end = array_col.getOffsets()[i];

        parsePolygonPart(*tuple_columns[0], *tuple_columns[1], begin, end, out_polygon.outer());
    }

    void parsePolygonFromSingleColumn2D(const IColumn & column, size_t i, Polygon & out_polygon) const
    {
        const auto & array_col = static_cast<const ColumnArray &>(column);
        const auto & nested_array_col = static_cast<const ColumnArray &>(array_col.getData());
        const auto & tuple_columns = static_cast<const ColumnTuple &>(nested_array_col.getData()).getColumns();

        size_t rings_begin = array_col.getOffsets()[i - 1];
        size_t rings_end = array_col.getOffsets()[i];

        for (size_t j = rings_begin; j < rings_end; ++j)
        {
            size_t begin = nested_array_col.getOffsets()[j - 1];
            size_t end = nested_array_col.getOffsets()[j];

            if (out_polygon.outer().empty())
            {
                parsePolygonPart(*tuple_columns[0], *tuple_columns[1], begin, end, out_polygon.outer());
            }
            else
            {
                out_polygon.inners().emplace_back();
                parsePolygonPart(*tuple_columns[0], *tuple_columns[1], begin, end, out_polygon.inners().back());
            }
        }
    }

    void parseConstPolygonFromMultipleColumns(Block & block, const ColumnNumbers & arguments, Polygon & out_polygon) const
    {
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

            if (!out_polygon.outer().empty())
                out_polygon.inners().emplace_back();

            auto & container = out_polygon.outer().empty() ? out_polygon.outer() : out_polygon.inners().back();

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
    }

    bool isTwoDimensionalArray(const IDataType & type) const
    {
        return WhichDataType(type).isArray()
            && WhichDataType(static_cast<const DataTypeArray &>(type).getNestedType()).isArray();
    }

    void parseConstPolygon(Block & block, const ColumnNumbers & arguments, Polygon & out_polygon) const
    {
        if (arguments.size() == 2)
            parseConstPolygonFromSingleColumn(block, arguments, out_polygon);
        else
            parseConstPolygonFromMultipleColumns(block, arguments, out_polygon);

        boost::geometry::correct(out_polygon);

#if !defined(__clang_analyzer__) /// It does not like boost.
        if (validate)
        {
            std::string failure_message;
            auto is_valid = boost::geometry::is_valid(out_polygon, failure_message);
            if (!is_valid)
                throw Exception("Polygon is not valid: " + failure_message, ErrorCodes::BAD_ARGUMENTS);
        }
#endif
    }
};


void registerFunctionPointInPolygon(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPointInPolygon<PointInPolygonWithGrid<Float64>, PointInPolygonTrivial<Float64>>>();
}

}
