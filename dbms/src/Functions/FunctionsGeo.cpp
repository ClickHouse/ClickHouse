#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsGeo.h>
#include <Functions/GeoUtils.h>
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
        auto ptr = std::make_unique<PointInPolygonImpl>(polygon);

        /// To allocate memory.
        ptr->init();

        ProfileEvents::increment(ProfileEvents::PolygonsAddedToPool);
        ProfileEvents::increment(ProfileEvents::PolygonsInPoolAllocatedBytes, ptr->getAllocatedBytes());

        return ptr.release();
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

template <template <typename> typename PointInPolygonImpl, bool use_object_pool = false>
class FunctionPointInPolygon : public IFunction
{
public:

    template <typename Type>
    using Point = boost::geometry::model::d2::point_xy<Type>;
    template <typename Type>
    using Polygon = boost::geometry::model::polygon<Point<Type>, false>;
    template <typename Type>
    using Box = boost::geometry::model::box<Point<Type>>;

    static const char * name;

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionPointInPolygon<PointInPolygonImpl, use_object_pool>>();
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

        auto getMsgPrefix = [this](size_t i) { return "Argument " + toString(i + 1) + " for function " + getName(); };

        for (size_t i = 1; i < arguments.size(); ++i)
        {
            auto * array = checkAndGetDataType<DataTypeArray>(arguments[i].get());
            if (array == nullptr && i != 1)
                throw Exception(getMsgPrefix(i) + " must be array of tuples.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            auto * tuple = checkAndGetDataType<DataTypeTuple>(array ? array->getNestedType().get() : arguments[i].get());
            if (tuple == nullptr)
                throw Exception(getMsgPrefix(i) + " must contains tuple.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const DataTypes & elements = tuple->getElements();

            if (elements.size() != 2)
                throw Exception(getMsgPrefix(i) + " must have exactly two elements.", ErrorCodes::BAD_ARGUMENTS);

            for (auto j : ext::range(0, elements.size()))
            {
                if (!isNumber(elements[j]))
                {
                    throw Exception(getMsgPrefix(i) + " must contains numeric tuple at position " + toString(j + 1),
                                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                }
            }
        }

        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {

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

        const auto & tuple_columns = tuple_col->getColumns();
        const DataTypes & tuple_types = typeid_cast<const DataTypeTuple &>(*block.getByPosition(arguments[0]).type).getElements();

        bool use_float64 = WhichDataType(tuple_types[0]).isFloat64() || WhichDataType(tuple_types[1]).isFloat64();

        auto & result_column = block.safeGetByPosition(result).column;

        if (use_float64)
            result_column = executeForType<Float64>(*tuple_columns[0], *tuple_columns[1], block, arguments);
        else
            result_column = executeForType<Float32>(*tuple_columns[0], *tuple_columns[1], block, arguments);

        if (const_tuple_col)
            result_column = ColumnConst::create(result_column, const_tuple_col->size());
    }

private:

    Float64 getCoordinateFromField(const Field & field)
    {
        switch (field.getType())
        {
            case Field::Types::Float64:
                return field.get<Float64>();
            case Field::Types::Int64:
                return field.get<Int64>();
            case Field::Types::UInt64:
                return field.get<UInt64>();
            default:
            {
                std::string msg = "Expected numeric field, but got ";
                throw Exception(msg + Field::Types::toString(field.getType()), ErrorCodes::LOGICAL_ERROR);
            }
        }
    }

    template <typename Type>
    ColumnPtr executeForType(const IColumn & x, const IColumn & y, Block & block, const ColumnNumbers & arguments)
    {
        Polygon<Type> polygon;

        auto getMsgPrefix = [this](size_t i) { return "Argument " + toString(i + 1) + " for function " + getName(); };

        for (size_t i = 1; i < arguments.size(); ++i)
        {
            auto const_col = checkAndGetColumn<ColumnConst>(block.getByPosition(arguments[i]).column.get());
            auto array_col = const_col ? checkAndGetColumn<ColumnArray>(&const_col->getDataColumn()) : nullptr;
            auto tuple_col = array_col ? checkAndGetColumn<ColumnTuple>(&array_col->getData()) : nullptr;

            if (!tuple_col)
                throw Exception(getMsgPrefix(i) + " must be constant array of tuples.", ErrorCodes::ILLEGAL_COLUMN);

            const auto & tuple_columns = tuple_col->getColumns();
            const auto & column_x = tuple_columns[0];
            const auto & column_y = tuple_columns[1];

            if (!polygon.outer().empty())
                polygon.inners().emplace_back();

            auto & container = polygon.outer().empty() ? polygon.outer() : polygon.inners().back();

            auto size = column_x->size();

            if (size == 0)
                throw Exception(getMsgPrefix(i) + " shouldn't be empty.", ErrorCodes::ILLEGAL_COLUMN);

            for (auto j : ext::range(0, size))
            {
                Type x_coord = getCoordinateFromField((*column_x)[j]);
                Type y_coord = getCoordinateFromField((*column_y)[j]);
                container.push_back(Point<Type>(x_coord, y_coord));
            }

            /// Polygon assumed to be closed. Allow user to escape repeating of first point.
            if (!boost::geometry::equals(container.front(), container.back()))
                container.push_back(container.front());
        }

        auto callImpl = use_object_pool
            ? FunctionPointInPolygonDetail::callPointInPolygonImplWithPool<Polygon<Type>, PointInPolygonImpl<Type>>
            : FunctionPointInPolygonDetail::callPointInPolygonImpl<Polygon<Type>, PointInPolygonImpl<Type>>;

        return callImpl(x, y, polygon);
    }

};

const size_t GEOHASH_MAX_TEXT_LENGTH = 16;

// geohashEncode(lon float32/64, lat float32/64, length UInt8) => string
class FunctionGeohashEncode : public IFunction
{
public:
    static constexpr auto name = "geohashEncode";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionGeohashEncode>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {2}; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        validateArgumentType(*this, arguments, 0, isFloat, "float");
        validateArgumentType(*this, arguments, 1, isFloat, "float");
        if (arguments.size() == 3)
        {
            validateArgumentType(*this, arguments, 2, isInteger, "integer");
        }
        if (arguments.size() > 3)
        {
            throw Exception("Too many arguments for function " + getName() +
                            " expected at most 3",
                            ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION);
        }

        return std::make_shared<DataTypeString>();
    }

    template <typename LonType, typename LatType>
    bool tryExecute(const IColumn * lon_column, const IColumn * lat_column, UInt64 precision_value, ColumnPtr & result)
    {
        const ColumnVector<LonType> * longitude = checkAndGetColumn<ColumnVector<LonType>>(lon_column);
        const ColumnVector<LatType> * latitude = checkAndGetColumn<ColumnVector<LatType>>(lat_column);
        if (!latitude || !longitude)
            return false;

        auto col_str = ColumnString::create();
        ColumnString::Chars & out_vec = col_str->getChars();
        ColumnString::Offsets & out_offsets = col_str->getOffsets();

        const size_t size = lat_column->size();

        out_offsets.resize(size);
        out_vec.resize(size * (GEOHASH_MAX_TEXT_LENGTH + 1));

        char * begin = reinterpret_cast<char *>(out_vec.data());
        char * pos = begin;

        for (size_t i = 0; i < size; ++i)
        {
            const Float64 longitude_value = longitude->getElement(i);
            const Float64 latitude_value = latitude->getElement(i);

            const size_t encoded_size = GeoUtils::geohashEncode(longitude_value, latitude_value, precision_value, pos);

            pos += encoded_size;
            *pos = '\0';
            out_offsets[i] = ++pos - begin;
        }
        out_vec.resize(pos - begin);

        if (!out_offsets.empty() && out_offsets.back() != out_vec.size())
            throw Exception("Column size mismatch (internal logical error)", ErrorCodes::LOGICAL_ERROR);

        result = std::move(col_str);

        return true;

    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const IColumn * longitude = block.getByPosition(arguments[0]).column.get();
        const IColumn * latitude = block.getByPosition(arguments[1]).column.get();

        const UInt64 precision_value = std::min(GEOHASH_MAX_TEXT_LENGTH,
                arguments.size() == 3 ? block.getByPosition(arguments[2]).column->get64(0) : GEOHASH_MAX_TEXT_LENGTH);

        ColumnPtr & res_column = block.getByPosition(result).column;

        if (tryExecute<Float32, Float32>(longitude, latitude, precision_value, res_column) ||
            tryExecute<Float64, Float32>(longitude, latitude, precision_value, res_column) ||
            tryExecute<Float32, Float64>(longitude, latitude, precision_value, res_column) ||
            tryExecute<Float64, Float64>(longitude, latitude, precision_value, res_column))
            return;

        const char sep[] = ", ";
        std::string arguments_description = "";
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            arguments_description += block.getByPosition(arguments[i]).column->getName() + sep;
        }
        if (arguments_description.size() > sizeof(sep))
        {
            arguments_description.erase(arguments_description.size() - sizeof(sep) - 1);
        }

        throw Exception("Unsupported argument types: " + arguments_description +
                        + " for function " + getName(),
                        ErrorCodes::ILLEGAL_COLUMN);
    }
};

// geohashDecode(string) => (lon float64, lat float64)
class FunctionGeohashDecode : public IFunction
{
public:
    static constexpr auto name = "geohashDecode";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionGeohashDecode>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        validateArgumentType(*this, arguments, 0, isStringOrFixedString, "string or fixed string");

        return std::make_shared<DataTypeTuple>(
                DataTypes{std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()},
                Strings{"longitude", "latitude"});
    }

    template <typename ColumnTypeEncoded>
    bool tryExecute(const IColumn * encoded_column, ColumnPtr & result_column)
    {
        const auto * encoded = checkAndGetColumn<ColumnTypeEncoded>(encoded_column);
        if (!encoded)
            return false;

        const size_t count = encoded->size();

        auto latitude = ColumnFloat64::create(count);
        auto longitude = ColumnFloat64::create(count);

        ColumnFloat64::Container & lon_data = longitude->getData();
        ColumnFloat64::Container & lat_data = latitude->getData();

        for (size_t i = 0; i < count; ++i)
        {
            StringRef encoded_string = encoded->getDataAt(i);
            GeoUtils::geohashDecode(encoded_string.data, encoded_string.size, &lon_data[i], &lat_data[i]);
        }

        MutableColumns result;
        result.emplace_back(std::move(longitude));
        result.emplace_back(std::move(latitude));
        result_column = ColumnTuple::create(std::move(result));

        return true;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        const IColumn * encoded = block.getByPosition(arguments[0]).column.get();
        ColumnPtr & res_column = block.getByPosition(result).column;

        if (tryExecute<ColumnString>(encoded, res_column) ||
            tryExecute<ColumnFixedString>(encoded, res_column))
            return;

        throw Exception("Unsupported argument type:" + block.getByPosition(arguments[0]).column->getName()
                        + " of argument of function " + getName(),
                        ErrorCodes::ILLEGAL_COLUMN);
    }
};

template <typename Type>
using Point = boost::geometry::model::d2::point_xy<Type>;

template <typename Type>
using PointInPolygonWithGrid = GeoUtils::PointInPolygonWithGrid<Type>;

template <>
const char * FunctionPointInPolygon<PointInPolygonWithGrid, true>::name = "pointInPolygon";

void registerFunctionsGeo(FunctionFactory & factory)
{
    factory.registerFunction<FunctionGreatCircleDistance>();
    factory.registerFunction<FunctionPointInEllipses>();

    factory.registerFunction<FunctionPointInPolygon<PointInPolygonWithGrid, true>>();
    factory.registerFunction<FunctionGeohashEncode>();
    factory.registerFunction<FunctionGeohashDecode>();
}
}
