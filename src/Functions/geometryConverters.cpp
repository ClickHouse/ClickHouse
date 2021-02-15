#include <Functions/geometryConverters.h>
#include <DataTypes/DataTypeCustomGeo.h>

#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

size_t getArrayDepth(DataTypePtr data_type, size_t max_depth)
{
    size_t depth = 0;
    while (data_type && isArray(data_type) && depth != max_depth + 1)
    {
        depth++;
        data_type = static_cast<const DataTypeArray &>(*data_type).getNestedType();
    }

    return depth;
}

template <typename Geometry>
class ContainerCreator : public boost::static_visitor<Geometry>
{
public:
    template <class T>
    Geometry operator()(const T & parser) const
    {
        return parser.createContainer();
    }
};

template <typename Point>
class Getter : public boost::static_visitor<void>
{
public:
    constexpr Getter(Geometry<Point> & container_, size_t i_)
        : container(container_)
        , i(i_)
    {}

    template <class T>
    void operator()(const T & parser) const
    {
        parser.get(container, i);
    }

private:
    Geometry<Point> & container;
    size_t i;
};

template <class DataType, class Parser>
Parser makeParser(const ColumnWithTypeAndName & col)
{
    auto wanted_data_type = DataType::nestedDataType();
    ColumnPtr casted = castColumn(col, DataType::nestedDataType());
    if (!casted)
    {
        throw Exception("Failed to cast " + col.type->getName() + " to " + wanted_data_type->getName(), ErrorCodes::ILLEGAL_COLUMN);
    }
    return Parser(std::move(casted->convertToFullColumnIfConst()));
}

}

template <typename Point>
Geometry<Point> createContainer(const GeometryFromColumnParser<Point> & parser)
{
    static ContainerCreator<Geometry<Point>> creator;
    return boost::apply_visitor(creator, parser);
}

template <typename Point>
void get(const GeometryFromColumnParser<Point> & parser, Geometry<Point> & container, size_t i)
{
    boost::apply_visitor(Getter<Point>(container, i), parser);
}

template <typename Point>
GeometryFromColumnParser<Point> makeGeometryFromColumnParser(const ColumnWithTypeAndName & col)
{
    switch (getArrayDepth(col.type, 3))
    {
        case 0: return makeParser<DataTypeCustomPointSerialization, PointFromColumnParser<Point>>(col);
        case 1: return makeParser<DataTypeCustomRingSerialization, RingFromColumnParser<Point>>(col);
        case 2: return makeParser<DataTypeCustomPolygonSerialization, PolygonFromColumnParser<Point>>(col);
        case 3: return makeParser<DataTypeCustomMultiPolygonSerialization, MultiPolygonFromColumnParser<Point>>(col);
        default: throw Exception("Cannot parse geometry from column with type " + col.type->getName()
                + ", array depth is too big", ErrorCodes::ILLEGAL_COLUMN);
    }
}

/// Explicit instantiations to avoid linker errors.

template Geometry<CartesianPoint> createContainer(const GeometryFromColumnParser<CartesianPoint> &);
template Geometry<GeographicPoint> createContainer(const GeometryFromColumnParser<GeographicPoint> &);
template void get(const GeometryFromColumnParser<CartesianPoint> & parser, Geometry<CartesianPoint> & container, size_t i);
template void get(const GeometryFromColumnParser<GeographicPoint> & parser, Geometry<GeographicPoint> & container, size_t i);
template GeometryFromColumnParser<CartesianPoint> makeGeometryFromColumnParser(const ColumnWithTypeAndName & col);
template GeometryFromColumnParser<GeographicPoint> makeGeometryFromColumnParser(const ColumnWithTypeAndName & col);


template <typename Point, template<typename> typename Desired>
void checkColumnTypeOrThrow(const ColumnWithTypeAndName & column)
{
    DataTypePtr desired_type;
    if constexpr (std::is_same_v<Desired<Point>, Ring<Point>>)
        desired_type = DataTypeCustomRingSerialization::nestedDataType();
    else if constexpr (std::is_same_v<Desired<Point>, Polygon<Point>>)
        desired_type = DataTypeCustomPolygonSerialization::nestedDataType();
    else if constexpr (std::is_same_v<Desired<Point>, MultiPolygon<Point>>)
        desired_type = DataTypeCustomMultiPolygonSerialization::nestedDataType();
    else
        throw Exception("Unexpected Desired type.", ErrorCodes::LOGICAL_ERROR);

    if (!desired_type->equals(*column.type))
            throw Exception(fmt::format("Expected type {} (MultiPolygon), but got {}", desired_type->getName(), column.type->getName()), ErrorCodes::BAD_ARGUMENTS);
}

template void checkColumnTypeOrThrow<CartesianPoint, Ring>(const ColumnWithTypeAndName &);
template void checkColumnTypeOrThrow<CartesianPoint, Polygon>(const ColumnWithTypeAndName &);
template void checkColumnTypeOrThrow<CartesianPoint, MultiPolygon>(const ColumnWithTypeAndName &);
template void checkColumnTypeOrThrow<GeographicPoint, Ring>(const ColumnWithTypeAndName &);
template void checkColumnTypeOrThrow<GeographicPoint, Polygon>(const ColumnWithTypeAndName &);
template void checkColumnTypeOrThrow<GeographicPoint, MultiPolygon>(const ColumnWithTypeAndName &);

}
