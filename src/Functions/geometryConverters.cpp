#include <Functions/geometryConverters.h>
#include <DataTypes/DataTypeCustomGeo.h>

#include <common/logger_useful.h>

namespace DB {

namespace {

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

template <typename Geometry>
class Getter : public boost::static_visitor<void>
{
public:
    constexpr Getter(Geometry & container_, size_t i_)
        : container(container_)
        , i(i_)
    {}

    template <class T>
    void operator()(const T & parser) const
    {
        parser.get(container, i);
    }

private:
    Geometry & container;
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
    return Parser(std::move(casted));
}

}

CartesianGeometryFromColumnParser makeCartesianGeometryFromColumnParser(const ColumnWithTypeAndName & col)
{
    switch (getArrayDepth(col.type, 3))
    {
        case 0: return makeParser<DataTypeCustomPointSerialization, PointFromColumnParser<CartesianPoint>>(col);
        case 1: return makeParser<DataTypeCustomRingSerialization, CartesianRingFromColumnParser>(col);
        case 2: return makeParser<DataTypeCustomPolygonSerialization, CartesianPolygonFromColumnParser>(col);
        case 3: return makeParser<DataTypeCustomMultiPolygonSerialization, CartesianMultiPolygonFromColumnParser>(col);
        default: throw Exception("Cannot parse geometry from column with type " + col.type->getName()
                + ", array depth is too big", ErrorCodes::ILLEGAL_COLUMN);
    }
}

CartesianGeometry createContainer(const CartesianGeometryFromColumnParser & parser)
{
    static ContainerCreator<CartesianGeometry> creator;
    return boost::apply_visitor(creator, parser);
}

void get(const CartesianGeometryFromColumnParser & parser, CartesianGeometry & container, size_t i)
{
    boost::apply_visitor(Getter(container, i), parser);
}

GeographicGeometryFromColumnParser makeGeographicGeometryFromColumnParser(const ColumnWithTypeAndName & col)
{
    switch (getArrayDepth(col.type, 3))
    {
        case 0: return makeParser<DataTypeCustomPointSerialization, PointFromColumnParser<GeographicPoint>>(col);
        case 1: return makeParser<DataTypeCustomRingSerialization, GeographicRingFromColumnParser>(col);
        case 2: return makeParser<DataTypeCustomPolygonSerialization, GeographicPolygonFromColumnParser>(col);
        case 3: return makeParser<DataTypeCustomMultiPolygonSerialization, GeographicMultiPolygonFromColumnParser>(col);
        default: throw Exception("Cannot parse geometry from column with type " + col.type->getName()
                + ", array depth is too big", ErrorCodes::ILLEGAL_COLUMN);
    }
}

GeographicGeometry createContainer(const GeographicGeometryFromColumnParser & parser)
{
    static ContainerCreator<GeographicGeometry> creator;
    return boost::apply_visitor(creator, parser);
}

void get(const GeographicGeometryFromColumnParser & parser, GeographicGeometry & container, size_t i)
{
    boost::apply_visitor(Getter(container, i), parser);
}

}
