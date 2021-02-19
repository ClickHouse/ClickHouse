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

template <typename Point>
std::vector<Point> PointFromColumnParser<Point>::parse(size_t shift, size_t count) const
{
    const auto * tuple = typeid_cast<const ColumnTuple *>(col.get());
    const auto & tuple_columns = tuple->getColumns();

    const auto * x_data = typeid_cast<const ColumnFloat64 *>(tuple_columns[0].get());
    const auto * y_data = typeid_cast<const ColumnFloat64 *>(tuple_columns[1].get());

    const auto * first_container = x_data->getData().data() + shift;
    const auto * second_container = y_data->getData().data() + shift;

    std::vector<Point> answer(count);

    for (size_t i = 0; i < count; ++i)
    {
        const Float64 first = first_container[i];
        const Float64 second = second_container[i];

        if (isNaN(first) || isNaN(second))
            throw Exception("Point's component must not be NaN", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (isinf(first) || isinf(second))
            throw Exception("Point's component must not be infinite", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        answer[i] = Point(first, second);
    }

    return answer;
}

template <typename Point>
std::vector<Ring<Point>> RingFromColumnParser<Point>::parse(size_t /*shift*/, size_t /*size*/) const
{
    size_t prev_offset = 0;
    std::vector<Ring<Point>> answer;
    answer.reserve(offsets.size());
    for (size_t offset : offsets)
    {
        auto tmp = point_parser.parse(prev_offset, offset - prev_offset);
        answer.emplace_back(tmp.begin(), tmp.end());
        prev_offset = offset;
    }
    return answer;
}

template <typename Point>
std::vector<Polygon<Point>> PolygonFromColumnParser<Point>::parse(size_t /*shift*/, size_t /*size*/) const
{
    std::vector<Polygon<Point>> answer(offsets.size());
    auto all_rings = ring_parser.parse(0, 0);

    auto prev_offset = 0;
    for (size_t iter = 0; iter < offsets.size(); ++iter)
    {
        const auto current_array_size = offsets[iter] - prev_offset;
        answer[iter].outer() = std::move(all_rings[prev_offset]);
        answer[iter].inners().reserve(current_array_size);
        for (size_t inner_holes = prev_offset + 1; inner_holes < offsets[iter]; ++inner_holes)
            answer[iter].inners().emplace_back(std::move(all_rings[inner_holes]));
        prev_offset = offsets[iter];
    }

    return answer;
}


template <typename Point>
std::vector<MultiPolygon<Point>> MultiPolygonFromColumnParser<Point>::parse(size_t /*shift*/, size_t /*size*/) const
{
    size_t prev_offset = 0;
    std::vector<MultiPolygon<Point>> answer(offsets.size());

    auto all_polygons = polygon_parser.parse(0, 0);

    for (size_t iter = 0; iter < offsets.size(); ++iter)
    {
        for (size_t polygon_iter = prev_offset; polygon_iter < offsets[iter]; ++polygon_iter)
            answer[iter].emplace_back(std::move(all_polygons[polygon_iter])); 
        prev_offset = offsets[iter];
    }

    return answer;
}

template <typename ContainterWithFigures>
class ParserVisitor : public boost::static_visitor<ContainterWithFigures>
{
public:
    template <class T>
    ContainterWithFigures operator()(const T & parser) const
    {
        auto parsed = parser.parse(0, 0);
        ContainterWithFigures figures;
        figures.reserve(parsed.size());
        for (auto & value : parsed)
            figures.emplace_back(value);
        return figures;
    }
};

template <typename Point>
std::vector<Figure<Point>> parseFigure(const GeometryFromColumnParser<Point> & parser)
{
    static ParserVisitor<std::vector<Figure<Point>>> creator;
    return boost::apply_visitor(creator, parser);
}


template std::vector<Figure<CartesianPoint>> parseFigure(const GeometryFromColumnParser<CartesianPoint> &);
template std::vector<Figure<GeographicPoint>> parseFigure(const GeometryFromColumnParser<GeographicPoint> &);


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

template <typename Point>
GeometryFromColumnParser<Point> getConverterBasedOnType(const ColumnWithTypeAndName & column)
{
    if (DataTypeCustomRingSerialization::nestedDataType()->equals(*column.type))
    {
        return RingFromColumnParser<Point>(std::move(column.column->convertToFullColumnIfConst()));
    } 
    else if (DataTypeCustomPolygonSerialization::nestedDataType()->equals(*column.type)) 
    {
        return PolygonFromColumnParser<Point>(std::move(column.column->convertToFullColumnIfConst()));
    }
    else if (DataTypeCustomMultiPolygonSerialization::nestedDataType()->equals(*column.type))
    {
        return MultiPolygonFromColumnParser<Point>(std::move(column.column->convertToFullColumnIfConst()));
    }
    else
    {
        throw Exception(fmt::format("Unexpected type of column {}", column.type->getName()), ErrorCodes::BAD_ARGUMENTS); 
    }
}


template GeometryFromColumnParser<CartesianPoint> getConverterBasedOnType(const ColumnWithTypeAndName & column);
template GeometryFromColumnParser<GeographicPoint> getConverterBasedOnType(const ColumnWithTypeAndName & column);

}
