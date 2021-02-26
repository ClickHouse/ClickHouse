#include <Functions/geometryConverters.h>

#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <typename Point>
std::vector<Point> PointFromColumnConverter<Point>::convertImpl(size_t shift, size_t count) const
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
std::vector<Ring<Point>> RingFromColumnConverter<Point>::convert() const
{
    const IColumn::Offsets & offsets = typeid_cast<const ColumnArray &>(*col).getOffsets();
    size_t prev_offset = 0;
    std::vector<Ring<Point>> answer;
    answer.reserve(offsets.size());
    for (size_t offset : offsets)
    {
        auto tmp = point_converter.convertImpl(prev_offset, offset - prev_offset);
        answer.emplace_back(tmp.begin(), tmp.end());
        prev_offset = offset;
    }
    return answer;
}

template <typename Point>
std::vector<Polygon<Point>> PolygonFromColumnConverter<Point>::convert() const
{
    const IColumn::Offsets & offsets = typeid_cast<const ColumnArray &>(*col).getOffsets();
    std::vector<Polygon<Point>> answer(offsets.size());
    auto all_rings = ring_converter.convert();

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
std::vector<MultiPolygon<Point>> MultiPolygonFromColumnConverter<Point>::convert() const
{
    const IColumn::Offsets & offsets = typeid_cast<const ColumnArray &>(*col).getOffsets();
    size_t prev_offset = 0;
    std::vector<MultiPolygon<Point>> answer(offsets.size());

    auto all_polygons = polygon_converter.convert();

    for (size_t iter = 0; iter < offsets.size(); ++iter)
    {
        for (size_t polygon_iter = prev_offset; polygon_iter < offsets[iter]; ++polygon_iter)
            answer[iter].emplace_back(std::move(all_polygons[polygon_iter]));
        prev_offset = offsets[iter];
    }

    return answer;
}


template class PointFromColumnConverter<CartesianPoint>;
template class PointFromColumnConverter<SphericalPoint>;
template class RingFromColumnConverter<CartesianPoint>;
template class RingFromColumnConverter<SphericalPoint>;
template class PolygonFromColumnConverter<CartesianPoint>;
template class PolygonFromColumnConverter<SphericalPoint>;
template class MultiPolygonFromColumnConverter<CartesianPoint>;
template class MultiPolygonFromColumnConverter<SphericalPoint>;

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
template void checkColumnTypeOrThrow<SphericalPoint, Ring>(const ColumnWithTypeAndName &);
template void checkColumnTypeOrThrow<SphericalPoint, Polygon>(const ColumnWithTypeAndName &);
template void checkColumnTypeOrThrow<SphericalPoint, MultiPolygon>(const ColumnWithTypeAndName &);

}
