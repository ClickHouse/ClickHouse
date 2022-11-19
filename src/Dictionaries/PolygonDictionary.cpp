#include "PolygonDictionary.h"

#include <numeric>
#include <cmath>

#include <base/sort.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionarySource.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int BAD_ARGUMENTS;
    extern const int UNSUPPORTED_METHOD;
}


IPolygonDictionary::IPolygonDictionary(
        const StorageID & dict_id_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_,
        const DictionaryLifetime dict_lifetime_,
        Configuration configuration_)
        : IDictionary(dict_id_)
        , dict_struct(dict_struct_)
        , source_ptr(std::move(source_ptr_))
        , dict_lifetime(dict_lifetime_)
        , configuration(configuration_)
{
    setup();
    loadData();
    calculateBytesAllocated();
}

void IPolygonDictionary::convertKeyColumns(Columns & key_columns, DataTypes & key_types) const
{
    if (key_columns.size() != 2)
        throw Exception(ErrorCodes::TYPE_MISMATCH,
            "Dictionary {} key lookup structure does not match, expected two columns of coordinates with type Float64",
            getFullName());

    auto float_64_type = std::make_shared<DataTypeFloat64>();
    size_t key_types_size = key_types.size();
    for (size_t key_type_index = 0; key_type_index < key_types_size; ++key_type_index)
    {
        auto & key_type = key_types[key_type_index];
        if (float_64_type->equals(*key_type))
            continue;

        auto & key_column_to_cast = key_columns[key_type_index];
        ColumnWithTypeAndName column_to_cast = {key_column_to_cast, key_type, ""};
        auto casted_column = castColumnAccurate(column_to_cast, float_64_type);
        key_column_to_cast = std::move(casted_column);
        key_type = float_64_type;
    }
}

ColumnPtr IPolygonDictionary::getColumn(
    const std::string & attribute_name,
    const DataTypePtr & result_type,
    const Columns & key_columns,
    const DataTypes &,
    const ColumnPtr & default_values_column) const
{
    const auto requested_key_points = extractPoints(key_columns);

    const auto & attribute = dict_struct.getAttribute(attribute_name, result_type);
    DefaultValueProvider default_value_provider(attribute.null_value, default_values_column);

    size_t attribute_index = dict_struct.attribute_name_to_index.find(attribute_name)->second;
    const auto & attribute_values_column = attributes_columns[attribute_index];

    auto result = attribute_values_column->cloneEmpty();
    result->reserve(requested_key_points.size());

    if (unlikely(attribute.is_nullable))
    {
        getItemsImpl<Field>(
            requested_key_points,
            [&](size_t row) { return (*attribute_values_column)[row]; },
            [&](Field & value) { result->insert(value); },
            default_value_provider);
    }
    else
    {
        auto type_call = [&](const auto & dictionary_attribute_type)
        {
            using Type = std::decay_t<decltype(dictionary_attribute_type)>;
            using AttributeType = typename Type::AttributeType;
            using ValueType = DictionaryValueType<AttributeType>;
            using ColumnProvider = DictionaryAttributeColumnProvider<AttributeType>;
            using ColumnType = typename ColumnProvider::ColumnType;

            const auto attribute_values_column_typed = typeid_cast<const ColumnType *>(attribute_values_column.get());
            if (!attribute_values_column_typed)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "An attribute type should be same as dictionary type");

            ColumnType & result_column_typed = static_cast<ColumnType &>(*result);

            if constexpr (std::is_same_v<ValueType, Array>)
            {
                getItemsImpl<ValueType>(
                    requested_key_points,
                    [&](size_t row) { return (*attribute_values_column)[row].get<Array>(); },
                    [&](Array & value) { result_column_typed.insert(value); },
                    default_value_provider);
            }
            else if constexpr (std::is_same_v<ValueType, StringRef>)
            {
                getItemsImpl<ValueType>(
                    requested_key_points,
                    [&](size_t row) { return attribute_values_column->getDataAt(row); },
                    [&](StringRef value) { result_column_typed.insertData(value.data, value.size); },
                    default_value_provider);
            }
            else
            {
                auto & attribute_data = attribute_values_column_typed->getData();
                auto & result_data = result_column_typed.getData();

                getItemsImpl<ValueType>(
                    requested_key_points,
                    [&](size_t row) { return attribute_data[row]; },
                    [&](auto value) { result_data.emplace_back(value); },
                    default_value_provider);
            }
        };

        callOnDictionaryAttributeType(attribute.underlying_type, type_call);
    }

    return result;
}

Pipe IPolygonDictionary::read(const Names & column_names, size_t, size_t) const
{
    if (!configuration.store_polygon_key_column)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Set `store_polygon_key_column` setting in dictionary configuration to true to support reading from PolygonDictionary.");

    const auto & dictionary_structure_keys = *dict_struct.key;
    const auto & dictionary_key_attribute = dictionary_structure_keys[0];

    ColumnsWithTypeAndName result_columns;
    result_columns.reserve(column_names.size());

    for (const auto & column_name : column_names)
    {
        ColumnWithTypeAndName column_with_type;

        if (column_name == dictionary_key_attribute.name)
        {
            column_with_type.column = key_attribute_column;
            column_with_type.type = dictionary_key_attribute.type;
        }
        else
        {
            const auto & dictionary_attribute = dict_struct.getAttribute(column_name);
            size_t attribute_index = dict_struct.attribute_name_to_index.find(dictionary_attribute.name)->second;

            column_with_type.column = attributes_columns[attribute_index];
            column_with_type.type = dictionary_attribute.type;
        }

        column_with_type.name = column_name;

        result_columns.emplace_back(column_with_type);
    }

    auto source = std::make_shared<SourceFromSingleChunk>(Block(result_columns));
    return Pipe(std::move(source));
}

void IPolygonDictionary::setup()
{
    const auto & dictionary_structure_keys = *dict_struct.key;
    key_attribute_column = dictionary_structure_keys[0].type->createColumn();

    attributes_columns.reserve(dict_struct.attributes.size());

    for (const auto & attribute : dict_struct.attributes)
    {
        auto column = attribute.type->createColumn();
        attributes_columns.emplace_back(std::move(column));

        if (attribute.hierarchical)
            throw Exception(ErrorCodes::TYPE_MISMATCH,
                            "{}: hierarchical attributes not supported for dictionary of polygonal type",
                            getDictionaryID().getNameForLogs());
    }
}

void IPolygonDictionary::blockToAttributes(const DB::Block & block)
{
    const auto rows = block.rows();

    size_t skip_key_column_offset = 1;
    for (size_t i = 0; i < attributes_columns.size(); ++i)
    {
        const auto & block_column = block.safeGetByPosition(i + skip_key_column_offset);
        const auto & column = block_column.column;

        attributes_columns[i]->assumeMutable()->insertRangeFrom(*column, 0, column->size());
    }

    /** Multi-polygons could cause bigger sizes, but this is better than nothing. */
    polygons.reserve(polygons.size() + rows);
    polygon_index_to_attribute_value_index.reserve(polygon_index_to_attribute_value_index.size() + rows);

    const auto & key_column = block.safeGetByPosition(0).column;

    if (configuration.store_polygon_key_column)
        key_attribute_column->assumeMutable()->insertRangeFrom(*key_column, 0, key_column->size());

    extractPolygons(key_column);
}

void IPolygonDictionary::loadData()
{
    QueryPipeline pipeline(source_ptr->loadAll());

    PullingPipelineExecutor executor(pipeline);
    Block block;
    while (executor.pull(block))
        blockToAttributes(block);

    /// Correct and sort polygons by area and update polygon_index_to_attribute_value_index after sort
    PaddedPODArray<double> areas;
    areas.resize_fill(polygons.size());

    std::vector<std::pair<Polygon, size_t>> polygon_ids;
    polygon_ids.reserve(polygons.size());

    for (size_t i = 0; i < polygons.size(); ++i)
    {
        auto & polygon = polygons[i];
        bg::correct(polygon);

        areas[i] = bg::area(polygon);
        polygon_ids.emplace_back(polygon, i);
    }

    ::sort(polygon_ids.begin(), polygon_ids.end(), [& areas](const auto & lhs, const auto & rhs)
    {
        return areas[lhs.second] < areas[rhs.second];
    });

    std::vector<size_t> correct_ids;
    correct_ids.reserve(polygon_ids.size());

    for (size_t i = 0; i < polygon_ids.size(); ++i)
    {
        auto & polygon = polygon_ids[i];
        correct_ids.emplace_back(polygon_index_to_attribute_value_index[polygon.second]);
        polygons[i] = polygon.first;
    }

    polygon_index_to_attribute_value_index = std::move(correct_ids);
}

void IPolygonDictionary::calculateBytesAllocated()
{
    /// Index allocated by subclass not counted because it take a small part in relation to attributes and polygons

    if (configuration.store_polygon_key_column)
        bytes_allocated += key_attribute_column->allocatedBytes();

    for (const auto & column : attributes_columns)
        bytes_allocated += column->allocatedBytes();

    for (auto & polygon : polygons)
        bytes_allocated += bg::num_points(polygon) * sizeof(Point);
}

std::vector<IPolygonDictionary::Point> IPolygonDictionary::extractPoints(const Columns & key_columns)
{
    if (key_columns.size() != 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected two columns of coordinates with type Float64");

    const auto * column_x = typeid_cast<const ColumnVector<Float64>*>(key_columns[0].get());
    const auto * column_y = typeid_cast<const ColumnVector<Float64>*>(key_columns[1].get());

    if (!column_x || !column_y)
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected columns of Float64");

    const auto rows = key_columns.front()->size();

    std::vector<Point> result;
    result.reserve(rows);

    for (size_t row = 0; row < rows; ++row)
    {
        auto x = column_x->getElement(row);
        auto y = column_y->getElement(row);

        if (isNaN(x) || isNaN(y))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "PolygonDictionary input point component must not be NaN");

        if (std::isinf(x) || std::isinf(y))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "PolygonDictionary input point component must not be infinite");

        result.emplace_back(x, y);
    }

    return result;
}

ColumnUInt8::Ptr IPolygonDictionary::hasKeys(const Columns & key_columns, const DataTypes &) const
{
    std::vector<IPolygonDictionary::Point> points = extractPoints(key_columns);

    auto result = ColumnUInt8::create(points.size());
    auto & out = result->getData();

    size_t keys_found = 0;

    for (size_t i = 0; i < points.size(); ++i)
    {
        size_t unused_find_result = 0;
        auto & point = points[i];
        out[i] = find(point, unused_find_result);
        keys_found += out[i];
    }

    query_count.fetch_add(points.size(), std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);

    return result;
}

template <typename AttributeType, typename ValueGetter, typename ValueSetter, typename DefaultValueExtractor>
void IPolygonDictionary::getItemsImpl(
    const std::vector<IPolygonDictionary::Point> & requested_key_points,
    ValueGetter && get_value,
    ValueSetter && set_value,
    DefaultValueExtractor & default_value_extractor) const
{
    size_t polygon_index = 0;
    size_t keys_found = 0;

    for (size_t requested_key_index = 0; requested_key_index < requested_key_points.size(); ++requested_key_index)
    {
        const auto found = find(requested_key_points[requested_key_index], polygon_index);

        if (found)
        {
            size_t attribute_values_index = polygon_index_to_attribute_value_index[polygon_index];
            auto value = get_value(attribute_values_index);
            set_value(value);
            ++keys_found;
        }
        else
        {
            Field default_value = default_value_extractor.getDefaultValue(requested_key_index);

            if constexpr (std::is_same_v<AttributeType, Field>)
            {
                set_value(default_value);
            }
            else if constexpr (std::is_same_v<AttributeType, Array>)
            {
                set_value(default_value.get<Array>());
            }
            else if constexpr (std::is_same_v<AttributeType, StringRef>)
            {
                auto default_value_string = default_value.get<String>();
                set_value(default_value_string);
            }
            else
            {
                set_value(default_value.get<NearestFieldType<AttributeType>>());
            }
        }
    }

    query_count.fetch_add(requested_key_points.size(), std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);
}

namespace
{

struct Offset
{
    Offset() = default;

    IColumn::Offsets ring_offsets;
    IColumn::Offsets polygon_offsets;
    IColumn::Offsets multi_polygon_offsets;

    IColumn::Offset points_added = 0;
    IColumn::Offset current_ring = 0;
    IColumn::Offset current_polygon = 0;
    IColumn::Offset current_multi_polygon = 0;

    Offset& operator++()
    {
        ++points_added;
        if (points_added <= ring_offsets[current_ring])
            return *this;

        ++current_ring;
        if (current_ring < polygon_offsets[current_polygon])
            return *this;

        ++current_polygon;
        if (current_polygon < multi_polygon_offsets[current_multi_polygon])
            return *this;

        ++current_multi_polygon;
        return *this;
    }

    bool atLastPolygonOfMultiPolygon() { return current_polygon + 1 == multi_polygon_offsets[current_multi_polygon]; }
    bool atLastRingOfPolygon() { return current_ring + 1 == polygon_offsets[current_polygon]; }
    bool atLastPointOfRing() { return points_added == ring_offsets[current_ring]; }

    bool allRingsHaveAPositiveArea()
    {
        IColumn::Offset prev_offset = 0;
        for (const auto offset : ring_offsets)
        {
            if (offset - prev_offset < 3)
                return false;
            prev_offset = offset;
        }
        return true;
    }
};

struct Data
{
    std::vector<IPolygonDictionary::Polygon> & dest;
    std::vector<size_t> & ids;

    void addPolygon(bool new_multi_polygon = false)
    {
        dest.emplace_back();
        ids.push_back((ids.empty() ? 0 : ids.back() + new_multi_polygon));
    }

    void addPoint(IPolygonDictionary::Coord x, IPolygonDictionary::Coord y)
    {
        auto & last_polygon = dest.back();
        auto & last_ring = (last_polygon.inners().empty() ? last_polygon.outer() : last_polygon.inners().back());
        last_ring.emplace_back(x, y);
    }
};

void addNewPoint(IPolygonDictionary::Coord x, IPolygonDictionary::Coord y, Data & data, Offset & offset)
{
    if (offset.atLastPointOfRing())
    {
        if (offset.atLastRingOfPolygon())
            data.addPolygon(offset.atLastPolygonOfMultiPolygon());
        else
        {
            /** An outer ring is added automatically with a new polygon, thus we need the else statement here.
             *  This also implies that if we are at this point we have to add an inner ring.
             */
            auto & last_polygon = data.dest.back();
            last_polygon.inners().emplace_back();
        }
    }
    data.addPoint(x, y);
    ++offset;
}

const IColumn * unrollMultiPolygons(const ColumnPtr & column, Offset & offset)
{
    const auto * ptr_multi_polygons = typeid_cast<const ColumnArray*>(column.get());
    if (!ptr_multi_polygons)
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected a column containing arrays of polygons");
    offset.multi_polygon_offsets.assign(ptr_multi_polygons->getOffsets());

    const auto * ptr_polygons = typeid_cast<const ColumnArray*>(&ptr_multi_polygons->getData());
    if (!ptr_polygons)
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected a column containing arrays of rings when reading polygons");
    offset.polygon_offsets.assign(ptr_polygons->getOffsets());

    const auto * ptr_rings = typeid_cast<const ColumnArray*>(&ptr_polygons->getData());
    if (!ptr_rings)
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected a column containing arrays of points when reading rings");
    offset.ring_offsets.assign(ptr_rings->getOffsets());

    return ptr_rings->getDataPtr().get();
}

const IColumn * unrollSimplePolygons(const ColumnPtr & column, Offset & offset)
{
    const auto * ptr_polygons = typeid_cast<const ColumnArray*>(column.get());
    if (!ptr_polygons)
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected a column containing arrays of points");
    offset.ring_offsets.assign(ptr_polygons->getOffsets());
    std::iota(offset.polygon_offsets.begin(), offset.polygon_offsets.end(), 1);
    offset.multi_polygon_offsets.assign(offset.polygon_offsets);

    return ptr_polygons->getDataPtr().get();
}

void handlePointsReprByArrays(const IColumn * column, Data & data, Offset & offset)
{
    const auto * ptr_points = typeid_cast<const ColumnArray*>(column);
    const auto * ptr_coord = typeid_cast<const ColumnVector<Float64>*>(&ptr_points->getData());
    if (!ptr_coord)
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected coordinates to be of type Float64");
    const auto & offsets = ptr_points->getOffsets();
    IColumn::Offset prev_offset = 0;
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        if (offsets[i] - prev_offset != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "All points should be two-dimensional");
        prev_offset = offsets[i];
        addNewPoint(ptr_coord->getElement(2 * i), ptr_coord->getElement(2 * i + 1), data, offset);
    }
}

void handlePointsReprByTuples(const IColumn * column, Data & data, Offset & offset)
{
    const auto * ptr_points = typeid_cast<const ColumnTuple*>(column);
    if (!ptr_points)
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected a column of tuples representing points");
    if (ptr_points->tupleSize() != 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Points should be two-dimensional");
    const auto * column_x = typeid_cast<const ColumnVector<Float64>*>(&ptr_points->getColumn(0));
    const auto * column_y = typeid_cast<const ColumnVector<Float64>*>(&ptr_points->getColumn(1));
    if (!column_x || !column_y)
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected coordinates to be of type Float64");
    for (size_t i = 0; i < column_x->size(); ++i)
    {
        addNewPoint(column_x->getElement(i), column_y->getElement(i), data, offset);
    }
}

}

void IPolygonDictionary::extractPolygons(const ColumnPtr & column)
{
    Data data = {polygons, polygon_index_to_attribute_value_index};
    Offset offset;

    const IColumn * points_collection = nullptr;
    switch (configuration.input_type)
    {
        case InputType::MultiPolygon:
            points_collection = unrollMultiPolygons(column, offset);
            break;
        case InputType::SimplePolygon:
            points_collection = unrollSimplePolygons(column, offset);
            break;
    }

    if (!offset.allRingsHaveAPositiveArea())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Every ring included in a polygon or excluded from it should contain at least 3 points");

    /** Adding the first empty polygon */
    data.addPolygon(true);

    switch (configuration.point_type)
    {
        case PointType::Array:
            handlePointsReprByArrays(points_collection, data, offset);
            break;
        case PointType::Tuple:
            handlePointsReprByTuples(points_collection, data, offset);
            break;
    }
}

}
