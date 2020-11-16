#include "PolygonDictionary.h"
#include "DictionaryBlockInputStream.h"
#include "DictionaryFactory.h"

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>

#include <numeric>

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
        InputType input_type_,
        PointType point_type_)
        : IDictionaryBase(dict_id_)
        , dict_struct(dict_struct_)
        , source_ptr(std::move(source_ptr_))
        , dict_lifetime(dict_lifetime_)
        , input_type(input_type_)
        , point_type(point_type_)
{
    createAttributes();
    loadData();
}

std::string IPolygonDictionary::getTypeName() const
{
    return "Polygon";
}

std::string IPolygonDictionary::getKeyDescription() const
{
    return dict_struct.getKeyDescription();
}

size_t IPolygonDictionary::getBytesAllocated() const
{
    return bytes_allocated;
}

size_t IPolygonDictionary::getQueryCount() const
{
    return query_count.load(std::memory_order_relaxed);
}

double IPolygonDictionary::getHitRate() const
{
    return 1.0;
}

size_t IPolygonDictionary::getElementCount() const
{
    return element_count;
}

double IPolygonDictionary::getLoadFactor() const
{
    return 1.0;
}

const IDictionarySource * IPolygonDictionary::getSource() const
{
    return source_ptr.get();
}

const DictionaryLifetime & IPolygonDictionary::getLifetime() const
{
    return dict_lifetime;
}

const DictionaryStructure & IPolygonDictionary::getStructure() const
{
    return dict_struct;
}

bool IPolygonDictionary::isInjective(const std::string &) const
{
    return false;
}

BlockInputStreamPtr IPolygonDictionary::getBlockInputStream(const Names &, size_t) const
{
    // TODO: In order for this to work one would first have to support retrieving arrays from dictionaries.
    //  I believe this is a separate task done by some other people.
    throw Exception{"Reading the dictionary is not allowed", ErrorCodes::UNSUPPORTED_METHOD};
}

template <typename T>
void IPolygonDictionary::appendNullValueImpl(const Field & null_value)
{
    null_values.emplace_back(T(null_value.get<NearestFieldType<T>>()));
}

void IPolygonDictionary::appendNullValue(AttributeUnderlyingType type, const Field & null_value)
{
    switch (type)
    {
        case AttributeUnderlyingType::utUInt8:
            appendNullValueImpl<UInt8>(null_value);
            break;
        case AttributeUnderlyingType::utUInt16:
            appendNullValueImpl<UInt16>(null_value);
            break;
        case AttributeUnderlyingType::utUInt32:
            appendNullValueImpl<UInt32>(null_value);
            break;
        case AttributeUnderlyingType::utUInt64:
            appendNullValueImpl<UInt64>(null_value);
            break;
        case AttributeUnderlyingType::utUInt128:
            appendNullValueImpl<UInt128>(null_value);
            break;
        case AttributeUnderlyingType::utInt8:
            appendNullValueImpl<Int8>(null_value);
            break;
        case AttributeUnderlyingType::utInt16:
            appendNullValueImpl<Int16>(null_value);
            break;
        case AttributeUnderlyingType::utInt32:
            appendNullValueImpl<Int32>(null_value);
            break;
        case AttributeUnderlyingType::utInt64:
            appendNullValueImpl<Int64>(null_value);
            break;
        case AttributeUnderlyingType::utFloat32:
            appendNullValueImpl<Float32>(null_value);
            break;
        case AttributeUnderlyingType::utFloat64:
            appendNullValueImpl<Float64>(null_value);
            break;
        case AttributeUnderlyingType::utDecimal32:
            appendNullValueImpl<Decimal32>(null_value);
            break;
        case AttributeUnderlyingType::utDecimal64:
            appendNullValueImpl<Decimal64>(null_value);
            break;
        case AttributeUnderlyingType::utDecimal128:
            appendNullValueImpl<Decimal128>(null_value);
            break;
        case AttributeUnderlyingType::utString:
            appendNullValueImpl<String>(null_value);
            break;
    }
}

void IPolygonDictionary::createAttributes()
{
    attributes.resize(dict_struct.attributes.size());
    for (size_t i = 0; i < dict_struct.attributes.size(); ++i)
    {
        const auto & attr = dict_struct.attributes[i];
        attribute_index_by_name.emplace(attr.name, i);

        appendNullValue(attr.underlying_type, attr.null_value);

        if (attr.hierarchical)
            throw Exception{ErrorCodes::TYPE_MISMATCH,
                            "{}: hierarchical attributes not supported for dictionary of polygonal type",
                            getDictionaryID().getNameForLogs()};
    }
}

void IPolygonDictionary::blockToAttributes(const DB::Block & block)
{
    const auto rows = block.rows();
    element_count += rows;
    for (size_t i = 0; i < attributes.size(); ++i)
    {
        const auto & column = block.safeGetByPosition(i + 1);
        if (attributes[i])
        {
            MutableColumnPtr mutated = IColumn::mutate(std::move(attributes[i]));
            mutated->insertRangeFrom(*column.column, 0, column.column->size());
            attributes[i] = std::move(mutated);
        }
        else
            attributes[i] = column.column;
    }
    /** Multi-polygons could cause bigger sizes, but this is better than nothing. */
    polygons.reserve(polygons.size() + rows);
    ids.reserve(ids.size() + rows);
    const auto & key = block.safeGetByPosition(0).column;
    extractPolygons(key);
}

void IPolygonDictionary::loadData()
{
    auto stream = source_ptr->loadAll();
    stream->readPrefix();
    while (const auto block = stream->read())
        blockToAttributes(block);
    stream->readSuffix();

    std::vector<double> areas;
    areas.reserve(polygons.size());

    std::vector<std::pair<Polygon, size_t>> polygon_ids;
    polygon_ids.reserve(polygons.size());
    for (size_t i = 0; i < polygons.size(); ++i)
    {
        auto & polygon = polygons[i];
        bg::correct(polygon);
        areas.push_back(bg::area(polygon));
        polygon_ids.emplace_back(polygon, i);
    }
    sort(polygon_ids.begin(), polygon_ids.end(), [& areas](const auto & lhs, const auto & rhs)
    {
        return areas[lhs.second] < areas[rhs.second];
    });
    std::vector<size_t> correct_ids;
    correct_ids.reserve(polygon_ids.size());
    for (size_t i = 0; i < polygon_ids.size(); ++i)
    {
        auto & polygon = polygon_ids[i];
        correct_ids.emplace_back(ids[polygon.second]);
        polygons[i] = polygon.first;
    }
    ids = correct_ids;
}

void IPolygonDictionary::calculateBytesAllocated()
{
    // TODO:: Account for key.
    for (const auto & column : attributes)
        bytes_allocated += column->allocatedBytes();
}

std::vector<IPolygonDictionary::Point> IPolygonDictionary::extractPoints(const Columns & key_columns)
{
    if (key_columns.size() != 2)
        throw Exception{"Expected two columns of coordinates", ErrorCodes::BAD_ARGUMENTS};
    const auto * column_x = typeid_cast<const ColumnVector<Float64>*>(key_columns[0].get());
    const auto * column_y = typeid_cast<const ColumnVector<Float64>*>(key_columns[1].get());
    if (!column_x || !column_y)
        throw Exception{"Expected columns of Float64", ErrorCodes::TYPE_MISMATCH};
    const auto rows = key_columns.front()->size();
    std::vector<Point> result;
    result.reserve(rows);
    for (const auto row : ext::range(0, rows))
        result.emplace_back(column_x->getElement(row), column_y->getElement(row));
    return result;
}

void IPolygonDictionary::has(const Columns & key_columns, const DataTypes &, PaddedPODArray<UInt8> & out) const
{
    size_t row = 0;
    for (const auto & pt : extractPoints(key_columns))
    {
        size_t trash = 0;
        out[row] = find(pt, trash);
        ++row;
    }

    query_count.fetch_add(row, std::memory_order_relaxed);
}

size_t IPolygonDictionary::getAttributeIndex(const std::string & attribute_name) const
{
    const auto it = attribute_index_by_name.find(attribute_name);
    if (it == attribute_index_by_name.end())
        throw Exception{"No such attribute: " + attribute_name, ErrorCodes::BAD_ARGUMENTS};
    return it->second;
}

#define DECLARE(TYPE) \
    void IPolygonDictionary::get##TYPE( \
        const std::string & attribute_name, const Columns & key_columns, const DataTypes &, ResultArrayType<TYPE> & out) const \
    { \
        const auto ind = getAttributeIndex(attribute_name); \
        checkAttributeType(this, attribute_name, dict_struct.attributes[ind].underlying_type, AttributeUnderlyingType::ut##TYPE); \
\
        const auto null_value = std::get<TYPE>(null_values[ind]); \
\
        getItemsImpl<TYPE, TYPE>( \
            ind, \
            key_columns, \
            [&](const size_t row, const auto value) { out[row] = value; }, \
            [&](const size_t) { return null_value; }); \
    }
    DECLARE(UInt8)
    DECLARE(UInt16)
    DECLARE(UInt32)
    DECLARE(UInt64)
    DECLARE(UInt128)
    DECLARE(Int8)
    DECLARE(Int16)
    DECLARE(Int32)
    DECLARE(Int64)
    DECLARE(Float32)
    DECLARE(Float64)
    DECLARE(Decimal32)
    DECLARE(Decimal64)
    DECLARE(Decimal128)
#undef DECLARE

void IPolygonDictionary::getString(
        const std::string & attribute_name, const Columns & key_columns, const DataTypes &, ColumnString * out) const
{
    const auto ind = getAttributeIndex(attribute_name);
    checkAttributeType(this, attribute_name, dict_struct.attributes[ind].underlying_type, AttributeUnderlyingType::utString);

    const auto & null_value = StringRef{std::get<String>(null_values[ind])};

    getItemsImpl<String, StringRef>(
            ind,
            key_columns,
            [&](const size_t, const StringRef & value) { out->insertData(value.data, value.size); },
            [&](const size_t) { return null_value; });
}

#define DECLARE(TYPE) \
    void IPolygonDictionary::get##TYPE( \
        const std::string & attribute_name, \
        const Columns & key_columns, \
        const DataTypes &, \
        const PaddedPODArray<TYPE> & def, \
        ResultArrayType<TYPE> & out) const \
    { \
        const auto ind = getAttributeIndex(attribute_name); \
        checkAttributeType(this, attribute_name, dict_struct.attributes[ind].underlying_type, AttributeUnderlyingType::ut##TYPE); \
\
        getItemsImpl<TYPE, TYPE>( \
            ind, \
            key_columns, \
            [&](const size_t row, const auto value) { out[row] = value; }, \
            [&](const size_t row) { return def[row]; }); \
    }
    DECLARE(UInt8)
    DECLARE(UInt16)
    DECLARE(UInt32)
    DECLARE(UInt64)
    DECLARE(UInt128)
    DECLARE(Int8)
    DECLARE(Int16)
    DECLARE(Int32)
    DECLARE(Int64)
    DECLARE(Float32)
    DECLARE(Float64)
    DECLARE(Decimal32)
    DECLARE(Decimal64)
    DECLARE(Decimal128)
#undef DECLARE

void IPolygonDictionary::getString(
        const std::string & attribute_name,
        const Columns & key_columns,
        const DataTypes &,
        const ColumnString * const def,
        ColumnString * const out) const
{
    const auto ind = getAttributeIndex(attribute_name);
    checkAttributeType(this, attribute_name, dict_struct.attributes[ind].underlying_type, AttributeUnderlyingType::utString);

    getItemsImpl<String, StringRef>(
            ind,
            key_columns,
            [&](const size_t, const StringRef value) { out->insertData(value.data, value.size); },
            [&](const size_t row) { return def->getDataAt(row); });
}

#define DECLARE(TYPE) \
    void IPolygonDictionary::get##TYPE( \
        const std::string & attribute_name, \
        const Columns & key_columns, \
        const DataTypes &, \
        const TYPE def, \
        ResultArrayType<TYPE> & out) const \
    { \
        const auto ind = getAttributeIndex(attribute_name); \
        checkAttributeType(this, attribute_name, dict_struct.attributes[ind].underlying_type, AttributeUnderlyingType::ut##TYPE); \
\
        getItemsImpl<TYPE, TYPE>( \
            ind, key_columns, [&](const size_t row, const auto value) { out[row] = value; }, [&](const size_t) { return def; }); \
    }
    DECLARE(UInt8)
    DECLARE(UInt16)
    DECLARE(UInt32)
    DECLARE(UInt64)
    DECLARE(UInt128)
    DECLARE(Int8)
    DECLARE(Int16)
    DECLARE(Int32)
    DECLARE(Int64)
    DECLARE(Float32)
    DECLARE(Float64)
    DECLARE(Decimal32)
    DECLARE(Decimal64)
    DECLARE(Decimal128)
#undef DECLARE

void IPolygonDictionary::getString(
        const std::string & attribute_name,
        const Columns & key_columns,
        const DataTypes &,
        const String & def,
        ColumnString * const out) const
{
    const auto ind = getAttributeIndex(attribute_name);
    checkAttributeType(this, attribute_name, dict_struct.attributes[ind].underlying_type, AttributeUnderlyingType::utString);

    getItemsImpl<String, StringRef>(
            ind,
            key_columns,
            [&](const size_t, const StringRef value) { out->insertData(value.data, value.size); },
            [&](const size_t) { return StringRef{def}; });
}

template <typename AttributeType, typename OutputType, typename ValueSetter, typename DefaultGetter>
void IPolygonDictionary::getItemsImpl(
        size_t attribute_ind, const Columns & key_columns, ValueSetter && set_value, DefaultGetter && get_default) const
{
    const auto points = extractPoints(key_columns);

    using ColVecType = std::conditional_t<IsDecimalNumber<AttributeType>, ColumnDecimal<AttributeType>, ColumnVector<AttributeType>>;
    using ColType = std::conditional_t<std::is_same<AttributeType, String>::value, ColumnString, ColVecType>;
    const auto column = typeid_cast<const ColType *>(attributes[attribute_ind].get());
    if (!column)
        throw Exception{"An attribute should be a column of its type", ErrorCodes::BAD_ARGUMENTS};
    for (const auto i : ext::range(0, points.size()))
    {
        size_t id = 0;
        const auto found = find(points[i], id);
        id = ids[id];
        if (!found)
        {
            set_value(i, static_cast<OutputType>(get_default(i)));
            continue;
        }
        if constexpr (std::is_same<AttributeType, String>::value)
            set_value(i, static_cast<OutputType>(column->getDataAt(id)));
        else
            set_value(i, static_cast<OutputType>(column->getElement(id)));
    }

    query_count.fetch_add(points.size(), std::memory_order_relaxed);
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
        throw Exception{"Expected a column containing arrays of polygons", ErrorCodes::TYPE_MISMATCH};
    offset.multi_polygon_offsets.assign(ptr_multi_polygons->getOffsets());

    const auto * ptr_polygons = typeid_cast<const ColumnArray*>(&ptr_multi_polygons->getData());
    if (!ptr_polygons)
        throw Exception{"Expected a column containing arrays of rings when reading polygons", ErrorCodes::TYPE_MISMATCH};
    offset.polygon_offsets.assign(ptr_polygons->getOffsets());

    const auto * ptr_rings = typeid_cast<const ColumnArray*>(&ptr_polygons->getData());
    if (!ptr_rings)
        throw Exception{"Expected a column containing arrays of points when reading rings", ErrorCodes::TYPE_MISMATCH};
    offset.ring_offsets.assign(ptr_rings->getOffsets());

    return ptr_rings->getDataPtr().get();
}

const IColumn * unrollSimplePolygons(const ColumnPtr & column, Offset & offset)
{
    const auto * ptr_polygons = typeid_cast<const ColumnArray*>(column.get());
    if (!ptr_polygons)
        throw Exception{"Expected a column containing arrays of points", ErrorCodes::TYPE_MISMATCH};
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
        throw Exception{"Expected coordinates to be of type Float64", ErrorCodes::TYPE_MISMATCH};
    const auto & offsets = ptr_points->getOffsets();
    IColumn::Offset prev_offset = 0;
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        if (offsets[i] - prev_offset != 2)
            throw Exception{"All points should be two-dimensional", ErrorCodes::BAD_ARGUMENTS};
        prev_offset = offsets[i];
        addNewPoint(ptr_coord->getElement(2 * i), ptr_coord->getElement(2 * i + 1), data, offset);
    }
}

void handlePointsReprByTuples(const IColumn * column, Data & data, Offset & offset)
{
    const auto * ptr_points = typeid_cast<const ColumnTuple*>(column);
    if (!ptr_points)
        throw Exception{"Expected a column of tuples representing points", ErrorCodes::TYPE_MISMATCH};
    if (ptr_points->tupleSize() != 2)
        throw Exception{"Points should be two-dimensional", ErrorCodes::BAD_ARGUMENTS};
    const auto * column_x = typeid_cast<const ColumnVector<Float64>*>(&ptr_points->getColumn(0));
    const auto * column_y = typeid_cast<const ColumnVector<Float64>*>(&ptr_points->getColumn(1));
    if (!column_x || !column_y)
        throw Exception{"Expected coordinates to be of type Float64", ErrorCodes::TYPE_MISMATCH};
    for (size_t i = 0; i < column_x->size(); ++i)
    {
        addNewPoint(column_x->getElement(i), column_y->getElement(i), data, offset);
    }
}

}

void IPolygonDictionary::extractPolygons(const ColumnPtr & column)
{
    Data data = {polygons, ids};
    Offset offset;

    const IColumn * points_collection = nullptr;
    switch (input_type)
    {
        case InputType::MultiPolygon:
            points_collection = unrollMultiPolygons(column, offset);
            break;
        case InputType::SimplePolygon:
            points_collection = unrollSimplePolygons(column, offset);
            break;
    }

    if (!offset.allRingsHaveAPositiveArea())
        throw Exception{"Every ring included in a polygon or excluded from it should contain at least 3 points",
                        ErrorCodes::BAD_ARGUMENTS};

    /** Adding the first empty polygon */
    data.addPolygon(true);

    switch (point_type)
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

