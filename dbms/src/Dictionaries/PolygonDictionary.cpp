#include <ext/map.h>
#include "PolygonDictionary.h"
#include "DictionaryBlockInputStream.h"
#include "DictionaryFactory.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TYPE;
    extern const int UNSUPPORTED_METHOD;
}


IPolygonDictionary::IPolygonDictionary(
        const std::string & name_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_,
        const DictionaryLifetime dict_lifetime_)
        : name(name_)
        , dict_struct(dict_struct_)
        , source_ptr(std::move(source_ptr_))
        , dict_lifetime(dict_lifetime_)
{
    createAttributes();
    loadData();
}

std::string IPolygonDictionary::getName() const
{
    return name;
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

BlockInputStreamPtr IPolygonDictionary::getBlockInputStream(const Names &, size_t) const {
    // TODO: Better error message.
    throw Exception{"Reading the dictionary is not allowed", ErrorCodes::UNSUPPORTED_METHOD};
}

void IPolygonDictionary::createAttributes() {
    attributes.resize(dict_struct.attributes.size());
    for (size_t i = 0; i < dict_struct.attributes.size(); ++i)
    {
        attribute_index_by_name.emplace(dict_struct.attributes[i].name, i);

        if (dict_struct.attributes[i].hierarchical)
            throw Exception{name + ": hierarchical attributes not supported for dictionary of type " + getTypeName(),
                            ErrorCodes::TYPE_MISMATCH};
    }
}

void IPolygonDictionary::blockToAttributes(const DB::Block &block) {
    const auto rows = block.rows();
    element_count += rows;
    for (size_t i = 0; i < attributes.size(); ++i) {
        const auto & column = block.safeGetByPosition(i + 1);
        if (attributes[i])
        {
            MutableColumnPtr mutated = std::move(*attributes[i]).mutate();
            mutated->insertRangeFrom(*column.column, 0, column.column->size());
            attributes[i] = std::move(mutated);
        }
        else
            attributes[i] = column.column;
    }
    polygons.reserve(polygons.size() + rows);

    const auto & key = block.safeGetByPosition(0).column;

    for (const auto row : ext::range(0, rows))
    {
        const auto & field = (*key)[row];
        // TODO: Get data more efficiently using
        polygons.push_back(fieldToMultiPolygon(field));
    }
}

void IPolygonDictionary::loadData() {
    auto stream = source_ptr->loadAll();
    stream->readPrefix();
    while (const auto block = stream->read()) {
        blockToAttributes(block);
    }
    stream->readSuffix();
}

void IPolygonDictionary::calculateBytesAllocated()
{
    for (const auto & block : blocks)
        bytes_allocated += block.allocatedBytes();

}

std::vector<IPolygonDictionary::Point> IPolygonDictionary::extractPoints(const Columns &key_columns)
{
    if (key_columns.size() != DIM)
        throw Exception{"Expected " + std::to_string(DIM) + " columns of coordinates", ErrorCodes::BAD_ARGUMENTS};
    const auto column_x = typeid_cast<const ColumnVector<Float64>*>(key_columns[0].get());
    const auto column_y = typeid_cast<const ColumnVector<Float64>*>(key_columns[1].get());
    if (!column_x || !column_y)
        throw Exception{"Expected columns of Float64", ErrorCodes::TYPE_MISMATCH};
    const auto rows = key_columns.front()->size();
    std::vector<Point> result;
    result.reserve(rows);
    for (const auto row : ext::range(0, rows))
        result.emplace_back(column_x->getElement(row), column_y->getElement(row));
    return result;
}

void IPolygonDictionary::has(const Columns &key_columns, const DataTypes &, PaddedPODArray<UInt8> &out) const {
    size_t row = 0;
    for (const auto & pt : extractPoints(key_columns))
    {
        // TODO: Check whether this will be optimized by the compiler.
        size_t trash = 0;
        out[row] = find(pt, trash);
        ++row;
    }
}

size_t IPolygonDictionary::getAttributeIndex(const std::string & attribute_name) const {
    const auto it = attribute_index_by_name.find(attribute_name);
    if (it == attribute_index_by_name.end())
        throw Exception{"No such attribute: " + attribute_name, ErrorCodes::BAD_ARGUMENTS};
    return it->second;
}

template <typename T>
T IPolygonDictionary::getNullValue(const DB::Field &field) const
{
    return field.get<NearestFieldType<T>>();
}

#define DECLARE(TYPE) \
    void IPolygonDictionary::get##TYPE( \
        const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types, ResultArrayType<TYPE> & out) const \
    { \
        const auto ind = getAttributeIndex(attribute_name); \
        checkAttributeType(name, attribute_name, dict_struct.attributes[ind].type, AttributeUnderlyingType::ut##TYPE); \
\
        const auto null_value = getNullValue(dict_struct.attributes[ind].null_value); \
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
        const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types, ColumnString * out) const
{
    dict_struct.validateKeyTypes(key_types);

    const auto ind = getAttributeIndex(attribute_name);
    checkAttributeType(name, attribute_name, dict_struct.attributes[ind].type, AttributeUnderlyingType::utString);

    const auto & null_value = StringRef{getNullValue(dict_struct.attributes[ind].null_value)};

    getItemsImpl<StringRef, StringRef>(
            ind,
            key_columns,
            [&](const size_t, const StringRef value) { out->insertData(value.data, value.size); },
            [&](const size_t) { return null_value; });
}

#define DECLARE(TYPE) \
    void IPolygonDictionary::get##TYPE( \
        const std::string & attribute_name, \
        const Columns & key_columns, \
        const DataTypes & key_types, \
        const PaddedPODArray<TYPE> & def, \
        ResultArrayType<TYPE> & out) const \
    { \
        const auto ind = getAttributeIndex(attribute_name); \
        checkAttributeType(name, attribute_name, dict_struct.attributes[ind].type, AttributeUnderlyingType::ut##TYPE); \
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
        const DataTypes & key_types,
        const ColumnString * const def,
        ColumnString * const out) const
{
    const auto ind = getAttributeIndex(attribute_name);
    checkAttributeType(name, attribute_name, dict_struct.attributes[ind].type, AttributeUnderlyingType::utString);

    getItemsImpl<StringRef, StringRef>(
            ind,
            key_columns,
            [&](const size_t, const StringRef value) { out->insertData(value.data, value.size); },
            [&](const size_t row) { return def->getDataAt(row); });
}

#define DECLARE(TYPE) \
    void IPolygonDictionary::get##TYPE( \
        const std::string & attribute_name, \
        const Columns & key_columns, \
        const DataTypes & key_types, \
        const TYPE def, \
        ResultArrayType<TYPE> & out) const \
    { \
        const auto ind = getAttributeIndex(attribute_name); \
        checkAttributeType(name, attribute_name, dict_struct.attributes[ind].type, AttributeUnderlyingType::ut##TYPE); \
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
        const DataTypes & key_types,
        const String & def,
        ColumnString * const out) const
{
    const auto ind = getAttributeIndex(attribute_name);
    checkAttributeType(name, attribute_name, dict_struct.attributes[ind].type, AttributeUnderlyingType::utString);

    getItemsImpl<StringRef, StringRef>(
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

    for (const auto i : ext::range(0, points.size()))
    {
        size_t id = 0;
        auto found = find(points[i], id);
        set_value(i, found ? static_cast<OutputType>((*attributes[attribute_ind])[id].get<AttributeType>()) : get_default(i));
    }

    query_count.fetch_add(points.size(), std::memory_order_relaxed);
}

IPolygonDictionary::Point IPolygonDictionary::fieldToPoint(const Field &field)
{
    if (field.getType() == Field::Types::Array)
    {
        auto coordinate_array = field.get<Array>();
        if (coordinate_array.size() != DIM)
            throw Exception{"All points should be two-dimensional", ErrorCodes::LOGICAL_ERROR};
        Float64 values[DIM];
        for (size_t i = 0; i < DIM; ++i)
        {
            if (coordinate_array[i].getType() != Field::Types::Float64)
                throw Exception{"Coordinates should be Float64", ErrorCodes::TYPE_MISMATCH};
            values[i] = coordinate_array[i].get<Float64>();
        }
        return {values[0], values[1]};
    }
    else
        throw Exception{"Point is not represented by an array", ErrorCodes::TYPE_MISMATCH};
}

IPolygonDictionary::Polygon IPolygonDictionary::fieldToPolygon(const Field & field) {
    Polygon result;
    if (field.getType() == Field::Types::Array)
    {
        const auto & ring_array = field.get<Array>();
        if (ring_array.empty())
            throw Exception{"Empty polygons are not allowed", ErrorCodes::LOGICAL_ERROR};
        result.inners().resize(ring_array.size() - 1);
        if (ring_array[0].getType() != Field::Types::Array)
            throw Exception{"Outer polygon ring is not represented by an array", ErrorCodes::TYPE_MISMATCH};
        for (const auto & point : ring_array[0].get<Array>())
            bg::append(result.outer(), fieldToPoint(point));
        for (size_t i = 0; i < result.inners().size(); ++i) {
            if (ring_array[i + 1].getType() != Field::Types::Array)
                throw Exception{"Inner polygon ring is not represented by an array", ErrorCodes::TYPE_MISMATCH};
            for (const auto & point : ring_array[i + 1].get<Array>())
                bg::append(result.inners()[i], fieldToPoint(point));
        }
    }
    else
        throw Exception{"Polygon is not represented by an array", ErrorCodes::TYPE_MISMATCH};
    return result;
}

IPolygonDictionary::MultiPolygon IPolygonDictionary::fieldToMultiPolygon(const Field &field) {
    MultiPolygon result;
    if (field.getType() == Field::Types::Array)
    {
        const auto& polygon_array = field.get<Array>();
        result.reserve(polygon_array.size());
        for (const auto & polygon : polygon_array)
            result.push_back(fieldToPolygon(polygon));
    }
    else
        throw Exception{"MultiPolygon is not represented by an array", ErrorCodes::TYPE_MISMATCH};
    return result;
}

SimplePolygonDictionary::SimplePolygonDictionary(
    const std::string & name_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    const DictionaryLifetime dict_lifetime_)
    : IPolygonDictionary(name_, dict_struct_, std::move(source_ptr_), dict_lifetime_)
{
}

std::shared_ptr<const IExternalLoadable> SimplePolygonDictionary::clone() const
{
    return std::make_shared<SimplePolygonDictionary>(
            this->name,
            this->dict_struct,
            this->source_ptr->clone(),
            this->dict_lifetime);
}

bool SimplePolygonDictionary::find(const Point &point, size_t & id) const
{
    for (size_t i = 0; i < (this->polygons).size(); ++i)
    {
        if (bg::within(point, (this->polygons)[i])) {
            id = i;
            return true;
        }
    }
    return false;
}

void registerDictionaryPolygon(DictionaryFactory & factory)
{
    auto create_layout = [=](const std::string & name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr) -> DictionaryPtr
    {
        // TODO: Check that there is only one key and it is of the correct type.
        if (dict_struct.range_min || dict_struct.range_max)
            throw Exception{name
                            + ": elements .structure.range_min and .structure.range_max should be defined only "
                              "for a dictionary of layout 'range_hashed'",
                            ErrorCodes::BAD_ARGUMENTS};
        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
        return std::make_unique<SimplePolygonDictionary>(name, dict_struct, std::move(source_ptr), dict_lifetime);
    };
    factory.registerLayout("polygon", create_layout, true);
}

}