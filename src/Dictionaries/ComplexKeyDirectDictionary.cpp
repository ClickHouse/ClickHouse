#include "ComplexKeyDirectDictionary.h"
#include <IO/WriteHelpers.h>
#include "DictionaryBlockInputStream.h"
#include "DictionaryFactory.h"
#include <Core/Defines.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int BAD_ARGUMENTS;
    extern const int UNSUPPORTED_METHOD;
}


ComplexKeyDirectDictionary::ComplexKeyDirectDictionary(
    const StorageID & dict_id_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    BlockPtr saved_block_)
    : IDictionaryBase(dict_id_)
    , dict_struct(dict_struct_)
    , source_ptr{std::move(source_ptr_)}
    , saved_block{std::move(saved_block_)}
{
    if (!this->source_ptr->supportsSelectiveLoad())
        throw Exception{full_name + ": source cannot be used with ComplexKeyDirectDictionary", ErrorCodes::UNSUPPORTED_METHOD};


    createAttributes();
}

#define DECLARE(TYPE) \
    void ComplexKeyDirectDictionary::get##TYPE(const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types, ResultArrayType<TYPE> & out) const \
    { \
        dict_struct.validateKeyTypes(key_types); \
        const auto & attribute = getAttribute(attribute_name); \
        checkAttributeType(this, attribute_name, attribute.type, AttributeUnderlyingType::ut##TYPE); \
\
        const auto null_value = std::get<TYPE>(attribute.null_values); \
\
        getItemsImpl<TYPE, TYPE>( \
            attribute, key_columns, [&](const size_t row, const auto value) { out[row] = value; }, [&](const size_t) { return null_value; }); \
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

void ComplexKeyDirectDictionary::getString(
    const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types, ColumnString * out) const
{
    dict_struct.validateKeyTypes(key_types);
    const auto & attribute = getAttribute(attribute_name);
    checkAttributeType(this, attribute_name, attribute.type, AttributeUnderlyingType::utString);

    const auto & null_value = std::get<StringRef>(attribute.null_values);
    getItemsStringImpl<StringRef, StringRef>(
        attribute,
        key_columns,
        [&](const size_t, const String value) { const auto ref = StringRef{value}; out->insertData(ref.data, ref.size); },
        [&](const size_t) { return String(null_value.data, null_value.size); });
}

#define DECLARE(TYPE) \
    void ComplexKeyDirectDictionary::get##TYPE( \
        const std::string & attribute_name, \
        const Columns & key_columns, \
        const DataTypes & key_types, \
        const PaddedPODArray<TYPE> & def, \
        ResultArrayType<TYPE> & out) const \
    { \
        dict_struct.validateKeyTypes(key_types); \
        const auto & attribute = getAttribute(attribute_name); \
        checkAttributeType(this, attribute_name, attribute.type, AttributeUnderlyingType::ut##TYPE); \
\
        getItemsImpl<TYPE, TYPE>( \
            attribute, key_columns, [&](const size_t row, const auto value) { out[row] = value; }, [&](const size_t row) { return def[row]; }); \
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

void ComplexKeyDirectDictionary::getString(
    const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types, const ColumnString * const def, ColumnString * const out) const
{
    dict_struct.validateKeyTypes(key_types);

    const auto & attribute = getAttribute(attribute_name);
    checkAttributeType(this, attribute_name, attribute.type, AttributeUnderlyingType::utString);

    getItemsStringImpl<StringRef, StringRef>(
        attribute,
        key_columns,
        [&](const size_t, const String value) { const auto ref = StringRef{value}; out->insertData(ref.data, ref.size); },
        [&](const size_t row) { const auto ref = def->getDataAt(row); return String(ref.data, ref.size); });
}

#define DECLARE(TYPE) \
    void ComplexKeyDirectDictionary::get##TYPE( \
        const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types, const TYPE def, ResultArrayType<TYPE> & out) const \
    { \
        dict_struct.validateKeyTypes(key_types); \
        const auto & attribute = getAttribute(attribute_name); \
        checkAttributeType(this, attribute_name, attribute.type, AttributeUnderlyingType::ut##TYPE); \
\
        getItemsImpl<TYPE, TYPE>( \
            attribute, key_columns, [&](const size_t row, const auto value) { out[row] = value; }, [&](const size_t) { return def; }); \
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

void ComplexKeyDirectDictionary::getString(
    const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types, const String & def, ColumnString * const out) const
{
    dict_struct.validateKeyTypes(key_types);

    const auto & attribute = getAttribute(attribute_name);
    checkAttributeType(this, attribute_name, attribute.type, AttributeUnderlyingType::utString);

    ComplexKeyDirectDictionary::getItemsStringImpl<StringRef, StringRef>(
        attribute,
        key_columns,
        [&](const size_t, const String value) { const auto ref = StringRef{value}; out->insertData(ref.data, ref.size); },
        [&](const size_t) { return def; });
}


void ComplexKeyDirectDictionary::has(const Columns & key_columns, const DataTypes & key_types, PaddedPODArray<UInt8> & out) const
{
    dict_struct.validateKeyTypes(key_types);
    const auto & attribute = attributes.front();

    switch (attribute.type)
    {
        case AttributeUnderlyingType::utUInt8:
            has<UInt8>(attribute, key_columns, out);
            break;
        case AttributeUnderlyingType::utUInt16:
            has<UInt16>(attribute, key_columns, out);
            break;
        case AttributeUnderlyingType::utUInt32:
            has<UInt32>(attribute, key_columns, out);
            break;
        case AttributeUnderlyingType::utUInt64:
            has<UInt64>(attribute, key_columns, out);
            break;
        case AttributeUnderlyingType::utUInt128:
            has<UInt128>(attribute, key_columns, out);
            break;
        case AttributeUnderlyingType::utInt8:
            has<Int8>(attribute, key_columns, out);
            break;
        case AttributeUnderlyingType::utInt16:
            has<Int16>(attribute, key_columns, out);
            break;
        case AttributeUnderlyingType::utInt32:
            has<Int32>(attribute, key_columns, out);
            break;
        case AttributeUnderlyingType::utInt64:
            has<Int64>(attribute, key_columns, out);
            break;
        case AttributeUnderlyingType::utFloat32:
            has<Float32>(attribute, key_columns, out);
            break;
        case AttributeUnderlyingType::utFloat64:
            has<Float64>(attribute, key_columns, out);
            break;
        case AttributeUnderlyingType::utString:
            has<String>(attribute, key_columns, out);
            break;

        case AttributeUnderlyingType::utDecimal32:
            has<Decimal32>(attribute, key_columns, out);
            break;
        case AttributeUnderlyingType::utDecimal64:
            has<Decimal64>(attribute, key_columns, out);
            break;
        case AttributeUnderlyingType::utDecimal128:
            has<Decimal128>(attribute, key_columns, out);
            break;
    }
}


void ComplexKeyDirectDictionary::createAttributes()
{
    const auto size = dict_struct.attributes.size();
    attributes.reserve(size);

    for (const auto & attribute : dict_struct.attributes)
    {
        attribute_index_by_name.emplace(attribute.name, attributes.size());
        attribute_name_by_index.emplace(attributes.size(), attribute.name);
        attributes.push_back(createAttributeWithType(attribute.underlying_type, attribute.null_value, attribute.name));

        if (attribute.hierarchical)
            throw Exception{full_name + ": hierarchical attributes not supported for dictionary of type " + getTypeName(),
                            ErrorCodes::TYPE_MISMATCH};
    }
}


template <typename T>
void ComplexKeyDirectDictionary::createAttributeImpl(Attribute & attribute, const Field & null_value)
{
    attribute.null_values = T(null_value.get<NearestFieldType<T>>());
}

template <>
void ComplexKeyDirectDictionary::createAttributeImpl<String>(Attribute & attribute, const Field & null_value)
{
    attribute.string_arena = std::make_unique<Arena>();
    const String & string = null_value.get<String>();
    const char * string_in_arena = attribute.string_arena->insert(string.data(), string.size());
    attribute.null_values.emplace<StringRef>(string_in_arena, string.size());
}


ComplexKeyDirectDictionary::Attribute ComplexKeyDirectDictionary::createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value, const std::string & attr_name)
{
    Attribute attr{type, {}, {}, attr_name};

    switch (type)
    {
        case AttributeUnderlyingType::utUInt8:
            createAttributeImpl<UInt8>(attr, null_value);
            break;
        case AttributeUnderlyingType::utUInt16:
            createAttributeImpl<UInt16>(attr, null_value);
            break;
        case AttributeUnderlyingType::utUInt32:
            createAttributeImpl<UInt32>(attr, null_value);
            break;
        case AttributeUnderlyingType::utUInt64:
            createAttributeImpl<UInt64>(attr, null_value);
            break;
        case AttributeUnderlyingType::utUInt128:
            createAttributeImpl<UInt128>(attr, null_value);
            break;
        case AttributeUnderlyingType::utInt8:
            createAttributeImpl<Int8>(attr, null_value);
            break;
        case AttributeUnderlyingType::utInt16:
            createAttributeImpl<Int16>(attr, null_value);
            break;
        case AttributeUnderlyingType::utInt32:
            createAttributeImpl<Int32>(attr, null_value);
            break;
        case AttributeUnderlyingType::utInt64:
            createAttributeImpl<Int64>(attr, null_value);
            break;
        case AttributeUnderlyingType::utFloat32:
            createAttributeImpl<Float32>(attr, null_value);
            break;
        case AttributeUnderlyingType::utFloat64:
            createAttributeImpl<Float64>(attr, null_value);
            break;
        case AttributeUnderlyingType::utString:
            createAttributeImpl<String>(attr, null_value);
            break;

        case AttributeUnderlyingType::utDecimal32:
            createAttributeImpl<Decimal32>(attr, null_value);
            break;
        case AttributeUnderlyingType::utDecimal64:
            createAttributeImpl<Decimal64>(attr, null_value);
            break;
        case AttributeUnderlyingType::utDecimal128:
            createAttributeImpl<Decimal128>(attr, null_value);
            break;
    }

    return attr;
}

template <typename Pool>
StringRef ComplexKeyDirectDictionary::placeKeysInPool(
    const size_t row, const Columns & key_columns, StringRefs & keys, const std::vector<DictionaryAttribute> & key_attributes, Pool & pool) const
{
    const auto keys_size = key_columns.size();
    size_t sum_keys_size{};

    for (size_t j = 0; j < keys_size; ++j)
    {
        keys[j] = key_columns[j]->getDataAt(row);
        sum_keys_size += keys[j].size;
        if (key_attributes[j].underlying_type == AttributeUnderlyingType::utString)
            sum_keys_size += sizeof(size_t) + 1;
    }

    auto place = pool.alloc(sum_keys_size);

    auto key_start = place;
    for (size_t j = 0; j < keys_size; ++j)
    {
        if (key_attributes[j].underlying_type == AttributeUnderlyingType::utString)
        {
            auto start = key_start;
            auto key_size = keys[j].size + 1;
            memcpy(key_start, &key_size, sizeof(size_t));
            key_start += sizeof(size_t);
            memcpy(key_start, keys[j].data, keys[j].size);
            key_start += keys[j].size;
            *key_start = '\0';
            ++key_start;
            keys[j].data = start;
            keys[j].size += sizeof(size_t) + 1;
        }
        else
        {
            memcpy(key_start, keys[j].data, keys[j].size);
            keys[j].data = key_start;
            key_start += keys[j].size;
        }
    }

    return {place, sum_keys_size};
}


template <typename AttributeType, typename OutputType, typename ValueSetter, typename DefaultGetter>
void ComplexKeyDirectDictionary::getItemsImpl(
    const Attribute & attribute, const Columns & key_columns, ValueSetter && set_value, DefaultGetter && get_default) const
{
    const auto rows = key_columns.front()->size();
    const auto keys_size = dict_struct.key->size();
    StringRefs keys_array(keys_size);
    MapType<OutputType> value_by_key;
    Arena temporary_keys_pool;
    std::vector<size_t> to_load(rows);
    PODArray<StringRef> keys(rows);

    for (const auto row : ext::range(0, rows))
    {
        const StringRef key = placeKeysInPool(row, key_columns, keys_array, *dict_struct.key, temporary_keys_pool);
        keys[row] = key;
        value_by_key[key] = get_default(row);
        to_load[row] = row;
    }

    auto stream = source_ptr->loadKeys(key_columns, to_load);
    const auto attributes_size = attributes.size();

    stream->readPrefix();

    while (const auto block = stream->read())
    {
        const auto columns = ext::map<Columns>(
            ext::range(0, keys_size), [&](const size_t attribute_idx) { return block.safeGetByPosition(attribute_idx).column; });

        const auto attribute_columns = ext::map<Columns>(ext::range(0, attributes_size), [&](const size_t attribute_idx)
            {
                return block.safeGetByPosition(keys_size + attribute_idx).column;
            });
        for (const size_t attribute_idx : ext::range(0, attributes.size()))
        {
            const IColumn & attribute_column = *attribute_columns[attribute_idx];
            Arena pool;

            StringRefs keys_temp(keys_size);

            const auto columns_size = columns.front()->size();

            for (const auto row_idx : ext::range(0, columns_size))
            {
                const StringRef key = placeKeysInPool(row_idx, columns, keys_temp, *dict_struct.key, pool);
                if (value_by_key.has(key) && attribute.name == attribute_name_by_index.at(attribute_idx))
                {
                    if (attribute.type == AttributeUnderlyingType::utFloat32)
                    {
                        value_by_key[key] = static_cast<Float32>(attribute_column[row_idx].template get<Float64>());
                    }
                    else
                    {
                        value_by_key[key] = static_cast<OutputType>(attribute_column[row_idx].template get<AttributeType>());
                    }

                }
            }
        }
    }

    stream->readSuffix();

    for (const auto row : ext::range(0, rows))
    {
        set_value(row, value_by_key[keys[row]]);
    }

    query_count.fetch_add(rows, std::memory_order_relaxed);
}

template <typename AttributeType, typename OutputType, typename ValueSetter, typename DefaultGetter>
void ComplexKeyDirectDictionary::getItemsStringImpl(
    const Attribute & attribute, const Columns & key_columns, ValueSetter && set_value, DefaultGetter && get_default) const
{
    const auto rows = key_columns.front()->size();
    const auto keys_size = dict_struct.key->size();
    StringRefs keys_array(keys_size);
    MapType<String> value_by_key;
    Arena temporary_keys_pool;
    std::vector<size_t> to_load(rows);
    PODArray<StringRef> keys(rows);

    for (const auto row : ext::range(0, rows))
    {
        const StringRef key = placeKeysInPool(row, key_columns, keys_array, *dict_struct.key, temporary_keys_pool);
        keys[row] = key;
        value_by_key[key] = get_default(row);
        to_load[row] = row;
    }

    auto stream = source_ptr->loadKeys(key_columns, to_load);
    const auto attributes_size = attributes.size();

    stream->readPrefix();

    while (const auto block = stream->read())
    {
        const auto columns = ext::map<Columns>(
            ext::range(0, keys_size), [&](const size_t attribute_idx) { return block.safeGetByPosition(attribute_idx).column; });

        const auto attribute_columns = ext::map<Columns>(ext::range(0, attributes_size), [&](const size_t attribute_idx)
            {
                return block.safeGetByPosition(keys_size + attribute_idx).column;
            });
        for (const size_t attribute_idx : ext::range(0, attributes.size()))
        {
            const IColumn & attribute_column = *attribute_columns[attribute_idx];
            Arena pool;

            StringRefs keys_temp(keys_size);

            const auto columns_size = columns.front()->size();

            for (const auto row_idx : ext::range(0, columns_size))
            {
                const StringRef key = placeKeysInPool(row_idx, columns, keys_temp, *dict_struct.key, pool);
                if (value_by_key.has(key) && attribute.name == attribute_name_by_index.at(attribute_idx))
                {
                    const String from_source = attribute_column[row_idx].template get<String>();
                    value_by_key[key] = from_source;
                }
            }
        }
    }

    stream->readSuffix();

    for (const auto row : ext::range(0, rows))
    {
        set_value(row, value_by_key[keys[row]]);
    }

    query_count.fetch_add(rows, std::memory_order_relaxed);
}


const ComplexKeyDirectDictionary::Attribute & ComplexKeyDirectDictionary::getAttribute(const std::string & attribute_name) const
{
    const auto it = attribute_index_by_name.find(attribute_name);
    if (it == std::end(attribute_index_by_name))
        throw Exception{full_name + ": no such attribute '" + attribute_name + "'", ErrorCodes::BAD_ARGUMENTS};

    return attributes[it->second];
}


template <typename T>
void ComplexKeyDirectDictionary::has(const Attribute & attribute, const Columns & key_columns, PaddedPODArray<UInt8> & out) const
{
    const auto rows = key_columns.front()->size();
    const auto keys_size = dict_struct.key->size();
    StringRefs keys_array(keys_size);
    MapType<UInt8> has_key;
    Arena temporary_keys_pool;
    std::vector<size_t> to_load(rows);
    PODArray<StringRef> keys(rows);

    for (const auto row : ext::range(0, rows))
    {
        const StringRef key = placeKeysInPool(row, key_columns, keys_array, *dict_struct.key, temporary_keys_pool);
        keys[row] = key;
        has_key[key] = 0;
        to_load[row] = row;
    }

    auto stream = source_ptr->loadKeys(key_columns, to_load);

    stream->readPrefix();

    while (const auto block = stream->read())
    {
        const auto columns = ext::map<Columns>(
            ext::range(0, keys_size), [&](const size_t attribute_idx) { return block.safeGetByPosition(attribute_idx).column; });

        for (const size_t attribute_idx : ext::range(0, attributes.size()))
        {
            Arena pool;

            StringRefs keys_temp(keys_size);

            const auto columns_size = columns.front()->size();

            for (const auto row_idx : ext::range(0, columns_size))
            {
                const StringRef key = placeKeysInPool(row_idx, columns, keys_temp, *dict_struct.key, pool);
                if (has_key.has(key) && attribute.name == attribute_name_by_index.at(attribute_idx))
                {
                    has_key[key] = 1;
                }
            }
        }
    }

    stream->readSuffix();

    for (const auto row : ext::range(0, rows))
    {
        out[row] = has_key[keys[row]];
    }

    query_count.fetch_add(rows, std::memory_order_relaxed);
}


BlockInputStreamPtr ComplexKeyDirectDictionary::getBlockInputStream(const Names & /* column_names */, size_t /* max_block_size */) const
{
    return source_ptr->loadAll();
}


void registerDictionaryComplexKeyDirect(DictionaryFactory & factory)
{
    auto create_layout = [=](const std::string & full_name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr) -> DictionaryPtr
    {
        if (!dict_struct.key)
            throw Exception{"'key' is required for dictionary of layout 'complex_key_direct'", ErrorCodes::BAD_ARGUMENTS};

        if (dict_struct.range_min || dict_struct.range_max)
            throw Exception{full_name
                                + ": elements .structure.range_min and .structure.range_max should be defined only "
                                  "for a dictionary of layout 'range_hashed'",
                            ErrorCodes::BAD_ARGUMENTS};

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);

        if (config.has(config_prefix + ".lifetime.min") || config.has(config_prefix + ".lifetime.max"))
            throw Exception{"'lifetime' parameter is redundant for the dictionary' of layout 'direct'", ErrorCodes::BAD_ARGUMENTS};


        return std::make_unique<ComplexKeyDirectDictionary>(dict_id, dict_struct, std::move(source_ptr));
    };
    factory.registerLayout("complex_key_direct", create_layout, true);
}


}
