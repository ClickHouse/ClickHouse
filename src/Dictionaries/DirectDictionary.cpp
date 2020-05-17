#include "DirectDictionary.h"
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


DirectDictionary::DirectDictionary(
    const std::string & database_,
    const std::string & name_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    BlockPtr saved_block_)
    : database(database_)
    , name(name_)
    , full_name{database_.empty() ? name_ : (database_ + "." + name_)}
    , dict_struct(dict_struct_)
    , source_ptr{std::move(source_ptr_)}
    , saved_block{std::move(saved_block_)}
{
    if (!this->source_ptr->supportsSelectiveLoad())
        throw Exception{full_name + ": source cannot be used with DirectDictionary", ErrorCodes::UNSUPPORTED_METHOD};

    createAttributes();
}


void DirectDictionary::toParent(const PaddedPODArray<Key> & ids, PaddedPODArray<Key> & out) const
{
    const auto null_value = std::get<UInt64>(hierarchical_attribute->null_values);
    getItemsImpl<UInt64, UInt64>(
        *hierarchical_attribute,
        ids,
        [&](const size_t row, const UInt64 value) { out[row] = value; },
        [&](const size_t) { return null_value; });
}


static inline DirectDictionary::Key getAt(const PaddedPODArray<DirectDictionary::Key> & arr, const size_t idx)
{
    return arr[idx];
}
static inline DirectDictionary::Key getAt(const DirectDictionary::Key & value, const size_t)
{
    return value;
}

DirectDictionary::Key DirectDictionary::getValueOrNullByKey(const Key & to_find) const
{
    std::vector<Key> required_key = {to_find};

    auto stream = source_ptr->loadIds(required_key);
    stream->readPrefix();

    bool is_found = false;
    Key result = std::get<Key>(hierarchical_attribute->null_values);
    while (const auto block = stream->read())
    {
        const IColumn & id_column = *block.safeGetByPosition(0).column;

        for (const size_t attribute_idx : ext::range(0, attributes.size()))
        {
            if (is_found)
                break;

            const IColumn & attribute_column = *block.safeGetByPosition(attribute_idx + 1).column;

            for (const auto row_idx : ext::range(0, id_column.size()))
            {
                const auto key = id_column[row_idx].get<UInt64>();

                if (key == to_find && hierarchical_attribute->name == attribute_name_by_index.at(attribute_idx))
                {
                    result = attribute_column[row_idx].get<Key>();
                    is_found = true;
                    break;
                }
            }
        }
    }

    stream->readSuffix();

    return result;
}

template <typename ChildType, typename AncestorType>
void DirectDictionary::isInImpl(const ChildType & child_ids, const AncestorType & ancestor_ids, PaddedPODArray<UInt8> & out) const
{
    const auto null_value = std::get<UInt64>(hierarchical_attribute->null_values);
    const auto rows = out.size();

    for (const auto row : ext::range(0, rows))
    {
        auto id = getAt(child_ids, row);
        const auto ancestor_id = getAt(ancestor_ids, row);

        for (size_t i = 0; id != null_value && id != ancestor_id && i < DBMS_HIERARCHICAL_DICTIONARY_MAX_DEPTH; ++i)
            id = getValueOrNullByKey(id);

        out[row] = id != null_value && id == ancestor_id;
    }

    query_count.fetch_add(rows, std::memory_order_relaxed);
}


void DirectDictionary::isInVectorVector(
    const PaddedPODArray<Key> & child_ids, const PaddedPODArray<Key> & ancestor_ids, PaddedPODArray<UInt8> & out) const
{
    isInImpl(child_ids, ancestor_ids, out);
}

void DirectDictionary::isInVectorConstant(const PaddedPODArray<Key> & child_ids, const Key ancestor_id, PaddedPODArray<UInt8> & out) const
{
    isInImpl(child_ids, ancestor_id, out);
}

void DirectDictionary::isInConstantVector(const Key child_id, const PaddedPODArray<Key> & ancestor_ids, PaddedPODArray<UInt8> & out) const
{
    isInImpl(child_id, ancestor_ids, out);
}


#define DECLARE(TYPE) \
    void DirectDictionary::get##TYPE(const std::string & attribute_name, const PaddedPODArray<Key> & ids, ResultArrayType<TYPE> & out) const \
    { \
        const auto & attribute = getAttribute(attribute_name); \
        checkAttributeType(full_name, attribute_name, attribute.type, AttributeUnderlyingType::ut##TYPE); \
\
        const auto null_value = std::get<TYPE>(attribute.null_values); \
\
        getItemsImpl<TYPE, TYPE>( \
            attribute, ids, [&](const size_t row, const auto value) { out[row] = value; }, [&](const size_t) { return null_value; }); \
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

void DirectDictionary::getString(const std::string & attribute_name, const PaddedPODArray<Key> & ids, ColumnString * out) const
{
    const auto & attribute = getAttribute(attribute_name);
    checkAttributeType(full_name, attribute_name, attribute.type, AttributeUnderlyingType::utString);

    const auto & null_value = std::get<StringRef>(attribute.null_values);
    getItemsStringImpl<StringRef, StringRef>(
        attribute,
        ids,
        [&](const size_t, const String value) { const auto ref = StringRef{value}; out->insertData(ref.data, ref.size); },
        [&](const size_t) { return String(null_value.data, null_value.size); });
}

#define DECLARE(TYPE) \
    void DirectDictionary::get##TYPE( \
        const std::string & attribute_name, \
        const PaddedPODArray<Key> & ids, \
        const PaddedPODArray<TYPE> & def, \
        ResultArrayType<TYPE> & out) const \
    { \
        const auto & attribute = getAttribute(attribute_name); \
        checkAttributeType(full_name, attribute_name, attribute.type, AttributeUnderlyingType::ut##TYPE); \
\
        getItemsImpl<TYPE, TYPE>( \
            attribute, ids, [&](const size_t row, const auto value) { out[row] = value; }, [&](const size_t row) { return def[row]; }); \
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

void DirectDictionary::getString(
    const std::string & attribute_name, const PaddedPODArray<Key> & ids, const ColumnString * const def, ColumnString * const out) const
{
    const auto & attribute = getAttribute(attribute_name);
    checkAttributeType(full_name, attribute_name, attribute.type, AttributeUnderlyingType::utString);

    getItemsStringImpl<StringRef, StringRef>(
        attribute,
        ids,
        [&](const size_t, const String value) { const auto ref = StringRef{value}; out->insertData(ref.data, ref.size); },
        [&](const size_t row) { const auto ref = def->getDataAt(row); return String(ref.data, ref.size); });
}

#define DECLARE(TYPE) \
    void DirectDictionary::get##TYPE( \
        const std::string & attribute_name, const PaddedPODArray<Key> & ids, const TYPE def, ResultArrayType<TYPE> & out) const \
    { \
        const auto & attribute = getAttribute(attribute_name); \
        checkAttributeType(full_name, attribute_name, attribute.type, AttributeUnderlyingType::ut##TYPE); \
\
        getItemsImpl<TYPE, TYPE>( \
            attribute, ids, [&](const size_t row, const auto value) { out[row] = value; }, [&](const size_t) { return def; }); \
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

void DirectDictionary::getString(
    const std::string & attribute_name, const PaddedPODArray<Key> & ids, const String & def, ColumnString * const out) const
{
    const auto & attribute = getAttribute(attribute_name);
    checkAttributeType(full_name, attribute_name, attribute.type, AttributeUnderlyingType::utString);

    DirectDictionary::getItemsStringImpl<StringRef, StringRef>(
        attribute,
        ids,
        [&](const size_t, const String value) { const auto ref = StringRef{value}; out->insertData(ref.data, ref.size); },
        [&](const size_t) { return def; });
}


void DirectDictionary::has(const PaddedPODArray<Key> & ids, PaddedPODArray<UInt8> & out) const
{
    const auto & attribute = attributes.front();

    switch (attribute.type)
    {
        case AttributeUnderlyingType::utUInt8:
            has<UInt8>(attribute, ids, out);
            break;
        case AttributeUnderlyingType::utUInt16:
            has<UInt16>(attribute, ids, out);
            break;
        case AttributeUnderlyingType::utUInt32:
            has<UInt32>(attribute, ids, out);
            break;
        case AttributeUnderlyingType::utUInt64:
            has<UInt64>(attribute, ids, out);
            break;
        case AttributeUnderlyingType::utUInt128:
            has<UInt128>(attribute, ids, out);
            break;
        case AttributeUnderlyingType::utInt8:
            has<Int8>(attribute, ids, out);
            break;
        case AttributeUnderlyingType::utInt16:
            has<Int16>(attribute, ids, out);
            break;
        case AttributeUnderlyingType::utInt32:
            has<Int32>(attribute, ids, out);
            break;
        case AttributeUnderlyingType::utInt64:
            has<Int64>(attribute, ids, out);
            break;
        case AttributeUnderlyingType::utFloat32:
            has<Float32>(attribute, ids, out);
            break;
        case AttributeUnderlyingType::utFloat64:
            has<Float64>(attribute, ids, out);
            break;
        case AttributeUnderlyingType::utString:
            has<String>(attribute, ids, out);
            break;

        case AttributeUnderlyingType::utDecimal32:
            has<Decimal32>(attribute, ids, out);
            break;
        case AttributeUnderlyingType::utDecimal64:
            has<Decimal64>(attribute, ids, out);
            break;
        case AttributeUnderlyingType::utDecimal128:
            has<Decimal128>(attribute, ids, out);
            break;
    }
}


void DirectDictionary::createAttributes()
{
    const auto size = dict_struct.attributes.size();
    attributes.reserve(size);

    for (const auto & attribute : dict_struct.attributes)
    {
        attribute_index_by_name.emplace(attribute.name, attributes.size());
        attribute_name_by_index.emplace(attributes.size(), attribute.name);
        attributes.push_back(createAttributeWithType(attribute.underlying_type, attribute.null_value, attribute.name));

        if (attribute.hierarchical)
        {
            hierarchical_attribute = &attributes.back();

            if (hierarchical_attribute->type != AttributeUnderlyingType::utUInt64)
                throw Exception{full_name + ": hierarchical attribute must be UInt64.", ErrorCodes::TYPE_MISMATCH};
        }
    }
}


template <typename T>
void DirectDictionary::createAttributeImpl(Attribute & attribute, const Field & null_value)
{
    attribute.null_values = T(null_value.get<NearestFieldType<T>>());
}

template <>
void DirectDictionary::createAttributeImpl<String>(Attribute & attribute, const Field & null_value)
{
    attribute.string_arena = std::make_unique<Arena>();
    const String & string = null_value.get<String>();
    const char * string_in_arena = attribute.string_arena->insert(string.data(), string.size());
    attribute.null_values.emplace<StringRef>(string_in_arena, string.size());
}


DirectDictionary::Attribute DirectDictionary::createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value, const std::string & attr_name)
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


template <typename AttributeType, typename OutputType, typename ValueSetter, typename DefaultGetter>
void DirectDictionary::getItemsImpl(
    const Attribute & attribute, const PaddedPODArray<Key> & ids, ValueSetter && set_value, DefaultGetter && get_default) const
{
    const auto rows = ext::size(ids);

    HashMap<Key, OutputType> value_by_key;
    for (const auto row : ext::range(0, rows))
        value_by_key[ids[row]] = get_default(row);

    std::vector<Key> to_load;
    to_load.reserve(value_by_key.size());
    for (auto it = value_by_key.begin(); it != value_by_key.end(); ++it)
        to_load.emplace_back(static_cast<Key>(it->getKey()));

    auto stream = source_ptr->loadIds(to_load);
    stream->readPrefix();

    while (const auto block = stream->read())
    {
        const IColumn & id_column = *block.safeGetByPosition(0).column;

        for (const size_t attribute_idx : ext::range(0, attributes.size()))
        {
            const IColumn & attribute_column = *block.safeGetByPosition(attribute_idx + 1).column;

            for (const auto row_idx : ext::range(0, id_column.size()))
            {
                const auto key = id_column[row_idx].get<UInt64>();

                if (value_by_key.find(key) != value_by_key.end() && attribute.name == attribute_name_by_index.at(attribute_idx))
                {
                    if (attribute.type == AttributeUnderlyingType::utFloat32)
                    {
                        value_by_key[key] = static_cast<Float32>(attribute_column[row_idx].get<Float64>());
                    }
                    else
                    {
                        value_by_key[key] = static_cast<OutputType>(attribute_column[row_idx].get<AttributeType>());
                    }

                }
            }
        }
    }

    stream->readSuffix();

    for (const auto row : ext::range(0, rows))
        set_value(row, value_by_key[ids[row]]);

    query_count.fetch_add(rows, std::memory_order_relaxed);
}

template <typename AttributeType, typename OutputType, typename ValueSetter, typename DefaultGetter>
void DirectDictionary::getItemsStringImpl(
    const Attribute & attribute, const PaddedPODArray<Key> & ids, ValueSetter && set_value, DefaultGetter && get_default) const
{
    const auto rows = ext::size(ids);

    HashMap<Key, String> value_by_key;
    for (const auto row : ext::range(0, rows))
        value_by_key[ids[row]] = get_default(row);

    std::vector<Key> to_load;
    to_load.reserve(value_by_key.size());
    for (auto it = value_by_key.begin(); it != value_by_key.end(); ++it)
        to_load.emplace_back(static_cast<Key>(it->getKey()));

    auto stream = source_ptr->loadIds(to_load);
    stream->readPrefix();

    while (const auto block = stream->read())
    {
        const IColumn & id_column = *block.safeGetByPosition(0).column;

        for (const size_t attribute_idx : ext::range(0, attributes.size()))
        {

            const IColumn & attribute_column = *block.safeGetByPosition(attribute_idx + 1).column;

            for (const auto row_idx : ext::range(0, id_column.size()))
            {
                const auto key = id_column[row_idx].get<UInt64>();
                if (value_by_key.find(key) != value_by_key.end() && attribute.name == attribute_name_by_index.at(attribute_idx))
                {
                    const String from_source = attribute_column[row_idx].get<String>();
                    value_by_key[key] = from_source;
                }
            }
        }
    }
    stream->readSuffix();

    for (const auto row : ext::range(0, rows))
        set_value(row, value_by_key[ids[row]]);

    query_count.fetch_add(rows, std::memory_order_relaxed);
}


const DirectDictionary::Attribute & DirectDictionary::getAttribute(const std::string & attribute_name) const
{
    const auto it = attribute_index_by_name.find(attribute_name);
    if (it == std::end(attribute_index_by_name))
        throw Exception{full_name + ": no such attribute '" + attribute_name + "'", ErrorCodes::BAD_ARGUMENTS};

    return attributes[it->second];
}


template <typename T>
void DirectDictionary::has(const Attribute &, const PaddedPODArray<Key> & ids, PaddedPODArray<UInt8> & out) const
{
    const auto rows = ext::size(ids);

    HashMap<Key, UInt8> has_key;
    for (const auto row : ext::range(0, rows))
        has_key[ids[row]] = 0;

    std::vector<Key> to_load;
    to_load.reserve(has_key.size());
    for (auto it = has_key.begin(); it != has_key.end(); ++it)
        to_load.emplace_back(static_cast<Key>(it->getKey()));

    auto stream = source_ptr->loadIds(to_load);
    stream->readPrefix();

    while (const auto block = stream->read())
    {
        const IColumn & id_column = *block.safeGetByPosition(0).column;

        for (const auto row_idx : ext::range(0, id_column.size()))
        {
            const auto key = id_column[row_idx].get<UInt64>();
            has_key[key] = 1;
        }
    }

    stream->readSuffix();

    for (const auto row : ext::range(0, rows))
        out[row] = has_key[ids[row]];

    query_count.fetch_add(rows, std::memory_order_relaxed);
}


BlockInputStreamPtr DirectDictionary::getBlockInputStream(const Names & /* column_names */, size_t /* max_block_size */) const
{
    return source_ptr->loadAll();
}


void registerDictionaryDirect(DictionaryFactory & factory)
{
    auto create_layout = [=](const std::string & full_name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr) -> DictionaryPtr
    {
        if (dict_struct.key)
            throw Exception{"'key' is not supported for dictionary of layout 'direct'", ErrorCodes::UNSUPPORTED_METHOD};

        if (dict_struct.range_min || dict_struct.range_max)
            throw Exception{full_name
                                + ": elements .structure.range_min and .structure.range_max should be defined only "
                                  "for a dictionary of layout 'range_hashed'",
                            ErrorCodes::BAD_ARGUMENTS};

        const String database = config.getString(config_prefix + ".database", "");
        const String name = config.getString(config_prefix + ".name");

        if (config.has(config_prefix + ".lifetime.min") || config.has(config_prefix + ".lifetime.max"))
            throw Exception{"'lifetime' parameter is redundant for the dictionary' of layout 'direct'", ErrorCodes::BAD_ARGUMENTS};


        return std::make_unique<DirectDictionary>(database, name, dict_struct, std::move(source_ptr));
    };
    factory.registerLayout("direct", create_layout, false);
}


}
