#include <ext/map.h>
#include <ext/range.h>
#include <Dictionaries/ComplexKeyHashedDictionary.h>
#include <Dictionaries/DictionaryBlockInputStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int BAD_ARGUMENTS;
    extern const int DICTIONARY_IS_EMPTY;
}

ComplexKeyHashedDictionary::ComplexKeyHashedDictionary(
    const std::string & name, const DictionaryStructure & dict_struct, DictionarySourcePtr source_ptr,
    const DictionaryLifetime dict_lifetime, bool require_nonempty)
    : name{name}, dict_struct(dict_struct), source_ptr{std::move(source_ptr)}, dict_lifetime(dict_lifetime),
    require_nonempty(require_nonempty)
{

    createAttributes();

    try
    {
        loadData();
        calculateBytesAllocated();
    }
    catch (...)
    {
        creation_exception = std::current_exception();
    }

    creation_time = std::chrono::system_clock::now();
}

ComplexKeyHashedDictionary::ComplexKeyHashedDictionary(const ComplexKeyHashedDictionary & other)
    : ComplexKeyHashedDictionary{other.name, other.dict_struct, other.source_ptr->clone(), other.dict_lifetime, other.require_nonempty}
{
}

#define DECLARE(TYPE)\
void ComplexKeyHashedDictionary::get##TYPE(\
    const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types,\
    PaddedPODArray<TYPE> & out) const\
{\
    dict_struct.validateKeyTypes(key_types);\
    \
    const auto & attribute = getAttribute(attribute_name);\
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::TYPE))\
        throw Exception{\
            name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
            ErrorCodes::TYPE_MISMATCH};\
    \
    const auto null_value = std::get<TYPE>(attribute.null_values);\
    \
    getItemsNumber<TYPE>(attribute, key_columns,\
        [&] (const size_t row, const auto value) { out[row] = value; },\
        [&] (const size_t) { return null_value; });\
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
#undef DECLARE

void ComplexKeyHashedDictionary::getString(
    const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types,
    ColumnString * out) const
{
    dict_struct.validateKeyTypes(key_types);

    const auto & attribute = getAttribute(attribute_name);
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::String))
        throw Exception{
            name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
            ErrorCodes::TYPE_MISMATCH};

    const auto & null_value = StringRef{std::get<String>(attribute.null_values)};

    getItemsImpl<StringRef, StringRef>(attribute, key_columns,
        [&] (const size_t, const StringRef value) { out->insertData(value.data, value.size); },
        [&] (const size_t) { return null_value; });
}

#define DECLARE(TYPE)\
void ComplexKeyHashedDictionary::get##TYPE(\
    const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types,\
    const PaddedPODArray<TYPE> & def, PaddedPODArray<TYPE> & out) const\
{\
    dict_struct.validateKeyTypes(key_types);\
    \
    const auto & attribute = getAttribute(attribute_name);\
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::TYPE))\
        throw Exception{\
            name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
            ErrorCodes::TYPE_MISMATCH};\
    \
    getItemsNumber<TYPE>(attribute, key_columns,\
        [&] (const size_t row, const auto value) { out[row] = value; },\
        [&] (const size_t row) { return def[row]; });\
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
#undef DECLARE

void ComplexKeyHashedDictionary::getString(
    const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types,
    const ColumnString * const def, ColumnString * const out) const
{
    dict_struct.validateKeyTypes(key_types);

    const auto & attribute = getAttribute(attribute_name);
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::String))
        throw Exception{
            name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
            ErrorCodes::TYPE_MISMATCH};

    getItemsImpl<StringRef, StringRef>(attribute, key_columns,
        [&] (const size_t, const StringRef value) { out->insertData(value.data, value.size); },
        [&] (const size_t row) { return def->getDataAt(row); });
}

#define DECLARE(TYPE)\
void ComplexKeyHashedDictionary::get##TYPE(\
    const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types,\
    const TYPE def, PaddedPODArray<TYPE> & out) const\
{\
    dict_struct.validateKeyTypes(key_types);\
    \
    const auto & attribute = getAttribute(attribute_name);\
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::TYPE))\
        throw Exception{\
            name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
            ErrorCodes::TYPE_MISMATCH};\
    \
    getItemsNumber<TYPE>(attribute, key_columns,\
        [&] (const size_t row, const auto value) { out[row] = value; },\
        [&] (const size_t) { return def; });\
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
#undef DECLARE

void ComplexKeyHashedDictionary::getString(
    const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types,
    const String & def, ColumnString * const out) const
{
    dict_struct.validateKeyTypes(key_types);

    const auto & attribute = getAttribute(attribute_name);
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::String))
        throw Exception{
            name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
            ErrorCodes::TYPE_MISMATCH};

    getItemsImpl<StringRef, StringRef>(attribute, key_columns,
        [&] (const size_t, const StringRef value) { out->insertData(value.data, value.size); },
        [&] (const size_t) { return StringRef{def}; });
}

void ComplexKeyHashedDictionary::has(const Columns & key_columns, const DataTypes & key_types, PaddedPODArray<UInt8> & out) const
{
    dict_struct.validateKeyTypes(key_types);

    const auto & attribute = attributes.front();

    switch (attribute.type)
    {
        case AttributeUnderlyingType::UInt8: has<UInt8>(attribute, key_columns, out); break;
        case AttributeUnderlyingType::UInt16: has<UInt16>(attribute, key_columns, out); break;
        case AttributeUnderlyingType::UInt32: has<UInt32>(attribute, key_columns, out); break;
        case AttributeUnderlyingType::UInt64: has<UInt64>(attribute, key_columns, out); break;
        case AttributeUnderlyingType::UInt128: has<UInt128>(attribute, key_columns, out); break;
        case AttributeUnderlyingType::Int8: has<Int8>(attribute, key_columns, out); break;
        case AttributeUnderlyingType::Int16: has<Int16>(attribute, key_columns, out); break;
        case AttributeUnderlyingType::Int32: has<Int32>(attribute, key_columns, out); break;
        case AttributeUnderlyingType::Int64: has<Int64>(attribute, key_columns, out); break;
        case AttributeUnderlyingType::Float32: has<Float32>(attribute, key_columns, out); break;
        case AttributeUnderlyingType::Float64: has<Float64>(attribute, key_columns, out); break;
        case AttributeUnderlyingType::String: has<StringRef>(attribute, key_columns, out); break;
    }
}

void ComplexKeyHashedDictionary::createAttributes()
{
    const auto size = dict_struct.attributes.size();
    attributes.reserve(size);

    for (const auto & attribute : dict_struct.attributes)
    {
        attribute_index_by_name.emplace(attribute.name, attributes.size());
        attributes.push_back(createAttributeWithType(attribute.underlying_type, attribute.null_value));

        if (attribute.hierarchical)
            throw Exception{
                name + ": hierarchical attributes not supported for dictionary of type " + getTypeName(),
                ErrorCodes::TYPE_MISMATCH};
    }
}

void ComplexKeyHashedDictionary::loadData()
{
    auto stream = source_ptr->loadAll();
    stream->readPrefix();

    /// created upfront to avoid excess allocations
    const auto keys_size = dict_struct.key->size();
    StringRefs keys(keys_size);

    const auto attributes_size = attributes.size();

    while (const auto block = stream->read())
    {
        const auto rows = block.rows();
        element_count += rows;

        const auto key_column_ptrs = ext::map<Columns>(ext::range(0, keys_size),
            [&] (const size_t attribute_idx) {
                return block.safeGetByPosition(attribute_idx).column;
            });

        const auto attribute_column_ptrs = ext::map<Columns>(ext::range(0, attributes_size),
            [&] (const size_t attribute_idx) {
                return block.safeGetByPosition(keys_size + attribute_idx).column;
            });

        for (const auto row_idx : ext::range(0, rows))
        {
            /// calculate key once per row
            const auto key = placeKeysInPool(row_idx, key_column_ptrs, keys, keys_pool);

            auto should_rollback = false;

            for (const auto attribute_idx : ext::range(0, attributes_size))
            {
                const auto & attribute_column = *attribute_column_ptrs[attribute_idx];
                auto & attribute = attributes[attribute_idx];
                const auto inserted = setAttributeValue(attribute, key, attribute_column[row_idx]);
                if (!inserted)
                    should_rollback = true;
            }

            /// @note on multiple equal keys the mapped value for the first one is stored
            if (should_rollback)
                keys_pool.rollback(key.size);
        }

    }

    stream->readSuffix();

    if (require_nonempty && 0 == element_count)
        throw Exception{
            name + ": dictionary source is empty and 'require_nonempty' property is set.",
            ErrorCodes::DICTIONARY_IS_EMPTY};
}

template <typename T>
void ComplexKeyHashedDictionary::addAttributeSize(const Attribute & attribute)
{
    const auto & map_ref = std::get<ContainerPtrType<T>>(attribute.maps);
    bytes_allocated += sizeof(ContainerType<T>) + map_ref->getBufferSizeInBytes();
    bucket_count = map_ref->getBufferSizeInCells();
}

void ComplexKeyHashedDictionary::calculateBytesAllocated()
{
    bytes_allocated += attributes.size() * sizeof(attributes.front());

    for (const auto & attribute : attributes)
    {
        switch (attribute.type)
        {
            case AttributeUnderlyingType::UInt8: addAttributeSize<UInt8>(attribute); break;
            case AttributeUnderlyingType::UInt16: addAttributeSize<UInt16>(attribute); break;
            case AttributeUnderlyingType::UInt32: addAttributeSize<UInt32>(attribute); break;
            case AttributeUnderlyingType::UInt64: addAttributeSize<UInt64>(attribute); break;
            case AttributeUnderlyingType::UInt128: addAttributeSize<UInt128>(attribute); break;
            case AttributeUnderlyingType::Int8: addAttributeSize<Int8>(attribute); break;
            case AttributeUnderlyingType::Int16: addAttributeSize<Int16>(attribute); break;
            case AttributeUnderlyingType::Int32: addAttributeSize<Int32>(attribute); break;
            case AttributeUnderlyingType::Int64: addAttributeSize<Int64>(attribute); break;
            case AttributeUnderlyingType::Float32: addAttributeSize<Float32>(attribute); break;
            case AttributeUnderlyingType::Float64: addAttributeSize<Float64>(attribute); break;
            case AttributeUnderlyingType::String:
            {
                addAttributeSize<StringRef>(attribute);
                bytes_allocated += sizeof(Arena) + attribute.string_arena->size();

                break;
            }
        }
    }

    bytes_allocated += keys_pool.size();
}

template <typename T>
void ComplexKeyHashedDictionary::createAttributeImpl(Attribute & attribute, const Field & null_value)
{
    std::get<T>(attribute.null_values) = null_value.get<typename NearestFieldType<T>::Type>();
    std::get<ContainerPtrType<T>>(attribute.maps) = std::make_unique<ContainerType<T>>();
}

ComplexKeyHashedDictionary::Attribute ComplexKeyHashedDictionary::createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value)
{
    Attribute attr{type, {}, {}, {}};

    switch (type)
    {
        case AttributeUnderlyingType::UInt8: createAttributeImpl<UInt8>(attr, null_value); break;
        case AttributeUnderlyingType::UInt16: createAttributeImpl<UInt16>(attr, null_value); break;
        case AttributeUnderlyingType::UInt32: createAttributeImpl<UInt32>(attr, null_value); break;
        case AttributeUnderlyingType::UInt64: createAttributeImpl<UInt64>(attr, null_value); break;
        case AttributeUnderlyingType::UInt128: createAttributeImpl<UInt128>(attr, null_value); break;
        case AttributeUnderlyingType::Int8: createAttributeImpl<Int8>(attr, null_value); break;
        case AttributeUnderlyingType::Int16: createAttributeImpl<Int16>(attr, null_value); break;
        case AttributeUnderlyingType::Int32: createAttributeImpl<Int32>(attr, null_value); break;
        case AttributeUnderlyingType::Int64: createAttributeImpl<Int64>(attr, null_value); break;
        case AttributeUnderlyingType::Float32: createAttributeImpl<Float32>(attr, null_value); break;
        case AttributeUnderlyingType::Float64: createAttributeImpl<Float64>(attr, null_value); break;
        case AttributeUnderlyingType::String:
        {
            std::get<String>(attr.null_values) = null_value.get<String>();
            std::get<ContainerPtrType<StringRef>>(attr.maps) = std::make_unique<ContainerType<StringRef>>();
            attr.string_arena = std::make_unique<Arena>();
            break;
        }
    }

    return attr;
}


template <typename OutputType, typename ValueSetter, typename DefaultGetter>
void ComplexKeyHashedDictionary::getItemsNumber(
    const Attribute & attribute,
    const Columns & key_columns,
    ValueSetter && set_value,
    DefaultGetter && get_default) const
{
    if (false) {}
#define DISPATCH(TYPE) \
    else if (attribute.type == AttributeUnderlyingType::TYPE) \
        getItemsImpl<TYPE, OutputType>(attribute, key_columns, std::forward<ValueSetter>(set_value), std::forward<DefaultGetter>(get_default));
    DISPATCH(UInt8)
    DISPATCH(UInt16)
    DISPATCH(UInt32)
    DISPATCH(UInt64)
    DISPATCH(UInt128)
    DISPATCH(Int8)
    DISPATCH(Int16)
    DISPATCH(Int32)
    DISPATCH(Int64)
    DISPATCH(Float32)
    DISPATCH(Float64)
#undef DISPATCH
    else
        throw Exception("Unexpected type of attribute: " + toString(attribute.type), ErrorCodes::LOGICAL_ERROR);
}

template <typename AttributeType, typename OutputType, typename ValueSetter, typename DefaultGetter>
void ComplexKeyHashedDictionary::getItemsImpl(
    const Attribute & attribute,
    const Columns & key_columns,
    ValueSetter && set_value,
    DefaultGetter && get_default) const
{
    const auto & attr = *std::get<ContainerPtrType<AttributeType>>(attribute.maps);

    const auto keys_size = key_columns.size();
    StringRefs keys(keys_size);
    Arena temporary_keys_pool;

    const auto rows = key_columns.front()->size();
    for (const auto i : ext::range(0, rows))
    {
        /// copy key data to arena so it is contiguous and return StringRef to it
        const auto key = placeKeysInPool(i, key_columns, keys, temporary_keys_pool);

        const auto it = attr.find(key);
        set_value(i, it != attr.end() ? static_cast<OutputType>(it->second) : get_default(i));

        /// free memory allocated for the key
        temporary_keys_pool.rollback(key.size);
    }

    query_count.fetch_add(rows, std::memory_order_relaxed);
}


template <typename T>
bool ComplexKeyHashedDictionary::setAttributeValueImpl(Attribute & attribute, const StringRef key, const T value)
{
    auto & map = *std::get<ContainerPtrType<T>>(attribute.maps);
    const auto pair = map.insert({ key, value });
    return pair.second;
}

bool ComplexKeyHashedDictionary::setAttributeValue(Attribute & attribute, const StringRef key, const Field & value)
{
    switch (attribute.type)
    {
        case AttributeUnderlyingType::UInt8: return setAttributeValueImpl<UInt8>(attribute, key, value.get<UInt64>());
        case AttributeUnderlyingType::UInt16: return setAttributeValueImpl<UInt16>(attribute, key, value.get<UInt64>());
        case AttributeUnderlyingType::UInt32: return setAttributeValueImpl<UInt32>(attribute, key, value.get<UInt64>());
        case AttributeUnderlyingType::UInt64: return setAttributeValueImpl<UInt64>(attribute, key, value.get<UInt64>());
        case AttributeUnderlyingType::UInt128: return setAttributeValueImpl<UInt128>(attribute, key, value.get<UInt128>());
        case AttributeUnderlyingType::Int8: return setAttributeValueImpl<Int8>(attribute, key, value.get<Int64>());
        case AttributeUnderlyingType::Int16: return setAttributeValueImpl<Int16>(attribute, key, value.get<Int64>());
        case AttributeUnderlyingType::Int32: return setAttributeValueImpl<Int32>(attribute, key, value.get<Int64>());
        case AttributeUnderlyingType::Int64: return setAttributeValueImpl<Int64>(attribute, key, value.get<Int64>());
        case AttributeUnderlyingType::Float32: return setAttributeValueImpl<Float32>(attribute, key, value.get<Float64>());
        case AttributeUnderlyingType::Float64: return setAttributeValueImpl<Float64>(attribute, key, value.get<Float64>());
        case AttributeUnderlyingType::String:
        {
            auto & map = *std::get<ContainerPtrType<StringRef>>(attribute.maps);
            const auto & string = value.get<String>();
            const auto string_in_arena = attribute.string_arena->insert(string.data(), string.size());
            const auto pair = map.insert({ key, StringRef{string_in_arena, string.size()} });
            return pair.second;
        }
    }

    return {};
}

const ComplexKeyHashedDictionary::Attribute & ComplexKeyHashedDictionary::getAttribute(const std::string & attribute_name) const
{
    const auto it = attribute_index_by_name.find(attribute_name);
    if (it == std::end(attribute_index_by_name))
        throw Exception{
            name + ": no such attribute '" + attribute_name + "'",
            ErrorCodes::BAD_ARGUMENTS};

    return attributes[it->second];
}

StringRef ComplexKeyHashedDictionary::placeKeysInPool(
    const size_t row, const Columns & key_columns, StringRefs & keys, Arena & pool)
{
    const auto keys_size = key_columns.size();
    size_t sum_keys_size{};

    const char * block_start = nullptr;
    for (size_t j = 0; j < keys_size; ++j)
    {
        keys[j] = key_columns[j]->serializeValueIntoArena(row, pool, block_start);
        sum_keys_size += keys[j].size;
    }

    auto key_start = block_start;
    for (size_t j = 0; j < keys_size; ++j)
    {
        keys[j].data = key_start;
        key_start += keys[j].size;
    }

    return { block_start, sum_keys_size };
}

template <typename T>
void ComplexKeyHashedDictionary::has(const Attribute & attribute, const Columns & key_columns, PaddedPODArray<UInt8> & out) const
{
    const auto & attr = *std::get<ContainerPtrType<T>>(attribute.maps);
    const auto keys_size = key_columns.size();
    StringRefs keys(keys_size);
    Arena temporary_keys_pool;
    const auto rows = key_columns.front()->size();

    for (const auto i : ext::range(0, rows))
    {
        /// copy key data to arena so it is contiguous and return StringRef to it
        const auto key = placeKeysInPool(i, key_columns, keys, temporary_keys_pool);

        const auto it = attr.find(key);
        out[i] = it != attr.end();

        /// free memory allocated for the key
        temporary_keys_pool.rollback(key.size);
    }

    query_count.fetch_add(rows, std::memory_order_relaxed);
}

std::vector<StringRef> ComplexKeyHashedDictionary::getKeys() const
{
    const Attribute & attribute = attributes.front();

    switch (attribute.type)
    {
        case AttributeUnderlyingType::UInt8: return getKeys<UInt8>(attribute); break;
        case AttributeUnderlyingType::UInt16: return getKeys<UInt16>(attribute); break;
        case AttributeUnderlyingType::UInt32: return getKeys<UInt32>(attribute); break;
        case AttributeUnderlyingType::UInt64: return getKeys<UInt64>(attribute); break;
        case AttributeUnderlyingType::UInt128: return getKeys<UInt128>(attribute); break;
        case AttributeUnderlyingType::Int8: return getKeys<Int8>(attribute); break;
        case AttributeUnderlyingType::Int16: return getKeys<Int16>(attribute); break;
        case AttributeUnderlyingType::Int32: return getKeys<Int32>(attribute); break;
        case AttributeUnderlyingType::Int64: return getKeys<Int64>(attribute); break;
        case AttributeUnderlyingType::Float32: return getKeys<Float32>(attribute); break;
        case AttributeUnderlyingType::Float64: return getKeys<Float64>(attribute); break;
        case AttributeUnderlyingType::String: return getKeys<StringRef>(attribute); break;
    }
    return {};
}

template <typename T>
std::vector<StringRef> ComplexKeyHashedDictionary::getKeys(const Attribute & attribute) const
{
    const ContainerType<T> & attr = *std::get<ContainerPtrType<T>>(attribute.maps);
    std::vector<StringRef> keys;
    keys.reserve(attr.size());
    for (const auto & key : attr)
        keys.push_back(key.first);

    return keys;
}

BlockInputStreamPtr ComplexKeyHashedDictionary::getBlockInputStream(const Names & column_names, size_t max_block_size) const
{
    using BlockInputStreamType = DictionaryBlockInputStream<ComplexKeyHashedDictionary, UInt64>;
    return std::make_shared<BlockInputStreamType>(shared_from_this(), max_block_size, getKeys(), column_names);
}


}
