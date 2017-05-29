#include <Dictionaries/FlatDictionary.h>
#include <Dictionaries/DictionaryBlockInputStream.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int BAD_ARGUMENTS;
    extern const int DICTIONARY_IS_EMPTY;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TYPE;
}


static const auto initial_array_size = 1024;
static const auto max_array_size = 500000;


FlatDictionary::FlatDictionary(const std::string & name, const DictionaryStructure & dict_struct,
    DictionarySourcePtr source_ptr, const DictionaryLifetime dict_lifetime, bool require_nonempty)
    : name{name}, dict_struct(dict_struct),
        source_ptr{std::move(source_ptr)}, dict_lifetime(dict_lifetime),
        require_nonempty(require_nonempty),
        loaded_ids(initial_array_size, false)
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

FlatDictionary::FlatDictionary(const FlatDictionary & other)
    : FlatDictionary{other.name, other.dict_struct, other.source_ptr->clone(), other.dict_lifetime, other.require_nonempty}
{
}


void FlatDictionary::toParent(const PaddedPODArray<Key> & ids, PaddedPODArray<Key> & out) const
{
    const auto null_value = std::get<UInt64>(hierarchical_attribute->null_values);

    getItemsNumber<UInt64>(*hierarchical_attribute, ids,
        [&] (const std::size_t row, const UInt64 value) { out[row] = value; },
        [&] (const std::size_t) { return null_value; });
}


/// Allow to use single value in same way as array.
static inline FlatDictionary::Key getAt(const PaddedPODArray<FlatDictionary::Key> & arr, const size_t idx) { return arr[idx]; }
static inline FlatDictionary::Key getAt(const FlatDictionary::Key & value, const size_t idx) { return value; }

template <typename ChildType, typename AncestorType>
void FlatDictionary::isInImpl(
    const ChildType & child_ids,
    const AncestorType & ancestor_ids,
    PaddedPODArray<UInt8> & out) const
{
    const auto null_value = std::get<UInt64>(hierarchical_attribute->null_values);
    const auto & attr = *std::get<ContainerPtrType<Key>>(hierarchical_attribute->arrays);
    const auto rows = out.size();

    size_t loaded_size = attr.size();
    for (const auto row : ext::range(0, rows))
    {
        auto id = getAt(child_ids, row);
        const auto ancestor_id = getAt(ancestor_ids, row);

        while (id < loaded_size && id != null_value && id != ancestor_id)
            id = attr[id];

        out[row] = id != null_value && id == ancestor_id;
    }

    query_count.fetch_add(rows, std::memory_order_relaxed);
}


void FlatDictionary::isInVectorVector(
    const PaddedPODArray<Key> & child_ids,
    const PaddedPODArray<Key> & ancestor_ids,
    PaddedPODArray<UInt8> & out) const
{
    isInImpl(child_ids, ancestor_ids, out);
}

void FlatDictionary::isInVectorConstant(
    const PaddedPODArray<Key> & child_ids,
    const Key ancestor_id,
    PaddedPODArray<UInt8> & out) const
{
    isInImpl(child_ids, ancestor_id, out);
}

void FlatDictionary::isInConstantVector(
    const Key child_id,
    const PaddedPODArray<Key> & ancestor_ids,
    PaddedPODArray<UInt8> & out) const
{
    isInImpl(child_id, ancestor_ids, out);
}


#define DECLARE(TYPE)\
void FlatDictionary::get##TYPE(const std::string & attribute_name, const PaddedPODArray<Key> & ids, PaddedPODArray<TYPE> & out) const\
{\
    const auto & attribute = getAttribute(attribute_name);\
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::TYPE))\
        throw Exception{\
            name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
            ErrorCodes::TYPE_MISMATCH};\
    \
    const auto null_value = std::get<TYPE>(attribute.null_values);\
    \
    getItemsNumber<TYPE>(attribute, ids,\
        [&] (const std::size_t row, const auto value) { out[row] = value; },\
        [&] (const std::size_t) { return null_value; });\
}
DECLARE(UInt8)
DECLARE(UInt16)
DECLARE(UInt32)
DECLARE(UInt64)
DECLARE(Int8)
DECLARE(Int16)
DECLARE(Int32)
DECLARE(Int64)
DECLARE(Float32)
DECLARE(Float64)
#undef DECLARE

void FlatDictionary::getString(const std::string & attribute_name, const PaddedPODArray<Key> & ids, ColumnString * out) const
{
    const auto & attribute = getAttribute(attribute_name);
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::String))
        throw Exception{
            name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
            ErrorCodes::TYPE_MISMATCH};

    const auto & null_value = std::get<StringRef>(attribute.null_values);

    getItemsImpl<StringRef, StringRef>(attribute, ids,
        [&] (const std::size_t row, const StringRef value) { out->insertData(value.data, value.size); },
        [&] (const std::size_t) { return null_value; });
}

#define DECLARE(TYPE)\
void FlatDictionary::get##TYPE(\
    const std::string & attribute_name, const PaddedPODArray<Key> & ids, const PaddedPODArray<TYPE> & def,\
    PaddedPODArray<TYPE> & out) const\
{\
    const auto & attribute = getAttribute(attribute_name);\
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::TYPE))\
        throw Exception{\
            name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
            ErrorCodes::TYPE_MISMATCH};\
    \
    getItemsNumber<TYPE>(attribute, ids,\
        [&] (const std::size_t row, const auto value) { out[row] = value; },\
        [&] (const std::size_t row) { return def[row]; });\
}
DECLARE(UInt8)
DECLARE(UInt16)
DECLARE(UInt32)
DECLARE(UInt64)
DECLARE(Int8)
DECLARE(Int16)
DECLARE(Int32)
DECLARE(Int64)
DECLARE(Float32)
DECLARE(Float64)
#undef DECLARE

void FlatDictionary::getString(
    const std::string & attribute_name, const PaddedPODArray<Key> & ids, const ColumnString * const def,
    ColumnString * const out) const
{
    const auto & attribute = getAttribute(attribute_name);
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::String))
        throw Exception{
            name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
            ErrorCodes::TYPE_MISMATCH};

    getItemsImpl<StringRef, StringRef>(attribute, ids,
        [&] (const std::size_t row, const StringRef value) { out->insertData(value.data, value.size); },
        [&] (const std::size_t row) { return def->getDataAt(row); });
}

#define DECLARE(TYPE)\
void FlatDictionary::get##TYPE(\
    const std::string & attribute_name, const PaddedPODArray<Key> & ids, const TYPE def,\
    PaddedPODArray<TYPE> & out) const\
{\
    const auto & attribute = getAttribute(attribute_name);\
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::TYPE))\
        throw Exception{\
            name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
            ErrorCodes::TYPE_MISMATCH};\
    \
    getItemsNumber<TYPE>(attribute, ids,\
        [&] (const std::size_t row, const auto value) { out[row] = value; },\
        [&] (const std::size_t) { return def; });\
}
DECLARE(UInt8)
DECLARE(UInt16)
DECLARE(UInt32)
DECLARE(UInt64)
DECLARE(Int8)
DECLARE(Int16)
DECLARE(Int32)
DECLARE(Int64)
DECLARE(Float32)
DECLARE(Float64)
#undef DECLARE

void FlatDictionary::getString(
    const std::string & attribute_name, const PaddedPODArray<Key> & ids, const String & def,
    ColumnString * const out) const
{
    const auto & attribute = getAttribute(attribute_name);
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::String))
        throw Exception{
            name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
            ErrorCodes::TYPE_MISMATCH};

    FlatDictionary::getItemsImpl<StringRef, StringRef>(attribute, ids,
        [&] (const std::size_t row, const StringRef value) { out->insertData(value.data, value.size); },
        [&] (const std::size_t) { return StringRef{def}; });
}


void FlatDictionary::has(const PaddedPODArray<Key> & ids, PaddedPODArray<UInt8> & out) const
{
    const auto & attribute = attributes.front();

    switch (attribute.type)
    {
        case AttributeUnderlyingType::UInt8: has<UInt8>(attribute, ids, out); break;
        case AttributeUnderlyingType::UInt16: has<UInt16>(attribute, ids, out); break;
        case AttributeUnderlyingType::UInt32: has<UInt32>(attribute, ids, out); break;
        case AttributeUnderlyingType::UInt64: has<UInt64>(attribute, ids, out); break;
        case AttributeUnderlyingType::Int8: has<Int8>(attribute, ids, out); break;
        case AttributeUnderlyingType::Int16: has<Int16>(attribute, ids, out); break;
        case AttributeUnderlyingType::Int32: has<Int32>(attribute, ids, out); break;
        case AttributeUnderlyingType::Int64: has<Int64>(attribute, ids, out); break;
        case AttributeUnderlyingType::Float32: has<Float32>(attribute, ids, out); break;
        case AttributeUnderlyingType::Float64: has<Float64>(attribute, ids, out); break;
        case AttributeUnderlyingType::String: has<String>(attribute, ids, out); break;
    }
}


void FlatDictionary::createAttributes()
{
    const auto size = dict_struct.attributes.size();
    attributes.reserve(size);

    for (const auto & attribute : dict_struct.attributes)
    {
        attribute_index_by_name.emplace(attribute.name, attributes.size());
        attributes.push_back(createAttributeWithType(attribute.underlying_type, attribute.null_value));

        if (attribute.hierarchical)
        {
            hierarchical_attribute = &attributes.back();

            if (hierarchical_attribute->type != AttributeUnderlyingType::UInt64)
                throw Exception{
                    name + ": hierarchical attribute must be UInt64.",
                    ErrorCodes::TYPE_MISMATCH};
        }
    }
}


void FlatDictionary::loadData()
{
    auto stream = source_ptr->loadAll();
    stream->readPrefix();

    while (const auto block = stream->read())
    {
        const auto & id_column = *block.safeGetByPosition(0).column;

        element_count += id_column.size();

        for (const auto attribute_idx : ext::range(0, attributes.size()))
        {
            const auto & attribute_column = *block.safeGetByPosition(attribute_idx + 1).column;
            auto & attribute = attributes[attribute_idx];

            for (const auto row_idx : ext::range(0, id_column.size()))
                setAttributeValue(attribute, id_column[row_idx].get<UInt64>(), attribute_column[row_idx]);
        }
    }

    stream->readSuffix();

    if (require_nonempty && 0 == element_count)
        throw Exception{
            name + ": dictionary source is empty and 'require_nonempty' property is set.",
            ErrorCodes::DICTIONARY_IS_EMPTY};
}


template <typename T>
void FlatDictionary::addAttributeSize(const Attribute & attribute)
{
    const auto & array_ref = std::get<ContainerPtrType<T>>(attribute.arrays);
    bytes_allocated += sizeof(PaddedPODArray<T>) + array_ref->allocated_size();
    bucket_count = array_ref->capacity();
}


void FlatDictionary::calculateBytesAllocated()
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
}


template <typename T>
void FlatDictionary::createAttributeImpl(Attribute & attribute, const Field & null_value)
{
    const auto & null_value_ref = std::get<T>(attribute.null_values) =
        null_value.get<typename NearestFieldType<T>::Type>();
    std::get<ContainerPtrType<T>>(attribute.arrays) =
        std::make_unique<ContainerType<T>>(initial_array_size, null_value_ref);
}

template <>
void FlatDictionary::createAttributeImpl<String>(Attribute & attribute, const Field & null_value)
{
    attribute.string_arena = std::make_unique<Arena>();
    auto & null_value_ref = std::get<StringRef>(attribute.null_values);
    const String string = null_value.get<typename NearestFieldType<String>::Type>();
    const auto string_in_arena = attribute.string_arena->insert(string.data(), string.size());
    null_value_ref = StringRef{string_in_arena, string.size()};
    std::get<ContainerPtrType<StringRef>>(attribute.arrays) =
        std::make_unique<ContainerType<StringRef>>(initial_array_size, null_value_ref);
}


FlatDictionary::Attribute FlatDictionary::createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value)
{
    Attribute attr{type};

    switch (type)
    {
        case AttributeUnderlyingType::UInt8: createAttributeImpl<UInt8>(attr, null_value); break;
        case AttributeUnderlyingType::UInt16: createAttributeImpl<UInt16>(attr, null_value); break;
        case AttributeUnderlyingType::UInt32: createAttributeImpl<UInt32>(attr, null_value); break;
        case AttributeUnderlyingType::UInt64: createAttributeImpl<UInt64>(attr, null_value); break;
        case AttributeUnderlyingType::Int8: createAttributeImpl<Int8>(attr, null_value); break;
        case AttributeUnderlyingType::Int16: createAttributeImpl<Int16>(attr, null_value); break;
        case AttributeUnderlyingType::Int32: createAttributeImpl<Int32>(attr, null_value); break;
        case AttributeUnderlyingType::Int64: createAttributeImpl<Int64>(attr, null_value); break;
        case AttributeUnderlyingType::Float32: createAttributeImpl<Float32>(attr, null_value); break;
        case AttributeUnderlyingType::Float64: createAttributeImpl<Float64>(attr, null_value); break;
        case AttributeUnderlyingType::String: createAttributeImpl<String>(attr, null_value); break;
    }

    return attr;
}


template <typename OutputType, typename ValueSetter, typename DefaultGetter>
void FlatDictionary::getItemsNumber(
    const Attribute & attribute,
    const PaddedPODArray<Key> & ids,
    ValueSetter && set_value,
    DefaultGetter && get_default) const
{
    if (false) {}
#define DISPATCH(TYPE) \
    else if (attribute.type == AttributeUnderlyingType::TYPE) \
        getItemsImpl<TYPE, OutputType>(attribute, ids, std::forward<ValueSetter>(set_value), std::forward<DefaultGetter>(get_default));
    DISPATCH(UInt8)
    DISPATCH(UInt16)
    DISPATCH(UInt32)
    DISPATCH(UInt64)
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
void FlatDictionary::getItemsImpl(
    const Attribute & attribute,
    const PaddedPODArray<Key> & ids,
    ValueSetter && set_value,
    DefaultGetter && get_default) const
{
    const auto & attr = *std::get<ContainerPtrType<AttributeType>>(attribute.arrays);
    const auto rows = ext::size(ids);

    for (const auto row : ext::range(0, rows))
    {
        const auto id = ids[row];
        set_value(row, id < ext::size(attr) && loaded_ids[id] ? attr[id] : get_default(row));
    }

    query_count.fetch_add(rows, std::memory_order_relaxed);
}

template <typename T>
void FlatDictionary::resize(Attribute & attribute, const Key id)
{
    if (id >= max_array_size)
        throw Exception{
            name + ": identifier should be less than " + toString(max_array_size),
            ErrorCodes::ARGUMENT_OUT_OF_BOUND};

    auto & array = *std::get<ContainerPtrType<T>>(attribute.arrays);
    if (id >= array.size())
    {
        const size_t elements_count = id + 1; //id=0 -> elements_count=1
        loaded_ids.resize(elements_count, false);
        array.resize_fill(elements_count, std::get<T>(attribute.null_values));
    }
}

template <typename T>
void FlatDictionary::setAttributeValueImpl(Attribute & attribute, const Key id, const T & value)
{
    resize<T>(attribute, id);
    auto & array = *std::get<ContainerPtrType<T>>(attribute.arrays);
    array[id] = value;
    loaded_ids[id] = true;
}

template <>
void FlatDictionary::setAttributeValueImpl<String>(Attribute & attribute, const Key id, const String & string)
{
    resize<StringRef>(attribute, id);
    const auto string_in_arena = attribute.string_arena->insert(string.data(), string.size());
    auto & array = *std::get<ContainerPtrType<StringRef>>(attribute.arrays);
    array[id] = StringRef{string_in_arena, string.size()};
    loaded_ids[id] = true;
}

void FlatDictionary::setAttributeValue(Attribute & attribute, const Key id, const Field & value)
{
    switch (attribute.type)
    {
        case AttributeUnderlyingType::UInt8: setAttributeValueImpl<UInt8>(attribute, id, value.get<UInt64>()); break;
        case AttributeUnderlyingType::UInt16: setAttributeValueImpl<UInt16>(attribute, id, value.get<UInt64>()); break;
        case AttributeUnderlyingType::UInt32: setAttributeValueImpl<UInt32>(attribute, id, value.get<UInt64>()); break;
        case AttributeUnderlyingType::UInt64: setAttributeValueImpl<UInt64>(attribute, id, value.get<UInt64>()); break;
        case AttributeUnderlyingType::Int8: setAttributeValueImpl<Int8>(attribute, id, value.get<Int64>()); break;
        case AttributeUnderlyingType::Int16: setAttributeValueImpl<Int16>(attribute, id, value.get<Int64>()); break;
        case AttributeUnderlyingType::Int32: setAttributeValueImpl<Int32>(attribute, id, value.get<Int64>()); break;
        case AttributeUnderlyingType::Int64: setAttributeValueImpl<Int64>(attribute, id, value.get<Int64>()); break;
        case AttributeUnderlyingType::Float32: setAttributeValueImpl<Float32>(attribute, id, value.get<Float64>()); break;
        case AttributeUnderlyingType::Float64: setAttributeValueImpl<Float64>(attribute, id, value.get<Float64>()); break;
        case AttributeUnderlyingType::String: setAttributeValueImpl<String>(attribute, id, value.get<String>()); break;
    }
}


const FlatDictionary::Attribute & FlatDictionary::getAttribute(const std::string & attribute_name) const
{
    const auto it = attribute_index_by_name.find(attribute_name);
    if (it == std::end(attribute_index_by_name))
        throw Exception{
            name + ": no such attribute '" + attribute_name + "'",
            ErrorCodes::BAD_ARGUMENTS};

    return attributes[it->second];
}


template <typename T>
void FlatDictionary::has(const Attribute & attribute, const PaddedPODArray<Key> & ids, PaddedPODArray<UInt8> & out) const
{
    const auto ids_count = ext::size(ids);

    for (const auto i : ext::range(0, ids_count))
    {
        const auto id = ids[i];
        out[i] = id < loaded_ids.size() && loaded_ids[id];
    }

    query_count.fetch_add(ids_count, std::memory_order_relaxed);
}


PaddedPODArray<FlatDictionary::Key> FlatDictionary::getIds() const
{
    const auto ids_count = ext::size(loaded_ids);

    PaddedPODArray<Key> ids;
    for (auto idx : ext::range(0, ids_count))
    {
        if (loaded_ids[idx]) {
            ids.push_back(idx);
        }
    }
    return ids;
}

BlockInputStreamPtr FlatDictionary::getBlockInputStream(const Names & column_names, size_t max_block_size) const
{
    using BlockInputStreamType = DictionaryBlockInputStream<FlatDictionary, Key>;
    return std::make_shared<BlockInputStreamType>(shared_from_this(), max_block_size, getIds() ,column_names);
}


}
