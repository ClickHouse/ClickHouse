#include <stack>
#include <ext/map.h>
#include <ext/range.h>
#include <Poco/Net/IPAddress.h>
#include <Poco/ByteOrder.h>
#include <Dictionaries/TrieDictionary.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnFixedString.h>
#include <Dictionaries/DictionaryBlockInputStream.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <IO/WriteIntText.h>
#include <Common/formatIPv6.h>
#include <iostream>
#include <btrie.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int BAD_ARGUMENTS;
    extern const int DICTIONARY_IS_EMPTY;
}

TrieDictionary::TrieDictionary(
    const std::string & name, const DictionaryStructure & dict_struct, DictionarySourcePtr source_ptr,
    const DictionaryLifetime dict_lifetime, bool require_nonempty)
    : name{name}, dict_struct(dict_struct), source_ptr{std::move(source_ptr)}, dict_lifetime(dict_lifetime),
    require_nonempty(require_nonempty), logger(&Poco::Logger::get("TrieDictionary"))
{
    createAttributes();
    trie = btrie_create();

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

TrieDictionary::TrieDictionary(const TrieDictionary & other)
    : TrieDictionary{other.name, other.dict_struct, other.source_ptr->clone(), other.dict_lifetime, other.require_nonempty}
{
    trie = btrie_create();
}

TrieDictionary::~TrieDictionary()
{
    btrie_destroy(trie);
}

#define DECLARE(TYPE)\
void TrieDictionary::get##TYPE(\
    const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types,\
    PaddedPODArray<TYPE> & out) const\
{\
    validateKeyTypes(key_types);\
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

void TrieDictionary::getString(
    const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types,
    ColumnString * out) const
{
    validateKeyTypes(key_types);

    const auto & attribute = getAttribute(attribute_name);
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::String))
        throw Exception{
            name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
            ErrorCodes::TYPE_MISMATCH};

    const auto & null_value = StringRef{std::get<String>(attribute.null_values)};

    getItemsImpl<StringRef, StringRef>(attribute, key_columns,
        [&] (const std::size_t row, const StringRef value) { out->insertData(value.data, value.size); },
        [&] (const std::size_t) { return null_value; });
}

#define DECLARE(TYPE)\
void TrieDictionary::get##TYPE(\
    const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types,\
    const PaddedPODArray<TYPE> & def, PaddedPODArray<TYPE> & out) const\
{\
    validateKeyTypes(key_types);\
    \
    const auto & attribute = getAttribute(attribute_name);\
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::TYPE))\
        throw Exception{\
            name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
            ErrorCodes::TYPE_MISMATCH};\
    \
    getItemsNumber<TYPE>(attribute, key_columns,\
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

void TrieDictionary::getString(
    const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types,
    const ColumnString * const def, ColumnString * const out) const
{
    validateKeyTypes(key_types);

    const auto & attribute = getAttribute(attribute_name);
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::String))
        throw Exception{
            name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
            ErrorCodes::TYPE_MISMATCH};

    getItemsImpl<StringRef, StringRef>(attribute, key_columns,
        [&] (const std::size_t row, const StringRef value) { out->insertData(value.data, value.size); },
        [&] (const std::size_t row) { return def->getDataAt(row); });
}

#define DECLARE(TYPE)\
void TrieDictionary::get##TYPE(\
    const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types,\
    const TYPE def, PaddedPODArray<TYPE> & out) const\
{\
    validateKeyTypes(key_types);\
    \
    const auto & attribute = getAttribute(attribute_name);\
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::TYPE))\
        throw Exception{\
            name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),\
            ErrorCodes::TYPE_MISMATCH};\
    \
    getItemsNumber<TYPE>(attribute, key_columns,\
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

void TrieDictionary::getString(
    const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types,
    const String & def, ColumnString * const out) const
{
    validateKeyTypes(key_types);

    const auto & attribute = getAttribute(attribute_name);
    if (!isAttributeTypeConvertibleTo(attribute.type, AttributeUnderlyingType::String))
        throw Exception{
            name + ": type mismatch: attribute " + attribute_name + " has type " + toString(attribute.type),
            ErrorCodes::TYPE_MISMATCH};

    getItemsImpl<StringRef, StringRef>(attribute, key_columns,
        [&] (const std::size_t row, const StringRef value) { out->insertData(value.data, value.size); },
        [&] (const std::size_t) { return StringRef{def}; });
}

void TrieDictionary::has(const Columns & key_columns, const DataTypes & key_types, PaddedPODArray<UInt8> & out) const
{
    validateKeyTypes(key_types);

    const auto & attribute = attributes.front();

    switch (attribute.type)
    {
        case AttributeUnderlyingType::UInt8: has<UInt8>(attribute, key_columns, out); break;
        case AttributeUnderlyingType::UInt16: has<UInt16>(attribute, key_columns, out); break;
        case AttributeUnderlyingType::UInt32: has<UInt32>(attribute, key_columns, out); break;
        case AttributeUnderlyingType::UInt64: has<UInt64>(attribute, key_columns, out); break;
        case AttributeUnderlyingType::Int8: has<Int8>(attribute, key_columns, out); break;
        case AttributeUnderlyingType::Int16: has<Int16>(attribute, key_columns, out); break;
        case AttributeUnderlyingType::Int32: has<Int32>(attribute, key_columns, out); break;
        case AttributeUnderlyingType::Int64: has<Int64>(attribute, key_columns, out); break;
        case AttributeUnderlyingType::Float32: has<Float32>(attribute, key_columns, out); break;
        case AttributeUnderlyingType::Float64: has<Float64>(attribute, key_columns, out); break;
        case AttributeUnderlyingType::String: has<StringRef>(attribute, key_columns, out); break;
    }
}

void TrieDictionary::createAttributes()
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

void TrieDictionary::loadData()
{
    auto stream = source_ptr->loadAll();
    stream->readPrefix();

    /// created upfront to avoid excess allocations
    const auto keys_size = dict_struct.key.value().size();
    StringRefs keys(keys_size);

    const auto attributes_size = attributes.size();

    while (const auto block = stream->read())
    {
        const auto rows = block.rows();
        element_count += rows;

        const auto key_column_ptrs = ext::map<Columns>(ext::range(0, keys_size),
            [&] (const std::size_t attribute_idx)
            {
                return block.safeGetByPosition(attribute_idx).column;
            });

        const auto attribute_column_ptrs = ext::map<Columns>(ext::range(0, attributes_size),
            [&] (const std::size_t attribute_idx)
            {
                return block.safeGetByPosition(keys_size + attribute_idx).column;
            });

        for (const auto row_idx : ext::range(0, rows))
        {
            /// calculate key once per row
            const auto key_column = key_column_ptrs.front();

            for (const auto attribute_idx : ext::range(0, attributes_size))
            {
                const auto & attribute_column = *attribute_column_ptrs[attribute_idx];
                auto & attribute = attributes[attribute_idx];
                setAttributeValue(attribute, key_column->getDataAt(row_idx), attribute_column[row_idx]);
            }
        }

    }

    stream->readSuffix();

    if (require_nonempty && 0 == element_count)
        throw Exception{
            name + ": dictionary source is empty and 'require_nonempty' property is set.",
            ErrorCodes::DICTIONARY_IS_EMPTY};
}

template <typename T>
void TrieDictionary::addAttributeSize(const Attribute & attribute)
{
    const auto & vec = *std::get<ContainerPtrType<T>>(attribute.maps);
    bytes_allocated += sizeof(ContainerType<T>) + (vec.capacity() * sizeof(T));
    bucket_count = vec.size();
}

void TrieDictionary::calculateBytesAllocated()
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

    bytes_allocated += btrie_allocated(trie);
}

void TrieDictionary::validateKeyTypes(const DataTypes & key_types) const
{
    if (key_types.size() != 1)
        throw Exception{
            "Expected a single IP address",
            ErrorCodes::TYPE_MISMATCH};

    const auto & actual_type = key_types[0]->getName();

    if (actual_type != "UInt32" && actual_type != "FixedString(16)")
        throw Exception{
            "Key does not match, expected either UInt32 or FixedString(16)",
            ErrorCodes::TYPE_MISMATCH};
}


template <typename T>
void TrieDictionary::createAttributeImpl(Attribute & attribute, const Field & null_value)
{
    std::get<T>(attribute.null_values) = null_value.get<typename NearestFieldType<T>::Type>();
    std::get<ContainerPtrType<T>>(attribute.maps) = std::make_unique<ContainerType<T>>();
}

TrieDictionary::Attribute TrieDictionary::createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value)
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
void TrieDictionary::getItemsNumber(
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
void TrieDictionary::getItemsImpl(
    const Attribute & attribute,
    const Columns & key_columns,
    ValueSetter && set_value,
    DefaultGetter && get_default) const
{
    auto & vec = *std::get<ContainerPtrType<AttributeType>>(attribute.maps);

    const auto first_column = key_columns.front();
    const auto rows = first_column->size();
    if (first_column->isNumeric())
    {
        for (const auto i : ext::range(0, rows))
        {
            auto addr = Int32(first_column->get64(i));
            uintptr_t slot = btrie_find(trie, addr);
            set_value(i, slot != BTRIE_NULL ? vec[slot] : get_default(i));
        }
    }
    else
    {
        for (const auto i : ext::range(0, rows))
        {
            auto addr = first_column->getDataAt(i);
            if (addr.size != 16)
                throw Exception("Expected key to be FixedString(16)", ErrorCodes::LOGICAL_ERROR);

            uintptr_t slot = btrie_find_a6(trie, reinterpret_cast<const UInt8*>(addr.data));
            set_value(i, slot != BTRIE_NULL ? vec[slot] : get_default(i));
        }
    }

    query_count.fetch_add(rows, std::memory_order_relaxed);
}


template <typename T>
bool TrieDictionary::setAttributeValueImpl(Attribute & attribute, const StringRef key, const T value)
{
    // Insert value into appropriate vector type
    auto & vec = *std::get<ContainerPtrType<T>>(attribute.maps);
    size_t row = vec.size();
    vec.push_back(value);

    // Parse IP address and subnet length from string (e.g. 2a02:6b8::3/64)
    Poco::Net::IPAddress addr, mask;
    std::string addr_str(key.toString());
    size_t pos = addr_str.find('/');
    if (pos != std::string::npos)
    {

        addr = Poco::Net::IPAddress(addr_str.substr(0, pos));
        mask = Poco::Net::IPAddress(std::stoi(addr_str.substr(pos + 1), nullptr, 10), addr.family());
    }
    else
    {
        addr = Poco::Net::IPAddress(addr_str);
        mask = Poco::Net::IPAddress(addr.length() * 8, addr.family());
    }

    /*
     * Here we might overwrite the same key with the same slot as each key can map to multiple attributes.
     * However, all columns have equal number of rows so it is okay to store only row number for each key
     * instead of building a trie for each column. This comes at the cost of additional lookup in attribute
     * vector on lookup time to return cell from row + column. The reason for this is to save space,
     * and build only single trie instead of trie for each column.
     */
    if (addr.family() == Poco::Net::IPAddress::IPv4)
    {
        UInt32 addr_v4 = Poco::ByteOrder::toNetwork(*reinterpret_cast<const UInt32*>(addr.addr()));
        UInt32 mask_v4 = Poco::ByteOrder::toNetwork(*reinterpret_cast<const UInt32*>(mask.addr()));
        return btrie_insert(trie, addr_v4, mask_v4, row) == 0;
    }

    const uint8_t* addr_v6 = reinterpret_cast<const uint8_t*>(addr.addr());
    const uint8_t* mask_v6 = reinterpret_cast<const uint8_t*>(mask.addr());
    return btrie_insert_a6(trie, addr_v6, mask_v6, row) == 0;
}

bool TrieDictionary::setAttributeValue(Attribute & attribute, const StringRef key, const Field & value)
{
    switch (attribute.type)
    {
        case AttributeUnderlyingType::UInt8: return setAttributeValueImpl<UInt8>(attribute, key, value.get<UInt64>());
        case AttributeUnderlyingType::UInt16: return setAttributeValueImpl<UInt16>(attribute, key, value.get<UInt64>());
        case AttributeUnderlyingType::UInt32: return setAttributeValueImpl<UInt32>(attribute, key, value.get<UInt64>());
        case AttributeUnderlyingType::UInt64: return setAttributeValueImpl<UInt64>(attribute, key, value.get<UInt64>());
        case AttributeUnderlyingType::Int8: return setAttributeValueImpl<Int8>(attribute, key, value.get<Int64>());
        case AttributeUnderlyingType::Int16: return setAttributeValueImpl<Int16>(attribute, key, value.get<Int64>());
        case AttributeUnderlyingType::Int32: return setAttributeValueImpl<Int32>(attribute, key, value.get<Int64>());
        case AttributeUnderlyingType::Int64: return setAttributeValueImpl<Int64>(attribute, key, value.get<Int64>());
        case AttributeUnderlyingType::Float32: return setAttributeValueImpl<Float32>(attribute, key, value.get<Float64>());
        case AttributeUnderlyingType::Float64: return setAttributeValueImpl<Float64>(attribute, key, value.get<Float64>());
        case AttributeUnderlyingType::String:
        {
            const auto & string = value.get<String>();
            const auto string_in_arena = attribute.string_arena->insert(string.data(), string.size());
            setAttributeValueImpl<StringRef>(attribute, key, StringRef{string_in_arena, string.size()});
            return true;
        }
    }

    return {};
}

const TrieDictionary::Attribute & TrieDictionary::getAttribute(const std::string & attribute_name) const
{
    const auto it = attribute_index_by_name.find(attribute_name);
    if (it == std::end(attribute_index_by_name))
        throw Exception{
            name + ": no such attribute '" + attribute_name + "'",
            ErrorCodes::BAD_ARGUMENTS};

    return attributes[it->second];
}

template <typename T>
void TrieDictionary::has(const Attribute & attribute, const Columns & key_columns, PaddedPODArray<UInt8> & out) const
{
    const auto first_column = key_columns.front();
    const auto rows = first_column->size();
    if (first_column->isNumeric())
    {
        for (const auto i : ext::range(0, rows))
        {
            auto addr = Int32(first_column->get64(i));
            uintptr_t slot = btrie_find(trie, addr);
            out[i] = (slot != BTRIE_NULL);
        }
    }
    else
    {
        for (const auto i : ext::range(0, rows))
        {
            auto addr = first_column->getDataAt(i);
            if (unlikely(addr.size != 16))
                throw Exception("Expected key to be FixedString(16)", ErrorCodes::LOGICAL_ERROR);

            uintptr_t slot = btrie_find_a6(trie, reinterpret_cast<const UInt8*>(addr.data));
            out[i] = (slot != BTRIE_NULL);
        }
    }

    query_count.fetch_add(rows, std::memory_order_relaxed);
}

template <typename Getter, typename KeyType>
void TrieDictionary::trieTraverse(const btrie_t * tree, Getter && getter) const
{
    KeyType key = 0;
    const KeyType high_bit = ~((~key) >> 1);

    btrie_node_t * node;
    node = tree->root;

    std::stack<btrie_node_t *> stack;
    while (node)
    {
        stack.push(node);
        node = node->left;
    }

    auto getBit = [&high_bit](size_t size) { return size ? (high_bit >> (size - 1)) : 0; };

    while (!stack.empty())
    {
        node = stack.top();
        stack.pop();

        if (node && node->value != BTRIE_NULL)
            getter(key, stack.size());

        if (node && node->right)
        {
            stack.push(NULL);
            key |= getBit(stack.size());
            stack.push(node->right);
            while (stack.top()->left)
                stack.push(stack.top()->left);
        }
        else
            key &= ~getBit(stack.size());
    }
}

Columns TrieDictionary::getKeyColumns() const
{
    auto ip_column = std::make_shared<ColumnFixedString>(IPV6_BINARY_LENGTH);
    auto mask_column = std::make_shared<ColumnVector<UInt8>>();

    auto getter = [& ip_column, & mask_column](__uint128_t ip, size_t mask) {
        UInt64 * ip_array = reinterpret_cast<UInt64 *>(&ip);
        ip_array[0] = Poco::ByteOrder::fromNetwork(ip_array[0]);
        ip_array[1] = Poco::ByteOrder::fromNetwork(ip_array[1]);
        std::swap(ip_array[0], ip_array[1]);
        ip_column->insertData(reinterpret_cast<const char *>(ip_array), IPV6_BINARY_LENGTH);
        mask_column->insert(static_cast<UInt8>(mask));
    };

    trieTraverse<decltype(getter), __uint128_t>(trie, std::move(getter));
    return {ip_column, mask_column};
}

BlockInputStreamPtr TrieDictionary::getBlockInputStream(const Names & column_names, size_t max_block_size) const
{
    using BlockInputStreamType = DictionaryBlockInputStream<TrieDictionary, UInt64>;

    auto getKeys = [](const Columns& columns, const std::vector<DictionaryAttribute>& attributes)
    {
        const auto & attr = attributes.front();
        return ColumnsWithTypeAndName({ColumnWithTypeAndName(columns.front(),
            std::make_shared<DataTypeFixedString>(IPV6_BINARY_LENGTH), attr.name)});
    };
    auto getView = [](const Columns& columns, const std::vector<DictionaryAttribute>& attributes)
    {
        auto column = std::make_shared<ColumnString>();
        auto ip_column = std::static_pointer_cast<ColumnFixedString>(columns.front());
        auto mask_column = std::static_pointer_cast<ColumnVector<UInt8>>(columns.back());
        char buffer[48];
        for (size_t row : ext::range(0, ip_column->size()))
        {
            UInt8 mask = mask_column->getElement(row);
            char * ptr = buffer;
            formatIPv6(reinterpret_cast<const unsigned char *>(ip_column->getDataAt(row).data), ptr);
            *(ptr - 1) = '/';
            auto size = detail::writeUIntText(mask, ptr);
            column->insertData(buffer, size + (ptr - buffer));
        }
        return ColumnsWithTypeAndName{ColumnWithTypeAndName(column, std::make_shared<DataTypeString>(), attributes.front().name)};
    };
    return std::make_shared<BlockInputStreamType>(shared_from_this(), max_block_size, getKeyColumns(), column_names,
        std::move(getKeys), std::move(getView));
}

}
