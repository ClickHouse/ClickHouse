#include "TrieDictionary.h"
#include <stack>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <IO/WriteIntText.h>
#include <Poco/ByteOrder.h>
#include <Poco/Net/IPAddress.h>
#include <Common/formatIPv6.h>
#include <common/itoa.h>
#include <ext/map.h>
#include <ext/range.h>
#include "DictionaryBlockInputStream.h"
#include "DictionaryFactory.h"

#ifdef __clang__
    #pragma clang diagnostic ignored "-Wold-style-cast"
    #pragma clang diagnostic ignored "-Wnewline-eof"
#endif

#include <btrie.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TYPE_MISMATCH;
    extern const int BAD_ARGUMENTS;
    extern const int DICTIONARY_IS_EMPTY;
    extern const int NOT_IMPLEMENTED;
}

static void validateKeyTypes(const DataTypes & key_types)
{
    if (key_types.size() != 1)
        throw Exception{"Expected a single IP address", ErrorCodes::TYPE_MISMATCH};

    const auto & actual_type = key_types[0]->getName();

    if (actual_type != "UInt32" && actual_type != "FixedString(16)")
        throw Exception{"Key does not match, expected either UInt32 or FixedString(16)", ErrorCodes::TYPE_MISMATCH};
}


TrieDictionary::TrieDictionary(
    const std::string & database_,
    const std::string & name_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    const DictionaryLifetime dict_lifetime_,
    bool require_nonempty_)
    : database(database_)
    , name(name_)
    , full_name{database_.empty() ? name_ : (database_ + "." + name_)}
    , dict_struct(dict_struct_)
    , source_ptr{std::move(source_ptr_)}
    , dict_lifetime(dict_lifetime_)
    , require_nonempty(require_nonempty_)
    , logger(&Poco::Logger::get("TrieDictionary"))
{
    createAttributes();
    trie = btrie_create();
    loadData();
    calculateBytesAllocated();
}

TrieDictionary::~TrieDictionary()
{
    btrie_destroy(trie);
}

#define DECLARE(TYPE) \
    void TrieDictionary::get##TYPE( \
        const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types, ResultArrayType<TYPE> & out) const \
    { \
        validateKeyTypes(key_types); \
\
        const auto & attribute = getAttribute(attribute_name); \
        checkAttributeType(full_name, attribute_name, attribute.type, AttributeUnderlyingType::ut##TYPE); \
\
        const auto null_value = std::get<TYPE>(attribute.null_values); \
\
        getItemsImpl<TYPE, TYPE>( \
            attribute, \
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

void TrieDictionary::getString(
    const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types, ColumnString * out) const
{
    validateKeyTypes(key_types);

    const auto & attribute = getAttribute(attribute_name);
    checkAttributeType(full_name, attribute_name, attribute.type, AttributeUnderlyingType::utString);

    const auto & null_value = StringRef{std::get<String>(attribute.null_values)};

    getItemsImpl<StringRef, StringRef>(
        attribute,
        key_columns,
        [&](const size_t, const StringRef value) { out->insertData(value.data, value.size); },
        [&](const size_t) { return null_value; });
}

#define DECLARE(TYPE) \
    void TrieDictionary::get##TYPE( \
        const std::string & attribute_name, \
        const Columns & key_columns, \
        const DataTypes & key_types, \
        const PaddedPODArray<TYPE> & def, \
        ResultArrayType<TYPE> & out) const \
    { \
        validateKeyTypes(key_types); \
\
        const auto & attribute = getAttribute(attribute_name); \
        checkAttributeType(full_name, attribute_name, attribute.type, AttributeUnderlyingType::ut##TYPE); \
\
        getItemsImpl<TYPE, TYPE>( \
            attribute, \
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

void TrieDictionary::getString(
    const std::string & attribute_name,
    const Columns & key_columns,
    const DataTypes & key_types,
    const ColumnString * const def,
    ColumnString * const out) const
{
    validateKeyTypes(key_types);

    const auto & attribute = getAttribute(attribute_name);
    checkAttributeType(full_name, attribute_name, attribute.type, AttributeUnderlyingType::utString);

    getItemsImpl<StringRef, StringRef>(
        attribute,
        key_columns,
        [&](const size_t, const StringRef value) { out->insertData(value.data, value.size); },
        [&](const size_t row) { return def->getDataAt(row); });
}

#define DECLARE(TYPE) \
    void TrieDictionary::get##TYPE( \
        const std::string & attribute_name, \
        const Columns & key_columns, \
        const DataTypes & key_types, \
        const TYPE def, \
        ResultArrayType<TYPE> & out) const \
    { \
        validateKeyTypes(key_types); \
\
        const auto & attribute = getAttribute(attribute_name); \
        checkAttributeType(full_name, attribute_name, attribute.type, AttributeUnderlyingType::ut##TYPE); \
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

void TrieDictionary::getString(
    const std::string & attribute_name,
    const Columns & key_columns,
    const DataTypes & key_types,
    const String & def,
    ColumnString * const out) const
{
    validateKeyTypes(key_types);

    const auto & attribute = getAttribute(attribute_name);
    checkAttributeType(full_name, attribute_name, attribute.type, AttributeUnderlyingType::utString);

    getItemsImpl<StringRef, StringRef>(
        attribute,
        key_columns,
        [&](const size_t, const StringRef value) { out->insertData(value.data, value.size); },
        [&](const size_t) { return StringRef{def}; });
}

void TrieDictionary::has(const Columns & key_columns, const DataTypes & key_types, PaddedPODArray<UInt8> & out) const
{
    validateKeyTypes(key_types);

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
            has<StringRef>(attribute, key_columns, out);
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

void TrieDictionary::createAttributes()
{
    const auto size = dict_struct.attributes.size();
    attributes.reserve(size);

    for (const auto & attribute : dict_struct.attributes)
    {
        attribute_index_by_name.emplace(attribute.name, attributes.size());
        attributes.push_back(createAttributeWithType(attribute.underlying_type, attribute.null_value));

        if (attribute.hierarchical)
            throw Exception{full_name + ": hierarchical attributes not supported for dictionary of type " + getTypeName(),
                            ErrorCodes::TYPE_MISMATCH};
    }
}

void TrieDictionary::loadData()
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

        const auto key_column_ptrs = ext::map<Columns>(
            ext::range(0, keys_size), [&](const size_t attribute_idx) { return block.safeGetByPosition(attribute_idx).column; });

        const auto attribute_column_ptrs = ext::map<Columns>(ext::range(0, attributes_size), [&](const size_t attribute_idx)
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
        throw Exception{full_name + ": dictionary source is empty and 'require_nonempty' property is set.", ErrorCodes::DICTIONARY_IS_EMPTY};
}

template <typename T>
void TrieDictionary::addAttributeSize(const Attribute & attribute)
{
    const auto & vec = std::get<ContainerType<T>>(attribute.maps);
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
            case AttributeUnderlyingType::utUInt8:
                addAttributeSize<UInt8>(attribute);
                break;
            case AttributeUnderlyingType::utUInt16:
                addAttributeSize<UInt16>(attribute);
                break;
            case AttributeUnderlyingType::utUInt32:
                addAttributeSize<UInt32>(attribute);
                break;
            case AttributeUnderlyingType::utUInt64:
                addAttributeSize<UInt64>(attribute);
                break;
            case AttributeUnderlyingType::utUInt128:
                addAttributeSize<UInt128>(attribute);
                break;
            case AttributeUnderlyingType::utInt8:
                addAttributeSize<Int8>(attribute);
                break;
            case AttributeUnderlyingType::utInt16:
                addAttributeSize<Int16>(attribute);
                break;
            case AttributeUnderlyingType::utInt32:
                addAttributeSize<Int32>(attribute);
                break;
            case AttributeUnderlyingType::utInt64:
                addAttributeSize<Int64>(attribute);
                break;
            case AttributeUnderlyingType::utFloat32:
                addAttributeSize<Float32>(attribute);
                break;
            case AttributeUnderlyingType::utFloat64:
                addAttributeSize<Float64>(attribute);
                break;

            case AttributeUnderlyingType::utDecimal32:
                addAttributeSize<Decimal32>(attribute);
                break;
            case AttributeUnderlyingType::utDecimal64:
                addAttributeSize<Decimal64>(attribute);
                break;
            case AttributeUnderlyingType::utDecimal128:
                addAttributeSize<Decimal128>(attribute);
                break;

            case AttributeUnderlyingType::utString:
            {
                addAttributeSize<StringRef>(attribute);
                bytes_allocated += sizeof(Arena) + attribute.string_arena->size();

                break;
            }
        }
    }

    bytes_allocated += btrie_allocated(trie);
}


template <typename T>
void TrieDictionary::createAttributeImpl(Attribute & attribute, const Field & null_value)
{
    attribute.null_values = T(null_value.get<NearestFieldType<T>>());
    attribute.maps.emplace<ContainerType<T>>();
}

TrieDictionary::Attribute TrieDictionary::createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value)
{
    Attribute attr{type, {}, {}, {}};

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

        case AttributeUnderlyingType::utDecimal32:
            createAttributeImpl<Decimal32>(attr, null_value);
            break;
        case AttributeUnderlyingType::utDecimal64:
            createAttributeImpl<Decimal64>(attr, null_value);
            break;
        case AttributeUnderlyingType::utDecimal128:
            createAttributeImpl<Decimal128>(attr, null_value);
            break;

        case AttributeUnderlyingType::utString:
        {
            attr.null_values = null_value.get<String>();
            attr.maps.emplace<ContainerType<StringRef>>();
            attr.string_arena = std::make_unique<Arena>();
            break;
        }
    }

    return attr;
}


template <typename AttributeType, typename OutputType, typename ValueSetter, typename DefaultGetter>
void TrieDictionary::getItemsImpl(
    const Attribute & attribute, const Columns & key_columns, ValueSetter && set_value, DefaultGetter && get_default) const
{
    auto & vec = std::get<ContainerType<AttributeType>>(attribute.maps);

    const auto first_column = key_columns.front();
    const auto rows = first_column->size();
    if (first_column->isNumeric())
    {
        for (const auto i : ext::range(0, rows))
        {
            auto addr = Int32(first_column->get64(i));
            uintptr_t slot = btrie_find(trie, addr);
#pragma GCC diagnostic push
#pragma GCC diagnostic warning "-Wold-style-cast"
            set_value(i, slot != BTRIE_NULL ? static_cast<OutputType>(vec[slot]) : get_default(i));
#pragma GCC diagnostic pop
        }
    }
    else
    {
        for (const auto i : ext::range(0, rows))
        {
            auto addr = first_column->getDataAt(i);
            if (addr.size != 16)
                throw Exception("Expected key to be FixedString(16)", ErrorCodes::LOGICAL_ERROR);

            uintptr_t slot = btrie_find_a6(trie, reinterpret_cast<const uint8_t *>(addr.data));
#pragma GCC diagnostic push
#pragma GCC diagnostic warning "-Wold-style-cast"
            set_value(i, slot != BTRIE_NULL ? static_cast<OutputType>(vec[slot]) : get_default(i));
#pragma GCC diagnostic pop
        }
    }

    query_count.fetch_add(rows, std::memory_order_relaxed);
}


template <typename T>
bool TrieDictionary::setAttributeValueImpl(Attribute & attribute, const StringRef key, const T value)
{
    // Insert value into appropriate vector type
    auto & vec = std::get<ContainerType<T>>(attribute.maps);
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
        UInt32 addr_v4 = Poco::ByteOrder::toNetwork(*reinterpret_cast<const UInt32 *>(addr.addr()));
        UInt32 mask_v4 = Poco::ByteOrder::toNetwork(*reinterpret_cast<const UInt32 *>(mask.addr()));
        return btrie_insert(trie, addr_v4, mask_v4, row) == 0;
    }

    const uint8_t * addr_v6 = reinterpret_cast<const uint8_t *>(addr.addr());
    const uint8_t * mask_v6 = reinterpret_cast<const uint8_t *>(mask.addr());
    return btrie_insert_a6(trie, addr_v6, mask_v6, row) == 0;
}

bool TrieDictionary::setAttributeValue(Attribute & attribute, const StringRef key, const Field & value)
{
    switch (attribute.type)
    {
        case AttributeUnderlyingType::utUInt8:
            return setAttributeValueImpl<UInt8>(attribute, key, value.get<UInt64>());
        case AttributeUnderlyingType::utUInt16:
            return setAttributeValueImpl<UInt16>(attribute, key, value.get<UInt64>());
        case AttributeUnderlyingType::utUInt32:
            return setAttributeValueImpl<UInt32>(attribute, key, value.get<UInt64>());
        case AttributeUnderlyingType::utUInt64:
            return setAttributeValueImpl<UInt64>(attribute, key, value.get<UInt64>());
        case AttributeUnderlyingType::utUInt128:
            return setAttributeValueImpl<UInt128>(attribute, key, value.get<UInt128>());
        case AttributeUnderlyingType::utInt8:
            return setAttributeValueImpl<Int8>(attribute, key, value.get<Int64>());
        case AttributeUnderlyingType::utInt16:
            return setAttributeValueImpl<Int16>(attribute, key, value.get<Int64>());
        case AttributeUnderlyingType::utInt32:
            return setAttributeValueImpl<Int32>(attribute, key, value.get<Int64>());
        case AttributeUnderlyingType::utInt64:
            return setAttributeValueImpl<Int64>(attribute, key, value.get<Int64>());
        case AttributeUnderlyingType::utFloat32:
            return setAttributeValueImpl<Float32>(attribute, key, value.get<Float64>());
        case AttributeUnderlyingType::utFloat64:
            return setAttributeValueImpl<Float64>(attribute, key, value.get<Float64>());

        case AttributeUnderlyingType::utDecimal32:
            return setAttributeValueImpl<Decimal32>(attribute, key, value.get<Decimal32>());
        case AttributeUnderlyingType::utDecimal64:
            return setAttributeValueImpl<Decimal64>(attribute, key, value.get<Decimal64>());
        case AttributeUnderlyingType::utDecimal128:
            return setAttributeValueImpl<Decimal128>(attribute, key, value.get<Decimal128>());

        case AttributeUnderlyingType::utString:
        {
            const auto & string = value.get<String>();
            const auto * string_in_arena = attribute.string_arena->insert(string.data(), string.size());
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
        throw Exception{full_name + ": no such attribute '" + attribute_name + "'", ErrorCodes::BAD_ARGUMENTS};

    return attributes[it->second];
}

template <typename T>
void TrieDictionary::has(const Attribute &, const Columns & key_columns, PaddedPODArray<UInt8> & out) const
{
    const auto first_column = key_columns.front();
    const auto rows = first_column->size();
    if (first_column->isNumeric())
    {
        for (const auto i : ext::range(0, rows))
        {
            auto addr = Int32(first_column->get64(i));
            uintptr_t slot = btrie_find(trie, addr);
#pragma GCC diagnostic push
#pragma GCC diagnostic warning "-Wold-style-cast"
            out[i] = (slot != BTRIE_NULL);
#pragma GCC diagnostic pop
        }
    }
    else
    {
        for (const auto i : ext::range(0, rows))
        {
            auto addr = first_column->getDataAt(i);
            if (unlikely(addr.size != 16))
                throw Exception("Expected key to be FixedString(16)", ErrorCodes::LOGICAL_ERROR);

            uintptr_t slot = btrie_find_a6(trie, reinterpret_cast<const uint8_t *>(addr.data));
#pragma GCC diagnostic push
#pragma GCC diagnostic warning "-Wold-style-cast"
            out[i] = (slot != BTRIE_NULL);
#pragma GCC diagnostic pop
        }
    }

    query_count.fetch_add(rows, std::memory_order_relaxed);
}

template <typename Getter, typename KeyType>
static void trieTraverse(const btrie_t * trie, Getter && getter)
{
    KeyType key = 0;
    const KeyType high_bit = ~((~key) >> 1);

    btrie_node_t * node;
    node = trie->root;

    std::stack<btrie_node_t *> stack;
    while (node)
    {
        stack.push(node);
        node = node->left;
    }

    auto get_bit = [&high_bit](size_t size) { return size ? (high_bit >> (size - 1)) : 0; };

    while (!stack.empty())
    {
        node = stack.top();
        stack.pop();
#pragma GCC diagnostic push
#pragma GCC diagnostic warning "-Wold-style-cast"
        if (node && node->value != BTRIE_NULL)
#pragma GCC diagnostic pop
            getter(key, stack.size());

        if (node && node->right)
        {
            stack.push(nullptr);
            key |= get_bit(stack.size());
            stack.push(node->right);
            while (stack.top()->left)
                stack.push(stack.top()->left);
        }
        else
            key &= ~get_bit(stack.size());
    }
}

Columns TrieDictionary::getKeyColumns() const
{
    auto ip_column = ColumnFixedString::create(IPV6_BINARY_LENGTH);
    auto mask_column = ColumnVector<UInt8>::create();

#if defined(__SIZEOF_INT128__)
    auto getter = [&ip_column, &mask_column](__uint128_t ip, size_t mask)
    {
        Poco::UInt64 * ip_array = reinterpret_cast<Poco::UInt64 *>(&ip); // Poco:: for old poco + macos
        ip_array[0] = Poco::ByteOrder::fromNetwork(ip_array[0]);
        ip_array[1] = Poco::ByteOrder::fromNetwork(ip_array[1]);
        std::swap(ip_array[0], ip_array[1]);
        ip_column->insertData(reinterpret_cast<const char *>(ip_array), IPV6_BINARY_LENGTH);
        mask_column->insertValue(static_cast<UInt8>(mask));
    };

    trieTraverse<decltype(getter), __uint128_t>(trie, std::move(getter));
#else
    throw Exception("TrieDictionary::getKeyColumns is not implemented for 32bit arch", ErrorCodes::NOT_IMPLEMENTED);
#endif
    return {std::move(ip_column), std::move(mask_column)};
}

BlockInputStreamPtr TrieDictionary::getBlockInputStream(const Names & column_names, size_t max_block_size) const
{
    using BlockInputStreamType = DictionaryBlockInputStream<TrieDictionary, UInt64>;

    auto get_keys = [](const Columns & columns, const std::vector<DictionaryAttribute> & dict_attributes)
    {
        const auto & attr = dict_attributes.front();
        return ColumnsWithTypeAndName(
            {ColumnWithTypeAndName(columns.front(), std::make_shared<DataTypeFixedString>(IPV6_BINARY_LENGTH), attr.name)});
    };
    auto get_view = [](const Columns & columns, const std::vector<DictionaryAttribute> & dict_attributes)
    {
        auto column = ColumnString::create();
        const auto & ip_column = assert_cast<const ColumnFixedString &>(*columns.front());
        const auto & mask_column = assert_cast<const ColumnVector<UInt8> &>(*columns.back());
        char buffer[48];
        for (size_t row : ext::range(0, ip_column.size()))
        {
            UInt8 mask = mask_column.getElement(row);
            char * ptr = buffer;
            formatIPv6(reinterpret_cast<const unsigned char *>(ip_column.getDataAt(row).data), ptr);
            *(ptr - 1) = '/';
            ptr = itoa(mask, ptr);
            column->insertData(buffer, ptr - buffer);
        }
        return ColumnsWithTypeAndName{
            ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), dict_attributes.front().name)};
    };
    return std::make_shared<BlockInputStreamType>(
        shared_from_this(), max_block_size, getKeyColumns(), column_names, std::move(get_keys), std::move(get_view));
}


void registerDictionaryTrie(DictionaryFactory & factory)
{
    auto create_layout = [=](const std::string &,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr) -> DictionaryPtr
    {
        if (!dict_struct.key)
            throw Exception{"'key' is required for dictionary of layout 'ip_trie'", ErrorCodes::BAD_ARGUMENTS};

        const String database = config.getString(config_prefix + ".database", "");
        const String name = config.getString(config_prefix + ".name");
        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
        const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);
        // This is specialised trie for storing IPv4 and IPv6 prefixes.
        return std::make_unique<TrieDictionary>(database, name, dict_struct, std::move(source_ptr), dict_lifetime, require_nonempty);
    };
    factory.registerLayout("ip_trie", create_layout, true);
}

}
