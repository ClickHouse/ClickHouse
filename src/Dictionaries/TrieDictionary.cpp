#include "TrieDictionary.h"
#include <stack>
#include <Common/assert_cast.h>
#include <Common/IPv6ToBinary.h>
#include <Common/memcmpSmall.h>
#include <Common/memcpySmall.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <IO/WriteIntText.h>
#include <Poco/ByteOrder.h>
#include <Common/formatIPv6.h>
#include <common/itoa.h>
#include <ext/map.h>
#include <ext/range.h>
#include "DictionaryBlockInputStream.h"
#include "DictionaryFactory.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TYPE_MISMATCH;
    extern const int BAD_ARGUMENTS;
    extern const int DICTIONARY_IS_EMPTY;
}

namespace
{
    struct IPRecord
    {
        Poco::Net::IPAddress addr;
        UInt8 prefix;
        size_t row;
    };

    struct IPv4Subnet
    {
        UInt32 addr;
        UInt8 prefix;
    };

    struct IPv6Subnet
    {
        const uint8_t * addr;
        UInt8 prefix;
    };
}

static void validateKeyTypes(const DataTypes & key_types)
{
    if (key_types.size() < 1 && 2 < key_types.size())
        throw Exception{"Expected a single IP address or IP with mask", ErrorCodes::TYPE_MISMATCH};

    const auto & actual_type = key_types[0]->getName();
    if (actual_type != "UInt32" && actual_type != "FixedString(16)")
        throw Exception{"Key does not match, expected either UInt32 or FixedString(16)", ErrorCodes::TYPE_MISMATCH};

    if (key_types.size() > 1)
    {
        const auto * mask_col_type = typeid_cast<const DataTypeUInt8 *>(key_types[1].get());
        if (mask_col_type == nullptr)
            throw Exception{"Mask do not match, expected UInt8", ErrorCodes::TYPE_MISMATCH};
    }
}

static inline bool compPrefixes(UInt8 a, UInt8 b)
{
    return a < b;
}

/// Convert mapped IPv6 to IPv4 if possible
inline static UInt32 mappedIPv4ToBinary(const uint8_t * addr, bool & success)
{
    const UInt16* words = reinterpret_cast<const UInt16*>(addr);
    auto has_zero_prefix = words[0] == 0 && words[1] == 0 && words[2] == 0 && words[3] == 0 && words[4] == 0;
    success = has_zero_prefix && Poco::ByteOrder::fromNetwork(words[5]) == 0xFFFF;
    if (!success)
        return 0;
    return Poco::ByteOrder::fromNetwork(*reinterpret_cast<const UInt32 *>(&addr[6]));
}

/// Convert IPv4 to IPv6-mapped and save results to buf
inline static void mapIPv4ToIPv6(UInt32 addr, uint8_t * buf)
{
    memset(buf, 0, 10);
    buf[10] = '\xFF';
    buf[11] = '\xFF';
    addr = Poco::ByteOrder::toNetwork(addr);
    memcpy(&buf[12], reinterpret_cast<const uint8_t *>(&addr), 4);
}

/*
static UInt32 applyMask32(UInt32 val, UInt8 prefix)
{
    UInt32 mask = (prefix >= 32) ? 0xffffffff : ~(0xffffffff >> prefix);
    return val & mask;
}

static void applyMask128(const uint8_t * val, uint8_t * out, UInt8 prefix)
{
    if (prefix >= 128)
        prefix = 128;

    size_t i = 0;

    for (; prefix >= 8; ++i, prefix -= 8)
        out[i] = val[i];

    if (i >= 16)
        return;

    uint8_t mask = ~(0xff >> prefix);
    out[i] = val[i] & mask;

    i++;
    memset(&out[i], 0, 16 - i);
}
*/

static bool matchIPv4Subnet(UInt32 target, UInt32 addr, UInt8 prefix)
{
    UInt32 mask = (prefix >= 32) ? 0xffffffff : ~(0xffffffff >> prefix);
    return (target & mask) == addr;
}

static bool matchIPv6Subnet(const uint8_t * target, const uint8_t * addr, UInt8 prefix)
{
    if (prefix > IPV6_BINARY_LENGTH * 8)
        prefix = IPV6_BINARY_LENGTH * 8;

    size_t i = 0;
    for (; prefix >= 8; ++i, prefix -= 8)
    {
        if (target[i] != addr[i])
            return false;
    }
    if (prefix == 0)
        return true;

    auto mask = ~(0xff >> prefix);
    return (addr[i] & mask) == target[i];
}

const uint8_t * TrieDictionary::getIPv6FromOffset(const TrieDictionary::IPv6Container & ipv6_col, size_t i)
{
    return reinterpret_cast<const uint8_t *>(&ipv6_col[i * IPV6_BINARY_LENGTH]);
}

TrieDictionary::TrieDictionary(
    const StorageID & dict_id_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    const DictionaryLifetime dict_lifetime_,
    bool require_nonempty_)
    : IDictionaryBase(dict_id_)
    , dict_struct(dict_struct_)
    , source_ptr{std::move(source_ptr_)}
    , dict_lifetime(dict_lifetime_)
    , require_nonempty(require_nonempty_)
    , logger(&Poco::Logger::get("TrieDictionary"))
{
    createAttributes();

    loadData();
    calculateBytesAllocated();
}

#define DECLARE(TYPE) \
    void TrieDictionary::get##TYPE( \
        const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types, ResultArrayType<TYPE> & out) const \
    { \
        validateKeyTypes(key_types); \
\
        const auto & attribute = getAttribute(attribute_name); \
        checkAttributeType(this, attribute_name, attribute.type, AttributeUnderlyingType::ut##TYPE); \
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
    checkAttributeType(this, attribute_name, attribute.type, AttributeUnderlyingType::utString);

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
        checkAttributeType(this, attribute_name, attribute.type, AttributeUnderlyingType::ut##TYPE); \
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
    checkAttributeType(this, attribute_name, attribute.type, AttributeUnderlyingType::utString);

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

void TrieDictionary::getString(
    const std::string & attribute_name,
    const Columns & key_columns,
    const DataTypes & key_types,
    const String & def,
    ColumnString * const out) const
{
    validateKeyTypes(key_types);

    const auto & attribute = getAttribute(attribute_name);
    checkAttributeType(this, attribute_name, attribute.type, AttributeUnderlyingType::utString);

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

    const auto attributes_size = attributes.size();

    std::vector<IPRecord> ip_records;

    row_idx.reserve(keys_size);
    mask_column.reserve(keys_size);

    bool has_ipv6 = false;

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

        for (const auto row : ext::range(0, rows))
        {
            /// calculate key once per row
            const auto key_column = key_column_ptrs.front();

            for (const auto attribute_idx : ext::range(0, attributes_size))
            {
                const auto & attribute_column = *attribute_column_ptrs[attribute_idx];
                auto & attribute = attributes[attribute_idx];

                setAttributeValue(attribute, attribute_column[row]);
            }

            size_t row_number = ip_records.size();

            std::string addr_str(key_column->getDataAt(row).toString());
            size_t pos = addr_str.find('/');
            if (pos != std::string::npos)
            {
                IPAddress addr(addr_str.substr(0, pos));
                has_ipv6 = has_ipv6 || (addr.family() == Poco::Net::IPAddress::IPv6);

                UInt8 prefix = std::stoi(addr_str.substr(pos + 1), nullptr, 10);

                addr = addr & IPAddress(prefix, addr.family());
                ip_records.emplace_back(IPRecord{addr, prefix, row_number});
            }
            else
            {
                IPAddress addr(addr_str);
                has_ipv6 = has_ipv6 || (addr.family() == Poco::Net::IPAddress::IPv6);
                UInt8 prefix = addr.length() * 8;
                ip_records.emplace_back(IPRecord{addr, prefix, row_number});
            }
        }
    }

    stream->readSuffix();

    LOG_TRACE(logger, "{} ip records are read", ip_records.size());

    if (has_ipv6)
    {
        std::sort(ip_records.begin(), ip_records.end(),
            [](const auto & record_a, const auto & record_b)
            {
                auto a = IPv6ToBinary(record_a.addr);
                auto b = IPv6ToBinary(record_b.addr);
                auto cmpres = memcmp16(reinterpret_cast<const uint8_t *>(a.data()),
                                       reinterpret_cast<const uint8_t *>(b.data()));

                if (cmpres == 0)
                    return compPrefixes(record_a.prefix, record_b.prefix);
                return cmpres < 0;
            });

        auto & ipv6_col = ip_column.emplace<IPv6Container>();
        ipv6_col.resize_fill(IPV6_BINARY_LENGTH * ip_records.size());

        for (const auto & record : ip_records)
        {
            auto ip_array = IPv6ToBinary(record.addr);

            size_t i = row_idx.size();
            memcpySmallAllowReadWriteOverflow15(&ipv6_col[i * IPV6_BINARY_LENGTH],
                                                reinterpret_cast<const uint8_t *>(ip_array.data()),
                                                IPV6_BINARY_LENGTH);
            mask_column.push_back(record.prefix);
            row_idx.push_back(record.row);
        }
    }
    else
    {
        std::sort(ip_records.begin(), ip_records.end(),
            [](const auto & record_a, const auto & record_b)
            {
                UInt32 a = *reinterpret_cast<const UInt32 *>(record_a.addr.addr());
                a = Poco::ByteOrder::fromNetwork(a);

                UInt32 b = *reinterpret_cast<const UInt32 *>(record_b.addr.addr());
                b = Poco::ByteOrder::fromNetwork(b);

                if (a == b)
                    return compPrefixes(record_a.prefix, record_b.prefix);
                return a < b;
            });

        auto & ipv4_col = ip_column.emplace<IPv4Container>();
        ipv4_col.reserve(ip_records.size());
        for (const auto & record : ip_records)
        {
            auto addr = Poco::ByteOrder::fromNetwork(*reinterpret_cast<const UInt32 *>(record.addr.addr()));
            ipv4_col.push_back(addr);
            mask_column.push_back(record.prefix);
            row_idx.push_back(record.row);
        }
    }

    parent_subnet.resize(ip_records.size());
    std::stack<size_t> subnets_stack;
    for (const auto i : ext::range(0, ip_records.size()))
    {
        parent_subnet[i] = i;

        const auto & cur_address = ip_records[i].addr;
        while (!subnets_stack.empty())
        {
            size_t subnet_idx = subnets_stack.top();
            const auto cur_subnet = ip_records[subnet_idx];
            auto cur_addr_masked = cur_address & IPAddress(cur_subnet.prefix, cur_address.family());
            if (cur_subnet.addr == cur_addr_masked)
            {
                parent_subnet[i] = subnet_idx;
                break;
            }
            subnets_stack.pop();
        }
        subnets_stack.push(i);
    }

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
    if (auto * ipv4_col = std::get_if<IPv4Container>(&ip_column))
    {
        bytes_allocated += ipv4_col->size() * sizeof((*ipv4_col)[0]);
    }
    else if (auto * ipv6_col = std::get_if<IPv6Container>(&ip_column))
    {
        bytes_allocated += ipv6_col->size() * sizeof((*ipv6_col)[0]);
    }
    bytes_allocated += mask_column.size() * sizeof(mask_column[0]);
    bytes_allocated += row_idx.size() * sizeof(row_idx[0]);
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
void TrieDictionary::getItemsByTwoKeyColumnsImpl(
    const Attribute & attribute, const Columns & key_columns, ValueSetter && set_value, DefaultGetter && get_default) const
{
    const auto first_column = key_columns.front();
    const auto rows = first_column->size();
    auto & vec = std::get<ContainerType<AttributeType>>(attribute.maps);

    if (auto ipv4_col = std::get_if<IPv4Container>(&ip_column))
    {
        const auto * key_ip_column_ptr = typeid_cast<const ColumnVector<UInt32> *>(&*key_columns.front());
        if (key_ip_column_ptr == nullptr)
            throw Exception{"Expected a UInt32 IP column", ErrorCodes::TYPE_MISMATCH};

        const auto & key_mask_column = assert_cast<const ColumnVector<UInt8> &>(*key_columns.back());

        auto comp_v4 = [&](size_t elem, IPv4Subnet target)
        {
            UInt32 addr = (*ipv4_col)[elem];
            if (addr == target.addr)
                return compPrefixes(mask_column[elem], target.prefix);
            return addr < target.addr;
        };

        for (const auto i : ext::range(0, rows))
        {
            UInt32 addr = key_ip_column_ptr->getElement(i);
            UInt8 mask = key_mask_column.getElement(i);

            auto range = ext::range(0, row_idx.size());
            auto found_it = std::lower_bound(range.begin(), range.end(), IPv4Subnet{addr, mask}, comp_v4);

            if (likely(found_it != range.end() &&
                (*ipv4_col)[*found_it] == addr &&
                mask_column[*found_it] == mask))
            {
                set_value(i, static_cast<OutputType>(vec[row_idx[*found_it]]));
            }
            else
                set_value(i, get_default(i));
        }
        return;
    }

    const auto * key_ip_column_ptr = typeid_cast<const ColumnFixedString *>(&*key_columns.front());
    if (key_ip_column_ptr == nullptr)
        throw Exception{"Expected a UInt32 IP column", ErrorCodes::TYPE_MISMATCH};

    const auto & key_mask_column = assert_cast<const ColumnVector<UInt8> &>(*key_columns.back());

    auto ipv6_col = std::get_if<IPv6Container>(&ip_column);
    auto comp_v6 = [&](size_t i, IPv6Subnet target)
    {
        auto cmpres = memcmp16(getIPv6FromOffset(*ipv6_col, i), target.addr);
        if (cmpres == 0)
            return compPrefixes(mask_column[i], target.prefix);
        return cmpres < 0;
    };

    for (const auto i : ext::range(0, rows))
    {
        auto addr = key_ip_column_ptr->getDataAt(i);
        UInt8 mask = key_mask_column.getElement(i);

        IPv6Subnet target{reinterpret_cast<const uint8_t *>(addr.data), mask};

        auto range = ext::range(0, row_idx.size());
        auto found_it = std::lower_bound(range.begin(), range.end(), target, comp_v6);

        if (likely(found_it != range.end() &&
            memcmp16(getIPv6FromOffset(*ipv6_col, *found_it), target.addr) == 0 &&
            mask_column[*found_it] == mask))
            set_value(i, static_cast<OutputType>(vec[row_idx[*found_it]]));
        else
            set_value(i, get_default(i));
    }
}

template <typename AttributeType, typename OutputType, typename ValueSetter, typename DefaultGetter>
void TrieDictionary::getItemsImpl(
    const Attribute & attribute, const Columns & key_columns, ValueSetter && set_value, DefaultGetter && get_default) const
{
    const auto first_column = key_columns.front();
    const auto rows = first_column->size();

    // special case for getBlockInputStream
    if (unlikely(key_columns.size() == 2))
    {
        getItemsByTwoKeyColumnsImpl<AttributeType, OutputType>(
            attribute, key_columns, std::forward<ValueSetter>(set_value), std::forward<DefaultGetter>(get_default));
        query_count.fetch_add(rows, std::memory_order_relaxed);
        return;
    }

    auto & vec = std::get<ContainerType<AttributeType>>(attribute.maps);

    if (first_column->isNumeric())
    {
        uint8_t addrv6_buf[IPV6_BINARY_LENGTH];
        for (const auto i : ext::range(0, rows))
        {
            // addrv4 has native endianness
            auto addrv4 = UInt32(first_column->get64(i));
            auto found = tryLookupIPv4(addrv4, addrv6_buf);
            set_value(i, (found != ipNotFound()) ? static_cast<OutputType>(vec[*found]) : get_default(i));
        }
    }
    else
    {
        for (const auto i : ext::range(0, rows))
        {
            auto addr = first_column->getDataAt(i);
            if (addr.size != IPV6_BINARY_LENGTH)
                throw Exception("Expected key to be FixedString(16)", ErrorCodes::LOGICAL_ERROR);

            auto found = tryLookupIPv6(reinterpret_cast<const uint8_t *>(addr.data));
            set_value(i, (found != ipNotFound()) ? static_cast<OutputType>(vec[*found]) : get_default(i));
        }
    }

    query_count.fetch_add(rows, std::memory_order_relaxed);
}

template <typename T>
void TrieDictionary::setAttributeValueImpl(Attribute & attribute, const T value)
{
    auto & vec = std::get<ContainerType<T>>(attribute.maps);
    vec.push_back(value);
}

void TrieDictionary::setAttributeValue(Attribute & attribute, const Field & value)
{
    switch (attribute.type)
    {
        case AttributeUnderlyingType::utUInt8:
            return setAttributeValueImpl<UInt8>(attribute, value.get<UInt64>());
        case AttributeUnderlyingType::utUInt16:
            return setAttributeValueImpl<UInt16>(attribute, value.get<UInt64>());
        case AttributeUnderlyingType::utUInt32:
            return setAttributeValueImpl<UInt32>(attribute, value.get<UInt64>());
        case AttributeUnderlyingType::utUInt64:
            return setAttributeValueImpl<UInt64>(attribute, value.get<UInt64>());
        case AttributeUnderlyingType::utUInt128:
            return setAttributeValueImpl<UInt128>(attribute, value.get<UInt128>());
        case AttributeUnderlyingType::utInt8:
            return setAttributeValueImpl<Int8>(attribute, value.get<Int64>());
        case AttributeUnderlyingType::utInt16:
            return setAttributeValueImpl<Int16>(attribute, value.get<Int64>());
        case AttributeUnderlyingType::utInt32:
            return setAttributeValueImpl<Int32>(attribute, value.get<Int64>());
        case AttributeUnderlyingType::utInt64:
            return setAttributeValueImpl<Int64>(attribute, value.get<Int64>());
        case AttributeUnderlyingType::utFloat32:
            return setAttributeValueImpl<Float32>(attribute, value.get<Float64>());
        case AttributeUnderlyingType::utFloat64:
            return setAttributeValueImpl<Float64>(attribute, value.get<Float64>());

        case AttributeUnderlyingType::utDecimal32:
            return setAttributeValueImpl<Decimal32>(attribute, value.get<Decimal32>());
        case AttributeUnderlyingType::utDecimal64:
            return setAttributeValueImpl<Decimal64>(attribute, value.get<Decimal64>());
        case AttributeUnderlyingType::utDecimal128:
            return setAttributeValueImpl<Decimal128>(attribute, value.get<Decimal128>());

        case AttributeUnderlyingType::utString:
        {
            const auto & string = value.get<String>();
            const auto * string_in_arena = attribute.string_arena->insert(string.data(), string.size());
            return setAttributeValueImpl<StringRef>(attribute, StringRef{string_in_arena, string.size()});
        }
    }
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
        uint8_t addrv6_buf[IPV6_BINARY_LENGTH];
        for (const auto i : ext::range(0, rows))
        {
            auto addrv4 = UInt32(first_column->get64(i));
            auto found = tryLookupIPv4(addrv4, addrv6_buf);
            out[i] = (found != ipNotFound());
        }
    }
    else
    {
        for (const auto i : ext::range(0, rows))
        {
            auto addr = first_column->getDataAt(i);
            if (unlikely(addr.size != IPV6_BINARY_LENGTH))
                throw Exception("Expected key to be FixedString(16)", ErrorCodes::LOGICAL_ERROR);

            auto found = tryLookupIPv6(reinterpret_cast<const uint8_t *>(addr.data));
            out[i] = (found != ipNotFound());
        }
    }

    query_count.fetch_add(rows, std::memory_order_relaxed);
}

Columns TrieDictionary::getKeyColumns() const
{
    auto ipv4_col = std::get_if<IPv4Container>(&ip_column);
    if (ipv4_col)
    {
        auto key_ip_column = ColumnVector<UInt32>::create();
        auto key_mask_column = ColumnVector<UInt8>::create();
        for (size_t row : ext::range(0, row_idx.size()))
        {
            key_ip_column->insertValue((*ipv4_col)[row]);
            key_mask_column->insertValue(mask_column[row]);
        }
        return {std::move(key_ip_column), std::move(key_mask_column)};
    }

    auto ipv6_col = std::get_if<IPv6Container>(&ip_column);

    auto key_ip_column = ColumnFixedString::create(IPV6_BINARY_LENGTH);
    auto key_mask_column = ColumnVector<UInt8>::create();

    for (size_t row : ext::range(0, row_idx.size()))
    {
        auto data = reinterpret_cast<const char *>(getIPv6FromOffset(*ipv6_col, row));
        key_ip_column->insertData(data, IPV6_BINARY_LENGTH);
        key_mask_column->insertValue(mask_column[row]);
    }
    return {std::move(key_ip_column), std::move(key_mask_column)};
}

template <typename KeyColumnType, bool IsIPv4>
static auto keyViewGetter()
{
    return [](const Columns & columns, const std::vector<DictionaryAttribute> & dict_attributes)
    {
        auto column = ColumnString::create();
        const auto & key_ip_column = assert_cast<const KeyColumnType &>(*columns.front());
        const auto & key_mask_column = assert_cast<const ColumnVector<UInt8> &>(*columns.back());
        char buffer[48];
        for (size_t row : ext::range(0, key_ip_column.size()))
        {
            UInt8 mask = key_mask_column.getElement(row);
            char * ptr = buffer;
            if constexpr (IsIPv4)
                formatIPv4(reinterpret_cast<const unsigned char *>(&key_ip_column.getElement(row)), ptr);
            else
                formatIPv6(reinterpret_cast<const unsigned char *>(key_ip_column.getDataAt(row).data), ptr);
            *(ptr - 1) = '/';
            ptr = itoa(mask, ptr);
            column->insertData(buffer, ptr - buffer);
        }
        return ColumnsWithTypeAndName{
            ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), dict_attributes.front().name)};
    };
}

BlockInputStreamPtr TrieDictionary::getBlockInputStream(const Names & column_names, size_t max_block_size) const
{
    using BlockInputStreamType = DictionaryBlockInputStream<TrieDictionary, UInt64>;


    const bool isIPv4 = std::get_if<IPv4Container>(&ip_column) != nullptr;

    auto get_keys = [isIPv4](const Columns & columns, const std::vector<DictionaryAttribute> & dict_attributes)
    {
        const auto & attr = dict_attributes.front();
        std::shared_ptr<const IDataType> key_typ;
        if (isIPv4)
            key_typ = std::make_shared<DataTypeUInt32>();
        else
            key_typ = std::make_shared<DataTypeFixedString>(IPV6_BINARY_LENGTH);

        return ColumnsWithTypeAndName({
            ColumnWithTypeAndName(columns.front(), key_typ, attr.name),
            ColumnWithTypeAndName(columns.back(), std::make_shared<DataTypeUInt8>(), attr.name + ".mask")
        });
    };

    if (isIPv4)
    {
        auto get_view = keyViewGetter<ColumnVector<UInt32>, true>();
        return std::make_shared<BlockInputStreamType>(
            shared_from_this(), max_block_size, getKeyColumns(), column_names, std::move(get_keys), std::move(get_view));
    }

    auto get_view = keyViewGetter<ColumnFixedString, false>();
    return std::make_shared<BlockInputStreamType>(
        shared_from_this(), max_block_size, getKeyColumns(), column_names, std::move(get_keys), std::move(get_view));
}

TrieDictionary::RowIdxConstIter TrieDictionary::ipNotFound() const
{
    return row_idx.end();
}

TrieDictionary::RowIdxConstIter TrieDictionary::tryLookupIPv4(UInt32 addr, uint8_t * buf) const
{
    if (std::get_if<IPv6Container>(&ip_column))
    {
        mapIPv4ToIPv6(addr, buf);
        return lookupIP<IPv6Container>(buf);
    }
    return lookupIP<IPv4Container>(addr);
}

TrieDictionary::RowIdxConstIter TrieDictionary::tryLookupIPv6(const uint8_t * addr) const
{
    if (std::get_if<IPv4Container>(&ip_column))
    {
        bool is_mapped = false;
        UInt32 addrv4 = mappedIPv4ToBinary(addr, is_mapped);
        if (!is_mapped)
            return ipNotFound();
        return lookupIP<IPv4Container>(addrv4);
    }
    return lookupIP<IPv6Container>(addr);
}

template <typename IPContainerType, typename IPValueType>
TrieDictionary::RowIdxConstIter TrieDictionary::lookupIP(IPValueType target) const
{
    if (row_idx.empty())
        return ipNotFound();

    auto ipv4or6_col = std::get_if<IPContainerType>(&ip_column);
    if (ipv4or6_col == nullptr)
        return ipNotFound();

    auto comp = [&](auto value, auto idx) -> bool
    {
        if constexpr (std::is_same_v<IPContainerType, IPv4Container>)
            return value < (*ipv4or6_col)[idx];
        else
            return memcmp16(value, getIPv6FromOffset(*ipv4or6_col, idx)) < 0;
    };

    auto range = ext::range(0, row_idx.size());
    auto found_it = std::upper_bound(range.begin(), range.end(), target, comp);

    if (found_it == range.begin())
        return ipNotFound();

    --found_it;
    if constexpr (std::is_same_v<IPContainerType, IPv4Container>)
    {
        for (auto idx = *found_it;; idx = parent_subnet[idx])
        {
            if (matchIPv4Subnet(target, (*ipv4or6_col)[idx], mask_column[idx]))
                return row_idx.begin() + idx;

            if (idx == parent_subnet[idx])
                return ipNotFound();
        }
    }
    else
    {
        for (auto idx = *found_it;; idx = parent_subnet[idx])
        {
            if (matchIPv6Subnet(target, getIPv6FromOffset(*ipv4or6_col, idx), mask_column[idx]))
                return row_idx.begin() + idx;

            if (idx == parent_subnet[idx])
                return ipNotFound();
        }
    }

    return ipNotFound();
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

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);
        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
        const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);
        // This is specialised trie for storing IPv4 and IPv6 prefixes.
        return std::make_unique<TrieDictionary>(dict_id, dict_struct, std::move(source_ptr), dict_lifetime, require_nonempty);
    };
    factory.registerLayout("ip_trie", create_layout, true);
}

}
