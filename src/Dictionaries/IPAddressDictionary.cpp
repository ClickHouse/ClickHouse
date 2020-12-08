#include "IPAddressDictionary.h"
#include <stack>
#include <charconv>
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
    /// Intermediate structure that are used in loading procedure
    struct IPRecord
    {
        Poco::Net::IPAddress addr;
        UInt8 prefix;
        size_t row;
        bool isv6;

        IPRecord(const Poco::Net::IPAddress & addr_, UInt8 prefix_, size_t row_)
            : addr(addr_)
            , prefix(prefix_)
            , row(row_)
            , isv6(addr.family() == Poco::Net::IPAddress::IPv6)
        {
        }

        const uint8_t * asIPv6Binary(uint8_t * buf) const
        {
            if (isv6)
                return reinterpret_cast<const uint8_t *>(addr.addr());
            memset(buf, 0, 10);
            buf[10] = '\xFF';
            buf[11] = '\xFF';
            memcpy(&buf[12], addr.addr(), 4);

            return buf;
        }

        inline UInt8 prefixIPv6() const
        {
            return isv6 ? prefix : prefix + 96;
        }
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

static std::pair<Poco::Net::IPAddress, UInt8> parseIPFromString(const std::string_view addr_str)
{
    try
    {
        size_t pos = addr_str.find('/');
        if (pos != std::string::npos)
        {
            Poco::Net::IPAddress addr{std::string(addr_str.substr(0, pos))};

            uint8_t prefix = 0;
            const auto * addr_str_end = addr_str.data() + addr_str.size();
            auto [p, ec] = std::from_chars(addr_str.data() + pos + 1, addr_str_end, prefix);
            if (p != addr_str_end)
                throw DB::Exception("extra characters at the end", ErrorCodes::LOGICAL_ERROR);
            if (ec != std::errc())
                throw DB::Exception("mask is not a valid number", ErrorCodes::LOGICAL_ERROR);

            addr = addr & Poco::Net::IPAddress(prefix, addr.family());
            return {addr, prefix};
        }

        Poco::Net::IPAddress addr{std::string(addr_str)};
        return {addr, addr.length() * 8};
    }
    catch (Poco::Exception & ex)
    {
        throw DB::Exception("can't parse address \"" + std::string(addr_str) + "\": " + ex.what(),
            ErrorCodes::LOGICAL_ERROR);
    }
}

static void validateKeyTypes(const DataTypes & key_types)
{
    if (key_types.empty() || key_types.size() > 2)
        throw Exception{"Expected a single IP address or IP with mask", ErrorCodes::TYPE_MISMATCH};

    const auto * key_ipv4type = typeid_cast<const DataTypeUInt32 *>(key_types[0].get());
    const auto * key_ipv6type = typeid_cast<const DataTypeFixedString *>(key_types[0].get());

    if (key_ipv4type == nullptr && (key_ipv6type == nullptr || key_ipv6type->getN() != 16))
        throw Exception{"Key does not match, expected either `IPv4` (`UInt32`) or `IPv6` (`FixedString(16)`)",
                        ErrorCodes::TYPE_MISMATCH};

    if (key_types.size() > 1)
    {
        const auto * mask_col_type = typeid_cast<const DataTypeUInt8 *>(key_types[1].get());
        if (mask_col_type == nullptr)
            throw Exception{"Mask do not match, expected UInt8", ErrorCodes::TYPE_MISMATCH};
    }
}

template <typename T, typename Comp>
size_t sortAndUnique(std::vector<T> & vec, Comp comp)
{
    std::sort(vec.begin(), vec.end(),
              [&](const auto & a, const auto & b) { return comp(a, b) < 0; });

    auto new_end = std::unique(vec.begin(), vec.end(),
                               [&](const auto & a, const auto & b) { return comp(a, b) == 0; });
    size_t deleted_count = std::distance(new_end, vec.end());
    vec.erase(new_end, vec.end());
    return deleted_count;
}

template <typename T>
static inline int compareTo(T a, T b)
{
    return a > b ? 1 : (a < b ? -1 : 0);
}

inline static UInt32 IPv4AsUInt32(const void * addr)
{
    return Poco::ByteOrder::fromNetwork(*reinterpret_cast<const UInt32 *>(addr));
}

/// Convert mapped IPv6 to IPv4 if possible
inline static UInt32 mappedIPv4ToBinary(const uint8_t * addr, bool & success)
{
    success = addr[0] == 0x0 && addr[1] == 0x0 &&
              addr[2] == 0x0 && addr[3] == 0x0 &&
              addr[4] == 0x0 && addr[5] == 0x0 &&
              addr[6] == 0x0 && addr[7] == 0x0 &&
              addr[8] == 0x0 && addr[9] == 0x0 &&
              addr[10] == 0xff && addr[11] == 0xff;
    if (!success)
        return 0;
    return IPv4AsUInt32(&addr[12]);
}

/// Convert IPv4 to IPv6-mapped and save results to buf
inline static void mapIPv4ToIPv6(UInt32 addr, uint8_t * buf)
{
    memset(buf, 0, 10);
    buf[10] = '\xFF';
    buf[11] = '\xFF';
    addr = Poco::ByteOrder::toNetwork(addr);
    memcpy(&buf[12], &addr, 4);
}

static bool matchIPv4Subnet(UInt32 target, UInt32 addr, UInt8 prefix)
{
    UInt32 mask = (prefix >= 32) ? 0xffffffffu : ~(0xffffffffu >> prefix);
    return (target & mask) == addr;
}

#if defined(__SSE2__)
#include <emmintrin.h>

static bool matchIPv6Subnet(const uint8_t * target, const uint8_t * addr, UInt8 prefix)
{
    uint16_t mask = _mm_movemask_epi8(_mm_cmpeq_epi8(
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(target)),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(addr))));
    mask = ~mask;

    if (mask)
    {
        auto offset = __builtin_ctz(mask);

        if (prefix / 8 != offset)
            return prefix / 8 < offset;

        auto cmpmask = ~(0xff >> (prefix % 8));
        return (target[offset] & cmpmask) == addr[offset];
    }
    return true;
}

# else

static bool matchIPv6Subnet(const uint8_t * target, const uint8_t * addr, UInt8 prefix)
{
    if (prefix > IPV6_BINARY_LENGTH * 8U)
        prefix = IPV6_BINARY_LENGTH * 8U;

    size_t i = 0;
    for (; prefix >= 8; ++i, prefix -= 8)
    {
        if (target[i] != addr[i])
            return false;
    }
    if (prefix == 0)
        return true;

    auto mask = ~(0xff >> prefix);
    return (target[i] & mask) == addr[i];
}

#endif  // __SSE2__

IPAddressDictionary::IPAddressDictionary(
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
    , logger(&Poco::Logger::get("IPAddressDictionary"))
{
    createAttributes();

    loadData();
    calculateBytesAllocated();
}

#define DECLARE(TYPE) \
    void IPAddressDictionary::get##TYPE( \
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

void IPAddressDictionary::getString(
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
    void IPAddressDictionary::get##TYPE( \
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

void IPAddressDictionary::getString(
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
    void IPAddressDictionary::get##TYPE( \
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

void IPAddressDictionary::getString(
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

void IPAddressDictionary::has(const Columns & key_columns, const DataTypes & key_types, PaddedPODArray<UInt8> & out) const
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

void IPAddressDictionary::createAttributes()
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

void IPAddressDictionary::loadData()
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

            const auto [addr, prefix] = parseIPFromString(std::string_view(key_column->getDataAt(row)));
            has_ipv6 = has_ipv6 || (addr.family() == Poco::Net::IPAddress::IPv6);

            size_t row_number = ip_records.size();
            ip_records.emplace_back(addr, prefix, row_number);
        }
    }

    stream->readSuffix();

    if (has_ipv6)
    {
        auto deleted_count = sortAndUnique(ip_records,
            [](const auto & record_a, const auto & record_b)
            {
                uint8_t a_buf[IPV6_BINARY_LENGTH];
                uint8_t b_buf[IPV6_BINARY_LENGTH];

                auto cmpres = memcmp16(record_a.asIPv6Binary(a_buf), record_b.asIPv6Binary(b_buf));

                if (cmpres == 0)
                    return compareTo(record_a.prefixIPv6(), record_b.prefixIPv6());
                return cmpres;
            });
        if (deleted_count > 0)
            LOG_WARNING(logger, "removing {} non-unique subnets from input", deleted_count);

        auto & ipv6_col = ip_column.emplace<IPv6Container>();
        ipv6_col.resize_fill(IPV6_BINARY_LENGTH * ip_records.size());

        for (const auto & record : ip_records)
        {
            size_t i = row_idx.size();

            IPv6ToRawBinary(record.addr, reinterpret_cast<char *>(&ipv6_col[i * IPV6_BINARY_LENGTH]));
            mask_column.push_back(record.prefixIPv6());
            row_idx.push_back(record.row);
        }
    }
    else
    {
        auto deleted_count = sortAndUnique(ip_records,
            [](const auto & record_a, const auto & record_b)
            {
                UInt32 a = IPv4AsUInt32(record_a.addr.addr());
                UInt32 b = IPv4AsUInt32(record_b.addr.addr());

                if (a == b)
                    return compareTo(record_a.prefix, record_b.prefix);
                return compareTo(a, b);
            });
        if (deleted_count > 0)
            LOG_WARNING(logger, "removing {} non-unique subnets from input", deleted_count);

        auto & ipv4_col = ip_column.emplace<IPv4Container>();
        ipv4_col.reserve(ip_records.size());
        for (const auto & record : ip_records)
        {
            auto addr = IPv4AsUInt32(record.addr.addr());
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
        while (!subnets_stack.empty())
        {
            size_t pi = subnets_stack.top();
            if (has_ipv6)
            {
                uint8_t a_buf[IPV6_BINARY_LENGTH];
                uint8_t b_buf[IPV6_BINARY_LENGTH];
                const auto * cur_address = ip_records[i].asIPv6Binary(a_buf);
                const auto * cur_subnet = ip_records[pi].asIPv6Binary(b_buf);

                bool is_mask_smaller = ip_records[pi].prefixIPv6() < ip_records[i].prefixIPv6();
                if (is_mask_smaller && matchIPv6Subnet(cur_address, cur_subnet, ip_records[pi].prefixIPv6()))
                {
                    parent_subnet[i] = pi;
                    break;
                }
            }
            else
            {
                UInt32 cur_address = IPv4AsUInt32(ip_records[i].addr.addr());
                UInt32 cur_subnet = IPv4AsUInt32(ip_records[pi].addr.addr());

                bool is_mask_smaller = ip_records[pi].prefix < ip_records[i].prefix;
                if (is_mask_smaller && matchIPv4Subnet(cur_address, cur_subnet, ip_records[pi].prefix))
                {
                    parent_subnet[i] = pi;
                    break;
                }
            }
            subnets_stack.pop();
        }
        subnets_stack.push(i);
    }

    LOG_TRACE(logger, "{} ip records are read", ip_records.size());

    if (require_nonempty && 0 == element_count)
        throw Exception{full_name + ": dictionary source is empty and 'require_nonempty' property is set.", ErrorCodes::DICTIONARY_IS_EMPTY};
}

template <typename T>
void IPAddressDictionary::addAttributeSize(const Attribute & attribute)
{
    const auto & vec = std::get<ContainerType<T>>(attribute.maps);
    bytes_allocated += sizeof(ContainerType<T>) + (vec.capacity() * sizeof(T));
    bucket_count = vec.size();
}

void IPAddressDictionary::calculateBytesAllocated()
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
    bytes_allocated += parent_subnet.size() * sizeof(parent_subnet[0]);
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
void IPAddressDictionary::createAttributeImpl(Attribute & attribute, const Field & null_value)
{
    attribute.null_values = T(null_value.get<NearestFieldType<T>>());
    attribute.maps.emplace<ContainerType<T>>();
}

IPAddressDictionary::Attribute IPAddressDictionary::createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value)
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

const uint8_t * IPAddressDictionary::getIPv6FromOffset(const IPAddressDictionary::IPv6Container & ipv6_col, size_t i)
{
    return reinterpret_cast<const uint8_t *>(&ipv6_col[i * IPV6_BINARY_LENGTH]);
}

template <typename AttributeType, typename OutputType, typename ValueSetter, typename DefaultGetter>
void IPAddressDictionary::getItemsByTwoKeyColumnsImpl(
    const Attribute & attribute, const Columns & key_columns, ValueSetter && set_value, DefaultGetter && get_default) const
{
    const auto first_column = key_columns.front();
    const auto rows = first_column->size();
    auto & vec = std::get<ContainerType<AttributeType>>(attribute.maps);

    if (const auto * ipv4_col = std::get_if<IPv4Container>(&ip_column))
    {
        const auto * key_ip_column_ptr = typeid_cast<const ColumnVector<UInt32> *>(&*key_columns.front());
        if (key_ip_column_ptr == nullptr)
            throw Exception{"Expected a UInt32 IP column", ErrorCodes::TYPE_MISMATCH};

        const auto & key_mask_column = assert_cast<const ColumnVector<UInt8> &>(*key_columns.back());

        auto comp_v4 = [&](size_t elem, const IPv4Subnet & target)
        {
            UInt32 addr = (*ipv4_col)[elem];
            if (addr == target.addr)
                return mask_column[elem] < target.prefix;
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
    if (key_ip_column_ptr == nullptr || key_ip_column_ptr->getN() != IPV6_BINARY_LENGTH)
        throw Exception{"Expected a FixedString(16) IP column", ErrorCodes::TYPE_MISMATCH};

    const auto & key_mask_column = assert_cast<const ColumnVector<UInt8> &>(*key_columns.back());

    const auto * ipv6_col = std::get_if<IPv6Container>(&ip_column);
    auto comp_v6 = [&](size_t i, const IPv6Subnet & target)
    {
        auto cmpres = memcmp16(getIPv6FromOffset(*ipv6_col, i), target.addr);
        if (cmpres == 0)
            return mask_column[i] < target.prefix;
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
            memequal16(getIPv6FromOffset(*ipv6_col, *found_it), target.addr) &&
            mask_column[*found_it] == mask))
            set_value(i, static_cast<OutputType>(vec[row_idx[*found_it]]));
        else
            set_value(i, get_default(i));
    }
}

template <typename AttributeType, typename OutputType, typename ValueSetter, typename DefaultGetter>
void IPAddressDictionary::getItemsImpl(
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
            if (found != ipNotFound())
                set_value(i, static_cast<OutputType>(vec[*found]));
            else
                set_value(i, get_default(i));
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
            if (found != ipNotFound())
                set_value(i, static_cast<OutputType>(vec[*found]));
            else
                set_value(i, get_default(i));
        }
    }

    query_count.fetch_add(rows, std::memory_order_relaxed);
}

template <typename T>
void IPAddressDictionary::setAttributeValueImpl(Attribute & attribute, const T value)
{
    auto & vec = std::get<ContainerType<T>>(attribute.maps);
    vec.push_back(value);
}

void IPAddressDictionary::setAttributeValue(Attribute & attribute, const Field & value)
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

const IPAddressDictionary::Attribute & IPAddressDictionary::getAttribute(const std::string & attribute_name) const
{
    const auto it = attribute_index_by_name.find(attribute_name);
    if (it == std::end(attribute_index_by_name))
        throw Exception{full_name + ": no such attribute '" + attribute_name + "'", ErrorCodes::BAD_ARGUMENTS};

    return attributes[it->second];
}

template <typename T>
void IPAddressDictionary::has(const Attribute &, const Columns & key_columns, PaddedPODArray<UInt8> & out) const
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

Columns IPAddressDictionary::getKeyColumns() const
{
    const auto * ipv4_col = std::get_if<IPv4Container>(&ip_column);
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

    const auto * ipv6_col = std::get_if<IPv6Container>(&ip_column);

    auto key_ip_column = ColumnFixedString::create(IPV6_BINARY_LENGTH);
    auto key_mask_column = ColumnVector<UInt8>::create();

    for (size_t row : ext::range(0, row_idx.size()))
    {
        const char * data = reinterpret_cast<const char *>(getIPv6FromOffset(*ipv6_col, row));
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

BlockInputStreamPtr IPAddressDictionary::getBlockInputStream(const Names & column_names, size_t max_block_size) const
{
    using BlockInputStreamType = DictionaryBlockInputStream<IPAddressDictionary, UInt64>;


    const bool is_ipv4 = std::get_if<IPv4Container>(&ip_column) != nullptr;

    auto get_keys = [is_ipv4](const Columns & columns, const std::vector<DictionaryAttribute> & dict_attributes)
    {
        const auto & attr = dict_attributes.front();
        std::shared_ptr<const IDataType> key_typ;
        if (is_ipv4)
            key_typ = std::make_shared<DataTypeUInt32>();
        else
            key_typ = std::make_shared<DataTypeFixedString>(IPV6_BINARY_LENGTH);

        return ColumnsWithTypeAndName({
            ColumnWithTypeAndName(columns.front(), key_typ, attr.name),
            ColumnWithTypeAndName(columns.back(), std::make_shared<DataTypeUInt8>(), attr.name + ".mask")
        });
    };

    if (is_ipv4)
    {
        auto get_view = keyViewGetter<ColumnVector<UInt32>, true>();
        return std::make_shared<BlockInputStreamType>(
            shared_from_this(), max_block_size, getKeyColumns(), column_names, std::move(get_keys), std::move(get_view));
    }

    auto get_view = keyViewGetter<ColumnFixedString, false>();
    return std::make_shared<BlockInputStreamType>(
        shared_from_this(), max_block_size, getKeyColumns(), column_names, std::move(get_keys), std::move(get_view));
}

IPAddressDictionary::RowIdxConstIter IPAddressDictionary::ipNotFound() const
{
    return row_idx.end();
}

IPAddressDictionary::RowIdxConstIter IPAddressDictionary::tryLookupIPv4(UInt32 addr, uint8_t * buf) const
{
    if (std::get_if<IPv6Container>(&ip_column))
    {
        mapIPv4ToIPv6(addr, buf);
        return lookupIP<IPv6Container>(buf);
    }
    return lookupIP<IPv4Container>(addr);
}

IPAddressDictionary::RowIdxConstIter IPAddressDictionary::tryLookupIPv6(const uint8_t * addr) const
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
IPAddressDictionary::RowIdxConstIter IPAddressDictionary::lookupIP(IPValueType target) const
{
    if (row_idx.empty())
        return ipNotFound();

    const auto * ipv4or6_col = std::get_if<IPContainerType>(&ip_column);
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
        return std::make_unique<IPAddressDictionary>(dict_id, dict_struct, std::move(source_ptr), dict_lifetime, require_nonempty);
    };
    factory.registerLayout("ip_trie", create_layout, true);
}

}
