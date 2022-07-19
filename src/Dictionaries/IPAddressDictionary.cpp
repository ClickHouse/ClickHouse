#include "IPAddressDictionary.h"
#include <stack>
#include <charconv>
#include <Common/assert_cast.h>
#include <Common/IPv6ToBinary.h>
#include <Common/memcmpSmall.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Poco/ByteOrder.h>
#include <Common/formatIPv6.h>
#include <base/itoa.h>
#include <base/map.h>
#include <base/range.h>
#include <base/sort.h>
#include <Dictionaries/DictionarySource.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Functions/FunctionHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int DICTIONARY_IS_EMPTY;
    extern const int LOGICAL_ERROR;
    extern const int TYPE_MISMATCH;
    extern const int UNSUPPORTED_METHOD;
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
                throw DB::Exception("Extra characters at the end of IP address", ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
            if (ec != std::errc())
                throw DB::Exception("Mask for IP address is not a valid number", ErrorCodes::CANNOT_PARSE_NUMBER);

            addr = addr & Poco::Net::IPAddress(prefix, addr.family());
            return {addr, prefix};
        }

        Poco::Net::IPAddress addr{std::string(addr_str)};
        return {addr, addr.length() * 8};
    }
    catch (Poco::Exception & ex)
    {
        throw DB::Exception("Can't parse address \"" + std::string(addr_str) + "\": " + ex.what(),
            ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
    }
}

static size_t formatIPWithPrefix(const unsigned char * src, UInt8 prefix_len, bool isv4, char * dst)
{
    char * ptr = dst;
    if (isv4)
        formatIPv4(src, ptr);
    else
        formatIPv6(src, ptr);
    *(ptr - 1) = '/';
    ptr = itoa(prefix_len, ptr);
    return ptr - dst;
}

static void validateKeyTypes(const DataTypes & key_types)
{
    if (key_types.empty() || key_types.size() > 2)
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected a single IP address or IP with mask");

    const auto * key_ipv4type = typeid_cast<const DataTypeUInt32 *>(key_types[0].get());
    const auto * key_ipv6type = typeid_cast<const DataTypeFixedString *>(key_types[0].get());

    if (key_ipv4type == nullptr && (key_ipv6type == nullptr || key_ipv6type->getN() != 16))
        throw Exception(ErrorCodes::TYPE_MISMATCH,
            "Key does not match, expected either `IPv4` (`UInt32`) or `IPv6` (`FixedString(16)`)");

    if (key_types.size() > 1)
    {
        const auto * mask_col_type = typeid_cast<const DataTypeUInt8 *>(key_types[1].get());
        if (mask_col_type == nullptr)
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Mask do not match, expected UInt8");
    }
}

template <typename T, typename Comp>
size_t sortAndUnique(std::vector<T> & vec, Comp comp)
{
    ::sort(vec.begin(), vec.end(),
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

IPAddressDictionary::IPAddressDictionary(
    const StorageID & dict_id_,
    const DictionaryStructure & dict_struct_,
    DictionarySourcePtr source_ptr_,
    const DictionaryLifetime dict_lifetime_,
    bool require_nonempty_)
    : IDictionary(dict_id_)
    , dict_struct(dict_struct_)
    , source_ptr{std::move(source_ptr_)}
    , dict_lifetime(dict_lifetime_)
    , require_nonempty(require_nonempty_)
    , access_to_key_from_attributes(dict_struct_.access_to_key_from_attributes)
    , logger(&Poco::Logger::get("IPAddressDictionary"))
{
    createAttributes();
    loadData();
    calculateBytesAllocated();
}

void IPAddressDictionary::convertKeyColumns(Columns &, DataTypes &) const
{
    /// Do not perform any implicit keys conversion for IPAddressDictionary
}

ColumnPtr IPAddressDictionary::getColumn(
    const std::string & attribute_name,
    const DataTypePtr & result_type,
    const Columns & key_columns,
    const DataTypes & key_types,
    const ColumnPtr & default_values_column) const
{
    validateKeyTypes(key_types);

    ColumnPtr result;

    const auto & attribute = getAttribute(attribute_name);
    const auto & dictionary_attribute = dict_struct.getAttribute(attribute_name, result_type);

    auto size = key_columns.front()->size();

    auto type_call = [&](const auto &dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;
        using ValueType = DictionaryValueType<AttributeType>;
        using ColumnProvider = DictionaryAttributeColumnProvider<AttributeType>;

        const auto & null_value = std::get<AttributeType>(attribute.null_values);
        DictionaryDefaultValueExtractor<AttributeType> default_value_extractor(null_value, default_values_column);

        auto column = ColumnProvider::getColumn(dictionary_attribute, size);

        if constexpr (std::is_same_v<ValueType, Array>)
        {
            auto * out = column.get();

            getItemsImpl<ValueType>(
                attribute,
                key_columns,
                [&](const size_t, const Array & value) { out->insert(value); },
                default_value_extractor);
        }
        else if constexpr (std::is_same_v<ValueType, StringRef>)
        {
            auto * out = column.get();

            getItemsImpl<ValueType>(
                attribute,
                key_columns,
                [&](const size_t, const StringRef value) { out->insertData(value.data, value.size); },
                default_value_extractor);
        }
        else
        {
            auto & out = column->getData();

            getItemsImpl<ValueType>(
                attribute,
                key_columns,
                [&](const size_t row, const auto value) { return out[row] = value; },
                default_value_extractor);
        }

        result = std::move(column);
    };

    callOnDictionaryAttributeType(attribute.type, type_call);

    return result;
}


ColumnUInt8::Ptr IPAddressDictionary::hasKeys(const Columns & key_columns, const DataTypes & key_types) const
{
    validateKeyTypes(key_types);

    const auto first_column = key_columns.front();
    const auto rows = first_column->size();

    auto result = ColumnUInt8::create(rows);
    auto & out = result->getData();

    size_t keys_found = 0;

    if (first_column->isNumeric())
    {
        uint8_t addrv6_buf[IPV6_BINARY_LENGTH];
        for (const auto i : collections::range(0, rows))
        {
            auto addrv4 = UInt32(first_column->get64(i));
            auto found = tryLookupIPv4(addrv4, addrv6_buf);
            out[i] = (found != ipNotFound());
            keys_found += out[i];
        }
    }
    else
    {
        for (const auto i : collections::range(0, rows))
        {
            auto addr = first_column->getDataAt(i);
            if (unlikely(addr.size != IPV6_BINARY_LENGTH))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected key to be FixedString(16)");

            auto found = tryLookupIPv6(reinterpret_cast<const uint8_t *>(addr.data));
            out[i] = (found != ipNotFound());
            keys_found += out[i];
        }
    }

    query_count.fetch_add(rows, std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);

    return result;
}

void IPAddressDictionary::createAttributes()
{
    auto create_attributes_from_dictionary_attributes = [this](const std::vector<DictionaryAttribute> & dict_attrs)
    {
        attributes.reserve(attributes.size() + dict_attrs.size());

        for (const auto & attribute : dict_attrs)
        {
            if (attribute.is_nullable)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "{}: array or nullable attributes not supported for dictionary of type {}",
                    getFullName(),
                    getTypeName());

            attribute_index_by_name.emplace(attribute.name, attributes.size());
            attributes.push_back(createAttributeWithType(attribute.underlying_type, attribute.null_value));

            if (attribute.hierarchical)
                throw Exception(ErrorCodes::TYPE_MISMATCH,
                    "{}: hierarchical attributes not supported for dictionary of type {}",
                    getFullName(),
                    getTypeName());
        }
    };

    create_attributes_from_dictionary_attributes(dict_struct.attributes);
    if (access_to_key_from_attributes)
        create_attributes_from_dictionary_attributes(*dict_struct.key);
}

void IPAddressDictionary::loadData()
{
    QueryPipeline pipeline(source_ptr->loadAll());

    std::vector<IPRecord> ip_records;

    bool has_ipv6 = false;

    PullingPipelineExecutor executor(pipeline);
    Block block;
    while (executor.pull(block))
    {
        const auto rows = block.rows();
        element_count += rows;

        const ColumnPtr key_column_ptr = block.safeGetByPosition(0).column;
        const auto attribute_column_ptrs = collections::map<Columns>(
            collections::range(0, dict_struct.attributes.size()),
            [&](const size_t attribute_idx) { return block.safeGetByPosition(attribute_idx + 1).column; });

        for (const auto row : collections::range(0, rows))
        {
            for (const auto attribute_idx : collections::range(0, dict_struct.attributes.size()))
            {
                const auto & attribute_column = *attribute_column_ptrs[attribute_idx];
                auto & attribute = attributes[attribute_idx];

                setAttributeValue(attribute, attribute_column[row]);
            }

            const auto [addr, prefix] = parseIPFromString(std::string_view(key_column_ptr->getDataAt(row)));
            has_ipv6 = has_ipv6 || (addr.family() == Poco::Net::IPAddress::IPv6);

            size_t row_number = ip_records.size();
            ip_records.emplace_back(addr, prefix, row_number);
        }
    }

    if (access_to_key_from_attributes)
    {
        /// We format key attribute values here instead of filling with data from key_column
        /// because string representation can be normalized if bits beyond mask are set.
        /// Also all IPv4 will be displayed as mapped IPv6 if there are any IPv6.
        /// It's consistent with representation in table created with `ENGINE = Dictionary` from this dictionary.
        char str_buffer[48];
        if (has_ipv6)
        {
            uint8_t ip_buffer[IPV6_BINARY_LENGTH];
            for (const auto & record : ip_records)
            {
                size_t str_len = formatIPWithPrefix(record.asIPv6Binary(ip_buffer), record.prefixIPv6(), false, str_buffer);
                setAttributeValue(attributes.back(), String(str_buffer, str_len));
            }
        }
        else
        {
            for (const auto & record : ip_records)
            {
                UInt32 addr = IPv4AsUInt32(record.addr.addr());
                size_t str_len = formatIPWithPrefix(reinterpret_cast<const unsigned char *>(&addr), record.prefix, true, str_buffer);
                setAttributeValue(attributes.back(), String(str_buffer, str_len));
            }
        }
    }

    row_idx.reserve(ip_records.size());
    mask_column.reserve(ip_records.size());

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
            LOG_TRACE(logger, "removing {} non-unique subnets from input", deleted_count);

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
            LOG_TRACE(logger, "removing {} non-unique subnets from input", deleted_count);

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
    for (const auto i : collections::range(0, ip_records.size()))
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
        throw Exception(ErrorCodes::DICTIONARY_IS_EMPTY, "{}: dictionary source is empty and 'require_nonempty' property is set.", getFullName());
}

template <typename T>
void IPAddressDictionary::addAttributeSize(const Attribute & attribute)
{
    const auto & vec = std::get<ContainerType<T>>(attribute.maps);
    bytes_allocated += sizeof(ContainerType<T>) + (vec.capacity() * sizeof(T));
    bucket_count = vec.size();
}

template <>
void IPAddressDictionary::addAttributeSize<String>(const Attribute & attribute)
{
    addAttributeSize<StringRef>(attribute);
    bytes_allocated += sizeof(Arena) + attribute.string_arena->size();
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
        auto type_call = [&](const auto & dictionary_attribute_type)
        {
            using Type = std::decay_t<decltype(dictionary_attribute_type)>;
            using AttributeType = typename Type::AttributeType;

            addAttributeSize<AttributeType>(attribute);
        };

        callOnDictionaryAttributeType(attribute.type, type_call);
    }
}

template <typename T>
void IPAddressDictionary::createAttributeImpl(Attribute & attribute, const Field & null_value)
{
    attribute.null_values = null_value.isNull() ? T{} : T(null_value.get<T>());
    attribute.maps.emplace<ContainerType<T>>();
}

template <>
void IPAddressDictionary::createAttributeImpl<String>(Attribute & attribute, const Field & null_value)
{
    attribute.null_values = null_value.isNull() ? String() : null_value.get<String>();
    attribute.maps.emplace<ContainerType<StringRef>>();
    attribute.string_arena = std::make_unique<Arena>();
}

IPAddressDictionary::Attribute IPAddressDictionary::createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value)
{
    Attribute attr{type, {}, {}, {}};

    auto type_call = [&](const auto & dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;

        createAttributeImpl<AttributeType>(attr, null_value);
    };

    callOnDictionaryAttributeType(type, type_call);

    return attr;
}

const uint8_t * IPAddressDictionary::getIPv6FromOffset(const IPAddressDictionary::IPv6Container & ipv6_col, size_t i)
{
    return reinterpret_cast<const uint8_t *>(&ipv6_col[i * IPV6_BINARY_LENGTH]);
}

template <typename AttributeType, typename ValueSetter, typename DefaultValueExtractor>
void IPAddressDictionary::getItemsByTwoKeyColumnsImpl(
    const Attribute & attribute,
    const Columns & key_columns,
    ValueSetter && set_value,
    DefaultValueExtractor & default_value_extractor) const
{
    const auto first_column = key_columns.front();
    const auto rows = first_column->size();
    auto & vec = std::get<ContainerType<AttributeType>>(attribute.maps);

    if (const auto * ipv4_col = std::get_if<IPv4Container>(&ip_column))
    {
        const auto * key_ip_column_ptr = typeid_cast<const ColumnVector<UInt32> *>(&*key_columns.front());
        if (key_ip_column_ptr == nullptr)
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected a UInt32 IP column");

        const auto & key_mask_column = assert_cast<const ColumnVector<UInt8> &>(*key_columns.back());

        auto comp_v4 = [&](size_t elem, const IPv4Subnet & target)
        {
            UInt32 addr = (*ipv4_col)[elem];
            if (addr == target.addr)
                return mask_column[elem] < target.prefix;
            return addr < target.addr;
        };

        for (const auto i : collections::range(0, rows))
        {
            UInt32 addr = key_ip_column_ptr->getElement(i);
            UInt8 mask = key_mask_column.getElement(i);

            auto range = collections::range(0, row_idx.size());
            auto found_it = std::lower_bound(range.begin(), range.end(), IPv4Subnet{addr, mask}, comp_v4);

            if (likely(found_it != range.end() &&
                (*ipv4_col)[*found_it] == addr &&
                mask_column[*found_it] == mask))
            {
                set_value(i, vec[row_idx[*found_it]]);
            }
            else
                set_value(i, default_value_extractor[i]);
        }
        return;
    }

    const auto * key_ip_column_ptr = typeid_cast<const ColumnFixedString *>(&*key_columns.front());
    if (key_ip_column_ptr == nullptr || key_ip_column_ptr->getN() != IPV6_BINARY_LENGTH)
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Expected a FixedString(16) IP column");

    const auto & key_mask_column = assert_cast<const ColumnVector<UInt8> &>(*key_columns.back());

    const auto * ipv6_col = std::get_if<IPv6Container>(&ip_column);
    auto comp_v6 = [&](size_t i, const IPv6Subnet & target)
    {
        auto cmpres = memcmp16(getIPv6FromOffset(*ipv6_col, i), target.addr);
        if (cmpres == 0)
            return mask_column[i] < target.prefix;
        return cmpres < 0;
    };

    for (const auto i : collections::range(0, rows))
    {
        auto addr = key_ip_column_ptr->getDataAt(i);
        UInt8 mask = key_mask_column.getElement(i);

        IPv6Subnet target{reinterpret_cast<const uint8_t *>(addr.data), mask};

        auto range = collections::range(0, row_idx.size());
        auto found_it = std::lower_bound(range.begin(), range.end(), target, comp_v6);

        if (likely(found_it != range.end() &&
            memequal16(getIPv6FromOffset(*ipv6_col, *found_it), target.addr) &&
            mask_column[*found_it] == mask))
            set_value(i, vec[row_idx[*found_it]]);
        else
            set_value(i, default_value_extractor[i]);
    }
}

template <typename AttributeType, typename ValueSetter, typename DefaultValueExtractor>
void IPAddressDictionary::getItemsImpl(
    const Attribute & attribute,
    const Columns & key_columns,
    ValueSetter && set_value,
    DefaultValueExtractor & default_value_extractor) const
{
    const auto first_column = key_columns.front();
    const auto rows = first_column->size();

    // special case for getBlockInputStream
    if (unlikely(key_columns.size() == 2))
    {
        getItemsByTwoKeyColumnsImpl<AttributeType>(
            attribute, key_columns, std::forward<ValueSetter>(set_value), default_value_extractor);
        query_count.fetch_add(rows, std::memory_order_relaxed);
        return;
    }

    auto & vec = std::get<ContainerType<AttributeType>>(attribute.maps);

    size_t keys_found = 0;

    if (first_column->isNumeric())
    {
        uint8_t addrv6_buf[IPV6_BINARY_LENGTH];
        for (const auto i : collections::range(0, rows))
        {
            // addrv4 has native endianness
            auto addrv4 = UInt32(first_column->get64(i));
            auto found = tryLookupIPv4(addrv4, addrv6_buf);
            if (found != ipNotFound())
            {
                set_value(i, vec[*found]);
                ++keys_found;
            }
            else
                set_value(i, default_value_extractor[i]);
        }
    }
    else
    {
        for (const auto i : collections::range(0, rows))
        {
            auto addr = first_column->getDataAt(i);
            if (addr.size != IPV6_BINARY_LENGTH)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected key to be FixedString(16)");

            auto found = tryLookupIPv6(reinterpret_cast<const uint8_t *>(addr.data));
            if (found != ipNotFound())
            {
                set_value(i, vec[*found]);
                ++keys_found;
            }
            else
                set_value(i, default_value_extractor[i]);
        }
    }

    query_count.fetch_add(rows, std::memory_order_relaxed);
    found_count.fetch_add(keys_found, std::memory_order_relaxed);
}

template <typename T>
void IPAddressDictionary::setAttributeValueImpl(Attribute & attribute, const T value)
{
    auto & vec = std::get<ContainerType<T>>(attribute.maps);
    vec.push_back(value);
}

void IPAddressDictionary::setAttributeValue(Attribute & attribute, const Field & value)
{
    auto type_call = [&](const auto & dictionary_attribute_type)
    {
        using Type = std::decay_t<decltype(dictionary_attribute_type)>;
        using AttributeType = typename Type::AttributeType;

        if constexpr (std::is_same_v<AttributeType, String>)
        {
            const auto & string = value.get<String>();
            const auto * string_in_arena = attribute.string_arena->insert(string.data(), string.size());
            setAttributeValueImpl<StringRef>(attribute, StringRef{string_in_arena, string.size()});
        }
        else
        {
            setAttributeValueImpl<AttributeType>(attribute, value.get<AttributeType>());
        }
    };

    callOnDictionaryAttributeType(attribute.type, type_call);
}

const IPAddressDictionary::Attribute & IPAddressDictionary::getAttribute(const std::string & attribute_name) const
{
    const auto it = attribute_index_by_name.find(attribute_name);
    if (it == std::end(attribute_index_by_name))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}: no such attribute '{}'", getFullName(), attribute_name);

    return attributes[it->second];
}

Columns IPAddressDictionary::getKeyColumns() const
{
    const auto * ipv4_col = std::get_if<IPv4Container>(&ip_column);
    if (ipv4_col)
    {
        auto key_ip_column = ColumnVector<UInt32>::create();
        auto key_mask_column = ColumnVector<UInt8>::create();
        for (size_t row : collections::range(0, row_idx.size()))
        {
            key_ip_column->insertValue((*ipv4_col)[row]);
            key_mask_column->insertValue(mask_column[row]);
        }
        return {std::move(key_ip_column), std::move(key_mask_column)};
    }

    const auto * ipv6_col = std::get_if<IPv6Container>(&ip_column);

    auto key_ip_column = ColumnFixedString::create(IPV6_BINARY_LENGTH);
    auto key_mask_column = ColumnVector<UInt8>::create();

    for (size_t row : collections::range(0, row_idx.size()))
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
    return [](const Columns & columns, const std::vector<DictionaryAttribute> & dictonary_key_attributes)
    {
        auto column = ColumnString::create();
        const auto & key_ip_column = assert_cast<const KeyColumnType &>(*columns.front());
        const auto & key_mask_column = assert_cast<const ColumnVector<UInt8> &>(*columns.back());
        char buffer[48];
        for (size_t row : collections::range(0, key_ip_column.size()))
        {
            UInt8 mask = key_mask_column.getElement(row);
            size_t str_len;
            if constexpr (IsIPv4)
                str_len = formatIPWithPrefix(reinterpret_cast<const unsigned char *>(&key_ip_column.getElement(row)), mask, true, buffer);
            else
                str_len = formatIPWithPrefix(reinterpret_cast<const unsigned char *>(key_ip_column.getDataAt(row).data), mask, false, buffer);
            column->insertData(buffer, str_len);
        }
        return ColumnsWithTypeAndName{
            ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeString>(), dictonary_key_attributes.front().name)};
    };
}

Pipe IPAddressDictionary::read(const Names & column_names, size_t max_block_size, size_t num_streams) const
{
    const bool is_ipv4 = std::get_if<IPv4Container>(&ip_column) != nullptr;

    auto key_columns = getKeyColumns();

    std::shared_ptr<const IDataType> key_type;
    if (is_ipv4)
        key_type = std::make_shared<DataTypeUInt32>();
    else
        key_type = std::make_shared<DataTypeFixedString>(IPV6_BINARY_LENGTH);

    ColumnsWithTypeAndName key_columns_with_type = {
        ColumnWithTypeAndName(key_columns.front(), key_type, ""),
        ColumnWithTypeAndName(key_columns.back(), std::make_shared<DataTypeUInt8>(), "")
    };

    ColumnsWithTypeAndName view_columns;

    if (is_ipv4)
    {
        auto get_view = keyViewGetter<ColumnVector<UInt32>, true>();
        view_columns = get_view(key_columns, *dict_struct.key);
    }
    else
    {
        auto get_view = keyViewGetter<ColumnFixedString, false>();
        view_columns = get_view(key_columns, *dict_struct.key);
    }

    std::shared_ptr<const IDictionary> dictionary = shared_from_this();
    auto coordinator = DictionarySourceCoordinator::create(dictionary, column_names, std::move(key_columns_with_type), std::move(view_columns), max_block_size);
    auto result = coordinator->read(num_streams);

    return result;
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

    auto range = collections::range(0, row_idx.size());
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
                             DictionarySourcePtr source_ptr,
                             ContextPtr /* global_context */,
                             bool /*created_from_ddl*/) -> DictionaryPtr
    {
        if (!dict_struct.key || dict_struct.key->size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Dictionary of layout 'ip_trie' has to have one 'key'");

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);
        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
        const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);

        // This is specialised dictionary for storing IPv4 and IPv6 prefixes.
        return std::make_unique<IPAddressDictionary>(dict_id, dict_struct, std::move(source_ptr), dict_lifetime, require_nonempty);
    };
    factory.registerLayout("ip_trie", create_layout, true);
}

}
