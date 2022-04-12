#pragma once

#include <atomic>
#include <memory>
#include <variant>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnVector.h>
#include <Poco/Net/IPAddress.h>
#include <base/StringRef.h>
#include <base/logger_useful.h>
#include "DictionaryStructure.h"
#include "IDictionary.h"
#include "IDictionarySource.h"
#include "DictionaryHelpers.h"

namespace DB
{
class IPAddressDictionary final : public IDictionary
{
public:
    IPAddressDictionary(
        const StorageID & dict_id_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_,
        const DictionaryLifetime dict_lifetime_, /// NOLINT
        bool require_nonempty_);

    std::string getKeyDescription() const { return key_description; }

    std::string getTypeName() const override { return "Trie"; }

    size_t getBytesAllocated() const override { return bytes_allocated; }

    size_t getQueryCount() const override { return query_count.load(std::memory_order_relaxed); }

    double getFoundRate() const override
    {
        size_t queries = query_count.load(std::memory_order_relaxed);
        if (!queries)
            return 0;
        return static_cast<double>(found_count.load(std::memory_order_relaxed)) / queries;
    }

    double getHitRate() const override { return 1.0; }

    size_t getElementCount() const override { return element_count; }

    double getLoadFactor() const override { return static_cast<double>(element_count) / bucket_count; }

    std::shared_ptr<const IExternalLoadable> clone() const override
    {
        return std::make_shared<IPAddressDictionary>(getDictionaryID(), dict_struct, source_ptr->clone(), dict_lifetime, require_nonempty);
    }

    DictionarySourcePtr getSource() const override { return source_ptr; }

    const DictionaryLifetime & getLifetime() const override { return dict_lifetime; }

    const DictionaryStructure & getStructure() const override { return dict_struct; }

    bool isInjective(const std::string & attribute_name) const override
    {
        return dict_struct.getAttribute(attribute_name).injective;
    }

    DictionaryKeyType getKeyType() const override { return DictionaryKeyType::Complex; }

    void convertKeyColumns(Columns & key_columns, DataTypes & key_types) const override;

    ColumnPtr getColumn(
        const std::string& attribute_name,
        const DataTypePtr & result_type,
        const Columns & key_columns,
        const DataTypes & key_types,
        const ColumnPtr & default_values_column) const override;

    ColumnUInt8::Ptr hasKeys(const Columns & key_columns, const DataTypes & key_types) const override;

    Pipe read(const Names & column_names, size_t max_block_size, size_t num_streams) const override;

private:

    template <typename Value>
    using ContainerType = std::vector<Value>;

    using IPAddress = Poco::Net::IPAddress;

    using IPv4Container = PODArray<UInt32>;
    using IPv6Container = PaddedPODArray<UInt8>;
    using IPMaskContainer = PODArray<UInt8>;
    using RowIdxConstIter = ContainerType<size_t>::const_iterator;

    struct Attribute final
    {
        AttributeUnderlyingType type;
        std::variant<
            UInt8,
            UInt16,
            UInt32,
            UInt64,
            UInt128,
            UInt256,
            Int8,
            Int16,
            Int32,
            Int64,
            Int128,
            Int256,
            Decimal32,
            Decimal64,
            Decimal128,
            Decimal256,
            DateTime64,
            Float32,
            Float64,
            UUID,
            String,
            Array>
            null_values;
        std::variant<
            ContainerType<UInt8>,
            ContainerType<UInt16>,
            ContainerType<UInt32>,
            ContainerType<UInt64>,
            ContainerType<UInt128>,
            ContainerType<UInt256>,
            ContainerType<Int8>,
            ContainerType<Int16>,
            ContainerType<Int32>,
            ContainerType<Int64>,
            ContainerType<Int128>,
            ContainerType<Int256>,
            ContainerType<Decimal32>,
            ContainerType<Decimal64>,
            ContainerType<Decimal128>,
            ContainerType<Decimal256>,
            ContainerType<DateTime64>,
            ContainerType<Float32>,
            ContainerType<Float64>,
            ContainerType<UUID>,
            ContainerType<StringRef>,
            ContainerType<Array>>
            maps;
        std::unique_ptr<Arena> string_arena;
    };

    void createAttributes();

    void loadData();

    template <typename T>
    void addAttributeSize(const Attribute & attribute);

    void calculateBytesAllocated();

    template <typename T>
    static void createAttributeImpl(Attribute & attribute, const Field & null_value);

    static Attribute createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value); /// NOLINT

    template <typename AttributeType, typename ValueSetter, typename DefaultValueExtractor>
    void getItemsByTwoKeyColumnsImpl(
        const Attribute & attribute,
        const Columns & key_columns,
        ValueSetter && set_value,
        DefaultValueExtractor & default_value_extractor) const;

    template <typename AttributeType,typename ValueSetter, typename DefaultValueExtractor>
    void getItemsImpl(
        const Attribute & attribute,
        const Columns & key_columns,
        ValueSetter && set_value,
        DefaultValueExtractor & default_value_extractor) const;

    template <typename T>
    void setAttributeValueImpl(Attribute & attribute, const T value); /// NOLINT

    void setAttributeValue(Attribute & attribute, const Field & value);

    const Attribute & getAttribute(const std::string & attribute_name) const;

    Columns getKeyColumns() const;
    RowIdxConstIter ipNotFound() const;
    RowIdxConstIter tryLookupIPv4(UInt32 addr, uint8_t * buf) const;
    RowIdxConstIter tryLookupIPv6(const uint8_t * addr) const;

    template <typename IPContainerType, typename IPValueType>
    RowIdxConstIter lookupIP(IPValueType target) const;

    static const uint8_t * getIPv6FromOffset(const IPv6Container & ipv6_col, size_t i);

    DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;
    const bool require_nonempty;
    const bool access_to_key_from_attributes;
    const std::string key_description{dict_struct.getKeyDescription()};

    /// Contains sorted IP subnetworks. If some addresses equals, subnet with lower mask is placed first.
    std::variant<IPv4Container, IPv6Container> ip_column;

    /// Prefix lengths corresponding to ip_column.
    IPMaskContainer mask_column;

    /** Contains links to parent subnetworks in ip_column.
      * Array holds such ip_column's (and mask_column's) indices that
      * - if parent_subnet[i] < i, then ip_column[i] is subnetwork of ip_column[parent_subnet[i]],
      * - if parent_subnet[i] == i, then ip_column[i] doesn't belong to any other subnet.
      */
    ContainerType<size_t> parent_subnet;

    /// Contains corresponding indices in attributes array.
    ContainerType<size_t> row_idx;

    std::map<std::string, size_t> attribute_index_by_name;
    std::vector<Attribute> attributes;

    size_t bytes_allocated = 0;
    size_t element_count = 0;
    size_t bucket_count = 0;
    mutable std::atomic<size_t> query_count{0};
    mutable std::atomic<size_t> found_count{0};

    Poco::Logger * logger;
};

}
