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
#include <common/StringRef.h>
#include <common/logger_useful.h>
#include <ext/range.h>
#include "DictionaryStructure.h"
#include "IDictionary.h"
#include "IDictionarySource.h"

namespace DB
{
class IPAddressDictionary final : public IDictionaryBase
{
public:
    IPAddressDictionary(
        const StorageID & dict_id_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_,
        const DictionaryLifetime dict_lifetime_,
        bool require_nonempty_);

    std::string getKeyDescription() const { return key_description; }

    std::string getTypeName() const override { return "Trie"; }

    size_t getBytesAllocated() const override { return bytes_allocated; }

    size_t getQueryCount() const override { return query_count.load(std::memory_order_relaxed); }

    double getHitRate() const override { return 1.0; }

    size_t getElementCount() const override { return element_count; }

    double getLoadFactor() const override { return static_cast<double>(element_count) / bucket_count; }

    std::shared_ptr<const IExternalLoadable> clone() const override
    {
        return std::make_shared<IPAddressDictionary>(getDictionaryID(), dict_struct, source_ptr->clone(), dict_lifetime, require_nonempty);
    }

    const IDictionarySource * getSource() const override { return source_ptr.get(); }

    const DictionaryLifetime & getLifetime() const override { return dict_lifetime; }

    const DictionaryStructure & getStructure() const override { return dict_struct; }

    bool isInjective(const std::string & attribute_name) const override
    {
        return dict_struct.attributes[&getAttribute(attribute_name) - attributes.data()].injective;
    }

    template <typename T>
    using ResultArrayType = std::conditional_t<IsDecimalNumber<T>, DecimalPaddedPODArray<T>, PaddedPODArray<T>>;

#define DECLARE(TYPE) \
    void get##TYPE( \
        const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types, ResultArrayType<TYPE> & out) const;
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

    void getString(const std::string & attribute_name, const Columns & key_columns, const DataTypes & key_types, ColumnString * out) const;

#define DECLARE(TYPE) \
    void get##TYPE( \
        const std::string & attribute_name, \
        const Columns & key_columns, \
        const DataTypes & key_types, \
        const PaddedPODArray<TYPE> & def, \
        ResultArrayType<TYPE> & out) const;
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

    void getString(
        const std::string & attribute_name,
        const Columns & key_columns,
        const DataTypes & key_types,
        const ColumnString * const def,
        ColumnString * const out) const;

#define DECLARE(TYPE) \
    void get##TYPE( \
        const std::string & attribute_name, \
        const Columns & key_columns, \
        const DataTypes & key_types, \
        const TYPE def, \
        ResultArrayType<TYPE> & out) const;
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

    void getString(
        const std::string & attribute_name,
        const Columns & key_columns,
        const DataTypes & key_types,
        const String & def,
        ColumnString * const out) const;

    void has(const Columns & key_columns, const DataTypes & key_types, PaddedPODArray<UInt8> & out) const;

    BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const override;

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
            Int8,
            Int16,
            Int32,
            Int64,
            Decimal32,
            Decimal64,
            Decimal128,
            Float32,
            Float64,
            String>
            null_values;
        std::variant<
            ContainerType<UInt8>,
            ContainerType<UInt16>,
            ContainerType<UInt32>,
            ContainerType<UInt64>,
            ContainerType<UInt128>,
            ContainerType<Int8>,
            ContainerType<Int16>,
            ContainerType<Int32>,
            ContainerType<Int64>,
            ContainerType<Decimal32>,
            ContainerType<Decimal64>,
            ContainerType<Decimal128>,
            ContainerType<Float32>,
            ContainerType<Float64>,
            ContainerType<StringRef>>
            maps;
        std::unique_ptr<Arena> string_arena;
    };

    void createAttributes();

    void loadData();

    template <typename T>
    void addAttributeSize(const Attribute & attribute);

    void calculateBytesAllocated();

    template <typename T>
    void createAttributeImpl(Attribute & attribute, const Field & null_value);

    Attribute createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value);

    template <typename AttributeType, typename OutputType, typename ValueSetter, typename DefaultGetter>
    void getItemsByTwoKeyColumnsImpl(
        const Attribute & attribute, const Columns & key_columns, ValueSetter && set_value, DefaultGetter && get_default) const;

    template <typename AttributeType, typename OutputType, typename ValueSetter, typename DefaultGetter>
    void
    getItemsImpl(const Attribute & attribute, const Columns & key_columns, ValueSetter && set_value, DefaultGetter && get_default) const;

    template <typename T>
    void setAttributeValueImpl(Attribute & attribute, const T value);

    void setAttributeValue(Attribute & attribute, const Field & value);

    const Attribute & getAttribute(const std::string & attribute_name) const;

    template <typename T>
    void has(const Attribute & attribute, const Columns & key_columns, PaddedPODArray<UInt8> & out) const;

    Columns getKeyColumns() const;
    RowIdxConstIter ipNotFound() const;
    RowIdxConstIter tryLookupIPv4(UInt32 addr, uint8_t * buf) const;
    RowIdxConstIter tryLookupIPv6(const uint8_t * addr) const;

    template <typename IPContainerType, typename IPValueType>
    RowIdxConstIter lookupIP(IPValueType target) const;

    static const uint8_t * getIPv6FromOffset(const IPv6Container & ipv6_col, size_t i);

    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;
    const bool require_nonempty;
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

    Poco::Logger * logger;
};

}
