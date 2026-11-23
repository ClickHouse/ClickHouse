#pragma once

#include <atomic>
#include <variant>
#include <Common/VectorWithMemoryTracking.h>
#include <optional>

#include <Common/HashTable/HashSet.h>
#include <Common/Arena.h>
#include <QueryPipeline/Pipe.h>
#include <DataTypes/IDataType.h>
#include <Core/Block_fwd.h>

#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionary.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryHelpers.h>

namespace DB
{

class FlatDictionary final : public IDictionary
{
public:
    struct Configuration
    {
        size_t initial_array_size;
        size_t max_array_size;
        bool require_nonempty;
        DictionaryLifetime dict_lifetime;
        bool use_async_executor = false;
    };

    FlatDictionary(
        const StorageID & dict_id_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_,
        Configuration configuration_,
        BlockPtr update_field_loaded_block_ = nullptr);

    std::string getTypeName() const override { return "Flat"; }

    size_t getBytesAllocated() const override { return bytes_allocated; }

    size_t getQueryCount() const override { return query_count.load(); }

    double getFoundRate() const override
    {
        size_t queries = query_count.load();
        if (!queries)
            return 0;
        return std::min(1.0, static_cast<double>(found_count.load()) / static_cast<double>(queries));
    }

    double getHitRate() const override { return 1.0; }

    size_t getElementCount() const override { return element_count; }

    double getLoadFactor() const override { return static_cast<double>(element_count) / static_cast<double>(bucket_count); }

    std::shared_ptr<IExternalLoadable> clone() const override
    {
        return std::make_shared<FlatDictionary>(getDictionaryID(), dict_struct, source_ptr->clone(), configuration, update_field_loaded_block);
    }

    DictionarySourcePtr getSource() const override { return source_ptr; }

    const DictionaryLifetime & getLifetime() const override { return configuration.dict_lifetime; }

    const DictionaryStructure & getStructure() const override { return dict_struct; }

    bool isInjective(const std::string & attribute_name) const override
    {
        return dict_struct.getAttribute(attribute_name).injective;
    }

    DictionaryKeyType getKeyType() const override { return DictionaryKeyType::Simple; }

    ColumnPtr getColumn(
        const std::string & attribute_name,
        const DataTypePtr & attribute_type,
        const Columns & key_columns,
        const DataTypes & key_types,
        DefaultOrFilter default_or_filter) const override;

    ColumnUInt8::Ptr hasKeys(const Columns & key_columns, const DataTypes & key_types) const override;

    bool hasHierarchy() const override { return dict_struct.hierarchical_attribute_index.has_value(); }

    ColumnPtr getHierarchy(ColumnPtr key_column, const DataTypePtr & key_type) const override;

    ColumnUInt8::Ptr isInHierarchy(
        ColumnPtr key_column,
        ColumnPtr in_key_column,
        const DataTypePtr & key_type) const override;

    DictionaryHierarchicalParentToChildIndexPtr getHierarchicalIndex() const override;

    size_t getHierarchicalIndexBytesAllocated() const override { return hierarchical_index_bytes_allocated; }

    ColumnPtr getDescendants(
        ColumnPtr key_column,
        const DataTypePtr & key_type,
        size_t level,
        DictionaryHierarchicalParentToChildIndexPtr parent_to_child_index) const override;

    Pipe read(const Names & column_names, size_t max_block_size, size_t num_streams) const override;

private:
    /// A key may be loaded several times; each load appends a row and repoints the key.
    template <typename Value>
    struct ColumnAttributeKeyToRow
    {
        explicit ColumnAttributeKeyToRow(const DictionaryAttribute & dictionary_attribute) : values(dictionary_attribute) {}

        [[nodiscard]] size_t size() const { return key_to_row.size(); }
        [[nodiscard]] size_t capacity() const { return key_to_row.capacity(); }
        ColumnAttributeValue<Value> operator[](size_t key) const { return values[key_to_row[key]]; }

        void insert(UInt64 key, const Field & value)
        {
            UInt64 row = key_to_row[key];
            if (row < row_to_key.size() && row_to_key[row] == key)
                ++replaced_rows;

            values.insert(value);
            key_to_row[key] = row_to_key.size();
            row_to_key.push_back(key);
        }

        /// Removes the rows of replaced values once they are the majority, so loading keeps at most twice the live rows.
        /// When loading is finished, removes all of them and drops `row_to_key`, which is only needed for loading.
        void removeReplacedRows(bool loading_finished)
        {
            if (replaced_rows != 0 && (loading_finished || replaced_rows * 2 > row_to_key.size()))
            {
                IColumn::Filter mask(row_to_key.size());
                size_t kept_rows = 0;
                for (size_t row = 0; row < row_to_key.size(); ++row)
                {
                    UInt64 key = row_to_key[row];
                    mask[row] = key_to_row[key] == row;
                    if (mask[row])
                    {
                        key_to_row[key] = kept_rows;
                        row_to_key[kept_rows] = key;
                        ++kept_rows;
                    }
                }

                row_to_key.resize(kept_rows);
                values.filter(mask, kept_rows);
                replaced_rows = 0;
            }

            if (loading_finished)
                row_to_key = {};
        }

        ColumnAttributeContainer<Value> values;
        PaddedPODArray<UInt64> key_to_row;
        PaddedPODArray<UInt64> row_to_key;
        size_t replaced_rows = 0;
    };

    template <typename Value>
    using ContainerType = std::conditional_t<is_column_attribute_value<Value>, ColumnAttributeKeyToRow<Value>, PaddedPODArray<Value>>;

    using NullableSet = HashSet<UInt64, DefaultHash<UInt64>, HashTableGrower<>>;

    struct Attribute final
    {
        std::optional<NullableSet> is_nullable_set;

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
            ContainerType<IPv4>,
            ContainerType<IPv6>,
            ContainerType<std::string_view>,
            ContainerType<Array>,
            ContainerType<Map>,
            ContainerType<Object>>
            container;

        AttributeUnderlyingType type;
    };

    void createAttributes();

    void blockToAttributes(const Block & block);

    void removeReplacedRows(bool loading_finished);

    void updateData();

    void loadData();

    void buildHierarchyParentToChildIndexIfNeeded();

    void calculateBytesAllocated();

    Attribute createAttribute(const DictionaryAttribute & attribute);

    template <typename AttributeType, bool is_nullable, typename ValueSetter, typename DefaultValueExtractor>
    void getItemsImpl(
        const Attribute & attribute,
        const PaddedPODArray<UInt64> & keys,
        ValueSetter && set_value,
        DefaultValueExtractor & default_value_extractor) const;

    template <typename AttributeType, bool is_nullable, typename ValueSetter>
    void getItemsShortCircuitImpl(
        const Attribute & attribute, const PaddedPODArray<UInt64> & keys, ValueSetter && set_value, IColumn::Filter & default_mask) const;

    template <typename T>
    void resize(Attribute & attribute, UInt64 key);

    void setAttributeValue(Attribute & attribute, UInt64 key, const Field & value);

    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const Configuration configuration;

    VectorWithMemoryTracking<Attribute> attributes;
    VectorWithMemoryTracking<bool> loaded_keys;

    size_t bytes_allocated = 0;
    size_t hierarchical_index_bytes_allocated = 0;
    size_t element_count = 0;
    size_t bucket_count = 0;
    mutable std::atomic<size_t> query_count{0};
    mutable std::atomic<size_t> found_count{0};

    BlockPtr update_field_loaded_block;
    Arena string_arena;
    DictionaryHierarchicalParentToChildIndexPtr hierarhical_index;
};

}
