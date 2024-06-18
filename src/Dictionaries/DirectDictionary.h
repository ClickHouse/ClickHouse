#pragma once

#include <atomic>
#include <variant>
#include <vector>

#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionary.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryHelpers.h>


namespace DB
{

template <DictionaryKeyType dictionary_key_type>
class DirectDictionary final : public IDictionary
{
public:
    using KeyType = std::conditional_t<dictionary_key_type == DictionaryKeyType::Simple, UInt64, StringRef>;

    DirectDictionary(
        const StorageID & dict_id_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_);

    std::string getTypeName() const override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
            return "Direct";
        else
            return "ComplexKeyDirect";
    }

    size_t getBytesAllocated() const override { return 0; }

    size_t getQueryCount() const override { return query_count.load(); }

    double getFoundRate() const override
    {
        size_t queries = query_count.load();
        if (!queries)
            return 0;
        return std::min(1.0, static_cast<double>(found_count.load()) / queries);
    }

    double getHitRate() const override { return 1.0; }

    size_t getElementCount() const override { return 0; }

    double getLoadFactor() const override { return 0; }

    std::shared_ptr<IExternalLoadable> clone() const override
    {
        return std::make_shared<DirectDictionary>(getDictionaryID(), dict_struct, source_ptr->clone());
    }

    DictionarySourcePtr getSource() const override { return source_ptr; }

    const DictionaryLifetime & getLifetime() const override { return dict_lifetime; }

    const DictionaryStructure & getStructure() const override { return dict_struct; }

    bool isInjective(const std::string & attribute_name) const override
    {
        return dict_struct.getAttribute(attribute_name).injective;
    }

    DictionaryKeyType getKeyType() const override { return dictionary_key_type; }

    Columns getColumns(
        const Strings & attribute_names,
        const DataTypes & attribute_types,
        const Columns & key_columns,
        const DataTypes & key_types,
        DefaultsOrFilter defaults_or_filter) const override;

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

    Pipe read(const Names & column_names, size_t max_block_size, size_t num_streams) const override;

    void applySettings(const Settings & settings);

private:
    Pipe getSourcePipe(const Columns & key_columns, const PaddedPODArray<KeyType> & requested_keys) const;

    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;

    bool use_async_executor = false;

    mutable std::atomic<size_t> query_count{0};
    mutable std::atomic<size_t> found_count{0};
};

extern template class DirectDictionary<DictionaryKeyType::Simple>;
extern template class DirectDictionary<DictionaryKeyType::Complex>;

}
