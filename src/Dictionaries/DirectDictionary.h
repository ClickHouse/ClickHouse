#pragma once

#include <atomic>
#include <variant>
#include <vector>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Common/Arena.h>
#include <Core/Block.h>
#include <ext/range.h>
#include <ext/size.h>
#include <Common/HashTable/HashMap.h>
#include "DictionaryStructure.h"
#include "IDictionary.h"
#include "IDictionarySource.h"
#include "DictionaryHelpers.h"

namespace DB
{

template <DictionaryKeyType dictionary_key_type>
class DirectDictionary final : public IDictionary
{
public:
    static_assert(dictionary_key_type != DictionaryKeyType::range, "Range key type is not supported by direct dictionary");
    using KeyType = std::conditional_t<dictionary_key_type == DictionaryKeyType::simple, UInt64, StringRef>;

    DirectDictionary(
        const StorageID & dict_id_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_);

    std::string getTypeName() const override
    {
        if constexpr (dictionary_key_type == DictionaryKeyType::simple)
            return "Direct";
        else
            return "ComplexKeyDirect";
    }

    size_t getBytesAllocated() const override { return 0; }

    size_t getQueryCount() const override { return query_count.load(std::memory_order_relaxed); }

    double getHitRate() const override { return 1.0; }

    size_t getElementCount() const override { return 0; }

    double getLoadFactor() const override { return 0; }

    std::shared_ptr<const IExternalLoadable> clone() const override
    {
        return std::make_shared<DirectDictionary>(getDictionaryID(), dict_struct, source_ptr->clone());
    }

    const IDictionarySource * getSource() const override { return source_ptr.get(); }

    const DictionaryLifetime & getLifetime() const override { return dict_lifetime; }

    const DictionaryStructure & getStructure() const override { return dict_struct; }

    bool isInjective(const std::string & attribute_name) const override
    {
        return dict_struct.getAttribute(attribute_name).injective;
    }

    DictionaryKeyType getKeyType() const override { return dictionary_key_type; }

    Columns getColumns(
        const Strings & attribute_names,
        const DataTypes & result_types,
        const Columns & key_columns,
        const DataTypes & key_types,
        const Columns & default_values_columns) const override;

    ColumnPtr getColumn(
        const std::string& attribute_name,
        const DataTypePtr & result_type,
        const Columns & key_columns,
        const DataTypes & key_types,
        const ColumnPtr & default_values_column) const override;

    ColumnUInt8::Ptr hasKeys(const Columns & key_columns, const DataTypes & key_types) const override;

    bool hasHierarchy() const override { return dict_struct.hierarchical_attribute_index.has_value(); }

    ColumnPtr getHierarchy(ColumnPtr key_column, const DataTypePtr & key_type) const override;

    ColumnUInt8::Ptr isInHierarchy(
        ColumnPtr key_column,
        ColumnPtr in_key_column,
        const DataTypePtr & key_type) const override;

    BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const override;

private:
    BlockInputStreamPtr getSourceBlockInputStream(const Columns & key_columns, const PaddedPODArray<KeyType> & requested_keys) const;

    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;

    mutable std::atomic<size_t> query_count{0};
};

extern template class DirectDictionary<DictionaryKeyType::simple>;
extern template class DirectDictionary<DictionaryKeyType::complex>;

}
