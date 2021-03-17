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

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

template <DictionaryKeyType dictionary_key_type>
class DirectDictionary final : public IDictionary
{
public:
    static_assert(dictionary_key_type != DictionaryKeyType::range, "Range key type is not supported by direct dictionary");
    using KeyType = std::conditional_t<dictionary_key_type == DictionaryKeyType::simple, UInt64, StringRef>;

    DirectDictionary(
        const StorageID & dict_id_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_,
        BlockPtr saved_block_ = nullptr);

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
        return std::make_shared<DirectDictionary>(getDictionaryID(), dict_struct, source_ptr->clone(), saved_block);
    }

    const IDictionarySource * getSource() const override { return source_ptr.get(); }

    const DictionaryLifetime & getLifetime() const override { return dict_lifetime; }

    const DictionaryStructure & getStructure() const override { return dict_struct; }

    bool isInjective(const std::string & attribute_name) const override
    {
        auto it = attribute_index_by_name.find(attribute_name);

        if (it == attribute_index_by_name.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "({}): no attribute with name ({}) in dictionary",
                full_name,
                attribute_name);

        return dict_struct.attributes[it->second].injective;
    }

    bool hasHierarchy() const override { return hierarchical_attribute; }

    void toParent(const PaddedPODArray<UInt64> & ids, PaddedPODArray<UInt64> & out) const override;

    void isInVectorVector(
        const PaddedPODArray<UInt64> & child_ids, const PaddedPODArray<UInt64> & ancestor_ids, PaddedPODArray<UInt8> & out) const override;
    void isInVectorConstant(const PaddedPODArray<UInt64> & child_ids, const UInt64 ancestor_id, PaddedPODArray<UInt8> & out) const override;
    void isInConstantVector(const UInt64 child_id, const PaddedPODArray<UInt64> & ancestor_ids, PaddedPODArray<UInt8> & out) const override;

    DictionaryKeyType getKeyType() const override { return dictionary_key_type; }

    ColumnPtr getColumn(
        const std::string& attribute_name,
        const DataTypePtr & result_type,
        const Columns & key_columns,
        const DataTypes & key_types,
        const ColumnPtr & default_values_column) const override;

    ColumnUInt8::Ptr hasKeys(const Columns & key_columns, const DataTypes & key_types) const override;

    BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const override;

private:
    void setup();

    BlockInputStreamPtr getSourceBlockInputStream(const Columns & key_columns, const PaddedPODArray<KeyType> & requested_keys) const;

    UInt64 getValueOrNullByKey(const UInt64 & to_find) const;

    template <typename ChildType, typename AncestorType>
    void isInImpl(const ChildType & child_ids, const AncestorType & ancestor_ids, PaddedPODArray<UInt8> & out) const;

    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;

    std::unordered_map<std::string, size_t> attribute_index_by_name;
    std::unordered_map<size_t, std::string> attribute_name_by_index;

    const DictionaryAttribute * hierarchical_attribute = nullptr;

    mutable std::atomic<size_t> query_count{0};

    BlockPtr saved_block;
};

extern template class DirectDictionary<DictionaryKeyType::simple>;
extern template class DirectDictionary<DictionaryKeyType::complex>;

}
