#pragma once

#include <atomic>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <variant>

#include <base/types.h>

#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashSet.h>

#include <DataTypes/IDataType.h>

#include <Columns/IColumn.h>

#include <QueryPipeline/Pipe.h>

#include <Core/Block.h>

#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionary.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int UNSUPPORTED_METHOD;
}

class RegExpTreeDictionary : public IDictionary
{
public:
    struct Configuration
    {
        bool require_nonempty;
        DictionaryLifetime lifetime;
    };

    const std::string name = "RegExpTree";

    RegExpTreeDictionary(
        const StorageID & id_, const DictionaryStructure & structure_, DictionarySourcePtr source_ptr_, Configuration configuration_);

    std::string getTypeName() const override { return name; }

    size_t getBytesAllocated() const override { return bytes_allocated; }

    size_t getQueryCount() const override { return query_count.load(std::memory_order_relaxed); }

    double getFoundRate() const override
    {
        const auto queries = query_count.load(std::memory_order_relaxed);
        if (!queries)
            return 0;
        return static_cast<double>(found_count.load(std::memory_order_relaxed)) / queries;
    }

    double getHitRate() const override { return 1.0; }

    size_t getElementCount() const override { return element_count; }

    double getLoadFactor() const override { return static_cast<double>(element_count) / bucket_count; }

    DictionarySourcePtr getSource() const override { return source_ptr; }

    const DictionaryLifetime & getLifetime() const override { return configuration.lifetime; }

    const DictionaryStructure & getStructure() const override { return structure; }

    bool isInjective(const std::string & attribute_name) const override { return structure.getAttribute(attribute_name).injective; }

    DictionaryKeyType getKeyType() const override { return DictionaryKeyType::Simple; }

    bool hasHierarchy() const override { return false; }

    std::shared_ptr<const IExternalLoadable> clone() const override
    {
        return std::make_shared<RegExpTreeDictionary>(getDictionaryID(), structure, source_ptr->clone(), configuration);
    }

    ColumnUInt8::Ptr hasKeys(const Columns &, const DataTypes &) const override
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Dictionary {} does not support method `hasKeys`", name);
    }

    Pipe read(const Names &, size_t, size_t) const override
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Dictionary {} does not support method `read`", name);
    }

    ColumnPtr getColumn(
        const std::string & attribute_name,
        const DataTypePtr & result_type,
        const Columns & key_columns,
        const DataTypes & key_types,
        const ColumnPtr & default_values_column) const override;

private:
    const DictionaryStructure structure;
    const DictionarySourcePtr source_ptr;
    const Configuration configuration;

    size_t bytes_allocated = 0;

    size_t bucket_count = 0;
    size_t element_count = 0;

    mutable std::atomic<size_t> query_count{0};
    mutable std::atomic<size_t> found_count{0};

    //////////////////////////////////////////////////////////////////////

    template <typename Value>
    using ContainerType = std::conditional_t<std::is_same_v<Value, Array>, std::vector<Value>, PaddedPODArray<Value>>;

    using NullableSet = HashSet<UInt64, DefaultHash<UInt64>>;

    struct Attribute final
    {
        AttributeUnderlyingType type;
        std::unordered_set<UInt64> nullable_set;

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
            container;
    };

    std::unordered_map<std::string, Attribute> names_to_attributes;
    std::unordered_map<UInt64, UInt64> keys_to_ids;

    std::vector<std::string> regexps;
    std::unordered_map<UInt64, UInt64> ids_to_parent_ids;
    std::unordered_map<UInt64, UInt64> ids_to_child_ids;

    Arena string_arena;

    //////////////////////////////////////////////////////////////////////

    void createAttributes();

    void resizeAttributes(size_t size);

    void calculateBytesAllocated();

    void setRegexps(const Block & block);

    void setIdToParentId(const Block & block);

    void setAttributeValue(Attribute & attribute, UInt64 key, const Field & value);

    void blockToAttributes(const Block & block);

    void loadData();

    std::unordered_set<UInt64> matchSearchAllIndices(const std::string & key) const;

    template <typename AttributeType, typename SetValueFunc, typename DefaultValueExtractor>
    void getColumnImpl(
        const Attribute & attribute,
        std::optional<UInt64> match_index,
        bool is_nullable,
        SetValueFunc && set_value_func,
        DefaultValueExtractor & default_value_extractor) const;

    UInt64 getRoot(const std::unordered_set<UInt64> & indices) const;

    std::optional<UInt64> getLastMatchIndex(std::unordered_set<UInt64> matches, const Attribute & attribute) const;
};

}
