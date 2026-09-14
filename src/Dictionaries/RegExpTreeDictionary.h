#pragma once

#include <atomic>
#include <string>
#include <variant>

#include <base/types.h>

#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <Functions/Regexps.h>
#include <QueryPipeline/Pipe.h>
#include <Common/Exception.h>
#include <Common/SetWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>

#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionary.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

class RegExpTreeDictionary : public IDictionary
{
    friend struct MatchContext;
public:
    struct Configuration
    {
        bool require_nonempty;
        DictionaryLifetime lifetime;
        bool use_async_executor = false;
    };

    const std::string name = "RegExpTree";

    RegExpTreeDictionary(
        const StorageID & id_,
        const DictionaryStructure & structure_,
        DictionarySourcePtr source_ptr_,
        Configuration configuration_,
        bool use_vectorscan_,
        bool flag_case_insensitive_,
        bool flag_dotall_);

    std::string getTypeName() const override { return name; }

    size_t getBytesAllocated() const override { return bytes_allocated; }

    size_t getQueryCount() const override { return query_count.load(); }

    double getFoundRate() const override
    {
        const auto queries = query_count.load();
        if (!queries)
            return 0;
        return std::min(1.0, static_cast<double>(found_count.load()) / static_cast<double>(queries));
    }

    double getHitRate() const override { return 1.0; }

    size_t getElementCount() const override { return element_count; }

    double getLoadFactor() const override { return static_cast<double>(element_count) / static_cast<double>(bucket_count); }

    DictionarySourcePtr getSource() const override { return source_ptr; }

    const DictionaryLifetime & getLifetime() const override { return configuration.lifetime; }

    const DictionaryStructure & getStructure() const override { return structure; }

    bool isInjective(const std::string & attribute_name) const override { return structure.getAttribute(attribute_name).injective; }

    DictionaryKeyType getKeyType() const override { return DictionaryKeyType::Simple; }

    bool hasHierarchy() const override { return false; }

    std::shared_ptr<IExternalLoadable> clone() const override
    {
        return std::make_shared<RegExpTreeDictionary>(
            getDictionaryID(), structure, source_ptr->clone(), configuration, use_vectorscan, flag_case_insensitive, flag_dotall);
    }

    ColumnUInt8::Ptr hasKeys(const Columns &, const DataTypes &) const override
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Dictionary {} does not support method `hasKeys`", name);
    }

    Pipe read(const Names & columns, size_t max_block_size, size_t num_streams) const override;

    ColumnPtr getColumn(
        const std::string & attribute_name,
        const DataTypePtr & attribute_type,
        const Columns & key_columns,
        const DataTypes & key_types,
        DefaultOrFilter default_or_filter) const override
    {
        bool is_short_circuit = std::holds_alternative<RefFilter>(default_or_filter);
        assert(is_short_circuit || std::holds_alternative<RefDefault>(default_or_filter));

        if (is_short_circuit)
        {
            IColumn::Filter & default_mask = std::get<RefFilter>(default_or_filter).get();
            return getColumns({attribute_name}, {attribute_type}, key_columns, key_types, default_mask).front();
        }

        const ColumnPtr & default_values_column = std::get<RefDefault>(default_or_filter).get();
        const Columns & columns = Columns({default_values_column});
        return getColumns({attribute_name}, {attribute_type}, key_columns, key_types, columns).front();
    }

    Columns getColumns(
        const Strings & attribute_names,
        const DataTypes & attribute_types,
        const Columns & key_columns,
        const DataTypes & key_types,
        DefaultsOrFilter defaults_or_filter) const override
    {
        return getColumnsImpl(attribute_names, attribute_types, key_columns, key_types, defaults_or_filter, std::nullopt);
    }

    ColumnPtr getColumnAllValues(
        const std::string & attribute_name,
        const DataTypePtr & result_type,
        const Columns & key_columns,
        const DataTypes & key_types,
        const ColumnPtr & default_values_column,
        size_t limit) const override
    {
        return getColumnsAllValues(
            Strings({attribute_name}), DataTypes({result_type}), key_columns, key_types, Columns({default_values_column}), limit)[0];
    }

    Columns getColumnsAllValues(
        const Strings & attribute_names,
        const DataTypes & result_types,
        const Columns & key_columns,
        const DataTypes & key_types,
        const Columns & default_values_columns,
        size_t limit) const override
    {
        return getColumnsImpl(attribute_names, result_types, key_columns, key_types, default_values_columns, limit);
    }

    Columns getColumnsImpl(
        const Strings & attribute_names,
        const DataTypes & result_types,
        const Columns & key_columns,
        const DataTypes & key_types,
        DefaultsOrFilter defaults_or_filter,
        std::optional<size_t> collect_values_limit) const;

private:
    const DictionaryStructure structure;
    DictionarySourcePtr source_ptr;
    const Configuration configuration;

    size_t bytes_allocated = 0;

    size_t bucket_count = 0;
    size_t element_count = 0;

    mutable std::atomic<size_t> query_count{0};
    mutable std::atomic<size_t> found_count{0};

    void calculateBytesAllocated();

    void loadData();

    void initRegexNodes(Block & block);
    void initTopologyOrder(UInt64 node_idx, SetWithMemoryTracking<UInt64> & visited, UInt64 & topology_id);
    void initGraph();

    using RefDefaultMap = std::reference_wrapper<const UnorderedMapWithMemoryTracking<String, ColumnPtr>>;
    using DefaultMapOrFilter = std::variant<RefDefaultMap, RefFilter>;
    UnorderedMapWithMemoryTracking<String, ColumnPtr> match(
        const ColumnString::Chars & keys_data,
        const ColumnString::Offsets & keys_offsets,
        const UnorderedMapWithMemoryTracking<String, const DictionaryAttribute &> & attributes,
        DefaultMapOrFilter default_or_filter,
        std::optional<size_t> collect_values_limit) const;

    class AttributeCollector;

    bool setAttributes(
        UInt64 id,
        AttributeCollector & attributes_to_set,
        const String & data,
        UnorderedSetWithMemoryTracking<UInt64> & visited_nodes,
        const UnorderedMapWithMemoryTracking<String, const DictionaryAttribute &> & attributes,
        const UnorderedMapWithMemoryTracking<String, ColumnPtr> & defaults,
        size_t key_index) const;

    bool setAttributesShortCircuit(
        UInt64 id,
        AttributeCollector & attributes_to_set,
        const String & data,
        UnorderedSetWithMemoryTracking<UInt64> & visited_nodes,
        const UnorderedMapWithMemoryTracking<String, const DictionaryAttribute &> & attributes,
        UnorderedSetWithMemoryTracking<String> * defaults) const;

    struct RegexTreeNode;
    using RegexTreeNodePtr = std::shared_ptr<RegexTreeNode>;

    bool use_vectorscan;
    bool flag_case_insensitive;
    bool flag_dotall;

    VectorWithMemoryTracking<std::string> simple_regexps;
    VectorWithMemoryTracking<UInt64>      regexp_ids;
    VectorWithMemoryTracking<RegexTreeNodePtr> complex_regexp_nodes;

    MapWithMemoryTracking<UInt64, RegexTreeNodePtr> regex_nodes;
    UnorderedMapWithMemoryTracking<UInt64, UInt64> topology_order;
#if USE_VECTORSCAN
    MultiRegexps::DeferredConstructedRegexpsPtr hyperscan_regex;
    MultiRegexps::ScratchPtr origin_scratch;
    MultiRegexps::DataBasePtr origin_db;
    #endif

    LoggerPtr logger;
};

}
