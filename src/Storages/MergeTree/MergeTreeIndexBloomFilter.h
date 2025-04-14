#pragma once

#include <Columns/IColumn.h>
#include <Common/HashTable/HashSet.h>
#include <Interpreters/BloomFilter.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class MergeTreeIndexGranuleBloomFilter final : public IMergeTreeIndexGranule
{
public:
    MergeTreeIndexGranuleBloomFilter(size_t bits_per_row_, size_t hash_functions_, size_t index_columns_);

    MergeTreeIndexGranuleBloomFilter(size_t bits_per_row_, size_t hash_functions_, const std::vector<HashSet<UInt64>> & column_hashes);

    bool empty() const override;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    const std::vector<BloomFilterPtr> & getFilters() const { return bloom_filters; }

private:
    const size_t bits_per_row;
    const size_t hash_functions;

    size_t total_rows = 0;
    std::vector<BloomFilterPtr> bloom_filters;

    void fillingBloomFilter(BloomFilterPtr & bf, const HashSet<UInt64> & hashes) const;
};

class MergeTreeIndexConditionBloomFilter final : public IMergeTreeIndexCondition, WithContext
{
public:
    struct RPNElement
    {
        enum Function
        {
            /// Atoms of a Boolean expression.
            FUNCTION_EQUALS,
            FUNCTION_NOT_EQUALS,
            FUNCTION_HAS,
            FUNCTION_HAS_ANY,
            FUNCTION_HAS_ALL,
            FUNCTION_IN,
            FUNCTION_NOT_IN,
            FUNCTION_UNKNOWN, /// Can take any value.
            /// Operators of the logical expression.
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
            /// Constants
            ALWAYS_FALSE,
            ALWAYS_TRUE,
        };

        RPNElement(Function function_ = FUNCTION_UNKNOWN) : function(function_) {} /// NOLINT

        Function function = FUNCTION_UNKNOWN;
        std::vector<std::pair<size_t, ColumnPtr>> predicate;
    };

    MergeTreeIndexConditionBloomFilter(const ActionsDAG * filter_actions_dag, ContextPtr context_, const Block & header_, size_t hash_functions_);

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr granule) const override
    {
        if (const auto & bf_granule = typeid_cast<const MergeTreeIndexGranuleBloomFilter *>(granule.get()))
            return mayBeTrueOnGranule(bf_granule);

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Requires bloom filter index granule.");
    }

private:
    const Block & header;
    const size_t hash_functions;
    std::vector<RPNElement> rpn;

    bool mayBeTrueOnGranule(const MergeTreeIndexGranuleBloomFilter * granule) const;

    bool extractAtomFromTree(const RPNBuilderTreeNode & node, RPNElement & out);

    bool traverseFunction(const RPNBuilderTreeNode & node, RPNElement & out, const RPNBuilderTreeNode * parent);

    bool traverseTreeIn(
        const String & function_name,
        const RPNBuilderTreeNode & key_node,
        const ConstSetPtr & prepared_set,
        const DataTypePtr & type,
        const ColumnPtr & column,
        RPNElement & out);

    bool traverseTreeEquals(
        const String & function_name,
        const RPNBuilderTreeNode & key_node,
        const DataTypePtr & value_type,
        const Field & value_field,
        RPNElement & out,
        const RPNBuilderTreeNode * parent);
};

class MergeTreeIndexAggregatorBloomFilter final : public IMergeTreeIndexAggregator
{
public:
    MergeTreeIndexAggregatorBloomFilter(size_t bits_per_row_, size_t hash_functions_, const Names & columns_name_);

    bool empty() const override;

    MergeTreeIndexGranulePtr getGranuleAndReset() override;

    void update(const Block & block, size_t * pos, size_t limit) override;

private:
    size_t bits_per_row;
    size_t hash_functions;
    const Names index_columns_name;

    std::vector<HashSet<UInt64>> column_hashes;
    size_t total_rows = 0;
};


class MergeTreeIndexBloomFilter final : public IMergeTreeIndex
{
public:
    MergeTreeIndexBloomFilter(
        const IndexDescription & index_,
        size_t bits_per_row_,
        size_t hash_functions_);

    MergeTreeIndexGranulePtr createIndexGranule() const override;

    MergeTreeIndexAggregatorPtr createIndexAggregator(const MergeTreeWriterSettings & settings) const override;

    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG * filter_actions_dag, ContextPtr context) const override;

private:
    size_t bits_per_row;
    size_t hash_functions;
};

}
