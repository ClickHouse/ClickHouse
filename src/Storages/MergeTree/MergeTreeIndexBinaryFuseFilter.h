#pragma once

#include <Columns/IColumn_fwd.h>
#include <Common/HashTable/HashSet.h>
#include <Interpreters/BinaryFuseFilter.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

namespace DB
{

class Set;
using ConstSetPtr = std::shared_ptr<const Set>;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class MergeTreeIndexGranuleBinaryFuseFilter final : public IMergeTreeIndexGranule
{
public:
    MergeTreeIndexGranuleBinaryFuseFilter(double false_positive_rate_, size_t f_bits_, size_t index_columns_);

    MergeTreeIndexGranuleBinaryFuseFilter(double false_positive_rate_, size_t f_bits_, const std::vector<HashSet<UInt64>> & column_hashes);

    bool empty() const override;

    size_t memoryUsageBytes() const override;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    const std::vector<BinaryFuseFilterPtr> & getFilters() const { return binary_fuse_filters; }

private:
    const size_t f_bits;

    size_t total_rows = 0;
    std::vector<BinaryFuseFilterPtr> binary_fuse_filters;
};

class MergeTreeIndexConditionBinaryFuseFilter final : public IMergeTreeIndexCondition, WithContext
{
public:
    struct RPNElement
    {
        enum Function
        {
            FUNCTION_EQUALS,
            FUNCTION_NOT_EQUALS,
            FUNCTION_HAS,
            FUNCTION_HAS_ANY,
            FUNCTION_HAS_ALL,
            FUNCTION_IN,
            FUNCTION_NOT_IN,
            FUNCTION_UNKNOWN,
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
            ALWAYS_FALSE,
            ALWAYS_TRUE,
        };

        RPNElement(Function function_ = FUNCTION_UNKNOWN) : function(function_) {} /// NOLINT

        Function function = FUNCTION_UNKNOWN;
        std::vector<std::pair<size_t, ColumnPtr>> predicate;
    };

    MergeTreeIndexConditionBinaryFuseFilter(const ActionsDAG::Node * predicate, ContextPtr context_, const Block & header_);

    bool alwaysUnknownOrTrue() const override;

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr granule, const UpdatePartialDisjunctionResultFn & update_partial_result_disjunction_fn) const override
    {
        if (const auto * cf_granule = typeid_cast<const MergeTreeIndexGranuleBinaryFuseFilter *>(granule.get()))
            return mayBeTrueOnGranule(cf_granule, update_partial_result_disjunction_fn);

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Requires binary fuse filter index granule.");
    }

    std::string getDescription() const override { return ""; }

private:
    const Block & header;
    std::vector<RPNElement> rpn;

    bool mayBeTrueOnGranule(const MergeTreeIndexGranuleBinaryFuseFilter * granule, const UpdatePartialDisjunctionResultFn & update_partial_result_disjunction_fn) const;

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

class MergeTreeIndexAggregatorBinaryFuseFilter final : public IMergeTreeIndexAggregator
{
public:
    MergeTreeIndexAggregatorBinaryFuseFilter(double false_positive_rate_, size_t f_bits_, const Names & columns_name_);

    bool empty() const override;

    MergeTreeIndexGranulePtr getGranuleAndReset() override;

    void update(const Block & block, size_t * pos, size_t limit) override;

private:
    double false_positive_rate;
    size_t f_bits;
    const Names index_columns_name;

    std::vector<HashSet<UInt64>> column_hashes;
    size_t total_rows = 0;
};

class MergeTreeIndexBinaryFuseFilter final : public IMergeTreeIndex
{
public:
    MergeTreeIndexBinaryFuseFilter(const IndexDescription & index_, double false_positive_rate_, size_t f_bits_);

    MergeTreeIndexGranulePtr createIndexGranule() const override;

    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;

private:
    double false_positive_rate;
    size_t f_bits;
};

}
