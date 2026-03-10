#pragma once

#include "config.h"

#if USE_TANTIVY

#include <memory>
#include <string>
#include <vector>

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace DB
{

/// Granule stores serialized Tantivy index for a single granule.
struct MergeTreeIndexGranuleTantivy final : public IMergeTreeIndexGranule
{
    explicit MergeTreeIndexGranuleTantivy(const String & index_name_, const String & column_name_);

    ~MergeTreeIndexGranuleTantivy() override = default;

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return !has_data; }
    size_t memoryUsageBytes() const override { return serialized_index.size(); }

    String index_name;
    String column_name;
    std::vector<uint8_t> serialized_index;
    bool has_data = false;
};

using MergeTreeIndexGranuleTantivyPtr = std::shared_ptr<MergeTreeIndexGranuleTantivy>;

/// Aggregator builds a Tantivy index from block data.
struct MergeTreeIndexAggregatorTantivy final : IMergeTreeIndexAggregator
{
    explicit MergeTreeIndexAggregatorTantivy(const String & index_name_, const String & column_name_);

    ~MergeTreeIndexAggregatorTantivy() override = default;

    bool empty() const override { return !granule || granule->empty(); }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;

    void update(const Block & block, size_t * pos, size_t limit) override;

private:
    String index_name;
    String column_name;
    std::vector<uint8_t> index_data;
    UInt64 current_row_id = 0;
    bool index_initialized = false;

    MergeTreeIndexGranuleTantivyPtr granule;
};

/// Condition evaluates search predicates against the Tantivy index.
class MergeTreeConditionTantivy final : public IMergeTreeIndexCondition
{
public:
    MergeTreeConditionTantivy(
        const ActionsDAG::Node * predicate,
        ContextPtr context,
        const Block & index_sample_block,
        const String & column_name);

    ~MergeTreeConditionTantivy() override = default;

    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule, const UpdatePartialDisjunctionResultFn & update_partial_disjunction_result_fn) const override;
    std::string getDescription() const override { return "tantivy"; }

private:
    struct RPNElement
    {
        enum Function
        {
            FUNCTION_EQUALS,
            FUNCTION_NOT_EQUALS,
            FUNCTION_HAS_TOKEN,
            FUNCTION_HAS_TOKEN_OR,
            FUNCTION_UNKNOWN,
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
            ALWAYS_FALSE,
            ALWAYS_TRUE,
        };

        RPNElement(Function function_ = FUNCTION_UNKNOWN, size_t key_column_ = 0)
            : function(function_), key_column(key_column_) {}

        Function function = FUNCTION_UNKNOWN;
        size_t key_column = 0;
        std::vector<String> search_terms;
        bool use_and_logic = false;
    };

    using RPN = std::vector<RPNElement>;

    bool extractAtomFromTree(const RPNBuilderTreeNode & node, RPNElement & out);
    bool traverseFunction(const String & function_name, const RPNBuilderTreeNode & key_node, const Field & value_field, RPNElement & out);

    String column_name;
    DataTypes index_data_types;
    RPN rpn;
};

/// Main index class that creates granules, aggregators, and conditions.
class MergeTreeIndexTantivy final : public IMergeTreeIndex
{
public:
    explicit MergeTreeIndexTantivy(const IndexDescription & index_);

    ~MergeTreeIndexTantivy() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;

    bool isTextIndex() const override { return true; }

private:
    String column_name;
};

}

#endif
