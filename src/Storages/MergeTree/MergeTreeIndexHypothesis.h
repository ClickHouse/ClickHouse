#pragma once

#include <Interpreters/ComparisonGraph.h>
#include <Storages/MergeTree/BoolMask.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

#include <list>

namespace DB
{

struct MergeTreeIndexGranuleHypothesis final : public IMergeTreeIndexGranule
{
    MergeTreeIndexGranuleHypothesis() = default;
    explicit MergeTreeIndexGranuleHypothesis(bool met_) : is_empty(false), met(met_) {}

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr, MergeTreeIndexVersion version) override;

    bool empty() const override { return is_empty; }
    size_t memoryUsageBytes() const override { return sizeof(*this); }

    bool is_empty = true;
    /// The index expression is true for every row of the granule.
    bool met = false;
};

struct MergeTreeIndexAggregatorHypothesis final : IMergeTreeIndexAggregator
{
    explicit MergeTreeIndexAggregatorHypothesis(String column_name_) : column_name(std::move(column_name_)) {}

    bool empty() const override { return is_empty; }
    MergeTreeIndexGranulePtr getGranuleAndReset() override;
    void update(const Block & block, size_t * pos, size_t limit) override;

private:
    String column_name;
    bool met = true;
    bool is_empty = true;
};

/// Skips a granule when the filter cannot hold under the table constraints together with the index
/// expression, where the granule says the expression held for all of its rows. Comparisons of the
/// filter are checked against a `ComparisonGraph` over the nodes of the filter's `ActionsDAG`.
class MergeTreeIndexConditionHypothesis final : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionHypothesis(
        const IndexDescription & index,
        const StorageMetadataPtr & metadata_snapshot,
        const ActionsDAG::Node * predicate,
        ContextPtr context);

    bool alwaysUnknownOrTrue() const override;
    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr granule, const UpdatePartialDisjunctionResultFn &) const override;
    std::string getDescription() const override { return description; }

    struct RPNElement
    {
        enum Function
        {
            FUNCTION_UNKNOWN,
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
            /// `left <expected> right`
            FUNCTION_COMPARE,
            ALWAYS_TRUE,
            ALWAYS_FALSE,
        };

        Function function = FUNCTION_UNKNOWN;
        ComparisonGraphCompareResult expected = ComparisonGraphCompareResult::UNKNOWN;
        const ActionsDAG::Node * left = nullptr;
        const ActionsDAG::Node * right = nullptr;
    };

private:
    using Graph = ComparisonGraph<const ActionsDAG::Node *>;

    BoolMask evaluate(const Graph & graph) const;

    String description;
    /// The graphs and the RPN point into these.
    std::list<ActionsDAGWithInversionPushDown> owned_dags;
    std::vector<RPNElement> rpn;
    /// Table constraints only.
    Graph graph_if_not_met;
    /// Table constraints and the index expression.
    Graph graph_if_met;
};

class MergeTreeIndexHypothesis final : public IMergeTreeIndex
{
public:
    using IMergeTreeIndex::IMergeTreeIndex;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;
    MergeTreeIndexConditionPtr createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const override;
};

}
