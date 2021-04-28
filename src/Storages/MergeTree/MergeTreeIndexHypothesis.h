#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <Interpreters/SetVariants.h>

#include <memory>
#include <set>


namespace DB
{

class MergeTreeIndexHyposesis;

struct MergeTreeIndexGranuleHypothesis : public IMergeTreeIndexGranule
{
    explicit MergeTreeIndexGranuleHypothesis(
        const String & index_name_);

    MergeTreeIndexGranuleHypothesis(
        const String & index_name_,
        const bool met_);

    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr) override;

    bool empty() const override { return is_empty; }

    ~MergeTreeIndexGranuleHypothesis() override = default;

    String index_name;
    bool is_empty = true;
    bool met = true;
};


struct MergeTreeIndexAggregatorHypothesis : IMergeTreeIndexAggregator
{
    explicit MergeTreeIndexAggregatorHypothesis(
        const String & index_name_, const String & column_name_);

    ~MergeTreeIndexAggregatorHypothesis() override = default;

    bool empty() const override { return is_empty; }

    MergeTreeIndexGranulePtr getGranuleAndReset() override;

    void update(const Block & block, size_t * pos, size_t limit) override;

private:
    String index_name;
    String column_name;

    bool met = true;
    bool is_empty = true;
};


class MergeTreeIndexConditionHypothesis : public IMergeTreeIndexCondition
{
public:
    MergeTreeIndexConditionHypothesis(
        const String & index_name_,
        const String & column_name_,
        const SelectQueryInfo & query,
        ContextPtr context);

    bool alwaysUnknownOrTrue() const override { return false; }

    bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const override;

    ~MergeTreeIndexConditionHypothesis() override = default;

private:
    std::pair<bool, bool> mayBeTrue(const ASTPtr & ast, const bool value) const;

    String index_name;

    String column_name;
    ASTPtr expression_ast;
};


class MergeTreeIndexHypothesis : public IMergeTreeIndex
{
public:
    MergeTreeIndexHypothesis(
        const IndexDescription & index_)
        : IMergeTreeIndex(index_)
    {}

    ~MergeTreeIndexHypothesis() override = default;

    MergeTreeIndexGranulePtr createIndexGranule() const override;
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override;

    MergeTreeIndexConditionPtr createIndexCondition(
        const SelectQueryInfo & query, ContextPtr context) const override;

    bool mayBenefitFromIndexForIn(const ASTPtr & node) const override;

    size_t max_rows = 0;
};

}
