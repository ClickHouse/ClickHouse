#pragma once

#include <Core/SortDescription.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

/** Helper class, that can analyze MergeTree order key
 *   and required sort description to get their
 *   common prefix, which is needed for
 *   performing reading in order of PK.
 */
class Context;

class ReadInOrderOptimizer
{
public:
    ReadInOrderOptimizer(
        const ASTSelectQuery & query,
        const ManyExpressionActions & elements_actions,
        const SortDescription & required_sort_description,
        const TreeRewriterResultPtr & syntax_result);

    InputOrderInfoPtr getInputOrder(const StorageMetadataPtr & metadata_snapshot, ContextPtr context, UInt64 limit = 0) const;

private:
    InputOrderInfoPtr getInputOrderImpl(
        const StorageMetadataPtr & metadata_snapshot,
        const SortDescription & description,
        const ManyExpressionActions & actions,
        UInt64 limit) const;

    /// Actions for every element of order expression to analyze functions for monotonicity
    ManyExpressionActions elements_actions;
    NameSet forbidden_columns;
    NameToNameMap array_join_result_to_source;
    SortDescription required_sort_description;
    const ASTSelectQuery & query;
};

class ReadInOrderOptimizerForDistinct
{
public:
    explicit ReadInOrderOptimizerForDistinct(const Names & source_columns_);
    InputOrderInfoPtr getInputOrder(const StorageMetadataPtr & metadata_snapshot) const;

private:
    NameSet source_columns;
};
}
