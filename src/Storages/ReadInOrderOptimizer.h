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
class ReadInOrderOptimizer
{
public:
    ReadInOrderOptimizer(
        const ManyExpressionActions & elements_actions,
        const SortDescription & required_sort_description,
        const TreeRewriterResultPtr & syntax_result);

    InputOrderInfoPtr getInputOrder(const StoragePtr & storage, const StorageMetadataPtr & metadata_snapshot) const;

private:
    /// Actions for every element of order expression to analyze functions for monotonicity
    ManyExpressionActions elements_actions;
    NameSet forbidden_columns;
    SortDescription required_sort_description;
};

}
