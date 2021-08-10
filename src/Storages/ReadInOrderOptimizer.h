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
        const ManyExpressionActions & elements_actions,
        const SortDescription & required_sort_description,
        const TreeRewriterResultPtr & syntax_result);

    InputOrderInfoPtr getInputOrder(const StorageMetadataPtr & metadata_snapshot, ContextPtr context) const;

private:
    /// Actions for every element of order expression to analyze functions for monotonicity
    ManyExpressionActions elements_actions;
    NameSet forbidden_columns;
    NameToNameMap array_join_result_to_source;
    SortDescription required_sort_description;
};
}
