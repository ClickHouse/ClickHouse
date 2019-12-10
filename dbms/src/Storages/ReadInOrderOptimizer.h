#pragma once

#include <Core/SortDescription.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

/** Helper class, that can analyze MergeTree order key
 *   and required sort description to get info needed for
 *   performing reading in order of PK.
 */
class ReadInOrderOptimizer
{
public:
    ReadInOrderOptimizer(
        /// Actions for every element of order expression to analyze functions for monotonicicy
        const ManyExpressionActions & elements_actions,
        const SortDescription & required_sort_description,
        const SyntaxAnalyzerResultPtr & syntax_result);

    InputSortingInfoPtr analyze(const MergeTreeData & storage);

private:
    ManyExpressionActions elements_actions;
    NameSet forbidden_columns;
    const SortDescription & required_sort_description;
};

}
