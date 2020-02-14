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
        const SyntaxAnalyzerResultPtr & syntax_result);

    InputSortingInfoPtr getInputOrder(const StoragePtr & storage) const;

private:
    /// Actions for every element of order expression to analyze functions for monotonicity
    ManyExpressionActions elements_actions;
    NameSet forbidden_columns;
    SortDescription required_sort_description;
};


/** Helper class, that can analyze MergeTree order key
*   and required group by description to get their
*   common prefix, which is needed for
*   performing reading in order of PK.
*/
class AggregateInOrderOptimizer
{
public:
    AggregateInOrderOptimizer(
        const Names & group_by_description,
        const SyntaxAnalyzerResultPtr & syntax_result);

    GroupByInfoPtr getGroupByCommonPrefix(const StoragePtr & storage) const;

private:
    /// Actions for every element of order expression to analyze functions for monotonicity
    NameSet forbidden_columns;
    Names group_by_description;
};

}
