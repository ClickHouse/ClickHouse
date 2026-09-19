#pragma once

#include <Analyzer/IQueryTreeNode.h>

#include <cstddef>
#include <optional>

namespace DB
{

class FunctionNode;

struct GroupingSpecializationShape
{
    /// The virtual `__grouping_set` column the analyzer prepends: 1, or 0 for `__groupingOrdinary`.
    size_t num_leading_arguments;
    /// The trailing constant arguments carrying the specialization parameters.
    size_t num_state_arguments;
};

/// Recognises a `grouping` call that the analyzer resolved into one of its specializations, and reports
/// how many arguments the resolution added around the user's ones. A specialization a query spelled
/// directly is not one: it must reach a remote server unchanged.
std::optional<GroupingSpecializationShape> getAnalyzerBuiltGroupingSpecialization(const FunctionNode & function);

void removeGroupingFunctionSpecializations(QueryTreeNodePtr & node);

}
