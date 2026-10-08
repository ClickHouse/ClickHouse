#pragma once

#include <Core/Names.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

void collectFilterInputColumns(const ActionsDAG::Node * node, NameSet & out);

/// TODO(yariks5s): an OR with the candidate's column on one side and another column on the other
/// The real read can combine them (use_skip_indexes_for_disjunctions) we can't, so we bail on those
bool disjunctionMixesIndexAndOtherColumns(const ActionsDAG::Node * node, const NameSet & index_columns);

}
