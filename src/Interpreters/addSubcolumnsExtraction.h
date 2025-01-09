#pragma once

#include <Interpreters/ActionsDAG.h>

namespace DB
{

/// Create a new ActionsDAG where all input subcolumns that are not presented in the header are replaced to the getSubcolumn function.
ActionsDAG addSubcolumnsExtraction(ActionsDAG dag, const Block & header, const ContextPtr & context);

}
