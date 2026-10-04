#pragma once

#include <Core/Names.h>
#include <Core/SortDescription.h>

namespace DB
{

class ActionsDAG;

/// How one output-column name of a stateless step maps to that step's input.
enum class TranslatedName : uint8_t
{
    Traced,       /// resolves to an `INPUT` column (through `ALIAS` and `materialize` chains)
    Passthrough,  /// not among the DAG outputs; may be an input column carried around the step
    Computed,     /// produced by a `FUNCTION` node; absent from the input
};

/// Traces `output_name` back to the original `INPUT` name through `ALIAS` chains and `materialize`
/// wrappers (which preserve values and thus hash-based distribution). Sets `input_name` for
/// `Traced` results only.
TranslatedName classifyOutputName(const ActionsDAG & dag, const String & output_name, String & input_name);

/// Translates distribution column names through the DAG to input names; computed columns drop
/// out of their equivalence set. Returns false if any set becomes empty (all computed - the
/// distribution is not derivable from the input).
bool translateDistributionColumns(const ActionsDAG & dag, std::vector<NameSet> & columns);

/// Translates sort column names through the DAG to input names. Returns false if any column is
/// computed (its order is not derivable from the input).
bool translateSortDescription(const ActionsDAG & dag, SortDescription & sort_desc);

}
