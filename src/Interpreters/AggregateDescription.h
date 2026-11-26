#pragma once

#include <Core/Block_fwd.h>

#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <Core/ColumnNumbers.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <Core/Names.h>
#include <Core/Types.h>

namespace DB
{

class WriteBuffer;
struct ExplainFormatSettings;

namespace JSONBuilder { class JSONMap; }

struct AggregateDescription
{
    AggregateFunctionPtr function;
    Array parameters;        /// Parameters of the (parametric) aggregate function.
    Names argument_names;
    String column_name;      /// What name to use for a column with aggregate function values

    void explain(WriteBuffer & out, const std::string & prefix, size_t additonal_indent) const; /// Get description for EXPLAIN query.
    void explain(JSONBuilder::JSONMap & map) const;

    void explainPretty(ExplainFormatSettings & settings) const;
};

using AggregateDescriptions = std::vector<AggregateDescription>;

void serializeAggregateDescriptions(
    const AggregateDescriptions & aggregates, WriteBuffer & out, bool for_cache_key = false, const Block * input_header = nullptr);
void deserializeAggregateDescriptions(AggregateDescriptions & aggregates, ReadBuffer & in, size_t max_type_complexity);

/// Variant for aggregates whose argument names the planner removed (the `Rollup` and `Cube` steps:
/// their transforms only merge states, so the argument columns do not exist in their input). The
/// writer takes the argument types from the resolved function; the reader resolves the same
/// function from them and leaves the argument names empty, mirroring the writer's state.
void serializeAggregateDescriptionsWithoutArguments(
    const AggregateDescriptions & aggregates, WriteBuffer & out, bool for_cache_key = false, const Block * input_header = nullptr);
void deserializeAggregateDescriptionsWithoutArguments(AggregateDescriptions & aggregates, ReadBuffer & in, size_t max_type_complexity);

/// The aggregate functions of a deserialized query plan are resolved from the types written in the plan,
/// but they are executed on the columns of the step's input. The plan may come from an untrusted client,
/// so check that they agree, or a function would interpret a column of another type. Both throw `INCORRECT_DATA`.

/// The arguments of the aggregates are found in the input by name and must have the types the functions are resolved for.
void checkDeserializedAggregateArguments(const AggregateDescriptions & aggregates, const Block & header);

/// The steps finalizing the states (`TotalsHaving`) find them in the input by name: every aggregate must name
/// a column of the input holding a state of the same representation as its function.
void checkDeserializedFinalizedAggregates(const AggregateDescriptions & aggregates, const Block & header);

/// The steps merging the states read the keys and then the states by position: `layout` (the input as it reaches
/// the `Aggregator`) must start with `keys` and then the states of `aggregates` (named by `column_name`), in order.
/// With `exact`, there must be no other columns.
void checkDeserializedMergedAggregatesLayout(
    const Names & keys, const AggregateDescriptions & aggregates, const ColumnsWithTypeAndName & layout, bool exact = false);

/// The names of the keys and the aggregates (the columns of the output) must be distinct.
void checkDeserializedAggregateOutputNames(const Names & keys, const AggregateDescriptions & aggregates);

/// The input of the merge-only `Aggregating` step: exactly the keys and then the states, finalized to the types of the plan
/// (`AggregatingInOrderTransform` and the finalization of a single chunk keep the functions of the input).
void checkDeserializedMergeOnlyInput(const Names & keys, const AggregateDescriptions & aggregates, const Block & header);

/// The input of `Rollup` and `Cube`: `final`, exactly the keys and then the states, finalized to the types of the plan.
void checkDeserializedGroupByModifierInput(
    const Names & keys, const AggregateDescriptions & aggregates, bool final, const Block & header);

}
