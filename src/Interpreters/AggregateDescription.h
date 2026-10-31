#pragma once

#include <Core/Block_fwd.h>

#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <Core/ColumnNumbers.h>
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
/// An argument type that differs from the input just by the aggregate state variant takes the input's:
/// its own column in `input_header`, or the argument of the aggregate's state column when the input holds states.
void deserializeAggregateDescriptions(
    AggregateDescriptions & aggregates, ReadBuffer & in, size_t max_type_complexity, const Block * input_header = nullptr);

/// Variant for aggregates whose argument names the planner removed (the `Rollup` and `Cube` steps:
/// their transforms only merge states, so the argument columns do not exist in their input). The
/// writer takes the argument types from the resolved function; the reader resolves the same
/// function from them and leaves the argument names empty, mirroring the writer's state.
void serializeAggregateDescriptionsWithoutArguments(
    const AggregateDescriptions & aggregates, WriteBuffer & out, bool for_cache_key = false, const Block * input_header = nullptr);
void deserializeAggregateDescriptionsWithoutArguments(
    AggregateDescriptions & aggregates, ReadBuffer & in, size_t max_type_complexity, const Block * input_header = nullptr);

}
