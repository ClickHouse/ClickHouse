#pragma once

#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <Core/ColumnNumbers.h>
#include <Core/Field.h>
#include <Core/Names.h>
#include <Core/Types.h>

namespace DB
{

class WriteBuffer;

namespace JSONBuilder { class JSONMap; }

struct AggregateDescription
{
    AggregateFunctionPtr function;
    Array parameters;        /// Parameters of the (parametric) aggregate function.
    Names argument_names;
    String column_name;      /// What name to use for a column with aggregate function values

    void explain(WriteBuffer & out, size_t indent) const; /// Get description for EXPLAIN query.
    void explain(JSONBuilder::JSONMap & map) const;
};

using AggregateDescriptions = std::vector<AggregateDescription>;

void serializeAggregateDescriptions(const AggregateDescriptions & aggregates, WriteBuffer & out);
void deserializeAggregateDescriptions(AggregateDescriptions & aggregates, ReadBuffer & in);

}
