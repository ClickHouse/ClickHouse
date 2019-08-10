#include <AggregateFunctions/IAggregateFunction.h>

namespace DB
{

void IAggregateFunction::addBatch(
    size_t batch_size,
    AggregateDataPtr * places,
    size_t place_offset,
    const IColumn ** columns,
    Arena * arena) const
{
    for (size_t i = 0; i < batch_size; ++i)
        add(places[i] + place_offset, columns, i, arena);
}

}
