#pragma once

#include <Interpreters/StorageIDMaybeEmpty.h>

#include <memory>
#include <mutex>
#include <unordered_map>

namespace DB
{

/// One gate per destination table of an INSERT query, shared by all the sinks that write into that
/// table on behalf of the query. With `max_insert_threads` greater than one the query writes through
/// several sinks running in parallel, and their `onStart` calls are not ordered with respect to each
/// other's writes. See `SinkToStorage::runOnceBeforeFirstWrite`.
using InsertStartGatePtr = std::shared_ptr<std::once_flag>;

/// The per-query registry the gates come from. `InsertDependenciesBuilder` creates one for the INSERT
/// query and takes the gate of every destination table it reaches from it, so all the sinks of the
/// query writing into the same physical table - the parallel streams of the destination table itself
/// as well as the branches of different materialized views converging on one target table - share one
/// gate. A destination that forwards the write through a nested INSERT running in this query's
/// context (an `Alias`) hands the registry over to that nested INSERT, so the sinks created there
/// keep sharing the gates of the outer query: otherwise every branch of the outer fan-out would
/// create its own gates and the pre-write checks would not be shared across the branches.
class InsertStartGates
{
public:
    InsertStartGatePtr get(const StorageIDMaybeEmpty & table_id)
    {
        std::lock_guard lock(mutex);
        auto & gate = gates[table_id];
        if (!gate)
            gate = std::make_shared<std::once_flag>();
        return gate;
    }

private:
    std::mutex mutex;
    std::unordered_map<StorageIDMaybeEmpty, InsertStartGatePtr, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual>
        gates;
};

using InsertStartGatesPtr = std::shared_ptr<InsertStartGates>;

}
