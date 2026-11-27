#pragma once

#include <Processors/SpillableAdapter.h>

#include <atomic>
#include <memory>
#include <span>
#include <vector>

namespace DB
{

class ActionsDAG;
class IColumn;
class Set;

/// A query-owned binding shared by the set builder and its consumers. A cache hit does not bind a set
/// owned by another query to this query's memory reservation.
class SetSpillState
{
public:
    void bind(std::shared_ptr<Set> set_);
    ProcessorMemoryStats getMemoryStats() const;
    size_t spill(size_t at_least_bytes);
    const TemporaryDataOnDiskScope * getSpillScope() const;
    ISpillable * getSpillable() { return &spillable; }

private:
    Set * getSet() const;

    /// Published once by the owning builder after its cache lookup and spill configuration.
    std::shared_ptr<Set> set;
    std::atomic<bool> bound{false};
    SpillableAdapter<SetSpillState> spillable{*this};
};

/// Keeps component identities alive and stable for the lifetime of a consuming processor.
class SetSpillables
{
public:
    void add(const ActionsDAG & actions);
    void add(const std::shared_ptr<SetSpillState> & state);
    std::span<ISpillable * const> get() const { return components; }

private:
    void add(const IColumn & column);

    std::vector<std::shared_ptr<SetSpillState>> states;
    std::vector<ISpillable *> components;
};

}
