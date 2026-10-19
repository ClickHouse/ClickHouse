#pragma once

#include <Processors/ISpillable.h>

#include <utility>

namespace DB
{

/// Owned by a processor or shared state to expose `ISpillable` through composition.
template <typename Owner>
class SpillableAdapter final : public ISpillable
{
public:
    explicit SpillableAdapter(Owner & owner_) : owner(owner_) {}

    ProcessorMemoryStats getMemoryStats() const override
    {
        return std::as_const(owner).getMemoryStats();
    }

    size_t spill(size_t at_least_bytes) override
    {
        return owner.spill(at_least_bytes);
    }

    const TemporaryDataOnDiskScope * getSpillScope() const override
    {
        if constexpr (requires { std::as_const(owner).getSpillScope(); })
            return std::as_const(owner).getSpillScope();
        else
            return nullptr;
    }

private:
    Owner & owner;
};

}
