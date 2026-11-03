#pragma once

#include <memory>

namespace DB
{

/// Shared by the sinks of all insert streams of one `INSERT` into a table, see `IStorage::createParallelInsertState`.
class IParallelInsertState
{
public:
    virtual ~IParallelInsertState() = default;
};

using ParallelInsertStatePtr = std::shared_ptr<IParallelInsertState>;

}
