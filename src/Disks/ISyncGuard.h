#pragma once

#include <memory>

namespace DB
{

/**
 * A guard, that should synchronize file's or directory's state
 * with storage device (e.g. fsync in POSIX) in its destructor.
 */
class ISyncGuard
{
public:
    ISyncGuard() = default;
    virtual ~ISyncGuard() = default;
};

using SyncGuardPtr = std::unique_ptr<ISyncGuard>;

}
