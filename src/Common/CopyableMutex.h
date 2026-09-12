#pragma once

#include <mutex>

namespace DB
{

/// A mutex that does not prevent its owner from being copyable. It guards the state of one object,
/// so a copy of that object gets its own fresh mutex instead of the state of the original's one.
struct CopyableMutex : public std::mutex
{
    CopyableMutex() = default;
    CopyableMutex(const CopyableMutex &) {}
    CopyableMutex & operator=(const CopyableMutex &) { return *this; }
};

}
