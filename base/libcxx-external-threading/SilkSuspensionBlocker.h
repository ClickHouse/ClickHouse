#pragma once

#include <utility>

namespace Silk
{

class SuspensionBlocker
{
public:
    SuspensionBlocker()
        : previous(std::exchange(active, true))
    {
    }

    ~SuspensionBlocker() { active = previous; }

    SuspensionBlocker(const SuspensionBlocker &) = delete;
    SuspensionBlocker & operator=(const SuspensionBlocker &) = delete;

    static bool isActive() { return active; }

private:
    static inline constinit thread_local bool active = false;

    bool previous;
};

}
