#pragma once
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class StartupScriptMode
{
private:
    static inline thread_local bool active = false;

    struct Guard
    {
        Guard()
        {
            if (active)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "startup scripts guard already initialized");
            }
            active = true;
        }

        ~Guard()
        {
            active = false;
        }
    };
public:
    static Guard getGuard()
    {
        return Guard{};
    }

    static bool isActive()
    {
        return active;
    }
};
}
