#pragma once

#include <Core/Defines.h>
#include <Common/StackTrace.h>

namespace DB
{

class ContextExpireChecker
{
public:
    ContextExpireChecker() = default;
    ~ContextExpireChecker();

private:
    struct Status
    {
        bool hasThreadStatus = false;
        bool hasAttachedThreadGroup = false;
        bool hasQueryID = false;
        bool hasValidQueryContext = false;
        bool hasProcessListElem = false;
        StackTrace stack;

        Status();
    };

    Status atStart;
};

}
