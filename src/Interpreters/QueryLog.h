#pragma once

#include <Interpreters/QueryLogElement.h>
#include <Interpreters/SystemLog.h>

namespace DB
{

/// Instead of typedef - to allow forward declaration.
class QueryLog : public SystemLog<QueryLogElement>
{
    using SystemLog<QueryLogElement>::SystemLog;
};

}
