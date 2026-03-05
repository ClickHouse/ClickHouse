#pragma once

#include <Interpreters/QueryLogElement.h>
#include <Interpreters/SystemLog.h>

namespace DB
{

/// Instead of typedef - to allow forward declaration.
class QueryLog : public SystemLog<QueryLogElement>
{
    friend class UserQueryLog;
    using SystemLog<QueryLogElement>::SystemLog;

    // We'll be creating a view over this for user_query_log, so perhaps it makes sense to create it at startup.
    bool mustBePreparedAtStartup() const override {
        return true;
    }
};

}
