#pragma once

#include <Access/ContextAccess.h>
#include <Access/QueryPrivilegesInfo.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

/// The privilege check of a function, prepared by the creator of the function and done when the function is built
/// (see `IFunctionOverloadResolver::checkAccessOnBuild`). Keeps only the access rights of the user, which also take
/// into account the settings that limit them (such as `readonly` and `allow_introspection_functions`), and the
/// privileges of the query reported in `system.query_log`. It does not keep the context.
class FunctionAccessCheck
{
public:
    explicit FunctionAccessCheck(const ContextPtr & context);

    template <typename... Args>
    void check(const Args &... args) const
    {
        access->checkAccess(*privileges_info, args...);
    }

private:
    std::shared_ptr<const ContextAccess> access;
    QueryPrivilegesInfoPtr privileges_info;
};

}
