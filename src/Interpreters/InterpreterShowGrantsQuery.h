#pragma once

#include <Core/UUID.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

class AccessControlManager;
class ASTShowGrantsQuery;
struct IAccessEntity;
using AccessEntityPtr = std::shared_ptr<const IAccessEntity>;

class InterpreterShowGrantsQuery : public IInterpreter, WithContext
{
public:
    InterpreterShowGrantsQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

    static ASTs getGrantQueries(const IAccessEntity & user_or_role, const AccessControlManager & access_control);
    static ASTs getAttachGrantQueries(const IAccessEntity & user_or_role);

    bool ignoreQuota() const override { return true; }
    bool ignoreLimits() const override { return true; }

private:
    BlockInputStreamPtr executeImpl();
    ASTs getGrantQueries() const;
    std::vector<AccessEntityPtr> getEntities() const;

    ASTPtr query_ptr;
};

}
