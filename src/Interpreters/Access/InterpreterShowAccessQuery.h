#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

struct IAccessEntity;
using AccessEntityPtr = std::shared_ptr<const IAccessEntity>;

/** Return all queries for creating access entities and grants.
  */
class InterpreterShowAccessQuery : public IInterpreter, WithContext
{
public:
    InterpreterShowAccessQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

    bool ignoreQuota() const override { return true; }
    bool ignoreLimits() const override { return true; }

private:
    QueryPipeline executeImpl() const;
    ASTs getCreateAndGrantQueries() const;
    std::vector<AccessEntityPtr> getEntities() const;

    ASTPtr query_ptr;
};

}
