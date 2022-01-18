#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class Context;
struct IAccessEntity;
using AccessEntityPtr = std::shared_ptr<const IAccessEntity>;

/** Return all queries for creating access entities and grants.
  */
class InterpreterShowAccessQuery : public IInterpreter
{
public:
    InterpreterShowAccessQuery(const ASTPtr & query_ptr_, Context & context_)
        : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

    bool ignoreQuota() const override { return true; }
    bool ignoreLimits() const override { return true; }

private:
    BlockInputStreamPtr executeImpl() const;
    ASTs getCreateAndGrantQueries() const;
    std::vector<AccessEntityPtr> getEntities() const;

    ASTPtr query_ptr;
    Context & context;
};


}
