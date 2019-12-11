#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class Context;
class ASTShowCreateAccessEntityQuery;


/** Returns a single item containing a statement which could be used to create a specified role.
  */
class InterpreterShowCreateAccessEntityQuery : public IInterpreter
{
public:
    InterpreterShowCreateAccessEntityQuery(const ASTPtr & query_ptr_, const Context & context_)
        : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

    bool ignoreQuota() const override { return true; }
    bool ignoreLimits() const override { return true; }

private:
    ASTPtr query_ptr;
    const Context & context;

    BlockInputStreamPtr executeImpl();
    ASTPtr getCreateQuotaQuery(const ASTShowCreateAccessEntityQuery & show_query) const;
};


}
