#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <Core/UUID.h>


namespace DB
{
class ASTShowGrantsQuery;
struct IAccessEntity;


class InterpreterShowGrantsQuery : public IInterpreter
{
public:
    InterpreterShowGrantsQuery(const ASTPtr & query_ptr_, Context & context_) : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override;

    static ASTs getAttachGrantQueries(const IAccessEntity & user_or_role);

private:
    BlockInputStreamPtr executeImpl();
    ASTs getGrantQueries(const ASTShowGrantsQuery & show_query) const;

    ASTPtr query_ptr;
    Context & context;
};
}
