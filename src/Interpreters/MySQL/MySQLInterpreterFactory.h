#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/queryToString.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/MySQL/ASTCreateQuery.h>

namespace DB
{

namespace MySQLInterpreter
{

class MySQLInterpreterFactory
{
public:
    static std::unique_ptr<IInterpreter> get(
        ASTPtr & query, Context & context, QueryProcessingStage::Enum stage = QueryProcessingStage::Complete);
};

struct InterpreterCreateImpl
{
    using TQuery = MySQLParser::ASTCreateQuery;

    static void validate(const TQuery & query, const Context & context);

    static ASTPtr getRewrittenQuery(const TQuery & query, const Context & context);
};

template <typename InterpreterImpl>
class InterpreterMySQLQuery : public IInterpreter
{
public:
    InterpreterMySQLQuery(const ASTPtr & query_ptr_, Context & context_) : query_ptr(query_ptr_), context(context_) {}

    BlockIO execute() override
    {
        const typename InterpreterImpl::TQuery & query = query_ptr->as<typename InterpreterImpl::TQuery &>();

        InterpreterImpl::validate(query, context);
        ASTPtr rewritten_query = InterpreterImpl::getRewrittenQuery(query, context);

        if (rewritten_query)
            return executeQuery("/* Rewritten MySQL DDL Query */ " + queryToString(rewritten_query), context, true);

        return BlockIO{};
    }

private:
    ASTPtr query_ptr;
    Context & context;
};

using InterpreterMySQLCreateQuery = InterpreterMySQLQuery<InterpreterCreateImpl>;

}

}
