#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/queryToString.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/MySQL/ASTCreateQuery.h>

namespace DB
{

namespace MySQLInterpreter
{

struct InterpreterDropImpl
{
    using TQuery = ASTDropQuery;

    static void validate(const TQuery & query, const Context & context);

    static ASTPtr getRewrittenQuery(const TQuery & drop_query, const Context & context, const String & clickhouse_db, const String & filter_mysql_db);
};

struct InterpreterRenameImpl
{
    using TQuery = ASTRenameQuery;

    static void validate(const TQuery & query, const Context & context);

    static ASTPtr getRewrittenQuery(const TQuery & rename_query, const Context & context, const String & clickhouse_db, const String & filter_mysql_db);
};

struct InterpreterCreateImpl
{
    using TQuery = MySQLParser::ASTCreateQuery;

    static void validate(const TQuery & query, const Context & context);

    static ASTPtr getRewrittenQuery(const TQuery & query, const Context & context, const String & clickhouse_db, const String & filter_mysql_db);
};

template <typename InterpreterImpl>
class InterpreterMySQLDDLQuery : public IInterpreter
{
public:
    InterpreterMySQLDDLQuery(const ASTPtr & query_ptr_, Context & context_, const String & clickhouse_db_, const String & mysql_db_)
        : query_ptr(query_ptr_), context(context_), clickhouse_db(clickhouse_db_), mysql_db(mysql_db_)
    {
    }

    BlockIO execute() override
    {
        const typename InterpreterImpl::TQuery & query = query_ptr->as<typename InterpreterImpl::TQuery &>();

        InterpreterImpl::validate(query, context);
        ASTPtr rewritten_query = InterpreterImpl::getRewrittenQuery(query, context, clickhouse_db, mysql_db);

        if (rewritten_query)
            return executeQuery("/* Rewritten MySQL DDL Query */ " + queryToString(rewritten_query), context, true);

        return BlockIO{};
    }

private:
    ASTPtr query_ptr;
    Context & context;
    const String clickhouse_db;
    const String mysql_db;
};

using InterpreterMySQLDropQuery = InterpreterMySQLDDLQuery<InterpreterDropImpl>;
using InterpreterMySQLRenameQuery = InterpreterMySQLDDLQuery<InterpreterRenameImpl>;
using InterpreterMySQLCreateQuery = InterpreterMySQLDDLQuery<InterpreterCreateImpl>;

}

}
