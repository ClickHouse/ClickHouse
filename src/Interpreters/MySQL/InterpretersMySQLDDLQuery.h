#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/queryToString.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/MySQL/ASTAlterQuery.h>
#include <Parsers/MySQL/ASTCreateQuery.h>

namespace DB
{

namespace MySQLInterpreter
{

struct InterpreterDropImpl
{
    using TQuery = ASTDropQuery;

    static void validate(const TQuery & query, const Context & context);

    static ASTs getRewrittenQueries(const TQuery & drop_query, const Context & context, const String & mapped_to_database, const String & mysql_database);
};

struct InterpreterAlterImpl
{
    using TQuery = MySQLParser::ASTAlterQuery;

    static void validate(const TQuery & query, const Context & context);

    static ASTs getRewrittenQueries(const TQuery & alter_query, const Context & context, const String & mapped_to_database, const String & mysql_database);
};

struct InterpreterRenameImpl
{
    using TQuery = ASTRenameQuery;

    static void validate(const TQuery & query, const Context & context);

    static ASTs getRewrittenQueries(const TQuery & rename_query, const Context & context, const String & mapped_to_database, const String & mysql_database);
};

struct InterpreterCreateImpl
{
    using TQuery = MySQLParser::ASTCreateQuery;

    static void validate(const TQuery & query, const Context & context);

    static ASTs getRewrittenQueries(const TQuery & create_query, const Context & context, const String & mapped_to_database, const String & mysql_database);
};

template <typename InterpreterImpl>
class InterpreterMySQLDDLQuery : public IInterpreter
{
public:
    InterpreterMySQLDDLQuery(const ASTPtr & query_ptr_, Context & context_, const String & mapped_to_database_, const String & mysql_database_)
        : query_ptr(query_ptr_), context(context_), mapped_to_database(mapped_to_database_), mysql_database(mysql_database_)
    {
    }

    BlockIO execute() override
    {
        const typename InterpreterImpl::TQuery & query = query_ptr->as<typename InterpreterImpl::TQuery &>();

        InterpreterImpl::validate(query, context);
        ASTs rewritten_queries = InterpreterImpl::getRewrittenQueries(query, context, mapped_to_database, mysql_database);

        for (const auto & rewritten_query : rewritten_queries)
            executeQuery("/* Rewritten MySQL DDL Query */ " + queryToString(rewritten_query), context, true);

        return BlockIO{};
    }

private:
    ASTPtr query_ptr;
    Context & context;
    const String mapped_to_database;
    const String mysql_database;
};

using InterpreterMySQLDropQuery = InterpreterMySQLDDLQuery<InterpreterDropImpl>;
using InterpreterMySQLAlterQuery = InterpreterMySQLDDLQuery<InterpreterAlterImpl>;
using InterpreterMySQLRenameQuery = InterpreterMySQLDDLQuery<InterpreterRenameImpl>;
using InterpreterMySQLCreateQuery = InterpreterMySQLDDLQuery<InterpreterCreateImpl>;

}

}
