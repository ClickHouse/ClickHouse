#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/IAST_fwd.h>

#include <boost/noncopyable.hpp>

namespace DB
{

class Context;

class InterpreterFactory : private boost::noncopyable
{
public:
    static InterpreterFactory & instance();

    struct Arguments
    {
        ASTPtr & query;
        ContextMutablePtr context;
        const SelectQueryOptions & options;
        bool allow_materialized = false;
    };

    using InterpreterPtr = std::unique_ptr<IInterpreter>;

     InterpreterPtr get(
        ASTPtr & query,
        ContextMutablePtr context,
        const SelectQueryOptions & options = {});

    using CreatorFn = std::function<InterpreterPtr(const Arguments & arguments)>;

    struct RegisteredInterpreter
    {
        CreatorFn creator_fn;
        bool supports_table_namespace_scope = false;
    };

    using Interpreters = std::unordered_map<String, RegisteredInterpreter>;

    void registerInterpreter(const std::string & name, CreatorFn creator_fn, bool supports_table_namespace_scope = false);

private:
    Interpreters interpreters;
};

}
