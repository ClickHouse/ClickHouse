#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/IAST_fwd.h>


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

    using Interpreters = std::unordered_map<String, CreatorFn>;

    void registerInterpreter(const std::string & name, CreatorFn creator_fn);

private:
    Interpreters interpreters;
};

}
