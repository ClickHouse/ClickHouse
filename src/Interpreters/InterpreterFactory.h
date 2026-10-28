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

    /// Increments the `Query`, `InitialQuery`, `SelectQuery`, ... counters that `get` increments for every
    /// query. For a code path that constructs an interpreter without `get`.
    static void incrementQueryProfileEvents(const IAST & query, const ContextPtr & context);

    using CreatorFn = std::function<InterpreterPtr(const Arguments & arguments)>;

    using Interpreters = std::unordered_map<String, CreatorFn>;

    void registerInterpreter(const std::string & name, CreatorFn creator_fn);

private:
    Interpreters interpreters;
};

}
