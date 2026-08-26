#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/IAST_fwd.h>
#include <Common/Documentation.h>

#include <map>
#include <vector>

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

    using Interpreters = std::unordered_map<String, CreatorFn>;

    void registerInterpreter(const std::string & name, CreatorFn creator_fn);

    /// SQL statements, such as `SELECT`, have no registry of their own, therefore this factory also keeps their
    /// embedded documentation, similar to how `FunctionFactory` keeps the documentation of SQL functions.
    /// The documentation is authored next to the parsers of the statements, registered by `registerStatements`,
    /// and exposed by `system.statements` and `system.documentation`.
    /// Statement names are unrelated to the interpreter names above, because not every statement has an interpreter
    /// of its own, e.g. the `WHERE` clause is interpreted as a part of `SELECT`.
    void registerStatement(const String & name, Documentation documentation);

    std::vector<String> getAllStatementNames() const;
    Documentation getStatementDocumentation(const String & name) const;

private:
    Interpreters interpreters;
    std::map<String, Documentation> statements;
};

}
