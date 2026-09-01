#pragma once

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/IAST.h>
#include <Common/Exception.h>

#include <algorithm>
#include <vector>


namespace DB
{

/// Keeps a function-local `SETTINGS` clause of a table function AST across an analysis that resolves the
/// arguments of that AST in place.
///
/// `parseArguments` of several table functions consumes such a clause by erasing it from the argument list
/// of the AST it is given - `mysql`, `PostgreSQL` and `ytsaurus` do this. That is harmless for an AST that
/// belongs to a query, but the table-function target of a `Distributed` / `Remote` / `RemoteSecure` table is
/// storage-owned: it is persisted in the table metadata exactly as the analysis leaves it. Analyzing a copy
/// is not an option there, because resolving the arguments in place is load-bearing - the same AST is
/// formatted and sent to the other shards, so a session-dependent argument has to be resolved on the
/// initiator (see `getStructureOfRemoteTableInShard`). Without this guard a `CREATE` over such a target
/// would store a definition without the settings the user wrote, and every later read would use the
/// defaults instead.
///
/// The guard remembers every `SETTINGS` clause of the AST - at any nesting depth, so a target such as
/// `loop(mysql(..., SETTINGS ...))` is covered too - and puts back the ones that were consumed, including
/// when the analysis throws (a failed analysis is tolerated for a target with an explicit column list, and
/// the definition is persisted anyway).
///
/// Today the clause cannot reach these targets: a table function written inside the argument list of a
/// table engine (or of another function) is parsed as an ordinary expression, and only the table-function
/// position of a query accepts a `SETTINGS` clause, so `Distributed(c, mysql(..., SETTINGS ...))` is a
/// syntax error. The guard exists so that the persisted definition cannot start losing settings if that
/// position ever accepts the clause.
class PreservedTableFunctionSettings
{
public:
    explicit PreservedTableFunctionSettings(const ASTPtr & table_function_ast)
    {
        if (table_function_ast)
            collect(table_function_ast);
    }

    PreservedTableFunctionSettings(const PreservedTableFunctionSettings &) = delete;
    PreservedTableFunctionSettings & operator=(const PreservedTableFunctionSettings &) = delete;

    ~PreservedTableFunctionSettings()
    {
        try
        {
            restore();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

private:
    void collect(const ASTPtr & node)
    {
        if (const auto * function = node->as<ASTFunction>(); function && function->arguments)
        {
            for (const auto & argument : function->arguments->children)
                if (argument->as<ASTSetQuery>())
                    preserved.emplace_back(function->arguments, argument);
        }

        for (const auto & child : node->children)
            collect(child);
    }

    void restore()
    {
        for (const auto & [arguments, settings_argument] : preserved)
        {
            auto & children = arguments->children;
            if (std::find(children.begin(), children.end(), settings_argument) == children.end())
                children.push_back(settings_argument);
        }
    }

    std::vector<std::pair<ASTPtr, ASTPtr>> preserved;
};

}
