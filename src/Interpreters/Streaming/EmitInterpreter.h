#pragma once

#include <Parsers/IAST_fwd.h>
#include <Core/Settings.h>
#include <Poco/Logger.h>


/// Note: The `Rule` can be any executable object(function/pseudo-function/lambada)
/// with a required parameter —— (ASTPtr & query)
namespace DB
{
class ASTSelectQuery;

namespace Streaming
{
class EmitInterpreter final
{
public:
    template <typename... Rules>
    static void handleRules(ASTPtr & query, Rules &&... rules)
    {
        (rules(query), ...);
    }

public:

    /// To check emit ast
    static void checkEmitAST(ASTPtr & query);
};
}
}
