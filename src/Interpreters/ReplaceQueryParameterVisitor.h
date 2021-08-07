#pragma once

#include <Core/Names.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context.h>

namespace DB
{

class ASTQueryParameter;

/// Visit substitutions in a query, replace ASTQueryParameter with ASTLiteral.
/// Rebuild ASTIdentifiers if some parts are ASTQueryParameter.
class ReplaceQueryParameterVisitor
{
public:
    ReplaceQueryParameterVisitor(ContextPtr context)
        : query_parameters(context->getQueryParameters())
        , settings(context->getSettingsRef())
    {}

    void visit(ASTPtr & ast);

private:
    const NameToNameMap & query_parameters;
    const Settings & settings;

    const String & getParamValue(const String & name);
    void visitIdentifier(ASTPtr & ast);
    void visitQueryParameter(ASTPtr & ast);
    void visitChildren(ASTPtr & ast);
};

}
