#pragma once

#include <Core/Names.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ASTQueryParameter;

/// Visit substitutions in a query, replace ASTQueryParameter with ASTLiteral.
class ReplaceQueryParameterVisitor
{
public:
    ReplaceQueryParameterVisitor(const NameToNameMap & parameters)
        : query_parameters(parameters)
    {}

    void visit(ASTPtr & ast);

private:
    const NameToNameMap & query_parameters;
    const String & getParamValue(const String & name);
    void visitQueryParameter(ASTPtr & ast);
    void visitChildren(ASTPtr & ast);
};

}
