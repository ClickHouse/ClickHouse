#pragma once

#include <Core/Names.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ASTQueryParameter;

/// Get prepared statements in query, replace ASTQueryParameter with ASTLiteral.
class ReplaceQueryParameterVisitor
{
public:
    ReplaceQueryParameterVisitor(const NameToNameMap & params)
    :   params_substitution(params)
    {}

    void visit(ASTPtr & ast);

private:
    const NameToNameMap params_substitution;
    void visitQP(ASTPtr & ast);
    String getParamValue(const String & name);
};

}
