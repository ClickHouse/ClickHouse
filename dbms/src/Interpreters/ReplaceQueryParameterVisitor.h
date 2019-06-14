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
    ReplaceQueryParameterVisitor(const NameToNameMap & parameters)
        : parameters_substitution(parameters)
    {}

    void visit(ASTPtr & ast);

private:
    const NameToNameMap parameters_substitution;
    String getParamValue(const String & name);
    void visitQueryParameters(ASTPtr & ast);
};

}
