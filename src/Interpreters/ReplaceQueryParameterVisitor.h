#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ASTQueryParameter;

/// Visit substitutions in a query, replace ASTQueryParameter with ASTLiteral.
/// Rebuild ASTIdentifiers if some parts are ASTQueryParameter.
class ReplaceQueryParameterVisitor
{
public:
    explicit ReplaceQueryParameterVisitor(const NameToNameMap & parameters)
        : query_parameters(parameters)
    {}

    void visit(ASTPtr & ast);

    size_t getNumberOfReplacedParameters() const { return num_replaced_parameters; }

private:
    const NameToNameMap & query_parameters;
    size_t num_replaced_parameters = 0;

    const String & getParamValue(const String & name);
    void resolveParametrizedAlias(ASTPtr & ast);
    void visitIdentifier(ASTPtr & ast);
    void visitQueryParameter(ASTPtr & ast);
    void visitChildren(ASTPtr & ast);
};

}
