#pragma once

#include <Core/Names.h>
#include <Parsers/IAST.h>

namespace DB
{

class ASTQueryParameter;

class QueryParameterVisitor
{
public:
    QueryParameterVisitor(NameSet & parameters_name);

    void visit(const ASTPtr & ast);

private:
    NameSet & query_parameters;

    void visitQueryParameter(const ASTQueryParameter & query_parameter);
};

NameSet analyzeReceiveQueryParams(const std::string & query);

}
