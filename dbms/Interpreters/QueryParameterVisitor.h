#pragma once

#include <Core/Names.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTQueryParameter.h>

namespace DB
{

class QueryParameterVisitor
{
public:
    QueryParameterVisitor(NameSet & parameters_name) : query_parameters(parameters_name) {}

    void visit(const ASTPtr & ast)
    {
        for (const auto & child : ast->children)
        {
            if (const auto & query_parameter = child->as<ASTQueryParameter>())
                visitQueryParameter(*query_parameter);
            else
                visit(child);
        }
    }

private:
    NameSet & query_parameters;

    void visitQueryParameter(const ASTQueryParameter & query_parameter)
    {
        query_parameters.insert(query_parameter.name);
    }
};

}
