#include <Parsers/QueryParameterVisitor.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>


namespace DB
{

class QueryParameterVisitor
{
public:
    explicit QueryParameterVisitor(NameToNameMap & parameters)
        : query_parameters(parameters)
    {
    }

    void visit(const ASTPtr & ast)
    {
        if (const auto & query_parameter = ast->as<ASTQueryParameter>())
            visitQueryParameter(*query_parameter);
        else
        {
            for (const auto & child : ast->children)
                visit(child);
        }
    }

private:
    NameToNameMap & query_parameters;

    void visitQueryParameter(const ASTQueryParameter & query_parameter)
    {
        query_parameters[query_parameter.name] = query_parameter.type;
    }
};


NameSet analyzeReceiveQueryParams(const std::string & query)
{
    NameToNameMap query_params;
    const char * query_begin = query.data();
    const char * query_end = query.data() + query.size();

    ParserQuery parser(query_end);
    ASTPtr extract_query_ast = parseQuery(parser, query_begin, query_end, "analyzeReceiveQueryParams", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    QueryParameterVisitor(query_params).visit(extract_query_ast);

    NameSet query_param_names;
    for (const auto & query_param : query_params)
        query_param_names.insert(query_param.first);
    return query_param_names;
}

NameSet analyzeReceiveQueryParams(const ASTPtr & ast)
{
    NameToNameMap query_params;
    QueryParameterVisitor(query_params).visit(ast);
    NameSet query_param_names;
    for (const auto & query_param : query_params)
        query_param_names.insert(query_param.first);
    return query_param_names;
}

NameToNameMap analyzeReceiveQueryParamsWithType(const ASTPtr & ast)
{
    NameToNameMap query_params;
    QueryParameterVisitor(query_params).visit(ast);
    return query_params;
}


}
