#include <Parsers/QueryParameterVisitor.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/FieldFromAST.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

#include <string_view>


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
        else if (const auto * set_query = ast->as<ASTSetQuery>())
            visitSetQuery(*set_query);
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

    /// A setting value can be a query parameter, e.g. `SETTINGS max_threads = {threads:UInt64}`.
    /// The parser stores it as an ASTQueryParameter wrapped into a Field (see ParserSetQuery),
    /// so it is not reachable via ast->children and must be discovered here explicitly.
    void visitSetQuery(const ASTSetQuery & set_query)
    {
        for (const auto & change : set_query.changes)
        {
            CustomType custom;
            if (!change.value.tryGet<CustomType>(custom) || std::string_view(custom.getTypeName()) != FieldFromASTImpl::name)
                continue;

            const auto & value_ast = dynamic_cast<const FieldFromASTImpl &>(custom.getImpl()).ast;
            if (const auto * query_parameter = value_ast->as<ASTQueryParameter>())
                visitQueryParameter(*query_parameter);
        }
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
