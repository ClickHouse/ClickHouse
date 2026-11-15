#include <Parsers/ASTCreateFunctionWithDriverQuery.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTNameTypePair.h>

#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

String ASTCreateFunctionWithDriverQuery::getID(char delim) const
{
    return fmt::format("CreateFunctionWithDriverQuery{}{}", delim, getFunctionName());
}

String ASTCreateFunctionWithDriverQuery::getFunctionName() const
{
    String name;
    if (!tryGetIdentifierNameInto(function_name_ast, name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected function name, got '{}'", function_name_ast->formatForErrorMessage());
    return name;
}

ASTPtr ASTCreateFunctionWithDriverQuery::clone() const
{
    auto res = make_intrusive<ASTCreateFunctionWithDriverQuery>(*this);
    res->children.clear();
    res->engine_arguments.clear();

    res->function_name_ast = function_name_ast->clone();
    res->children.push_back(res->function_name_ast);

    if (arguments_ast)
    {
        res->arguments_ast = arguments_ast->clone();
        res->children.push_back(res->arguments_ast);
    }
    if (return_type_ast)
    {
        res->return_type_ast = return_type_ast->clone();
        res->children.push_back(res->return_type_ast);
    }
    for (const auto & [name, value] : engine_arguments)
    {
        auto cloned_value = value->clone();
        res->children.push_back(cloned_value);
        res->engine_arguments.emplace_back(name, cloned_value);
    }
    return res;
}

void ASTCreateFunctionWithDriverQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (is_attach)
        ostr << "ATTACH ";
    else
        ostr << "CREATE ";

    if (or_replace)
        ostr << "OR REPLACE ";

    ostr << "FUNCTION ";

    if (if_not_exists)
        ostr << "IF NOT EXISTS ";

    if (function_name_ast)
        function_name_ast->format(ostr, settings, state, frame);

    formatOnCluster(ostr, settings);

    if (arguments_ast)
    {
        ostr << " ARGUMENTS (";
        arguments_ast->format(ostr, settings, state, frame);
        ostr << ")";
    }

    if (return_type_ast)
    {
        ostr << " RETURNS ";
        return_type_ast->format(ostr, settings, state, frame);
    }

    ostr << " ENGINE = " << backQuoteIfNeed(engine_name) << "(";
    bool first = true;
    for (const auto & [name, value] : engine_arguments)
    {
        if (!first)
            ostr << ", ";
        first = false;
        ostr << backQuoteIfNeed(name) << " = ";
        value->format(ostr, settings, state, frame);
    }
    ostr << ")";

    ostr << " AS " << quoteString(source_code);
}

}
