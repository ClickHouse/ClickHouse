#include <Parsers/ASTStatisticsDeclaration.h>
#include <Parsers/ASTIdentifier.h>

#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTFunction.h>


namespace DB
{

ASTPtr ASTStatisticsDeclaration::clone() const
{
    auto res = std::make_shared<ASTStatisticsDeclaration>();

    res->set(res->columns, columns->clone());
    if (types)
        res->set(res->types, types->clone());

    return res;
}

std::vector<String> ASTStatisticsDeclaration::getColumnNames() const
{
    std::vector<String> result;
    result.reserve(columns->children.size());
    for (const ASTPtr & column_ast : columns->children)
    {
        result.push_back(column_ast->as<ASTIdentifier &>().name());
    }
    return result;

}

std::vector<String> ASTStatisticsDeclaration::getTypeNames() const
{
    chassert(types != nullptr);
    std::vector<String> result;
    result.reserve(types->children.size());
    for (const ASTPtr & column_ast : types->children)
    {
        result.push_back(column_ast->as<ASTFunction &>().name);
    }
    return result;

}

void ASTStatisticsDeclaration::formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    columns->formatImpl(ostr, s, state, frame);
    ostr << (s.hilite ? hilite_keyword : "");
    if (types)
    {
        ostr << " TYPE " << (s.hilite ? hilite_none : "");
        types->formatImpl(ostr, s, state, frame);
    }
}

}

