#include <Parsers/ASTStatisticDeclaration.h>
#include <Parsers/ASTIdentifier.h>

#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTFunction.h>


namespace DB
{

ASTPtr ASTStatisticDeclaration::clone() const
{
    auto res = std::make_shared<ASTStatisticDeclaration>();

    res->set(res->columns, columns->clone());
    res->type = type;

    return res;
}

std::vector<String> ASTStatisticDeclaration::getColumnNames() const
{
    std::vector<String> result;
    result.reserve(columns->children.size());
    for (const ASTPtr & column_ast : columns->children)
    {
        result.push_back(column_ast->as<ASTIdentifier &>().name());
    }
    return result;

}

void ASTStatisticDeclaration::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    columns->formatImpl(s, state, frame);
    s.ostr << (s.hilite ? hilite_keyword : "") << " TYPE " << (s.hilite ? hilite_none : "");
    s.ostr << backQuoteIfNeed(type);
}

}

