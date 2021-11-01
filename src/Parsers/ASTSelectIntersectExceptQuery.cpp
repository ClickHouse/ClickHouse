#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>


namespace DB
{

ASTPtr ASTSelectIntersectExceptQuery::clone() const
{
    auto res = std::make_shared<ASTSelectIntersectExceptQuery>(*this);

    res->children.clear();
    for (const auto & child : children)
        res->children.push_back(child->clone());

    res->final_operator = final_operator;
    res->list_of_selects = list_of_selects->clone();
    return res;
}

void ASTSelectIntersectExceptQuery::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
        {
            settings.ostr << settings.nl_or_ws << indent_str << (settings.hilite ? hilite_keyword : "")
                          << (final_operator == Operator::INTERSECT ? "INTERSECT" : "EXCEPT")
                          << (settings.hilite ? hilite_none : "")
                          << settings.nl_or_ws;
        }

        (*it)->formatImpl(settings, state, frame);
    }
}

}
