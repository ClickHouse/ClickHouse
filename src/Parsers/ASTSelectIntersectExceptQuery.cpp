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

    if (res->list_of_selects)
        res->list_of_selects = list_of_selects->clone();

    res->list_of_operators = list_of_operators;
    res->final_operator = final_operator;

    cloneOutputOptions(*res);
    return res;
}

void ASTSelectIntersectExceptQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    auto operator_to_str = [&](auto current_operator)
    {
        if (current_operator == Operator::INTERSECT)
            return "INTERSECT";
        else
            return "EXCEPT";
    };

    for (ASTs::const_iterator it = list_of_selects->children.begin(); it != list_of_selects->children.end(); ++it)
    {
        if (it != list_of_selects->children.begin())
        {
            settings.ostr << settings.nl_or_ws << indent_str << (settings.hilite ? hilite_keyword : "")
                          << operator_to_str(list_of_operators[it - list_of_selects->children.begin() - 1])
                          << (settings.hilite ? hilite_none : "");
        }

        if (auto * node = (*it)->as<ASTSelectWithUnionQuery>())
        {
            settings.ostr << settings.nl_or_ws << indent_str;

            if (node->list_of_selects->children.size() == 1)
            {
                (node->list_of_selects->children.at(0))->formatImpl(settings, state, frame);
            }
            else
            {
                auto sub_query = std::make_shared<ASTSubquery>();
                sub_query->children.push_back(*it);
                sub_query->formatImpl(settings, state, frame);
            }
        }
        else
        {
            if (it != list_of_selects->children.begin())
                settings.ostr << settings.nl_or_ws;
            (*it)->formatImpl(settings, state, frame);
        }
    }
}

}
