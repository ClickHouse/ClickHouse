#include <Parsers/ASTIntersectOrExcept.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>


namespace DB
{

ASTPtr ASTIntersectOrExcept::clone() const
{
    auto res = std::make_shared<ASTIntersectOrExcept>(*this);
    res->children.clear();

    res->list_of_selects = list_of_selects->clone();
    res->children.push_back(res->list_of_selects);
    res->list_of_modes = list_of_modes;

    cloneOutputOptions(*res);
    return res;
}

void ASTIntersectOrExcept::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    auto mode_to_str = [&](auto mode)
    {
        if (mode == Mode::INTERSECT)
            return "INTERSECT";
        else
            return "EXCEPT";
    };

    for (ASTs::const_iterator it = list_of_selects->children.begin(); it != list_of_selects->children.end(); ++it)
    {
        if (it != list_of_selects->children.begin())
        {
            settings.ostr << settings.nl_or_ws << indent_str << (settings.hilite ? hilite_keyword : "")
                          << mode_to_str(list_of_modes[it - list_of_selects->children.begin() - 1])
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
