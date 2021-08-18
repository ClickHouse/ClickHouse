#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Common/typeid_cast.h>
#include <IO/Operators.h>

#include <iostream>

namespace DB
{

ASTPtr ASTSelectWithUnionQuery::clone() const
{
    auto res = std::make_shared<ASTSelectWithUnionQuery>(*this);
    res->children.clear();

    res->list_of_selects = list_of_selects->clone();
    res->children.push_back(res->list_of_selects);

    res->union_mode = union_mode;

    res->list_of_modes = list_of_modes;
    res->set_of_modes = set_of_modes;

    cloneOutputOptions(*res);
    return res;
}


void ASTSelectWithUnionQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    auto mode_to_str = [&](auto mode)
    {
        if (mode == Mode::Unspecified)
            return "";
        else if (mode == Mode::ALL)
            return " ALL";
        else
            return " DISTINCT";
    };

    for (ASTs::const_iterator it = list_of_selects->children.begin(); it != list_of_selects->children.end(); ++it)
    {
        if (it != list_of_selects->children.begin())
            settings.ostr << settings.nl_or_ws << indent_str << (settings.hilite ? hilite_keyword : "") << "UNION"
                          << mode_to_str((is_normalized) ? union_mode : list_of_modes[it - list_of_selects->children.begin() - 1])
                          << (settings.hilite ? hilite_none : "");

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


bool ASTSelectWithUnionQuery::hasNonDefaultUnionMode() const
{
    return set_of_modes.contains(Mode::DISTINCT);
}

}
