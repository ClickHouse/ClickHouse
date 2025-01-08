#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Common/typeid_cast.h>
#include <Parsers/SelectUnionMode.h>
#include <IO/Operators.h>
#include <Parsers/ASTSelectQuery.h>


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


void ASTSelectWithUnionQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    auto mode_to_str = [&](auto mode)
    {
        if (mode == SelectUnionMode::UNION_DEFAULT)
            return "UNION";
        if (mode == SelectUnionMode::UNION_ALL)
            return "UNION ALL";
        if (mode == SelectUnionMode::UNION_DISTINCT)
            return "UNION DISTINCT";
        if (mode == SelectUnionMode::EXCEPT_DEFAULT)
            return "EXCEPT";
        if (mode == SelectUnionMode::EXCEPT_ALL)
            return "EXCEPT ALL";
        if (mode == SelectUnionMode::EXCEPT_DISTINCT)
            return "EXCEPT DISTINCT";
        if (mode == SelectUnionMode::INTERSECT_DEFAULT)
            return "INTERSECT";
        if (mode == SelectUnionMode::INTERSECT_ALL)
            return "INTERSECT ALL";
        if (mode == SelectUnionMode::INTERSECT_DISTINCT)
            return "INTERSECT DISTINCT";
        return "";
    };

    for (ASTs::const_iterator it = list_of_selects->children.begin(); it != list_of_selects->children.end(); ++it)
    {
        if (it != list_of_selects->children.begin())
            ostr << settings.nl_or_ws << indent_str << (settings.hilite ? hilite_keyword : "")
                          << mode_to_str((is_normalized) ? union_mode : list_of_modes[it - list_of_selects->children.begin() - 1])
                          << (settings.hilite ? hilite_none : "");

        if (auto * /*node*/ _ = (*it)->as<ASTSelectWithUnionQuery>())
        {
            if (it != list_of_selects->children.begin())
                ostr << settings.nl_or_ws;

            ostr << indent_str;
            auto sub_query = std::make_shared<ASTSubquery>(*it);
            sub_query->formatImpl(ostr, settings, state, frame);
        }
        else
        {
            if (it != list_of_selects->children.begin())
                ostr << settings.nl_or_ws;
            (*it)->formatImpl(ostr, settings, state, frame);
        }
    }
}


bool ASTSelectWithUnionQuery::hasNonDefaultUnionMode() const
{
    return set_of_modes.contains(SelectUnionMode::UNION_DISTINCT) || set_of_modes.contains(SelectUnionMode::INTERSECT_DISTINCT)
        || set_of_modes.contains(SelectUnionMode::EXCEPT_DISTINCT);
}

bool ASTSelectWithUnionQuery::hasQueryParameters() const
{
    if (!has_query_parameters.has_value())
    {
        for (const auto & child : list_of_selects->children)
        {
            if (auto * select_node = child->as<ASTSelectQuery>())
            {
                if (select_node->hasQueryParameters())
                {
                    has_query_parameters = true;
                    return has_query_parameters.value();
                }
            }
        }
        has_query_parameters = false;
    }

    return  has_query_parameters.value();
}

}
