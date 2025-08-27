#include <Parsers/ASTUpdateQuery.h>
#include <Common/quoteString.h>

namespace DB
{

String ASTUpdateQuery::getID(char delim) const
{
    return "UpdateQuery" + (delim + getDatabase()) + delim + getTable();
}

ASTPtr ASTUpdateQuery::clone() const
{
    auto res = std::make_shared<ASTUpdateQuery>(*this);
    res->children.clear();

    const auto add_children_if_needed = [&](const auto & src, auto & dst)
    {
        if (!src)
            return;

        dst = src->clone();
        res->children.push_back(dst);
    };

    add_children_if_needed(partition, res->partition);
    add_children_if_needed(predicate, res->predicate);
    add_children_if_needed(assignments, res->assignments);
    add_children_if_needed(settings_ast, res->settings_ast);

    cloneTableOptions(*res);
    return res;
}

void ASTUpdateQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << (settings.hilite ? hilite_keyword : "") << "UPDATE " << (settings.hilite ? hilite_none : "");

    if (database)
    {
        ostr << backQuoteIfNeed(getDatabase());
        ostr << ".";
    }

    ostr << backQuoteIfNeed(getTable());
    formatOnCluster(ostr, settings);

    ostr << (settings.hilite ? hilite_keyword : "") << " SET " << (settings.hilite ? hilite_none : "");
    assignments->format(ostr, settings, state, frame);

    if (partition)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
        partition->format(ostr, settings, state, frame);
    }

    ostr << (settings.hilite ? hilite_keyword : "") << " WHERE " << (settings.hilite ? hilite_none : "");
    predicate->format(ostr, settings, state, frame);
}

}
