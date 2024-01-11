#include <IO/Operators.h>
#include <Parsers/ASTModifyEngineQuery.h>
#include <Common/quoteString.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNEXPECTED_AST_STRUCTURE;
}

/** Get the text that identifies this element. */
String ASTModifyEngineQuery::getID(char delim) const
{
    return "ModifyEngineQuery" + (delim + getDatabase()) + delim + getTable();
}

ASTPtr ASTModifyEngineQuery::clone() const
{
    auto res = std::make_shared<ASTModifyEngineQuery>(*this);
    res->children.clear();

    if (storage)
    {
        res->storage = storage->clone();
        res->children.push_back(res->storage);
    }

    return res;
}

void ASTModifyEngineQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    std::string indent_str = settings.one_line ? "" : std::string(4u * frame.indent, ' ');
    settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str;
    settings.ostr << "ALTER TABLE ";
    settings.ostr << (settings.hilite ? hilite_none : "");

    if (table)
    {
        if (database)
        {
            settings.ostr << indent_str << backQuoteIfNeed(getDatabase());
            settings.ostr << ".";
        }
        settings.ostr << indent_str << backQuoteIfNeed(getTable());
    }

    formatOnCluster(settings);

    settings.ostr << (settings.hilite ? hilite_keyword : "") << " MODIFY "
    << (settings.hilite ? hilite_none : "");

    storage->formatImpl(settings, state, frame);
}

}
