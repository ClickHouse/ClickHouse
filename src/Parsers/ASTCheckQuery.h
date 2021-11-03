#pragma once

#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Common/quoteString.h>


namespace DB
{

struct ASTCheckQuery : public ASTQueryWithTableAndOutput
{

    ASTPtr partition;

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "CheckQuery" + (delim + database) + delim + table; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTCheckQuery>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        return res;
    }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        std::string nl_or_nothing = settings.one_line ? "" : "\n";

        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
        std::string nl_or_ws = settings.one_line ? " " : "\n";

        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "CHECK TABLE " << (settings.hilite ? hilite_none : "");

        if (!table.empty())
        {
            if (!database.empty())
            {
                settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << backQuoteIfNeed(database) << (settings.hilite ? hilite_none : "");
                settings.ostr << ".";
            }
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << backQuoteIfNeed(table) << (settings.hilite ? hilite_none : "");
        }

        if (partition)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << " PARTITION " << (settings.hilite ? hilite_none : "");
            partition->formatImpl(settings, state, frame);
        }
    }
};

}
