#pragma once

#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Common/quoteString.h>


namespace DB
{

struct ASTCheckQuery : public ASTQueryWithTableAndOutput
{
    ASTPtr partition;

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "CheckQuery" + (delim + getDatabase()) + delim + getTable(); }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTCheckQuery>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        cloneTableOptions(*res);
        return res;
    }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        std::string indent_str = settings.isOneLine() ? "" : std::string(4 * frame.indent, ' ');

        settings.ostr << indent_str;
        settings.writeKeyword("CHECK TABLE ");

        if (table)
        {
            if (database)
            {
                settings.ostr << indent_str;
                settings.writeKeyword(backQuoteIfNeed(getDatabase()));
                settings.ostr << ".";
            }
            settings.ostr << indent_str;
            settings.writeKeyword(backQuoteIfNeed(getTable()));
        }

        if (partition)
        {
            settings.ostr << indent_str;
            settings.writeKeyword(" PARTITION ");
            partition->formatImpl(settings, state, frame);
        }
    }
};

}
