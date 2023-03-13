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

    QueryKind getQueryKind() const override { return QueryKind::Check; }

protected:
    void formatQueryImpl(const FormattingBuffer & out) const override
    {
        out.writeIndent();
        out.writeKeyword("CHECK TABLE ");

        if (table)
        {
            if (database)
            {
                out.writeIndent();
                out.writeKeyword(backQuoteIfNeed(getDatabase()));
                out.ostr << ".";
            }
            out.writeIndent();
            out.writeKeyword(backQuoteIfNeed(getTable()));
        }

        if (partition)
        {
            out.writeIndent();
            out.writeKeyword(" PARTITION ");
            partition->formatImpl(out);
        }
    }
};

}
