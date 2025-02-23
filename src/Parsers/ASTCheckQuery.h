#pragma once

#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Common/quoteString.h>


namespace DB
{

struct ASTCheckTableQuery : public ASTQueryWithTableAndOutput
{
    ASTPtr partition;
    String part_name;

    /** Get the text that identifies this element. */
    String getID(char delim) const override { return "CheckQuery" + (delim + getDatabase()) + delim + getTable(); }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTCheckTableQuery>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        cloneTableOptions(*res);
        return res;
    }

    QueryKind getQueryKind() const override { return QueryKind::Check; }

    std::variant<std::monostate, ASTPtr, String> getPartitionOrPartitionID() const
    {
        if (partition)
            return partition;
        if (!part_name.empty())
            return part_name;
        return std::monostate{};
    }

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
        ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "CHECK TABLE " << (settings.hilite ? hilite_none : "");

        if (table)
        {
            if (database)
            {
                database->formatImpl(ostr, settings, state, frame);
                ostr << '.';
            }

            chassert(table);
            table->formatImpl(ostr, settings, state, frame);
        }

        if (partition)
        {
            ostr << (settings.hilite ? hilite_keyword : "") << indent_str << " PARTITION " << (settings.hilite ? hilite_none : "");
            partition->formatImpl(ostr, settings, state, frame);
        }

        if (!part_name.empty())
        {
            ostr << (settings.hilite ? hilite_keyword : "") << indent_str << " PART " << (settings.hilite ? hilite_none : "")
                << quoteString(part_name);
        }
    }
};


struct ASTCheckAllTablesQuery : public ASTQueryWithOutput
{

    String getID(char /* delim */) const override { return "CheckAllQuery"; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTCheckAllTablesQuery>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        return res;
    }

    QueryKind getQueryKind() const override { return QueryKind::Check; }

protected:
    void formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & /* state */, FormatStateStacked frame) const override
    {
        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
        ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "CHECK ALL TABLES" << (settings.hilite ? hilite_none : "");
    }
};

}
