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
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        std::string nl_or_nothing = settings.one_line ? "" : "\n";

        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
        std::string nl_or_ws = settings.one_line ? " " : "\n";

        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "CHECK TABLE " << (settings.hilite ? hilite_none : "");

        if (table)
        {
            if (database)
            {
                settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << backQuoteIfNeed(getDatabase()) << (settings.hilite ? hilite_none : "");
                settings.ostr << ".";
            }
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << backQuoteIfNeed(getTable()) << (settings.hilite ? hilite_none : "");
        }

        if (partition)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << " PARTITION " << (settings.hilite ? hilite_none : "");
            partition->formatImpl(settings, state, frame);
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
    void formatQueryImpl(const FormatSettings & settings, FormatState & /* state */, FormatStateStacked frame) const override
    {
        std::string nl_or_nothing = settings.one_line ? "" : "\n";

        std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');
        std::string nl_or_ws = settings.one_line ? " " : "\n";

        settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "CHECK ALL TABLES" << (settings.hilite ? hilite_none : "");
    }
};

}
