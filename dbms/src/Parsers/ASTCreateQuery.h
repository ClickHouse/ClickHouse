#pragma once

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{


/// CREATE TABLE or ATTACH TABLE query
class ASTCreateQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    bool attach{false};    /// Query ATTACH TABLE, not CREATE TABLE.
    bool if_not_exists{false};
    bool is_view{false};
    bool is_materialized_view{false};
    bool is_populate{false};
    bool is_temporary{false};
    String database;
    String table;
    ASTPtr columns;
    ASTPtr storage;
    ASTPtr inner_storage;    /// Internal engine for the CREATE MATERIALIZED VIEW query
    String as_database;
    String as_table;
    ASTPtr select;

    ASTCreateQuery() = default;
    ASTCreateQuery(const StringRange range_) : IAST(range_) {}

    /** Get the text that identifies this element. */
    String getID() const override { return (attach ? "AttachQuery_" : "CreateQuery_") + database + "_" + table; };

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTCreateQuery>(*this);
        res->children.clear();

        if (columns)        { res->columns = columns->clone();              res->children.push_back(res->columns); }
        if (storage)        { res->storage = storage->clone();              res->children.push_back(res->storage); }
        if (select)         { res->select = select->clone();                res->children.push_back(res->select); }
        if (inner_storage)  { res->inner_storage = inner_storage->clone();  res->children.push_back(res->inner_storage); }

        return res;
    }

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string & new_database) const override
    {
        auto query_ptr = clone();
        ASTCreateQuery & query = static_cast<ASTCreateQuery &>(*query_ptr);

        query.cluster.clear();
        if (query.database.empty())
            query.database = new_database;

        return query_ptr;
    }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        frame.need_parens = false;

        if (!database.empty() && table.empty())
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "")
                << (attach ? "ATTACH DATABASE " : "CREATE DATABASE ")
                << (if_not_exists ? "IF NOT EXISTS " : "")
                << (settings.hilite ? hilite_none : "")
                << backQuoteIfNeed(database);
            formatOnCluster(settings);

            if (storage)
            {
                settings.ostr << (settings.hilite ? hilite_keyword : "") << " ENGINE" << (settings.hilite ? hilite_none : "") << " = ";
                storage->formatImpl(settings, state, frame);
            }

            return;
        }

        {
            std::string what = "TABLE";
            if (is_view)
                what = "VIEW";
            if (is_materialized_view)
                what = "MATERIALIZED VIEW";

            settings.ostr
                << (settings.hilite ? hilite_keyword : "")
                    << (attach ? "ATTACH " : "CREATE ")
                    << (is_temporary ? "TEMPORARY " : "")
                    << what << " "
                    << (if_not_exists ? "IF NOT EXISTS " : "")
                << (settings.hilite ? hilite_none : "")
                << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);
                formatOnCluster(settings);
        }

        if (!as_table.empty())
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "")
            << (!as_database.empty() ? backQuoteIfNeed(as_database) + "." : "") << backQuoteIfNeed(as_table);
        }

        if (columns)
        {
            settings.ostr << (settings.one_line ? " (" : "\n(");
            FormatStateStacked frame_nested = frame;
            ++frame_nested.indent;
            columns->formatImpl(settings, state, frame_nested);
            settings.ostr << (settings.one_line ? ")" : "\n)");
        }

        if (storage && !is_materialized_view && !is_view)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " ENGINE" << (settings.hilite ? hilite_none : "") << " = ";
            storage->formatImpl(settings, state, frame);
        }

        if (inner_storage)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " ENGINE" << (settings.hilite ? hilite_none : "") << " = ";
            inner_storage->formatImpl(settings, state, frame);
        }

        if (is_populate)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " POPULATE" << (settings.hilite ? hilite_none : "");
        }

        if (select)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS" << settings.nl_or_ws << (settings.hilite ? hilite_none : "");
            select->formatImpl(settings, state, frame);
        }
    }
};

}
