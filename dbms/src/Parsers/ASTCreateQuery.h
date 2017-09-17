#pragma once

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

class ASTStorage : public IAST
{
public:
    ASTFunction * engine = nullptr;

    ASTStorage() = default;
    ASTStorage(StringRange range_) : IAST(range_) {}
    String getID() const override { return "Storage definition"; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTStorage>(*this);
        res->children.clear();

        if (engine)
            res->set(res->engine, engine->clone());

        return res;
    }

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override
    {
        if (engine)
        {
            s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "ENGINE" << (s.hilite ? hilite_none : "") << " = ";
            engine->formatImpl(s, state, frame);
        }
    }
};


/// CREATE TABLE or ATTACH TABLE query
class ASTCreateQuery : public ASTQueryWithOutput, public ASTQueryWithOnCluster
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
    ASTExpressionList * columns = nullptr;
    ASTStorage * storage = nullptr;
    ASTStorage * inner_storage = nullptr; /// Internal engine for the CREATE MATERIALIZED VIEW query
    String as_database;
    String as_table;
    ASTSelectQuery * select = nullptr;

    ASTCreateQuery() = default;
    ASTCreateQuery(const StringRange range_) : ASTQueryWithOutput(range_) {}

    /** Get the text that identifies this element. */
    String getID() const override { return (attach ? "AttachQuery_" : "CreateQuery_") + database + "_" + table; };

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTCreateQuery>(*this);
        res->children.clear();

        if (columns)
            res->set(res->columns, columns->clone());
        if (storage)
            res->set(res->storage, storage->clone());
        if (select)
            res->set(res->select, select->clone());
        if (inner_storage)
            res->set(res->inner_storage, inner_storage->clone());

        cloneOutputOptions(*res);

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
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
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
                storage->formatImpl(settings, state, frame);

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
            storage->formatImpl(settings, state, frame);

        if (inner_storage)
            inner_storage->formatImpl(settings, state, frame);

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
