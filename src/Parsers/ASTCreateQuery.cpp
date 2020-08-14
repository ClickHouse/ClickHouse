#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Common/quoteString.h>
#include <Interpreters/StorageID.h>


namespace DB
{

ASTPtr ASTStorage::clone() const
{
    auto res = std::make_shared<ASTStorage>(*this);
    res->children.clear();

    if (engine)
        res->set(res->engine, engine->clone());
    if (partition_by)
        res->set(res->partition_by, partition_by->clone());
    if (primary_key)
        res->set(res->primary_key, primary_key->clone());
    if (order_by)
        res->set(res->order_by, order_by->clone());
    if (sample_by)
        res->set(res->sample_by, sample_by->clone());
    if (ttl_table)
        res->set(res->ttl_table, ttl_table->clone());

    if (settings)
        res->set(res->settings, settings->clone());

    return res;
}

void ASTStorage::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    if (engine)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "ENGINE" << (s.hilite ? hilite_none : "") << " = ";
        engine->formatImpl(s, state, frame);
    }
    if (partition_by)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "PARTITION BY " << (s.hilite ? hilite_none : "");
        partition_by->formatImpl(s, state, frame);
    }
    if (primary_key)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "PRIMARY KEY " << (s.hilite ? hilite_none : "");
        primary_key->formatImpl(s, state, frame);
    }
    if (order_by)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "ORDER BY " << (s.hilite ? hilite_none : "");
        order_by->formatImpl(s, state, frame);
    }
    if (sample_by)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "SAMPLE BY " << (s.hilite ? hilite_none : "");
        sample_by->formatImpl(s, state, frame);
    }
    if (ttl_table)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "TTL " << (s.hilite ? hilite_none : "");
        ttl_table->formatImpl(s, state, frame);
    }
    if (settings)
    {
        s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "SETTINGS " << (s.hilite ? hilite_none : "");
        settings->formatImpl(s, state, frame);
    }

}


class ASTColumnsElement : public IAST
{
public:
    String prefix;
    IAST * elem;

    String getID(char c) const override { return "ASTColumnsElement for " + elem->getID(c); }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

ASTPtr ASTColumnsElement::clone() const
{
    auto res = std::make_shared<ASTColumnsElement>();
    res->prefix = prefix;
    if (elem)
        res->set(res->elem, elem->clone());
    return res;
}

void ASTColumnsElement::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    if (!elem)
        return;

    if (prefix.empty())
    {
        elem->formatImpl(s, state, frame);
        return;
    }

    s.ostr << (s.hilite ? hilite_keyword : "") << prefix << (s.hilite ? hilite_none : "");
    s.ostr << ' ';
    elem->formatImpl(s, state, frame);
}


ASTPtr ASTColumns::clone() const
{
    auto res = std::make_shared<ASTColumns>();

    if (columns)
        res->set(res->columns, columns->clone());
    if (indices)
        res->set(res->indices, indices->clone());
    if (constraints)
        res->set(res->constraints, constraints->clone());

    return res;
}

void ASTColumns::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    ASTExpressionList list;

    if (columns)
    {
        for (const auto & column : columns->children)
        {
            auto elem = std::make_shared<ASTColumnsElement>();
            elem->prefix = "";
            elem->set(elem->elem, column->clone());
            list.children.push_back(elem);
        }
    }
    if (indices)
    {
        for (const auto & index : indices->children)
        {
            auto elem = std::make_shared<ASTColumnsElement>();
            elem->prefix = "INDEX";
            elem->set(elem->elem, index->clone());
            list.children.push_back(elem);
        }
    }
    if (constraints)
    {
        for (const auto & constraint : constraints->children)
        {
            auto elem = std::make_shared<ASTColumnsElement>();
            elem->prefix = "CONSTRAINT";
            elem->set(elem->elem, constraint->clone());
            list.children.push_back(elem);
        }
    }

    if (!list.children.empty())
    {
        if (s.one_line)
            list.formatImpl(s, state, frame);
        else
            list.formatImplMultiline(s, state, frame);
    }
}


ASTPtr ASTCreateQuery::clone() const
{
    auto res = std::make_shared<ASTCreateQuery>(*this);
    res->children.clear();

    if (columns_list)
        res->set(res->columns_list, columns_list->clone());
    if (storage)
        res->set(res->storage, storage->clone());
    if (select)
        res->set(res->select, select->clone());
    if (tables)
        res->set(res->tables, tables->clone());
    if (dictionary)
        res->set(res->dictionary, dictionary->clone());

    cloneOutputOptions(*res);

    return res;
}

void ASTCreateQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
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

    if (!is_dictionary)
    {
        std::string what = "TABLE";
        if (is_view)
            what = "VIEW";
        if (is_materialized_view)
            what = "MATERIALIZED VIEW";
        if (is_live_view)
            what = "LIVE VIEW";

        settings.ostr
            << (settings.hilite ? hilite_keyword : "")
                << (attach ? "ATTACH " : "CREATE ")
                << (temporary ? "TEMPORARY " : "")
                << (replace_view ? "OR REPLACE " : "")
                << what << " "
                << (if_not_exists ? "IF NOT EXISTS " : "")
            << (settings.hilite ? hilite_none : "")
            << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);

        if (uuid != UUIDHelpers::Nil)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " UUID " << (settings.hilite ? hilite_none : "")
                          << quoteString(toString(uuid));
        if (live_view_timeout)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " WITH TIMEOUT " << (settings.hilite ? hilite_none : "")
                          << *live_view_timeout;
        formatOnCluster(settings);
    }
    else
    {
        /// Always DICTIONARY
        settings.ostr << (settings.hilite ? hilite_keyword : "") << (attach ? "ATTACH " : "CREATE ") << "DICTIONARY "
                      << (if_not_exists ? "IF NOT EXISTS " : "") << (settings.hilite ? hilite_none : "")
                      << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);
        formatOnCluster(settings);
    }

    if (as_table_function)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "");
        as_table_function->formatImpl(settings, state, frame);
    }
    if (to_table_id)
    {
        settings.ostr
            << (settings.hilite ? hilite_keyword : "") << " TO " << (settings.hilite ? hilite_none : "")
            << (!to_table_id.database_name.empty() ? backQuoteIfNeed(to_table_id.database_name) + "." : "")
            << backQuoteIfNeed(to_table_id.table_name);
    }

    if (!as_table.empty())
    {
        settings.ostr
            << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "")
            << (!as_database.empty() ? backQuoteIfNeed(as_database) + "." : "") << backQuoteIfNeed(as_table);
    }

    frame.expression_list_always_start_on_new_line = true;

    if (columns_list)
    {
        settings.ostr << (settings.one_line ? " (" : "\n(");
        FormatStateStacked frame_nested = frame;
        columns_list->formatImpl(settings, state, frame_nested);
        settings.ostr << (settings.one_line ? ")" : "\n)");
    }

    if (dictionary_attributes_list)
    {
        settings.ostr << (settings.one_line ? " (" : "\n(");
        FormatStateStacked frame_nested = frame;
        if (settings.one_line)
            dictionary_attributes_list->formatImpl(settings, state, frame_nested);
        else
            dictionary_attributes_list->formatImplMultiline(settings, state, frame_nested);
        settings.ostr << (settings.one_line ? ")" : "\n)");
    }

    frame.expression_list_always_start_on_new_line = false;

    if (storage)
        storage->formatImpl(settings, state, frame);

    if (dictionary)
        dictionary->formatImpl(settings, state, frame);

    if (is_populate)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " POPULATE" << (settings.hilite ? hilite_none : "");

    if (select)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS" << settings.nl_or_ws << (settings.hilite ? hilite_none : "");
        select->formatImpl(settings, state, frame);
    }

    if (tables)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " WITH " << (settings.hilite ? hilite_none : "");
        tables->formatImpl(settings, state, frame);
    }
}

}
