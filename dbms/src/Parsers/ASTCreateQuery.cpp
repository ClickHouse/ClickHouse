#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTCreateQuery.h>


namespace DB
{


String ASTDictionaryRange::getID(char) const
{
    return "Dictionary range";
}


ASTPtr ASTDictionaryRange::clone() const
{
    auto res = std::make_shared<ASTDictionaryRange>(*this);
    res->children.clear();
    res->min_column_name = min_column_name;
    res->max_column_name = max_column_name;
    return res;
}


void ASTDictionaryRange::formatImpl(const FormatSettings & settings,
                                    FormatState &,
                                    FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "RANGE"
                  << (settings.hilite ? hilite_none : "")
                  << "("
                  << (settings.hilite ? hilite_keyword : "")
                  << "MIN "
                  << (settings.hilite ? hilite_none : "")
                  << min_column_name << ", "
                  << (settings.hilite ? hilite_keyword : "")
                  << "MAX "
                  << (settings.hilite ? hilite_none : "")
                  << max_column_name << ")";
}


String ASTDictionaryLifetime::getID(char) const
{
    return "Dictionary lifetime";
}


ASTPtr ASTDictionaryLifetime::clone() const
{
    auto res = std::make_shared<ASTDictionaryLifetime>(*this);
    res->children.clear();
    res->min_sec = min_sec;
    res->max_sec = max_sec;
    return res;
}


void ASTDictionaryLifetime::formatImpl(const FormatSettings & settings,
                                       FormatState &,
                                       FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "LIFETIME"
                  << (settings.hilite ? hilite_none : "")
                  << "("
                  << (settings.hilite ? hilite_keyword : "")
                  << "MIN "
                  << (settings.hilite ? hilite_none : "")
                  << min_sec << ", "
                  << (settings.hilite ? hilite_keyword : "")
                  << "MAX "
                  << (settings.hilite ? hilite_none : "")
                  << max_sec << ")";
}


String ASTDictionarySource::getID(char) const
{
    return "Source definition";
}


ASTPtr ASTDictionarySource::clone() const
{
    auto res = std::make_shared<ASTDictionarySource>(*this);
    res->children.clear();

    if (source)
        res->set(res->source, source->clone());

    if (primary_key)
        res->set(res->primary_key, primary_key->clone());

    if (composite_key)
        res->set(res->composite_key, composite_key->clone());

    if (lifetime)
        res->set(res->lifetime, lifetime->clone());

    if (layout)
        res->set(res->layout, layout->clone());

    if (range)
        res->set(res->range, range->clone());

    return res;
}

void ASTDictionarySource::formatImpl(const FormatSettings & settings,
                                     FormatState & state,
                                     FormatStateStacked frame) const
{
    if (primary_key)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws
                      << "PRIMARY KEY " << (settings.hilite ? hilite_none : "");
        primary_key->formatImpl(settings, state, frame);
    }

    if (composite_key)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws
                      << "COMPOSITE KEY " << (settings.hilite ? hilite_none : "");
        composite_key->formatImpl(settings, state, frame);
    }

    if (source)
    {
        settings.ostr << settings.nl_or_ws;
        source->formatImpl(settings, state, frame);
    }

    if (lifetime)
    {
        settings.ostr << settings.nl_or_ws;
        lifetime->formatImpl(settings, state, frame);
    }

    if (layout)
    {
        settings.ostr << settings.nl_or_ws;
        layout->formatImpl(settings, state, frame);
    }

    if (range)
    {
        settings.ostr << settings.nl_or_ws;
        range->formatImpl(settings, state, frame);
    }
}

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

    frame.need_parens = false;
    std::string indent_str = s.one_line ? "" : std::string(4 * frame.indent, ' ');

    s.ostr << s.nl_or_ws << indent_str;
    s.ostr << (s.hilite ? hilite_keyword : "") << prefix << (s.hilite ? hilite_none : "");

    FormatSettings nested_settings = s;
    nested_settings.one_line = true;
    nested_settings.nl_or_ws = ' ';

    elem->formatImpl(nested_settings, state, frame);
}


ASTPtr ASTColumns::clone() const
{
    auto res = std::make_shared<ASTColumns>();

    if (columns)
        res->set(res->columns, columns->clone());
    if (indices)
        res->set(res->indices, indices->clone());

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

    if (!list.children.empty())
        list.formatImpl(s, state, frame);
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
    if (dictionary_source)
        res->set(res->dictionary_source, dictionary_source->clone());

    cloneOutputOptions(*res);

    return res;
}

void ASTCreateQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    if (!database.empty() && table.empty() && dictionary.empty())
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

    if (!table.empty())
    {
        std::string what = "TABLE";
        if (is_view)
            what = "VIEW";
        if (is_materialized_view)
            what = "MATERIALIZED VIEW";

        settings.ostr
            << (settings.hilite ? hilite_keyword : "")
                << (attach ? "ATTACH " : "CREATE ")
                << (temporary ? "TEMPORARY " : "")
                << (replace_view ? "OR REPLACE " : "")
                << what << " "
                << (if_not_exists ? "IF NOT EXISTS " : "")
            << (settings.hilite ? hilite_none : "")
            << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);
            formatOnCluster(settings);
    }

    if (!dictionary.empty())
    {
        settings.ostr
            << (settings.hilite ? hilite_keyword : "")
            << "CREATE DICTIONARY "
            << (if_not_exists ? "IF NOT EXISTS " : "")
            << (settings.hilite ? hilite_none : "")
            << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(dictionary);
    }

    if (!to_table.empty())
    {
        settings.ostr
            << (settings.hilite ? hilite_keyword : "") << " TO " << (settings.hilite ? hilite_none : "")
            << (!to_database.empty() ? backQuoteIfNeed(to_database) + "." : "") << backQuoteIfNeed(to_table);
    }

    if (!as_table.empty())
    {
        settings.ostr
            << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "")
            << (!as_database.empty() ? backQuoteIfNeed(as_database) + "." : "") << backQuoteIfNeed(as_table);
    }

    if (columns_list)
    {
        settings.ostr << (settings.one_line ? " (" : "\n(");
        FormatStateStacked frame_nested = frame;
        ++frame_nested.indent;
        columns_list->formatImpl(settings, state, frame_nested);
        settings.ostr << (settings.one_line ? ")" : "\n)");
    }

    if (storage)
        storage->formatImpl(settings, state, frame);

    if (is_populate)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " POPULATE" << (settings.hilite ? hilite_none : "");
    }

    if (select)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS" << settings.nl_or_ws << (settings.hilite ? hilite_none : "");
        select->formatImpl(settings, state, frame);
    }

    if (dictionary_source)
        dictionary_source->formatImpl(settings, state, frame);
}

}

