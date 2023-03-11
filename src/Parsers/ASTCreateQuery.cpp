#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Common/quoteString.h>
#include <Interpreters/StorageID.h>
#include <IO/Operators.h>


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

void ASTStorage::formatImpl(const FormattingBuffer & out) const
{
    if (engine)
    {
        out.nlOrWs();
        out.writeKeyword("ENGINE");
        out.ostr << " = ";
        engine->formatImpl(out);
    }
    if (partition_by)
    {
        out.nlOrWs();
        out.writeKeyword("PARTITION BY ");
        partition_by->formatImpl(out);
    }
    if (primary_key)
    {
        out.nlOrWs();
        out.writeKeyword("PRIMARY KEY ");
        primary_key->formatImpl(out);
    }
    if (order_by)
    {
        out.nlOrWs();
        out.writeKeyword("ORDER BY ");
        order_by->formatImpl(out);
    }
    if (sample_by)
    {
        out.nlOrWs();
        out.writeKeyword("SAMPLE BY ");
        sample_by->formatImpl(out);
    }
    if (ttl_table)
    {
        out.nlOrWs();
        out.writeKeyword("TTL ");
        ttl_table->formatImpl(out);
    }
    if (settings)
    {
        out.nlOrWs();
        out.writeKeyword("SETTINGS ");
        settings->formatImpl(out);
    }
}


class ASTColumnsElement : public IAST
{
public:
    String prefix;
    IAST * elem;

    String getID(char c) const override { return "ASTColumnsElement for " + elem->getID(c); }

    ASTPtr clone() const override;

    void formatImpl(const FormattingBuffer & out) const override;
};

ASTPtr ASTColumnsElement::clone() const
{
    auto res = std::make_shared<ASTColumnsElement>();
    res->prefix = prefix;
    if (elem)
        res->set(res->elem, elem->clone());
    return res;
}

void ASTColumnsElement::formatImpl(const FormattingBuffer & out) const
{
    if (!elem)
        return;

    if (prefix.empty())
    {
        elem->formatImpl(out);
        return;
    }

    out.writeKeyword(prefix);
    out.ostr << ' ';
    elem->formatImpl(out);
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
    if (projections)
        res->set(res->projections, projections->clone());
    if (primary_key)
        res->set(res->primary_key, primary_key->clone());

    return res;
}

void ASTColumns::formatImpl(const FormattingBuffer & out) const
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
    if (projections)
    {
        for (const auto & projection : projections->children)
        {
            auto elem = std::make_shared<ASTColumnsElement>();
            elem->prefix = "PROJECTION";
            elem->set(elem->elem, projection->clone());
            list.children.push_back(elem);
        }
    }

    if (!list.children.empty())
        out.isOneLine() ? list.formatImpl(out) : list.formatImplMultiline(out);
}


ASTPtr ASTCreateQuery::clone() const
{
    auto res = std::make_shared<ASTCreateQuery>(*this);
    res->children.clear();

    if (columns_list)
        res->set(res->columns_list, columns_list->clone());
    if (storage)
        res->set(res->storage, storage->clone());
    if (inner_storage)
        res->set(res->inner_storage, inner_storage->clone());
    if (select)
        res->set(res->select, select->clone());
    if (table_overrides)
        res->set(res->table_overrides, table_overrides->clone());

    if (dictionary)
    {
        assert(is_dictionary);
        res->set(res->dictionary_attributes_list, dictionary_attributes_list->clone());
        res->set(res->dictionary, dictionary->clone());
    }

    if (as_table_function)
        res->set(res->as_table_function, as_table_function->clone());
    if (comment)
        res->set(res->comment, comment->clone());

    cloneOutputOptions(*res);
    cloneTableOptions(*res);

    return res;
}

void ASTCreateQuery::formatQueryImpl(const FormattingBuffer & out) const
{
    out.setNeedsParens(false);

    if (database && !table)
    {
        out.writeKeyword(attach ? "ATTACH DATABASE " : "CREATE DATABASE ");
        out.writeKeyword(if_not_exists ? "IF NOT EXISTS " : "");
        out.ostr << backQuoteIfNeed(getDatabase());

        if (uuid != UUIDHelpers::Nil)
        {
            out.writeKeyword(" UUID ");
            out.ostr << quoteString(toString(uuid));
        }

        formatOnCluster(out);

        if (storage)
            storage->formatImpl(out);

        if (table_overrides)
        {
            out.nlOrWs();
            table_overrides->formatImpl(out);
        }

        if (comment)
        {
            out.nlOrWs();
            out.writeKeyword("COMMENT ");
            comment->formatImpl(out);
        }

        return;
    }

    if (!is_dictionary)
    {
        String action = "CREATE";
        if (attach)
            action = "ATTACH";
        else if (replace_view)
            action = "CREATE OR REPLACE";
        else if (replace_table && create_or_replace)
            action = "CREATE OR REPLACE";
        else if (replace_table)
            action = "REPLACE";

        String what = "TABLE";
        if (is_ordinary_view)
            what = "VIEW";
        else if (is_materialized_view)
            what = "MATERIALIZED VIEW";
        else if (is_live_view)
            what = "LIVE VIEW";
        else if (is_window_view)
            what = "WINDOW VIEW";

        out.writeKeyword(action);
        out.ostr << " ";
        out.writeKeyword(temporary ? "TEMPORARY " : "");
        out.writeKeyword(what);
        out.ostr << " ";
        out.writeKeyword(if_not_exists ? "IF NOT EXISTS " : "");
        out.ostr << (database ? backQuoteIfNeed(getDatabase()) + "." : "") << backQuoteIfNeed(getTable());

        if (uuid != UUIDHelpers::Nil)
        {
            out.writeKeyword(" UUID ");
            out.ostr << quoteString(toString(uuid));
        }

        assert(attach || !attach_from_path);
        if (attach_from_path)
        {
            out.writeKeyword(" FROM ");
            out.ostr << quoteString(*attach_from_path);
        }

        if (live_view_periodic_refresh)
        {
            out.writeKeyword(" WITH");
            out.writeKeyword(" PERIODIC REFRESH ");
            out.ostr << *live_view_periodic_refresh;
        }

        formatOnCluster(out);
    }
    else
    {
        String action = "CREATE";
        if (attach)
            action = "ATTACH";
        else if (replace_table && create_or_replace)
            action = "CREATE OR REPLACE";
        else if (replace_table)
            action = "REPLACE";

        /// Always DICTIONARY
        out.writeKeyword(action);
        out.writeKeyword(" DICTIONARY ");
        out.writeKeyword(if_not_exists ? "IF NOT EXISTS " : "");
        out.ostr << (database ? backQuoteIfNeed(getDatabase()) + "." : "") << backQuoteIfNeed(getTable());
        if (uuid != UUIDHelpers::Nil)
        {
            out.writeKeyword(" UUID ");
            out.ostr << quoteString(toString(uuid));
        }
        formatOnCluster(out);
    }

    if (to_table_id)
    {
        assert((is_materialized_view || is_window_view) && to_inner_uuid == UUIDHelpers::Nil);
        out.writeKeyword(" TO ");
        out.ostr
            << (!to_table_id.database_name.empty() ? backQuoteIfNeed(to_table_id.database_name) + "." : "")
            << backQuoteIfNeed(to_table_id.table_name);
    }

    if (to_inner_uuid != UUIDHelpers::Nil)
    {
        assert(is_materialized_view && !to_table_id);
        out.writeKeyword(" TO INNER UUID ");
        out.ostr << quoteString(toString(to_inner_uuid));
    }

    if (!as_table.empty())
    {
        out.writeKeyword(" AS ");
        out.ostr
            << (!as_database.empty() ? backQuoteIfNeed(as_database) + "." : "") << backQuoteIfNeed(as_table);
    }

    if (as_table_function)
    {
        if (columns_list && !columns_list->empty())
        {
            out.setExpressionListAlwaysStartsOnNewLine();
            out.nlOrWs();
            out.ostr << "(";
            columns_list->formatImpl(out.copy());
            out.nlOrNothing();
            out.ostr << ")";
            out.setExpressionListAlwaysStartsOnNewLine(false); //-V519
        }

        out.writeKeyword(" AS ");
        as_table_function->formatImpl(out);
    }

    out.setExpressionListAlwaysStartsOnNewLine(true);

    if (columns_list && !columns_list->empty() && !as_table_function)
    {
        out.nlOrWs();
        out.ostr << "(";
        columns_list->formatImpl(out.copy());
        out.nlOrNothing();
        out.ostr << ")";
    }

    if (dictionary_attributes_list)
    {
        out.nlOrWs();
        out.ostr << "(";
        out.isOneLine() ? dictionary_attributes_list->formatImpl(out.copy())
                        : dictionary_attributes_list->formatImplMultiline(out.copy());
        out.nlOrNothing();
        out.ostr << ")";
    }

    out.setExpressionListAlwaysStartsOnNewLine(false); //-V519

    if (inner_storage)
    {
        out.writeKeyword(" INNER");
        inner_storage->formatImpl(out);
    }

    if (storage)
        storage->formatImpl(out);

    if (dictionary)
        dictionary->formatImpl(out);

    if (is_watermark_strictly_ascending)
    {
        out.writeKeyword(" WATERMARK STRICTLY_ASCENDING");
    }
    else if (is_watermark_ascending)
    {
        out.writeKeyword(" WATERMARK ASCENDING");
    }
    else if (is_watermark_bounded)
    {
        out.writeKeyword(" WATERMARK ");
        watermark_function->formatImpl(out);
    }

    if (allowed_lateness)
    {
        out.writeKeyword(" ALLOWED_LATENESS ");
        lateness_function->formatImpl(out);
    }

    if (is_populate)
        out.writeKeyword(" POPULATE");
    else if (is_create_empty)
        out.writeKeyword(" EMPTY");

    if (select)
    {
        out.writeKeyword(" AS");
        out.writeKeyword(comment ? "(" : "");
        out.nlOrWs();
        select->formatImpl(out);
        out.writeKeyword(comment ? ")" : "");
    }

    if (comment)
    {
        out.writeKeyword("COMMENT ");
        out.nlOrWs();
        comment->formatImpl(out);
    }
}

bool ASTCreateQuery::isParameterizedView() const
{
    if (is_ordinary_view && select && select->hasQueryParameters())
        return true;
    return false;
}

}
