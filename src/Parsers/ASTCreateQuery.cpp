#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Common/quoteString.h>
#include <Interpreters/StorageID.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{

void ASTSQLSecurity::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (!type)
        return;

    if (definer || is_definer_current_user)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "DEFINER" << (settings.hilite ? hilite_none : "");
        settings.ostr << " = ";
        if (definer)
            definer->formatImpl(settings, state, frame);
        else
            settings.ostr << "CURRENT_USER";
        settings.ostr << " ";
    }

    settings.ostr << (settings.hilite ? hilite_keyword : "") << "SQL SECURITY" << (settings.hilite ? hilite_none : "");
    switch (*type)
    {
        case SQLSecurityType::INVOKER:
            settings.ostr << " INVOKER";
            break;
        case SQLSecurityType::DEFINER:
            settings.ostr << " DEFINER";
            break;
        case SQLSecurityType::NONE:
            settings.ostr << " NONE";
            break;
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

bool ASTStorage::isExtendedStorageDefinition() const
{
    return partition_by || primary_key || order_by || sample_by || settings;
}


class ASTColumnsElement : public IAST
{
public:
    String prefix;
    IAST * elem;

    String getID(char c) const override { return "ASTColumnsElement for " + elem->getID(c); }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    void forEachPointerToChild(std::function<void(void**)> f) override
    {
        f(reinterpret_cast<void **>(&elem));
    }
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
    if (projections)
        res->set(res->projections, projections->clone());
    if (primary_key)
        res->set(res->primary_key, primary_key->clone());
    if (primary_key_from_columns)
        res->set(res->primary_key_from_columns, primary_key_from_columns->clone());

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

void ASTCreateQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    if (database && !table)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "")
            << (attach ? "ATTACH DATABASE " : "CREATE DATABASE ")
            << (if_not_exists ? "IF NOT EXISTS " : "")
            << (settings.hilite ? hilite_none : "");

        database->formatImpl(settings, state, frame);

        if (uuid != UUIDHelpers::Nil)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " UUID " << (settings.hilite ? hilite_none : "")
                          << quoteString(toString(uuid));
        }

        formatOnCluster(settings);

        if (storage)
            storage->formatImpl(settings, state, frame);

        if (table_overrides)
        {
            settings.ostr << settings.nl_or_ws;
            table_overrides->formatImpl(settings, state, frame);
        }

        if (comment)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << "COMMENT " << (settings.hilite ? hilite_none : "");
            comment->formatImpl(settings, state, frame);
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

        settings.ostr << (settings.hilite ? hilite_keyword : "") << action << (settings.hilite ? hilite_none : "");
        settings.ostr << " ";
        settings.ostr << (settings.hilite ? hilite_keyword : "") << (temporary ? "TEMPORARY " : "")
                << what << " "
                << (if_not_exists ? "IF NOT EXISTS " : "")
            << (settings.hilite ? hilite_none : "");

        if (database)
        {
            database->formatImpl(settings, state, frame);
            settings.ostr << '.';
        }

        chassert(table);
        table->formatImpl(settings, state, frame);

        if (uuid != UUIDHelpers::Nil)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " UUID " << (settings.hilite ? hilite_none : "")
                          << quoteString(toString(uuid));

        assert(attach || !attach_from_path);
        if (attach_from_path)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FROM " << (settings.hilite ? hilite_none : "")
                          << quoteString(*attach_from_path);

        formatOnCluster(settings);
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
        settings.ostr << (settings.hilite ? hilite_keyword : "") << action << " DICTIONARY "
                      << (if_not_exists ? "IF NOT EXISTS " : "") << (settings.hilite ? hilite_none : "");

        if (database)
        {
            database->formatImpl(settings, state, frame);
            settings.ostr << '.';
        }

        chassert(table);
        table->formatImpl(settings, state, frame);

        if (uuid != UUIDHelpers::Nil)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " UUID " << (settings.hilite ? hilite_none : "")
                          << quoteString(toString(uuid));
        formatOnCluster(settings);
    }

    if (refresh_strategy)
    {
        settings.ostr << settings.nl_or_ws;
        refresh_strategy->formatImpl(settings, state, frame);
    }

    if (to_table_id)
    {
        assert((is_materialized_view || is_window_view) && to_inner_uuid == UUIDHelpers::Nil);
        settings.ostr
            << (settings.hilite ? hilite_keyword : "") << " TO " << (settings.hilite ? hilite_none : "")
            << (!to_table_id.database_name.empty() ? backQuoteIfNeed(to_table_id.database_name) + "." : "")
            << backQuoteIfNeed(to_table_id.table_name);
    }

    if (to_inner_uuid != UUIDHelpers::Nil)
    {
        assert(is_materialized_view && !to_table_id);
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " TO INNER UUID " << (settings.hilite ? hilite_none : "")
                      << quoteString(toString(to_inner_uuid));
    }

    if (!as_table.empty())
    {
        settings.ostr
            << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "")
            << (!as_database.empty() ? backQuoteIfNeed(as_database) + "." : "") << backQuoteIfNeed(as_table);
    }

    if (as_table_function)
    {
        if (columns_list && !columns_list->empty())
        {
            frame.expression_list_always_start_on_new_line = true;
            settings.ostr << (settings.one_line ? " (" : "\n(");
            FormatStateStacked frame_nested = frame;
            columns_list->formatImpl(settings, state, frame_nested);
            settings.ostr << (settings.one_line ? ")" : "\n)");
            frame.expression_list_always_start_on_new_line = false;
        }

        settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "");
        as_table_function->formatImpl(settings, state, frame);
    }

    frame.expression_list_always_start_on_new_line = true;

    if (columns_list && !columns_list->empty() && !as_table_function)
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

    if (inner_storage)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " INNER" << (settings.hilite ? hilite_none : "");
        inner_storage->formatImpl(settings, state, frame);
    }

    if (storage)
        storage->formatImpl(settings, state, frame);

    if (dictionary)
        dictionary->formatImpl(settings, state, frame);

    if (is_watermark_strictly_ascending)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " WATERMARK STRICTLY_ASCENDING" << (settings.hilite ? hilite_none : "");
    }
    else if (is_watermark_ascending)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " WATERMARK ASCENDING" << (settings.hilite ? hilite_none : "");
    }
    else if (is_watermark_bounded)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " WATERMARK " << (settings.hilite ? hilite_none : "");
        watermark_function->formatImpl(settings, state, frame);
    }

    if (allowed_lateness)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " ALLOWED_LATENESS " << (settings.hilite ? hilite_none : "");
        lateness_function->formatImpl(settings, state, frame);
    }

    if (is_populate)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " POPULATE" << (settings.hilite ? hilite_none : "");
    else if (is_create_empty)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " EMPTY" << (settings.hilite ? hilite_none : "");

    if (sql_security && supportSQLSecurity() && sql_security->as<ASTSQLSecurity &>().type.has_value())
    {
        settings.ostr << settings.nl_or_ws;
        sql_security->formatImpl(settings, state, frame);
    }

    if (select)
    {
        settings.ostr << settings.nl_or_ws;
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "AS "
                      << (comment ? "(" : "") << (settings.hilite ? hilite_none : "");
        select->formatImpl(settings, state, frame);
        settings.ostr << (settings.hilite ? hilite_keyword : "") << (comment ? ")" : "") << (settings.hilite ? hilite_none : "");
    }

    if (comment)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << "COMMENT " << (settings.hilite ? hilite_none : "");
        comment->formatImpl(settings, state, frame);
    }
}

bool ASTCreateQuery::isParameterizedView() const
{
    if (is_ordinary_view && select && select->hasQueryParameters())
        return true;
    return false;
}


ASTCreateQuery::UUIDs::UUIDs(const ASTCreateQuery & query)
    : uuid(query.uuid)
    , to_inner_uuid(query.to_inner_uuid)
{
}

String ASTCreateQuery::UUIDs::toString() const
{
    WriteBufferFromOwnString out;
    out << "{" << uuid << "," << to_inner_uuid << "}";
    return out.str();
}

ASTCreateQuery::UUIDs ASTCreateQuery::UUIDs::fromString(const String & str)
{
    ReadBufferFromString in{str};
    ASTCreateQuery::UUIDs res;
    in >> "{" >> res.uuid >> "," >> res.to_inner_uuid >> "}";
    return res;
}

ASTCreateQuery::UUIDs ASTCreateQuery::generateRandomUUID(bool always_generate_new_uuid)
{
    if (always_generate_new_uuid)
        setUUID({});

    if (uuid == UUIDHelpers::Nil)
        uuid = UUIDHelpers::generateV4();

    /// If destination table (to_table_id) is not specified for materialized view,
    /// then MV will create inner table. We should generate UUID of inner table here.
    bool need_uuid_for_inner_table = !attach && is_materialized_view && !to_table_id;
    if (need_uuid_for_inner_table && (to_inner_uuid == UUIDHelpers::Nil))
        to_inner_uuid = UUIDHelpers::generateV4();

    return UUIDs{*this};
}

void ASTCreateQuery::setUUID(const UUIDs & uuids)
{
    uuid = uuids.uuid;
    to_inner_uuid = uuids.to_inner_uuid;
}

}
