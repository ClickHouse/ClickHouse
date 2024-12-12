#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/CreateQueryUUIDs.h>
#include <Common/quoteString.h>
#include <Interpreters/StorageID.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{

void ASTSQLSecurity::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (!type)
        return;

    if (definer || is_definer_current_user)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "DEFINER" << (settings.hilite ? hilite_none : "");
        ostr << " = ";
        if (definer)
            definer->formatImpl(ostr, settings, state, frame);
        else
            ostr << "CURRENT_USER";
        ostr << " ";
    }

    ostr << (settings.hilite ? hilite_keyword : "") << "SQL SECURITY" << (settings.hilite ? hilite_none : "");
    switch (*type)
    {
        case SQLSecurityType::INVOKER:
            ostr << " INVOKER";
            break;
        case SQLSecurityType::DEFINER:
            ostr << " DEFINER";
            break;
        case SQLSecurityType::NONE:
            ostr << " NONE";
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

void ASTStorage::formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    if (engine)
    {
        ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "ENGINE" << (s.hilite ? hilite_none : "") << " = ";
        engine->formatImpl(ostr, s, state, frame);
    }
    if (partition_by)
    {
        ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "PARTITION BY " << (s.hilite ? hilite_none : "");
        partition_by->formatImpl(ostr, s, state, frame);
    }
    if (primary_key)
    {
        ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "PRIMARY KEY " << (s.hilite ? hilite_none : "");
        primary_key->formatImpl(ostr, s, state, frame);
    }
    if (order_by)
    {
        ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "ORDER BY " << (s.hilite ? hilite_none : "");
        order_by->formatImpl(ostr, s, state, frame);
    }
    if (sample_by)
    {
        ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "SAMPLE BY " << (s.hilite ? hilite_none : "");
        sample_by->formatImpl(ostr, s, state, frame);
    }
    if (ttl_table)
    {
        ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "TTL " << (s.hilite ? hilite_none : "");
        ttl_table->formatImpl(ostr, s, state, frame);
    }
    if (settings)
    {
        ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "SETTINGS " << (s.hilite ? hilite_none : "");
        settings->formatImpl(ostr, s, state, frame);
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

    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

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

void ASTColumnsElement::formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    if (!elem)
        return;

    if (prefix.empty())
    {
        elem->formatImpl(ostr, s, state, frame);
        return;
    }

    ostr << (s.hilite ? hilite_keyword : "") << prefix << (s.hilite ? hilite_none : "");
    ostr << ' ';
    elem->formatImpl(ostr, s, state, frame);
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

void ASTColumns::formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
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
            list.formatImpl(ostr, s, state, frame);
        else
            list.formatImplMultiline(ostr, s, state, frame);
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
    if (table_overrides)
        res->set(res->table_overrides, table_overrides->clone());
    if (targets)
        res->set(res->targets, targets->clone());

    if (dictionary)
    {
        assert(is_dictionary);
        res->set(res->dictionary_attributes_list, dictionary_attributes_list->clone());
        res->set(res->dictionary, dictionary->clone());
    }

    if (refresh_strategy)
        res->set(res->refresh_strategy, refresh_strategy->clone());
    if (as_table_function)
        res->set(res->as_table_function, as_table_function->clone());
    if (comment)
        res->set(res->comment, comment->clone());

    cloneOutputOptions(*res);
    cloneTableOptions(*res);

    return res;
}

String ASTCreateQuery::getID(char delim) const
{
    String res = attach ? "AttachQuery" : "CreateQuery";
    String database = getDatabase();
    if (!database.empty())
        res += (delim + getDatabase());
    res += (delim + getTable());
    return res;
}

void ASTCreateQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    if (database && !table)
    {
        ostr << (settings.hilite ? hilite_keyword : "")
            << (attach ? "ATTACH DATABASE " : "CREATE DATABASE ")
            << (if_not_exists ? "IF NOT EXISTS " : "")
            << (settings.hilite ? hilite_none : "");

        database->formatImpl(ostr, settings, state, frame);

        if (uuid != UUIDHelpers::Nil)
        {
            ostr << (settings.hilite ? hilite_keyword : "") << " UUID " << (settings.hilite ? hilite_none : "")
                          << quoteString(toString(uuid));
        }

        formatOnCluster(ostr, settings);

        if (storage)
            storage->formatImpl(ostr, settings, state, frame);

        if (table_overrides)
        {
            ostr << settings.nl_or_ws;
            table_overrides->formatImpl(ostr, settings, state, frame);
        }

        if (comment)
        {
            ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << "COMMENT " << (settings.hilite ? hilite_none : "");
            comment->formatImpl(ostr, settings, state, frame);
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

        ostr << (settings.hilite ? hilite_keyword : "") << action << (settings.hilite ? hilite_none : "");
        ostr << " ";
        ostr << (settings.hilite ? hilite_keyword : "") << (temporary ? "TEMPORARY " : "")
                << what << " "
                << (if_not_exists ? "IF NOT EXISTS " : "")
            << (settings.hilite ? hilite_none : "");

        if (database)
        {
            database->formatImpl(ostr, settings, state, frame);
            ostr << '.';
        }

        chassert(table);
        table->formatImpl(ostr, settings, state, frame);

        if (uuid != UUIDHelpers::Nil)
            ostr << (settings.hilite ? hilite_keyword : "") << " UUID " << (settings.hilite ? hilite_none : "")
                          << quoteString(toString(uuid));

        assert(attach || !attach_from_path);
        if (attach_from_path)
            ostr << (settings.hilite ? hilite_keyword : "") << " FROM " << (settings.hilite ? hilite_none : "")
                          << quoteString(*attach_from_path);

        if (attach_as_replicated.has_value())
        {
            if (attach_as_replicated.value())
                ostr << (settings.hilite ? hilite_keyword : "") << " AS REPLICATED" << (settings.hilite ? hilite_none : "");
            else
                ostr << (settings.hilite ? hilite_keyword : "") << " AS NOT REPLICATED" << (settings.hilite ? hilite_none : "");
        }

        formatOnCluster(ostr, settings);
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
        ostr << (settings.hilite ? hilite_keyword : "") << action << " DICTIONARY "
                      << (if_not_exists ? "IF NOT EXISTS " : "") << (settings.hilite ? hilite_none : "");

        if (database)
        {
            database->formatImpl(ostr, settings, state, frame);
            ostr << '.';
        }

        chassert(table);
        table->formatImpl(ostr, settings, state, frame);

        if (uuid != UUIDHelpers::Nil)
            ostr << (settings.hilite ? hilite_keyword : "") << " UUID " << (settings.hilite ? hilite_none : "")
                          << quoteString(toString(uuid));
        formatOnCluster(ostr, settings);
    }

    if (refresh_strategy)
    {
        ostr << settings.nl_or_ws;
        refresh_strategy->formatImpl(ostr, settings, state, frame);
    }

    if (auto to_table_id = getTargetTableID(ViewTarget::To))
    {
        ostr <<  " " << (settings.hilite ? hilite_keyword : "") << toStringView(Keyword::TO)
                      << (settings.hilite ? hilite_none : "") << " "
                      << (!to_table_id.database_name.empty() ? backQuoteIfNeed(to_table_id.database_name) + "." : "")
                      << backQuoteIfNeed(to_table_id.table_name);
    }

    if (auto to_inner_uuid = getTargetInnerUUID(ViewTarget::To); to_inner_uuid != UUIDHelpers::Nil)
    {
        ostr << " " << (settings.hilite ? hilite_keyword : "") << toStringView(Keyword::TO_INNER_UUID)
                      << (settings.hilite ? hilite_none : "") << " " << quoteString(toString(to_inner_uuid));
    }

    bool should_add_empty = is_create_empty;
    auto add_empty_if_needed = [&]
    {
        if (!should_add_empty)
            return;
        should_add_empty = false;
        ostr << (settings.hilite ? hilite_keyword : "") << " EMPTY" << (settings.hilite ? hilite_none : "");
    };

    bool should_add_clone = is_clone_as;
    auto add_clone_if_needed = [&]
    {
        if (!should_add_clone)
            return;
        should_add_clone = false;
        ostr << (settings.hilite ? hilite_keyword : "") << " CLONE" << (settings.hilite ? hilite_none : "");
    };

    if (!as_table.empty())
    {
        add_empty_if_needed();
        add_clone_if_needed();
        ostr
            << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "")
            << (!as_database.empty() ? backQuoteIfNeed(as_database) + "." : "") << backQuoteIfNeed(as_table);
    }

    if (as_table_function)
    {
        if (columns_list && !columns_list->empty())
        {
            frame.expression_list_always_start_on_new_line = true;
            ostr << (settings.one_line ? " (" : "\n(");
            FormatStateStacked frame_nested = frame;
            columns_list->formatImpl(ostr, settings, state, frame_nested);
            ostr << (settings.one_line ? ")" : "\n)");
            frame.expression_list_always_start_on_new_line = false;
        }

        add_empty_if_needed();
        add_clone_if_needed();
        ostr << (settings.hilite ? hilite_keyword : "") << " AS " << (settings.hilite ? hilite_none : "");
        as_table_function->formatImpl(ostr, settings, state, frame);
    }

    frame.expression_list_always_start_on_new_line = true;

    if (columns_list && !columns_list->empty() && !as_table_function)
    {
        ostr << (settings.one_line ? " (" : "\n(");
        FormatStateStacked frame_nested = frame;
        columns_list->formatImpl(ostr, settings, state, frame_nested);
        ostr << (settings.one_line ? ")" : "\n)");
    }

    if (dictionary_attributes_list)
    {
        ostr << (settings.one_line ? " (" : "\n(");
        FormatStateStacked frame_nested = frame;
        if (settings.one_line)
            dictionary_attributes_list->formatImpl(ostr, settings, state, frame_nested);
        else
            dictionary_attributes_list->formatImplMultiline(ostr, settings, state, frame_nested);
        ostr << (settings.one_line ? ")" : "\n)");
    }

    frame.expression_list_always_start_on_new_line = false;

    if (storage)
        storage->formatImpl(ostr, settings, state, frame);

    if (auto inner_storage = getTargetInnerEngine(ViewTarget::Inner))
    {
        ostr << " " << (settings.hilite ? hilite_keyword : "") << toStringView(Keyword::INNER) << (settings.hilite ? hilite_none : "");
        inner_storage->formatImpl(ostr, settings, state, frame);
    }

    if (auto to_storage = getTargetInnerEngine(ViewTarget::To))
        to_storage->formatImpl(ostr, settings, state, frame);

    if (targets)
    {
        targets->formatTarget(ViewTarget::Data, ostr, settings, state, frame);
        targets->formatTarget(ViewTarget::Tags, ostr, settings, state, frame);
        targets->formatTarget(ViewTarget::Metrics, ostr, settings, state, frame);
    }

    if (dictionary)
        dictionary->formatImpl(ostr, settings, state, frame);

    if (is_watermark_strictly_ascending)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << " WATERMARK STRICTLY_ASCENDING" << (settings.hilite ? hilite_none : "");
    }
    else if (is_watermark_ascending)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << " WATERMARK ASCENDING" << (settings.hilite ? hilite_none : "");
    }
    else if (is_watermark_bounded)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << " WATERMARK " << (settings.hilite ? hilite_none : "");
        watermark_function->formatImpl(ostr, settings, state, frame);
    }

    if (allowed_lateness)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << " ALLOWED_LATENESS " << (settings.hilite ? hilite_none : "");
        lateness_function->formatImpl(ostr, settings, state, frame);
    }

    if (is_populate)
        ostr << (settings.hilite ? hilite_keyword : "") << " POPULATE" << (settings.hilite ? hilite_none : "");

    add_empty_if_needed();

    if (sql_security && supportSQLSecurity() && sql_security->as<ASTSQLSecurity &>().type.has_value())
    {
        ostr << settings.nl_or_ws;
        sql_security->formatImpl(ostr, settings, state, frame);
    }

    if (select)
    {
        ostr << settings.nl_or_ws;
        ostr << (settings.hilite ? hilite_keyword : "") << "AS "
                      << (comment ? "(" : "") << (settings.hilite ? hilite_none : "");
        select->formatImpl(ostr, settings, state, frame);
        ostr << (settings.hilite ? hilite_keyword : "") << (comment ? ")" : "") << (settings.hilite ? hilite_none : "");
    }

    if (comment)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << "COMMENT " << (settings.hilite ? hilite_none : "");
        comment->formatImpl(ostr, settings, state, frame);
    }
}

bool ASTCreateQuery::isParameterizedView() const
{
    if (is_ordinary_view && select && select->hasQueryParameters())
        return true;
    return false;
}


void ASTCreateQuery::generateRandomUUIDs()
{
    CreateQueryUUIDs{*this, /* generate_random= */ true}.copyToQuery(*this);
}

void ASTCreateQuery::resetUUIDs()
{
    CreateQueryUUIDs{}.copyToQuery(*this);
}


StorageID ASTCreateQuery::getTargetTableID(ViewTarget::Kind target_kind) const
{
    if (targets)
        return targets->getTableID(target_kind);
    return StorageID::createEmpty();
}

bool ASTCreateQuery::hasTargetTableID(ViewTarget::Kind target_kind) const
{
    if (targets)
        return targets->hasTableID(target_kind);
    return false;
}

UUID ASTCreateQuery::getTargetInnerUUID(ViewTarget::Kind target_kind) const
{
    if (targets)
        return targets->getInnerUUID(target_kind);
    return UUIDHelpers::Nil;
}

bool ASTCreateQuery::hasInnerUUIDs() const
{
    if (targets)
        return targets->hasInnerUUIDs();
    return false;
}

std::shared_ptr<ASTStorage> ASTCreateQuery::getTargetInnerEngine(ViewTarget::Kind target_kind) const
{
    if (targets)
        return targets->getInnerEngine(target_kind);
    return nullptr;
}

void ASTCreateQuery::setTargetInnerEngine(ViewTarget::Kind target_kind, ASTPtr storage_def)
{
    if (!targets)
        set(targets, std::make_shared<ASTViewTargets>());
    targets->setInnerEngine(target_kind, storage_def);
}

}
