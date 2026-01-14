#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/CreateQueryUUIDs.h>
#include <Common/quoteString.h>
#include <Interpreters/StorageID.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{

void ASTSQLSecurity::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (!type)
        return;

    if (definer || is_definer_current_user)
    {
        ostr << "DEFINER";
        ostr << " = ";
        if (definer)
            definer->format(ostr, settings, state, frame);
        else
            ostr << "CURRENT_USER";
        ostr << " ";
    }

    ostr << "SQL SECURITY";
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
    auto modified_frame{frame};
    if (engine)
    {
        modified_frame.create_engine_name = engine->name;
        ostr << s.nl_or_ws << "ENGINE" << " = ";
        engine->format(ostr, s, state, modified_frame);
    }
    if (partition_by)
    {
        ostr << s.nl_or_ws << "PARTITION BY ";
        partition_by->format(ostr, s, state, modified_frame);
    }
    if (primary_key)
    {
        ostr << s.nl_or_ws << "PRIMARY KEY ";
        primary_key->format(ostr, s, state, modified_frame);
    }
    if (order_by)
    {
        ostr << s.nl_or_ws << "ORDER BY ";
        order_by->format(ostr, s, state, modified_frame);
    }
    if (sample_by)
    {
        ostr << s.nl_or_ws << "SAMPLE BY ";
        sample_by->format(ostr, s, state, modified_frame);
    }
    if (ttl_table)
    {
        ostr << s.nl_or_ws << "TTL ";
        ttl_table->format(ostr, s, state, modified_frame);
    }
    if (settings)
    {
        ostr << s.nl_or_ws << "SETTINGS ";
        settings->format(ostr, s, state, modified_frame);
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

    void forEachPointerToChild(std::function<void(void**)> f) override
    {
        f(reinterpret_cast<void **>(&elem));
    }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
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
        elem->format(ostr, s, state, frame);
        return;
    }

    ostr << prefix << ' ';
    elem->format(ostr, s, state, frame);
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
            list.format(ostr, s, state, frame);
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
    if (aliases_list)
        res->set(res->aliases_list, aliases_list->clone());
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
        ostr
            << (attach ? "ATTACH DATABASE " : "CREATE DATABASE ")
            << (if_not_exists ? "IF NOT EXISTS " : "");

        database->format(ostr, settings, state, frame);

        if (uuid != UUIDHelpers::Nil)
            ostr << " UUID " << quoteString(toString(uuid));

        formatOnCluster(ostr, settings);

        if (storage)
            storage->format(ostr, settings, state, frame);

        if (table_overrides)
        {
            ostr << settings.nl_or_ws;
            table_overrides->format(ostr, settings, state, frame);
        }

        if (comment)
        {
            ostr << settings.nl_or_ws << "COMMENT ";
            comment->format(ostr, settings, state, frame);
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
        else if (is_window_view)
            what = "WINDOW VIEW";

        ostr << action;
        ostr << " ";
        ostr << (temporary ? "TEMPORARY " : "")
                << what << " "
                << (if_not_exists ? "IF NOT EXISTS " : "")
           ;

        if (database)
        {
            database->format(ostr, settings, state, frame);
            ostr << '.';
        }

        chassert(table);
        table->format(ostr, settings, state, frame);

        if (uuid != UUIDHelpers::Nil)
            ostr << " UUID " << quoteString(toString(uuid));

        assert(attach || !attach_from_path);
        if (attach_from_path)
            ostr << " FROM " << quoteString(*attach_from_path);

        if (attach_as_replicated.has_value())
        {
            if (attach_as_replicated.value())
                ostr << " AS REPLICATED";
            else
                ostr << " AS NOT REPLICATED";
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
        ostr << action << " DICTIONARY " << (if_not_exists ? "IF NOT EXISTS " : "");

        if (database)
        {
            database->format(ostr, settings, state, frame);
            ostr << '.';
        }

        chassert(table);
        table->format(ostr, settings, state, frame);

        if (uuid != UUIDHelpers::Nil)
            ostr << " UUID " << quoteString(toString(uuid));
        formatOnCluster(ostr, settings);
    }

    if (refresh_strategy)
    {
        ostr << settings.nl_or_ws;
        refresh_strategy->format(ostr, settings, state, frame);
    }

    if (auto to_table_id = getTargetTableID(ViewTarget::To))
    {
        ostr <<  " " << toStringView(Keyword::TO)
              << " "
              << (!to_table_id.database_name.empty() ? backQuoteIfNeed(to_table_id.database_name) + "." : "")
              << backQuoteIfNeed(to_table_id.table_name);
    }
    else if (targets && targets->hasTableASTWithQueryParams(ViewTarget::To))
    {
        auto to_table_ast = targets->getTableASTWithQueryParams(ViewTarget::To);

        chassert(to_table_ast);

        ostr << " " << toStringView(Keyword::TO) << " ";
        to_table_ast->format(ostr, settings, state, frame);
    }

    if (auto to_inner_uuid = getTargetInnerUUID(ViewTarget::To); to_inner_uuid != UUIDHelpers::Nil)
    {
        ostr << " " << toStringView(Keyword::TO_INNER_UUID)
                      << " " << quoteString(toString(to_inner_uuid));
    }

    bool should_add_empty = is_create_empty;
    auto add_empty_if_needed = [&]
    {
        if (!should_add_empty)
            return;
        should_add_empty = false;
        ostr << " EMPTY";
    };

    bool should_add_clone = is_clone_as;
    auto add_clone_if_needed = [&]
    {
        if (!should_add_clone)
            return;
        should_add_clone = false;
        ostr << " CLONE";
    };

    if (!as_table.empty())
    {
        add_empty_if_needed();
        add_clone_if_needed();
        ostr
            << " AS "
            << (!as_database.empty() ? backQuoteIfNeed(as_database) + "." : "") << backQuoteIfNeed(as_table);
    }

    if (as_table_function)
    {
        if (columns_list && !columns_list->empty())
        {
            frame.expression_list_always_start_on_new_line = true;
            ostr << (settings.one_line ? " (" : "\n(");
            columns_list->format(ostr, settings, state, frame);
            ostr << (settings.one_line ? ")" : "\n)");
            frame.expression_list_always_start_on_new_line = false;
        }

        add_empty_if_needed();
        add_clone_if_needed();
        ostr << " AS ";
        as_table_function->format(ostr, settings, state, frame);
    }

    frame.expression_list_always_start_on_new_line = true;

    if (columns_list && !columns_list->empty() && !as_table_function)
    {
        ostr << (settings.one_line ? " (" : "\n(");
        columns_list->format(ostr, settings, state, frame);
        ostr << (settings.one_line ? ")" : "\n)");
    }

    frame.expression_list_always_start_on_new_line = true;

    if (is_ordinary_view && aliases_list && !as_table_function)
    {
        ostr << (settings.one_line ? " (" : "\n(");
        aliases_list->format(ostr, settings, state, frame);
        ostr << (settings.one_line ? ")" : "\n)");
    }

    if (dictionary_attributes_list)
    {
        ostr << (settings.one_line ? " (" : "\n(");
        if (settings.one_line)
            dictionary_attributes_list->format(ostr, settings, state, frame);
        else
            dictionary_attributes_list->formatImplMultiline(ostr, settings, state, frame);
        ostr << (settings.one_line ? ")" : "\n)");
    }

    frame.expression_list_always_start_on_new_line = false;

    if (storage)
        storage->format(ostr, settings, state, frame);

    if (auto inner_storage = getTargetInnerEngine(ViewTarget::Inner))
    {
        ostr << " " << toStringView(Keyword::INNER);
        inner_storage->format(ostr, settings, state, frame);
    }

    if (auto to_storage = getTargetInnerEngine(ViewTarget::To))
        to_storage->format(ostr, settings, state, frame);

    if (targets)
    {
        targets->formatTarget(ViewTarget::Data, ostr, settings, state, frame);
        targets->formatTarget(ViewTarget::Tags, ostr, settings, state, frame);
        targets->formatTarget(ViewTarget::Metrics, ostr, settings, state, frame);
    }

    if (dictionary)
        dictionary->format(ostr, settings, state, frame);

    if (is_watermark_strictly_ascending)
    {
        ostr << " WATERMARK STRICTLY_ASCENDING";
    }
    else if (is_watermark_ascending)
    {
        ostr << " WATERMARK ASCENDING";
    }
    else if (is_watermark_bounded)
    {
        ostr << " WATERMARK ";
        watermark_function->format(ostr, settings, state, frame);
    }

    if (allowed_lateness)
    {
        ostr << " ALLOWED_LATENESS ";
        lateness_function->format(ostr, settings, state, frame);
    }

    if (is_populate)
        ostr << " POPULATE";

    add_empty_if_needed();

    if (sql_security && supportSQLSecurity() && sql_security->as<ASTSQLSecurity &>().type.has_value())
    {
        ostr << settings.nl_or_ws;
        sql_security->format(ostr, settings, state, frame);
    }

    if (select)
    {
        ostr << settings.nl_or_ws;
        ostr << "AS "
                      << (comment ? "(" : "");
        select->format(ostr, settings, state, frame);
        ostr << (comment ? ")" : "");
    }

    if (comment)
    {
        ostr << settings.nl_or_ws << "COMMENT ";
        comment->format(ostr, settings, state, frame);
    }
}

bool ASTCreateQuery::isParameterizedView() const
{
    if (is_ordinary_view && select && select->hasQueryParameters())
        return true;
    return false;
}

NameToNameMap ASTCreateQuery::getQueryParameters() const
{
    if (!select || !isParameterizedView())
        return {};

    return select->getQueryParameters();
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

bool ASTCreateQuery::isCreateQueryWithImmediateInsertSelect() const
{
    return select && !attach && !is_create_empty && !is_ordinary_view && (!(is_materialized_view || is_window_view) || is_populate);
}
}
