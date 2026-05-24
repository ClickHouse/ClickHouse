#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/CreateQueryUUIDs.h>
#include <Common/quoteString.h>
#include <Interpreters/StorageID.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

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
    auto res = make_intrusive<ASTStorage>(*this);
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
    if (unique_key)
        res->set(res->unique_key, unique_key->clone());

    if (settings)
        res->set(res->settings, settings->clone());

    return res;
}

void ASTColumns::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "Columns definition");
    w.writeChild("columns", columns);
    w.writeChild("indices", indices);
    w.writeChild("constraints", constraints);
    w.writeChild("projections", projections);
    w.writeChild("primary_key", primary_key);
    w.writeChild("primary_key_from_columns", primary_key_from_columns);
}

void ASTColumns::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    auto c = r.readChild("columns");
    if (c) set(columns, c);
    auto idx = r.readChild("indices");
    if (idx) set(indices, idx);
    auto con = r.readChild("constraints");
    if (con) set(constraints, con);
    auto proj = r.readChild("projections");
    if (proj) set(projections, proj);
    auto pk = r.readChild("primary_key");
    if (pk) set(primary_key, pk);
    auto pkfc = r.readChild("primary_key_from_columns");
    if (pkfc) set(primary_key_from_columns, pkfc);
}

void ASTStorage::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "Storage");
    w.writeChild("engine", engine);
    w.writeChild("partition_by", partition_by);
    w.writeChild("primary_key", primary_key);
    w.writeChild("order_by", order_by);
    w.writeChild("sample_by", sample_by);
    w.writeChild("ttl_table", ttl_table);
    w.writeChild("settings", settings);
}

void ASTStorage::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    auto child = r.readChild("engine");
    if (child)
        set(engine, child);

    child = r.readChild("partition_by");
    if (child)
        set(partition_by, child);

    child = r.readChild("primary_key");
    if (child)
        set(primary_key, child);

    child = r.readChild("order_by");
    if (child)
        set(order_by, child);

    child = r.readChild("sample_by");
    if (child)
        set(sample_by, child);

    child = r.readChild("ttl_table");
    if (child)
        set(ttl_table, child);

    child = r.readChild("settings");
    if (child)
        set(settings, child);
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
        auto nested_frame = modified_frame;
        if (auto * ast_alias = dynamic_cast<ASTWithAlias *>(partition_by); ast_alias && !ast_alias->tryGetAlias().empty())
            nested_frame.need_parens = true;
        partition_by->format(ostr, s, state, nested_frame);
    }
    if (primary_key)
    {
        ostr << s.nl_or_ws << "PRIMARY KEY ";
        auto nested_frame = modified_frame;
        if (auto * ast_alias = dynamic_cast<ASTWithAlias *>(primary_key); ast_alias && !ast_alias->tryGetAlias().empty())
            nested_frame.need_parens = true;
        primary_key->format(ostr, s, state, nested_frame);
    }
    if (order_by)
    {
        ostr << s.nl_or_ws << "ORDER BY ";
        auto nested_frame = modified_frame;
        if (auto * ast_alias = dynamic_cast<ASTWithAlias *>(order_by); ast_alias && !ast_alias->tryGetAlias().empty())
            nested_frame.need_parens = true;
        order_by->format(ostr, s, state, nested_frame);
    }
    if (unique_key)
    {
        ostr << s.nl_or_ws << "UNIQUE KEY ";
        auto nested_frame = modified_frame;
        if (auto * ast_alias = dynamic_cast<ASTWithAlias *>(unique_key); ast_alias && !ast_alias->tryGetAlias().empty())
            nested_frame.need_parens = true;
        unique_key->format(ostr, s, state, nested_frame);
    }
    if (sample_by)
    {
        ostr << s.nl_or_ws << "SAMPLE BY ";
        auto nested_frame = modified_frame;
        if (auto * ast_alias = dynamic_cast<ASTWithAlias *>(sample_by); ast_alias && !ast_alias->tryGetAlias().empty())
            nested_frame.need_parens = true;
        sample_by->format(ostr, s, state, nested_frame);
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

void ASTStorage::normalizeChildrenOrder()
{
    /// Keep old children alive while we rebuild the vector, because the raw
    /// member pointers (engine, primary_key, …) do not hold ownership —
    /// the intrusive_ptrs in `children` do.  Clearing first would destroy
    /// the objects and leave dangling raw pointers.
    ASTs old_children;
    old_children.swap(children);

    if (engine) children.emplace_back(engine);
    if (partition_by) children.emplace_back(partition_by);
    if (primary_key) children.emplace_back(primary_key);
    if (order_by) children.emplace_back(order_by);
    if (unique_key) children.emplace_back(unique_key);
    if (sample_by) children.emplace_back(sample_by);
    if (ttl_table) children.emplace_back(ttl_table);
    if (settings) children.emplace_back(settings);
}


bool ASTStorage::isExtendedStorageDefinition() const
{
    return partition_by || primary_key || order_by || unique_key || sample_by || settings;
}


class ASTColumnsElement : public IAST
{
public:
    String prefix;
    IAST * elem;

    String getID(char c) const override { return "ASTColumnsElement for " + elem->getID(c); }

    ASTPtr clone() const override;

    void forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f) override
    {
        f(&elem, nullptr);
    }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

ASTPtr ASTColumnsElement::clone() const
{
    auto res = make_intrusive<ASTColumnsElement>();
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
    auto res = make_intrusive<ASTColumns>();

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
            auto elem = make_intrusive<ASTColumnsElement>();
            elem->prefix = "";
            elem->set(elem->elem, column->clone());
            list.children.push_back(elem);
        }
    }
    if (indices)
    {
        for (const auto & index : indices->children)
        {
            auto elem = make_intrusive<ASTColumnsElement>();
            elem->prefix = "INDEX";
            elem->set(elem->elem, index->clone());
            list.children.push_back(elem);
        }
    }
    if (constraints)
    {
        for (const auto & constraint : constraints->children)
        {
            auto elem = make_intrusive<ASTColumnsElement>();
            elem->prefix = "CONSTRAINT";
            elem->set(elem->elem, constraint->clone());
            list.children.push_back(elem);
        }
    }
    if (projections)
    {
        for (const auto & projection : projections->children)
        {
            auto elem = make_intrusive<ASTColumnsElement>();
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
    auto res = make_intrusive<ASTCreateQuery>(*this);
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
    if (sql_security)
        res->set(res->sql_security, sql_security->clone());
    if (watermark_function)
        res->set(res->watermark_function, watermark_function->clone());
    if (lateness_function)
        res->set(res->lateness_function, lateness_function->clone());

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

void ASTCreateQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "CreateQuery");

    w.writeString("database", getDatabase());
    w.writeString("table", getTable());

    if (!cluster.empty())
        w.writeString("cluster", cluster);

    if (!as_database.empty())
        w.writeString("as_database", as_database);
    if (!as_table.empty())
        w.writeString("as_table", as_table);
    if (!attach_from_path.empty())
        w.writeString("attach_from_path", attach_from_path);

    w.writeBool("attach", attach);
    w.writeBool("if_not_exists", if_not_exists);
    w.writeBool("is_ordinary_view", is_ordinary_view);
    w.writeBool("is_materialized_view", is_materialized_view);
    w.writeBool("is_window_view", is_window_view);
    w.writeBool("is_time_series_table", is_time_series_table);
    w.writeBool("is_populate", is_populate);
    w.writeBool("is_create_empty", is_create_empty);
    w.writeBool("is_clone_as", is_clone_as);
    w.writeBool("replace_view", replace_view);
    w.writeBool("has_uuid", has_uuid);
    w.writeBool("has_uuid_clause", has_uuid_clause);
    w.writeBool("has_inner_uuid_clause", has_inner_uuid_clause);
    if (uuid != UUIDHelpers::Nil)
        w.writeString("uuid", toString(uuid));
    w.writeBool("is_dictionary", is_dictionary);
    w.writeBool("is_watermark_strictly_ascending", is_watermark_strictly_ascending);
    w.writeBool("is_watermark_ascending", is_watermark_ascending);
    w.writeBool("is_watermark_bounded", is_watermark_bounded);
    w.writeBool("allowed_lateness", allowed_lateness);
    w.writeBool("attach_short_syntax", attach_short_syntax);
    w.writeBool("replace_table", replace_table);
    w.writeBool("create_or_replace", create_or_replace);
    w.writeBool("has_attach_from_path", has_attach_from_path);
    if (attach_as_replicated.has_value())
        w.writeBool("attach_as_replicated", *attach_as_replicated);

    w.writeChild("columns_list", columns_list);
    w.writeChild("aliases_list", aliases_list);
    w.writeChild("storage", storage);
    w.writeChild("watermark_function", watermark_function);
    w.writeChild("lateness_function", lateness_function);
    w.writeChild("as_table_function", as_table_function);
    w.writeChild("select", select);
    w.writeChild("targets", targets);
    w.writeChild("comment", comment);
    w.writeChild("sql_security", sql_security);
    w.writeChild("table_overrides", table_overrides);
    w.writeChild("dictionary_attributes_list", dictionary_attributes_list);
    w.writeChild("dictionary", dictionary);
    w.writeChild("refresh_strategy", refresh_strategy);
}

void ASTCreateQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    String db = r.getString("database");
    if (!db.empty())
        setDatabase(db);
    String tbl = r.getString("table");
    if (!tbl.empty())
        setTable(tbl);

    cluster = r.getString("cluster");
    as_database = r.getString("as_database");
    as_table = r.getString("as_table");
    attach_from_path = r.getString("attach_from_path");

    attach = r.getBool("attach");
    if_not_exists = r.getBool("if_not_exists");
    is_ordinary_view = r.getBool("is_ordinary_view");
    is_materialized_view = r.getBool("is_materialized_view");
    is_window_view = r.getBool("is_window_view");
    is_time_series_table = r.getBool("is_time_series_table");
    is_populate = r.getBool("is_populate");
    is_create_empty = r.getBool("is_create_empty");
    is_clone_as = r.getBool("is_clone_as");
    replace_view = r.getBool("replace_view");
    has_uuid = r.getBool("has_uuid");
    has_uuid_clause = r.getBool("has_uuid_clause");
    has_inner_uuid_clause = r.getBool("has_inner_uuid_clause");
    if (r.has("uuid"))
        uuid = parseFromString<UUID>(r.getString("uuid"));
    is_dictionary = r.getBool("is_dictionary");
    is_watermark_strictly_ascending = r.getBool("is_watermark_strictly_ascending");
    is_watermark_ascending = r.getBool("is_watermark_ascending");
    is_watermark_bounded = r.getBool("is_watermark_bounded");
    allowed_lateness = r.getBool("allowed_lateness");
    attach_short_syntax = r.getBool("attach_short_syntax");
    replace_table = r.getBool("replace_table");
    create_or_replace = r.getBool("create_or_replace");
    has_attach_from_path = r.getBool("has_attach_from_path");
    if (r.has("attach_as_replicated"))
        attach_as_replicated = r.getBool("attach_as_replicated");

    auto child = r.readChild("columns_list");
    if (child)
        set(columns_list, child);

    child = r.readChild("aliases_list");
    if (child)
        set(aliases_list, child);

    child = r.readChild("storage");
    if (child)
        set(storage, child);

    child = r.readChild("watermark_function");
    if (child)
        set(watermark_function, child);

    child = r.readChild("lateness_function");
    if (child)
        set(lateness_function, child);

    child = r.readChild("as_table_function");
    if (child)
        set(as_table_function, child);

    child = r.readChild("select");
    if (child)
        set(select, child);

    child = r.readChild("targets");
    if (child)
        set(targets, child);

    child = r.readChild("comment");
    if (child)
        set(comment, child);

    child = r.readChild("sql_security");
    if (child)
        set(sql_security, child);

    child = r.readChild("table_overrides");
    if (child)
        set(table_overrides, child);

    child = r.readChild("dictionary_attributes_list");
    if (child)
        set(dictionary_attributes_list, child);

    child = r.readChild("dictionary");
    if (child)
        set(dictionary, child);

    child = r.readChild("refresh_strategy");
    if (child)
        set(refresh_strategy, child);

    /// `formatQueryImpl` only enters the `CREATE DATABASE` branch when `database` is set and `table` is unset.
    /// All other forms (`TABLE`, `VIEW`, `MATERIALIZED VIEW`, `WINDOW VIEW`, `DICTIONARY`, ...) require `table`;
    /// otherwise we fall into a `chassert(table); table->format(...)` path that null-derefs in release builds.
    /// Without form-shape validation, JSON such as `{"database":"db","is_ordinary_view":true}` would silently
    /// format as `CREATE DATABASE db`, dropping the view-specific flags instead of being rejected.
    if (!table && !database)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`CreateQuery` must specify at least one of 'database' or 'table' during AST JSON deserialization");

    const bool requires_table =
        is_ordinary_view || is_materialized_view || is_window_view
        || is_dictionary || is_time_series_table
        || is_populate || is_create_empty || is_clone_as
        || replace_view || replace_table || create_or_replace
        || has_attach_from_path || attach_as_replicated.has_value()
        || allowed_lateness
        || is_watermark_strictly_ascending || is_watermark_ascending || is_watermark_bounded
        || columns_list || aliases_list || select
        || watermark_function || lateness_function || as_table_function
        || targets || sql_security
        || dictionary_attributes_list || dictionary || refresh_strategy
        || !as_table.empty() || !attach_from_path.empty();
    if (requires_table && !table)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`CreateQuery` is missing 'table' during AST JSON deserialization, but the surrounding flags indicate a non-database form");
}

void ASTCreateQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
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
        ostr << (isTemporary() ? "TEMPORARY " : "")
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

        chassert(attach || !has_attach_from_path);
        if (has_attach_from_path)
            ostr << " FROM " << quoteString(attach_from_path);

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

    if ((is_ordinary_view || is_materialized_view) && aliases_list && !as_table_function)
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

    if (auto * inner_storage = getTargetInnerEngine(ViewTarget::Inner))
    {
        ostr << " " << toStringView(Keyword::INNER);
        inner_storage->format(ostr, settings, state, frame);
    }

    if (auto * to_storage = getTargetInnerEngine(ViewTarget::To))
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

    if (comment)
    {
        ostr << settings.nl_or_ws << "COMMENT ";
        comment->format(ostr, settings, state, frame);
    }

    if (select)
    {
        ostr << settings.nl_or_ws;
        ostr << "AS ";

        /// When the query has trailing output options like SETTINGS, FORMAT, or INTO OUTFILE
        /// (either from this CREATE query or from an outer query like EXPLAIN),
        /// we must wrap the AS-select in parentheses. Otherwise the trailing
        /// SETTINGS clause would be consumed by `ParserSelectQuery` as part of the
        /// last SELECT in the UNION/INTERSECT chain during re-parsing, instead of
        /// remaining on the outer query — breaking the formatting roundtrip.
        /// The outer parentheses already protect against consumption, so
        /// clear the flags to prevent inner nodes from adding redundant parentheses.
        if (settings_ast || frame.has_trailing_output_options)
        {
            ostr << "(";
            frame.parent_has_trailing_settings = false;
            frame.has_trailing_output_options = false;
            select->format(ostr, settings, state, frame);
            ostr << ")";
        }
        else
        {
            select->format(ostr, settings, state, frame);
        }
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

ASTStorage * ASTCreateQuery::getTargetInnerEngine(ViewTarget::Kind target_kind) const
{
    if (targets)
        return targets->getInnerEngine(target_kind);
    return nullptr;
}

void ASTCreateQuery::setTargetInnerEngine(ViewTarget::Kind target_kind, ASTPtr storage_def)
{
    if (!targets)
        set(targets, make_intrusive<ASTViewTargets>());
    targets->setInnerEngine(target_kind, storage_def);
}

bool ASTCreateQuery::isCreateQueryWithImmediateInsertSelect() const
{
    return select && !attach && !is_create_empty && !is_ordinary_view && (!(is_materialized_view || is_window_view) || is_populate);
}
}
