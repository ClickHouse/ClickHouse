#include <Parsers/ASTAlterQuery.h>

#include <Core/ServerSettings.h>
#include <IO/Operators.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Storages/DataDestinationType.h>
#include <base/scope_guard.h>
#include <Common/quoteString.h>
#include <base/EnumReflection.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int BAD_ARGUMENTS;
}

String ASTAlterCommand::getID(char delim) const
{
    return fmt::format("AlterCommand{}{}", delim, type);
}

ASTPtr ASTAlterCommand::clone() const
{
    auto res = make_intrusive<ASTAlterCommand>(*this);
    res->children.clear();

    if (col_decl)
        res->col_decl = res->children.emplace_back(col_decl->clone()).get();
    if (column)
        res->column = res->children.emplace_back(column->clone()).get();
    if (order_by)
        res->order_by = res->children.emplace_back(order_by->clone()).get();
    if (sample_by)
        res->sample_by = res->children.emplace_back(sample_by->clone()).get();
    if (index_decl)
        res->index_decl = res->children.emplace_back(index_decl->clone()).get();
    if (index)
        res->index = res->children.emplace_back(index->clone()).get();
    if (constraint_decl)
        res->constraint_decl = res->children.emplace_back(constraint_decl->clone()).get();
    if (constraint)
        res->constraint = res->children.emplace_back(constraint->clone()).get();
    if (projection_decl)
        res->projection_decl = res->children.emplace_back(projection_decl->clone()).get();
    if (projection)
        res->projection = res->children.emplace_back(projection->clone()).get();
    if (statistics_decl)
        res->statistics_decl = res->children.emplace_back(statistics_decl->clone()).get();
    if (partition)
        res->partition = res->children.emplace_back(partition->clone()).get();
    if (predicate)
        res->predicate = res->children.emplace_back(predicate->clone()).get();
    if (update_assignments)
        res->update_assignments = res->children.emplace_back(update_assignments->clone()).get();
    if (comment)
        res->comment = res->children.emplace_back(comment->clone()).get();
    if (ttl)
        res->ttl = res->children.emplace_back(ttl->clone()).get();
    if (settings_changes)
        res->settings_changes = res->children.emplace_back(settings_changes->clone()).get();
    if (settings_resets)
        res->settings_resets = res->children.emplace_back(settings_resets->clone()).get();
    if (select)
        res->select = res->children.emplace_back(select->clone()).get();
    if (sql_security)
        res->sql_security = res->children.emplace_back(sql_security->clone()).get();
    if (rename_to)
        res->rename_to = res->children.emplace_back(rename_to->clone()).get();
    if (execute_args)
        res->execute_args = res->children.emplace_back(execute_args->clone()).get();

    return res;
}

void ASTAlterCommand::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "AlterCommand");
    w.writeInt("command_type", static_cast<Int64>(type));

    w.writeBool("detach", detach);
    w.writeBool("part", part);
    w.writeBool("clear_column", clear_column);
    w.writeBool("clear_index", clear_index);
    w.writeBool("clear_statistics", clear_statistics);
    w.writeBool("clear_projection", clear_projection);
    w.writeBool("if_not_exists", if_not_exists);
    w.writeBool("if_exists", if_exists);
    w.writeBool("first", first);
    w.writeBool("replace", replace);

    if (type == ASTAlterCommand::MOVE_PARTITION)
        w.writeString("move_destination_type", std::string(magic_enum::enum_name(move_destination_type)));

    if (!move_destination_name.empty())
        w.writeString("move_destination_name", move_destination_name);
    if (!from.empty())
        w.writeString("from", from);
    if (!with_name.empty())
        w.writeString("with_name", with_name);
    if (!from_database.empty())
        w.writeString("from_database", from_database);
    if (!from_table.empty())
        w.writeString("from_table", from_table);
    if (!to_database.empty())
        w.writeString("to_database", to_database);
    if (!to_table.empty())
        w.writeString("to_table", to_table);
    if (!snapshot_name.empty())
        w.writeString("snapshot_name", snapshot_name);
    if (!execute_command_name.empty())
        w.writeString("execute_command_name", execute_command_name);
    if (!remove_property.empty())
        w.writeString("remove_property", remove_property);

    w.writeChild("col_decl", col_decl);
    w.writeChild("column", column);
    w.writeChild("order_by", order_by);
    w.writeChild("sample_by", sample_by);
    w.writeChild("index_decl", index_decl);
    w.writeChild("index", index);
    w.writeChild("constraint_decl", constraint_decl);
    w.writeChild("constraint", constraint);
    w.writeChild("projection_decl", projection_decl);
    w.writeChild("projection", projection);
    w.writeChild("statistics_decl", statistics_decl);
    w.writeChild("partition", partition);
    w.writeChild("predicate", predicate);
    w.writeChild("update_assignments", update_assignments);
    w.writeChild("comment", comment);
    w.writeChild("ttl", ttl);
    w.writeChild("settings_changes", settings_changes);
    w.writeChild("settings_resets", settings_resets);
    w.writeChild("select", select);
    w.writeChild("sql_security", sql_security);
    w.writeChild("rename_to", rename_to);
    w.writeChild("refresh", refresh);
    w.writeChild("snapshot_desc", snapshot_desc);
    w.writeChild("execute_args", execute_args);
}

void ASTAlterCommand::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    Int64 command_type_value = r.getInt("command_type");
    auto command_type_opt = magic_enum::enum_cast<Type>(static_cast<std::underlying_type_t<Type>>(command_type_value));
    if (!command_type_opt || static_cast<Int64>(*command_type_opt) != command_type_value)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown ALTER command_type: {}", command_type_value);
    type = *command_type_opt;

    detach = r.getBool("detach");
    part = r.getBool("part");
    clear_column = r.getBool("clear_column");
    clear_index = r.getBool("clear_index");
    clear_statistics = r.getBool("clear_statistics");
    clear_projection = r.getBool("clear_projection");
    if_not_exists = r.getBool("if_not_exists");
    if_exists = r.getBool("if_exists");
    first = r.getBool("first");
    replace = r.getBool("replace");

    if (r.has("move_destination_type"))
    {
        String move_dest_type_str = r.getString("move_destination_type");
        auto move_dest_opt = magic_enum::enum_cast<DataDestinationType>(move_dest_type_str);
        if (!move_dest_opt)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown move_destination_type: '{}'", move_dest_type_str);
        move_destination_type = *move_dest_opt;
    }

    move_destination_name = r.getString("move_destination_name");
    from = r.getString("from");
    with_name = r.getString("with_name");
    from_database = r.getString("from_database");
    from_table = r.getString("from_table");
    to_database = r.getString("to_database");
    to_table = r.getString("to_table");
    snapshot_name = r.getString("snapshot_name");
    execute_command_name = r.getString("execute_command_name");
    remove_property = r.getString("remove_property");

    auto readRawChild = [&](const char * key, IAST *& field)
    {
        auto child = r.readChild(key);
        if (child)
        {
            field = child.get();
            children.push_back(std::move(child));
        }
    };

    readRawChild("col_decl", col_decl);
    readRawChild("column", column);
    readRawChild("order_by", order_by);
    readRawChild("sample_by", sample_by);
    readRawChild("index_decl", index_decl);
    readRawChild("index", index);
    readRawChild("constraint_decl", constraint_decl);
    readRawChild("constraint", constraint);
    readRawChild("projection_decl", projection_decl);
    readRawChild("projection", projection);
    readRawChild("statistics_decl", statistics_decl);
    readRawChild("partition", partition);
    readRawChild("predicate", predicate);
    readRawChild("update_assignments", update_assignments);
    readRawChild("comment", comment);
    readRawChild("ttl", ttl);
    readRawChild("settings_changes", settings_changes);
    readRawChild("settings_resets", settings_resets);
    readRawChild("select", select);
    readRawChild("sql_security", sql_security);
    readRawChild("rename_to", rename_to);
    readRawChild("snapshot_desc", snapshot_desc);
    readRawChild("execute_args", execute_args);

    auto child = r.readChild("refresh");
    if (child)
    {
        refresh = child;
        children.push_back(refresh);
    }
}

/// When the alter command is about statistics, the Parentheses is necessary to avoid ambiguity.
bool needToFormatWithParentheses(ASTAlterCommand::Type type)
{
    return type == ASTAlterCommand::ADD_STATISTICS
        || type == ASTAlterCommand::DROP_STATISTICS
        || type == ASTAlterCommand::MATERIALIZE_STATISTICS
        || type == ASTAlterCommand::MODIFY_STATISTICS;
}

void ASTAlterCommand::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "(";
    auto closing_bracket_guard = make_scope_guard(std::function<void(void)>([&ostr]() { ostr << ")"; }));

    if (type == ASTAlterCommand::ADD_COLUMN)
    {
        ostr << "ADD COLUMN " << (if_not_exists ? "IF NOT EXISTS " : "")
                     ;
        col_decl->format(ostr, settings, state, frame);

        if (first)
            ostr << " FIRST ";
        else if (column) /// AFTER
        {
            ostr << " AFTER ";
            column->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::DROP_COLUMN)
    {
        ostr << (clear_column ? "CLEAR " : "DROP ") << "COLUMN "
                      << (if_exists ? "IF EXISTS " : "");
        column->format(ostr, settings, state, frame);
        if (partition)
        {
            ostr << " IN PARTITION ";
            partition->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::MODIFY_COLUMN)
    {
        ostr << "MODIFY COLUMN " << (if_exists ? "IF EXISTS " : "")
                     ;
        col_decl->format(ostr, settings, state, frame);

        if (!remove_property.empty())
        {
            ostr << " REMOVE " << remove_property;
        }
        else if (settings_changes)
        {
            ostr << " MODIFY SETTING ";
            settings_changes->format(ostr, settings, state, frame);
        }
        else if (settings_resets)
        {
            ostr << " RESET SETTING ";
            settings_resets->format(ostr, settings, state, frame);
        }
        else
        {
            if (first)
                ostr << " FIRST ";
            else if (column) /// AFTER
            {
                ostr << " AFTER ";
                column->format(ostr, settings, state, frame);
            }
        }
    }
    else if (type == ASTAlterCommand::MATERIALIZE_COLUMN)
    {
        ostr << "MATERIALIZE COLUMN ";
        column->format(ostr, settings, state, frame);
        if (partition)
        {
            ostr << " IN PARTITION ";
            partition->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::COMMENT_COLUMN)
    {
        ostr << "COMMENT COLUMN " << (if_exists ? "IF EXISTS " : "")
                     ;
        column->format(ostr, settings, state, frame);
        ostr << " ";
        comment->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_COMMENT || type == ASTAlterCommand::MODIFY_DATABASE_COMMENT)
    {
        ostr << "MODIFY COMMENT";
        ostr << " ";
        comment->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_ORDER_BY)
    {
        ostr << "MODIFY ORDER BY ";
        order_by->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_SAMPLE_BY)
    {
        ostr << "MODIFY SAMPLE BY ";
        sample_by->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::REMOVE_SAMPLE_BY)
    {
        ostr << "REMOVE SAMPLE BY";
    }
    else if (type == ASTAlterCommand::ADD_INDEX)
    {
        ostr << "ADD INDEX " << (if_not_exists ? "IF NOT EXISTS " : "")
                     ;
        index_decl->format(ostr, settings, state, frame);

        if (first)
            ostr << " FIRST ";
        else if (index) /// AFTER
        {
            ostr << " AFTER ";
            index->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::DROP_INDEX)
    {
        ostr << (clear_index ? "CLEAR " : "DROP ") << "INDEX "
                      << (if_exists ? "IF EXISTS " : "");
        index->format(ostr, settings, state, frame);
        if (partition)
        {
            ostr << " IN PARTITION ";
            partition->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::MATERIALIZE_INDEX)
    {
        ostr << "MATERIALIZE INDEX ";
        index->format(ostr, settings, state, frame);
        if (partition)
        {
            ostr << " IN PARTITION ";
            partition->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::ADD_STATISTICS)
    {
        ostr << "ADD STATISTICS " << (if_not_exists ? "IF NOT EXISTS " : "")
                     ;
        statistics_decl->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_STATISTICS)
    {
        ostr << "MODIFY STATISTICS "
                     ;
        statistics_decl->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::DROP_STATISTICS)
    {
        ostr << (clear_statistics ? "CLEAR " : "DROP ") << "STATISTICS "
                      << (if_exists ? "IF EXISTS " : "");

        if (statistics_decl)
            statistics_decl->format(ostr, settings, state, frame);
        else
            ostr << " ALL";

        if (partition)
        {
            ostr << " IN PARTITION ";
            partition->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::MATERIALIZE_STATISTICS)
    {
        ostr << "MATERIALIZE STATISTICS ";
        if (statistics_decl)
        {
            statistics_decl->format(ostr, settings, state, frame);
            if (partition)
            {
                ostr << " IN PARTITION ";
                partition->format(ostr, settings, state, frame);
            }
        }
        else
            ostr << " ALL";
    }
    else if (type == ASTAlterCommand::UNLOCK_SNAPSHOT)
    {
        ostr << "UNLOCK SNAPSHOT ";
        ostr << quoteString(snapshot_name);
        if (snapshot_desc != nullptr)
        {
            ostr << " FROM ";
            snapshot_desc->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::ADD_CONSTRAINT)
    {
        ostr << "ADD CONSTRAINT " << (if_not_exists ? "IF NOT EXISTS " : "")
                     ;
        constraint_decl->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::DROP_CONSTRAINT)
    {
        ostr << "DROP CONSTRAINT " << (if_exists ? "IF EXISTS " : "")
                     ;
        constraint->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::ADD_PROJECTION)
    {
        ostr << "ADD PROJECTION " << (if_not_exists ? "IF NOT EXISTS " : "")
                     ;
        projection_decl->format(ostr, settings, state, frame);

        if (first)
            ostr << " FIRST ";
        else if (projection)
        {
            ostr << " AFTER ";
            projection->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::DROP_PROJECTION)
    {
        ostr << (clear_projection ? "CLEAR " : "DROP ") << "PROJECTION "
                      << (if_exists ? "IF EXISTS " : "");
        projection->format(ostr, settings, state, frame);
        if (partition)
        {
            ostr << " IN PARTITION ";
            partition->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::MATERIALIZE_PROJECTION)
    {
        ostr << "MATERIALIZE PROJECTION ";
        projection->format(ostr, settings, state, frame);
        if (partition)
        {
            ostr << " IN PARTITION ";
            partition->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::DROP_PARTITION)
    {
        ostr << (detach ? "DETACH" : "DROP") << (part ? " PART " : " PARTITION ")
                     ;
        partition->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::DROP_DETACHED_PARTITION)
    {
        ostr << "DROP DETACHED" << (part ? " PART " : " PARTITION ")
                     ;
        partition->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::FORGET_PARTITION)
    {
        ostr << "FORGET PARTITION "
                     ;
        partition->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::ATTACH_PARTITION)
    {
        ostr << "ATTACH " << (part ? "PART " : "PARTITION ")
                     ;
        partition->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::MOVE_PARTITION)
    {
        ostr << "MOVE " << (part ? "PART " : "PARTITION ")
                     ;
        partition->format(ostr, settings, state, frame);
        ostr << " TO ";
        switch (move_destination_type)
        {
            case DataDestinationType::DISK:
                ostr << "DISK ";
                break;
            case DataDestinationType::VOLUME:
                ostr << "VOLUME ";
                break;
            case DataDestinationType::TABLE:
                ostr << "TABLE ";
                if (!to_database.empty())
                {
                    ostr << backQuoteIfNeed(to_database)
                                  << ".";
                }
                ostr << backQuoteIfNeed(to_table)
                             ;
                return;
            default:
                break;
        }
        if (move_destination_type != DataDestinationType::TABLE)
        {
            ostr << quoteString(move_destination_name);
        }
    }
    else if (type == ASTAlterCommand::REPLACE_PARTITION)
    {
        ostr << (replace ? "REPLACE" : "ATTACH") << " PARTITION "
                     ;
        partition->format(ostr, settings, state, frame);
        ostr << " FROM ";
        if (!from_database.empty())
        {
            ostr << backQuoteIfNeed(from_database)
                          << ".";
        }
        ostr << backQuoteIfNeed(from_table);
    }
    else if (type == ASTAlterCommand::FETCH_PARTITION)
    {
        ostr << "FETCH " << (part ? "PART " : "PARTITION ")
                     ;
        partition->format(ostr, settings, state, frame);
        ostr << " FROM " << DB::quote << from;
    }
    else if (type == ASTAlterCommand::FREEZE_PARTITION)
    {
        ostr << "FREEZE PARTITION ";
        partition->format(ostr, settings, state, frame);

        if (!with_name.empty())
        {
            ostr << " " << "WITH NAME" << " "
                          << DB::quote << with_name;
        }
    }
    else if (type == ASTAlterCommand::FREEZE_ALL)
    {
        ostr << "FREEZE";

        if (!with_name.empty())
        {
            ostr << " " << "WITH NAME" << " "
                          << DB::quote << with_name;
        }
    }
    else if (type == ASTAlterCommand::UNFREEZE_PARTITION)
    {
        ostr << "UNFREEZE PARTITION ";
        partition->format(ostr, settings, state, frame);

        if (!with_name.empty())
        {
            ostr << " " << "WITH NAME" << " "
                          << DB::quote << with_name;
        }
    }
    else if (type == ASTAlterCommand::UNFREEZE_ALL)
    {
        ostr << "UNFREEZE";

        if (!with_name.empty())
        {
            ostr << " " << "WITH NAME" << " "
                          << DB::quote << with_name;
        }
    }
    else if (type == ASTAlterCommand::DELETE)
    {
        ostr << "DELETE";

        if (partition)
        {
            ostr << " IN PARTITION ";
            partition->format(ostr, settings, state, frame);
        }

        ostr << " WHERE ";
        predicate->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::UPDATE)
    {
        ostr << "UPDATE ";
        update_assignments->format(ostr, settings, state, frame);

        if (partition)
        {
            ostr << " IN PARTITION ";
            partition->format(ostr, settings, state, frame);
        }

        ostr << " WHERE ";
        predicate->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_TTL)
    {
        ostr << "MODIFY TTL ";
        auto nested_frame = frame;
        nested_frame.expression_list_prepend_whitespace = false;
        ttl->format(ostr, settings, state, nested_frame);
    }
    else if (type == ASTAlterCommand::REMOVE_TTL)
    {
        ostr << "REMOVE TTL";
    }
    else if (type == ASTAlterCommand::MATERIALIZE_TTL)
    {
        ostr << "MATERIALIZE TTL";
        if (partition)
        {
            ostr << " IN PARTITION ";
            partition->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::REWRITE_PARTS)
    {
        ostr << "REWRITE PARTS";
        if (partition)
        {
            ostr << " IN PARTITION ";
            partition->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::MODIFY_SETTING)
    {
        ostr << "MODIFY SETTING ";
        settings_changes->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::RESET_SETTING)
    {
        ostr << "RESET SETTING ";
        settings_resets->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_DATABASE_SETTING)
    {
        ostr << "MODIFY SETTING ";
        settings_changes->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_QUERY)
    {
        ostr << "MODIFY QUERY" << settings.nl_or_ws;

        /// When the ALTER query has trailing SETTINGS (inherited from ASTQueryWithOutput),
        /// we must wrap the MODIFY QUERY select in parentheses. Otherwise the trailing
        /// SETTINGS clause would be consumed by `ParserSelectQuery` as part of the
        /// last SELECT during re-parsing, instead of remaining on the ALTER query.
        /// Clear the flags to prevent inner nodes from adding redundant parentheses.
        if (frame.parent_has_trailing_settings)
        {
            ostr << "(";
            frame.parent_has_trailing_settings = false;
            select->format(ostr, settings, state, frame);
            ostr << ")";
        }
        else
        {
            select->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::MODIFY_REFRESH)
    {
        ostr << "MODIFY" << settings.nl_or_ws
                     ;
        refresh->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::RENAME_COLUMN)
    {
        ostr << "RENAME COLUMN " << (if_exists ? "IF EXISTS " : "")
                     ;
        column->format(ostr, settings, state, frame);

        ostr << " TO ";
        rename_to->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_SQL_SECURITY)
    {
        ostr << "MODIFY ";
        sql_security->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::APPLY_DELETED_MASK)
    {
        ostr << "APPLY DELETED MASK";

        if (partition)
        {
            ostr << " IN PARTITION ";
            partition->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::APPLY_PATCHES)
    {
        ostr << "APPLY PATCHES";

        if (partition)
        {
            ostr << " IN PARTITION ";
            partition->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::EXECUTE_COMMAND)
    {
        ostr << "EXECUTE " << execute_command_name << "(";
        if (execute_args)
            execute_args->format(ostr, settings, state, frame);
        ostr << ")";
    }
    else
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Unexpected type of ALTER");
}

void ASTAlterCommand::forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f)
{
    f(&col_decl, nullptr);
    f(&column, nullptr);
    f(&order_by, nullptr);
    f(&sample_by, nullptr);
    f(&index_decl, nullptr);
    f(&index, nullptr);
    f(&constraint_decl, nullptr);
    f(&constraint, nullptr);
    f(&projection_decl, nullptr);
    f(&projection, nullptr);
    f(&statistics_decl, nullptr);
    f(&partition, nullptr);
    f(&predicate, nullptr);
    f(&update_assignments, nullptr);
    f(&comment, nullptr);
    f(&ttl, nullptr);
    f(&settings_changes, nullptr);
    f(&settings_resets, nullptr);
    f(&select, nullptr);
    f(&sql_security, nullptr);
    f(&rename_to, nullptr);
    f(&execute_args, nullptr);
}


bool ASTAlterQuery::isOneCommandTypeOnly(const ASTAlterCommand::Type & type) const
{
    if (command_list)
    {
        if (command_list->children.empty())
            return false;
        for (const auto & child : command_list->children)
        {
            const auto & command = child->as<const ASTAlterCommand &>();
            if (command.type != type)
                return false;
        }
        return true;
    }
    return false;
}

bool ASTAlterQuery::isSettingsAlter() const
{
    return isOneCommandTypeOnly(ASTAlterCommand::MODIFY_SETTING) || isOneCommandTypeOnly(ASTAlterCommand::RESET_SETTING);
}

bool ASTAlterQuery::isFreezeAlter() const
{
    return isOneCommandTypeOnly(ASTAlterCommand::FREEZE_PARTITION) || isOneCommandTypeOnly(ASTAlterCommand::FREEZE_ALL)
        || isOneCommandTypeOnly(ASTAlterCommand::UNFREEZE_PARTITION) || isOneCommandTypeOnly(ASTAlterCommand::UNFREEZE_ALL);
}

bool ASTAlterQuery::isUnlockSnapshot() const
{
    return isOneCommandTypeOnly(ASTAlterCommand::UNLOCK_SNAPSHOT);
}

bool ASTAlterQuery::isAttachAlter() const
{
    return isOneCommandTypeOnly(ASTAlterCommand::ATTACH_PARTITION);
}

bool ASTAlterQuery::isFetchAlter() const
{
    return isOneCommandTypeOnly(ASTAlterCommand::FETCH_PARTITION);
}

bool ASTAlterQuery::isDropPartitionAlter() const
{
    return isOneCommandTypeOnly(ASTAlterCommand::DROP_PARTITION) || isOneCommandTypeOnly(ASTAlterCommand::DROP_DETACHED_PARTITION);
}

bool ASTAlterQuery::isCommentAlter() const
{
    return isOneCommandTypeOnly(ASTAlterCommand::COMMENT_COLUMN) || isOneCommandTypeOnly(ASTAlterCommand::MODIFY_COMMENT);
}

bool ASTAlterQuery::isMovePartitionToDiskOrVolumeAlter() const
{
    if (command_list)
    {
        if (command_list->children.empty())
            return false;
        for (const auto & child : command_list->children)
        {
            const auto & command = child->as<const ASTAlterCommand &>();
            if (command.type != ASTAlterCommand::MOVE_PARTITION ||
                (command.move_destination_type != DataDestinationType::DISK && command.move_destination_type != DataDestinationType::VOLUME))
                return false;
        }
        return true;
    }
    return false;
}


/** Get the text that identifies this element. */
String ASTAlterQuery::getID(char delim) const
{
    return "AlterQuery" + (delim + getDatabase()) + delim + getTable();
}

ASTPtr ASTAlterQuery::clone() const
{
    auto res = make_intrusive<ASTAlterQuery>(*this);
    res->children.clear();

    if (command_list)
        res->set(res->command_list, command_list->clone());

    cloneOutputOptions(*res);
    cloneTableOptions(*res);

    return res;
}

void ASTAlterQuery::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "AlterQuery");

    w.writeString("database", getDatabase());
    w.writeString("table", getTable());

    if (!cluster.empty())
        w.writeString("cluster", cluster);

    const char * obj_type = "UNKNOWN";
    if (alter_object == AlterObjectType::TABLE)
        obj_type = "TABLE";
    else if (alter_object == AlterObjectType::DATABASE)
        obj_type = "DATABASE";
    w.writeString("alter_object", std::string_view(obj_type));

    w.writeChild("command_list", command_list);
}

void ASTAlterQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    String db = r.getString("database");
    if (!db.empty())
        setDatabase(db);
    String tbl = r.getString("table");
    if (!tbl.empty())
        setTable(tbl);

    cluster = r.getString("cluster");

    String obj_type = r.getString("alter_object");
    if (obj_type == "TABLE")
        alter_object = AlterObjectType::TABLE;
    else if (obj_type == "DATABASE")
        alter_object = AlterObjectType::DATABASE;
    else
        alter_object = AlterObjectType::UNKNOWN;

    auto child = r.readChild("command_list");
    if (!child)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'command_list' field in `AlterQuery` during AST JSON deserialization");
    set(command_list, child);
}

void ASTAlterQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    std::string indent_str = settings.one_line ? "" : std::string(4u * frame.indent, ' ');
    ostr << indent_str;

    switch (alter_object)
    {
        case AlterObjectType::TABLE:
            ostr << "ALTER TABLE ";
            break;
        case AlterObjectType::DATABASE:
            ostr << "ALTER DATABASE ";
            break;
        default:
            break;
    }

    if (table)
    {
        ostr << indent_str;
        if (database)
        {
            database->format(ostr, settings, state, frame);
            ostr << '.';
        }

        chassert(table);
        table->format(ostr, settings, state, frame);
    }
    else if (alter_object == AlterObjectType::DATABASE && database)
    {
        ostr << indent_str;
        database->format(ostr, settings, state, frame);
    }

    formatOnCluster(ostr, settings);

    FormatStateStacked frame_nested = frame;
    if (settings.one_line)
    {
        frame_nested.expression_list_prepend_whitespace = true;
        command_list->format(ostr, settings, state, frame_nested);
    }
    else
    {
        frame_nested.expression_list_always_start_on_new_line = true;
        command_list->as<ASTExpressionList &>().formatImplMultiline(ostr, settings, state, frame_nested);
    }
}

void ASTAlterQuery::forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f)
{
    for (const auto & child : command_list->children)
        child->as<ASTAlterCommand &>().forEachPointerToChild(f);
    f(reinterpret_cast<IAST **>(&command_list), nullptr);
}

}
