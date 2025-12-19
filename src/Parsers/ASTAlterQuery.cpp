#include <Parsers/ASTAlterQuery.h>

#include <Core/ServerSettings.h>
#include <IO/Operators.h>
#include <base/scope_guard.h>
#include <Common/quoteString.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNEXPECTED_AST_STRUCTURE;
}

String ASTAlterCommand::getID(char delim) const
{
    return fmt::format("AlterCommand{}{}", delim, type);
}

ASTPtr ASTAlterCommand::clone() const
{
    auto res = std::make_shared<ASTAlterCommand>(*this);
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

    return res;
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
            ostr << "FROM ";
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
        ttl->format(ostr, settings, state, frame);
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
        ostr << "MODIFY QUERY" << settings.nl_or_ws
                     ;
        select->format(ostr, settings, state, frame);
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
    else
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Unexpected type of ALTER");
}

void ASTAlterCommand::forEachPointerToChild(std::function<void(void**)> f)
{
    f(reinterpret_cast<void **>(&col_decl));
    f(reinterpret_cast<void **>(&column));
    f(reinterpret_cast<void **>(&order_by));
    f(reinterpret_cast<void **>(&sample_by));
    f(reinterpret_cast<void **>(&index_decl));
    f(reinterpret_cast<void **>(&index));
    f(reinterpret_cast<void **>(&constraint_decl));
    f(reinterpret_cast<void **>(&constraint));
    f(reinterpret_cast<void **>(&projection_decl));
    f(reinterpret_cast<void **>(&projection));
    f(reinterpret_cast<void **>(&statistics_decl));
    f(reinterpret_cast<void **>(&partition));
    f(reinterpret_cast<void **>(&predicate));
    f(reinterpret_cast<void **>(&update_assignments));
    f(reinterpret_cast<void **>(&comment));
    f(reinterpret_cast<void **>(&ttl));
    f(reinterpret_cast<void **>(&settings_changes));
    f(reinterpret_cast<void **>(&settings_resets));
    f(reinterpret_cast<void **>(&select));
    f(reinterpret_cast<void **>(&sql_security));
    f(reinterpret_cast<void **>(&rename_to));
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
    auto res = std::make_shared<ASTAlterQuery>(*this);
    res->children.clear();

    if (command_list)
        res->set(res->command_list, command_list->clone());

    return res;
}

void ASTAlterQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

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
    frame_nested.need_parens = false;
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

void ASTAlterQuery::forEachPointerToChild(std::function<void(void**)> f)
{
    for (const auto & child : command_list->children)
        child->as<ASTAlterCommand &>().forEachPointerToChild(f);
    f(reinterpret_cast<void **>(&command_list));
}

}
