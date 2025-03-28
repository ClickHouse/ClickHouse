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
    scope_guard closing_bracket_guard;
    if (format_alter_commands_with_parentheses || needToFormatWithParentheses(type))
    {
        ostr << "(";
        closing_bracket_guard = make_scope_guard(std::function<void(void)>([&ostr]() { ostr << ")"; }));
    }

    if (type == ASTAlterCommand::ADD_COLUMN)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "ADD COLUMN " << (if_not_exists ? "IF NOT EXISTS " : "")
                      << (settings.hilite ? hilite_none : "");
        col_decl->format(ostr, settings, state, frame);

        if (first)
            ostr << (settings.hilite ? hilite_keyword : "") << " FIRST " << (settings.hilite ? hilite_none : "");
        else if (column) /// AFTER
        {
            ostr << (settings.hilite ? hilite_keyword : "") << " AFTER " << (settings.hilite ? hilite_none : "");
            column->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::DROP_COLUMN)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << (clear_column ? "CLEAR " : "DROP ") << "COLUMN "
                      << (if_exists ? "IF EXISTS " : "") << (settings.hilite ? hilite_none : "");
        column->format(ostr, settings, state, frame);
        if (partition)
        {
            ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::MODIFY_COLUMN)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "MODIFY COLUMN " << (if_exists ? "IF EXISTS " : "")
                      << (settings.hilite ? hilite_none : "");
        col_decl->format(ostr, settings, state, frame);

        if (!remove_property.empty())
        {
            ostr << (settings.hilite ? hilite_keyword : "") << " REMOVE " << remove_property;
        }
        else if (settings_changes)
        {
            ostr << (settings.hilite ? hilite_keyword : "") << " MODIFY SETTING " << (settings.hilite ? hilite_none : "");
            settings_changes->format(ostr, settings, state, frame);
        }
        else if (settings_resets)
        {
            ostr << (settings.hilite ? hilite_keyword : "") << " RESET SETTING " << (settings.hilite ? hilite_none : "");
            settings_resets->format(ostr, settings, state, frame);
        }
        else
        {
            if (first)
                ostr << (settings.hilite ? hilite_keyword : "") << " FIRST " << (settings.hilite ? hilite_none : "");
            else if (column) /// AFTER
            {
                ostr << (settings.hilite ? hilite_keyword : "") << " AFTER " << (settings.hilite ? hilite_none : "");
                column->format(ostr, settings, state, frame);
            }
        }
    }
    else if (type == ASTAlterCommand::MATERIALIZE_COLUMN)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "MATERIALIZE COLUMN " << (settings.hilite ? hilite_none : "");
        column->format(ostr, settings, state, frame);
        if (partition)
        {
            ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::COMMENT_COLUMN)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "COMMENT COLUMN " << (if_exists ? "IF EXISTS " : "")
                      << (settings.hilite ? hilite_none : "");
        column->format(ostr, settings, state, frame);
        ostr << " " << (settings.hilite ? hilite_none : "");
        comment->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_COMMENT || type == ASTAlterCommand::MODIFY_DATABASE_COMMENT)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "MODIFY COMMENT" << (settings.hilite ? hilite_none : "");
        ostr << " " << (settings.hilite ? hilite_none : "");
        comment->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_ORDER_BY)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "MODIFY ORDER BY " << (settings.hilite ? hilite_none : "");
        order_by->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_SAMPLE_BY)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "MODIFY SAMPLE BY " << (settings.hilite ? hilite_none : "");
        sample_by->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::REMOVE_SAMPLE_BY)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "REMOVE SAMPLE BY" << (settings.hilite ? hilite_none : "");
    }
    else if (type == ASTAlterCommand::ADD_INDEX)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "ADD INDEX " << (if_not_exists ? "IF NOT EXISTS " : "")
                      << (settings.hilite ? hilite_none : "");
        index_decl->format(ostr, settings, state, frame);

        if (first)
            ostr << (settings.hilite ? hilite_keyword : "") << " FIRST " << (settings.hilite ? hilite_none : "");
        else if (index) /// AFTER
        {
            ostr << (settings.hilite ? hilite_keyword : "") << " AFTER " << (settings.hilite ? hilite_none : "");
            index->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::DROP_INDEX)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << (clear_index ? "CLEAR " : "DROP ") << "INDEX "
                      << (if_exists ? "IF EXISTS " : "") << (settings.hilite ? hilite_none : "");
        index->format(ostr, settings, state, frame);
        if (partition)
        {
            ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::MATERIALIZE_INDEX)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "MATERIALIZE INDEX " << (settings.hilite ? hilite_none : "");
        index->format(ostr, settings, state, frame);
        if (partition)
        {
            ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::ADD_STATISTICS)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "ADD STATISTICS " << (if_not_exists ? "IF NOT EXISTS " : "")
                      << (settings.hilite ? hilite_none : "");
        statistics_decl->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_STATISTICS)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "MODIFY STATISTICS "
                      << (settings.hilite ? hilite_none : "");
        statistics_decl->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::DROP_STATISTICS)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << (clear_statistics ? "CLEAR " : "DROP ") << "STATISTICS "
                      << (if_exists ? "IF EXISTS " : "") << (settings.hilite ? hilite_none : "");
        statistics_decl->format(ostr, settings, state, frame);
        if (partition)
        {
            ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::MATERIALIZE_STATISTICS)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "MATERIALIZE STATISTICS " << (settings.hilite ? hilite_none : "");
        statistics_decl->format(ostr, settings, state, frame);
        if (partition)
        {
            ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::UNLOCK_SNAPSHOT)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "UNLOCK SNAPSHOT " << (settings.hilite ? hilite_none : "");
        ostr << quoteString(snapshot_name);
        ostr << (settings.hilite ? hilite_keyword : "") << "FROM " << (settings.hilite ? hilite_none : "");
        snapshot_desc->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::ADD_CONSTRAINT)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "ADD CONSTRAINT " << (if_not_exists ? "IF NOT EXISTS " : "")
                      << (settings.hilite ? hilite_none : "");
        constraint_decl->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::DROP_CONSTRAINT)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "DROP CONSTRAINT " << (if_exists ? "IF EXISTS " : "")
                      << (settings.hilite ? hilite_none : "");
        constraint->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::ADD_PROJECTION)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "ADD PROJECTION " << (if_not_exists ? "IF NOT EXISTS " : "")
                      << (settings.hilite ? hilite_none : "");
        projection_decl->format(ostr, settings, state, frame);

        if (first)
            ostr << (settings.hilite ? hilite_keyword : "") << " FIRST " << (settings.hilite ? hilite_none : "");
        else if (projection)
        {
            ostr << (settings.hilite ? hilite_keyword : "") << " AFTER " << (settings.hilite ? hilite_none : "");
            projection->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::DROP_PROJECTION)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << (clear_projection ? "CLEAR " : "DROP ") << "PROJECTION "
                      << (if_exists ? "IF EXISTS " : "") << (settings.hilite ? hilite_none : "");
        projection->format(ostr, settings, state, frame);
        if (partition)
        {
            ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::MATERIALIZE_PROJECTION)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "MATERIALIZE PROJECTION " << (settings.hilite ? hilite_none : "");
        projection->format(ostr, settings, state, frame);
        if (partition)
        {
            ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::DROP_PARTITION)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << (detach ? "DETACH" : "DROP") << (part ? " PART " : " PARTITION ")
                      << (settings.hilite ? hilite_none : "");
        partition->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::DROP_DETACHED_PARTITION)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "DROP DETACHED" << (part ? " PART " : " PARTITION ")
                      << (settings.hilite ? hilite_none : "");
        partition->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::FORGET_PARTITION)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "FORGET PARTITION "
                      << (settings.hilite ? hilite_none : "");
        partition->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::ATTACH_PARTITION)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "ATTACH " << (part ? "PART " : "PARTITION ")
                      << (settings.hilite ? hilite_none : "");
        partition->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::MOVE_PARTITION)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "MOVE " << (part ? "PART " : "PARTITION ")
                      << (settings.hilite ? hilite_none : "");
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
                    ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(to_database)
                                  << (settings.hilite ? hilite_none : "") << ".";
                }
                ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(to_table)
                              << (settings.hilite ? hilite_none : "");
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
        ostr << (settings.hilite ? hilite_keyword : "") << (replace ? "REPLACE" : "ATTACH") << " PARTITION "
                      << (settings.hilite ? hilite_none : "");
        partition->format(ostr, settings, state, frame);
        ostr << (settings.hilite ? hilite_keyword : "") << " FROM " << (settings.hilite ? hilite_none : "");
        if (!from_database.empty())
        {
            ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(from_database)
                          << (settings.hilite ? hilite_none : "") << ".";
        }
        ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(from_table) << (settings.hilite ? hilite_none : "");
    }
    else if (type == ASTAlterCommand::FETCH_PARTITION)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "FETCH " << (part ? "PART " : "PARTITION ")
                      << (settings.hilite ? hilite_none : "");
        partition->format(ostr, settings, state, frame);
        ostr << (settings.hilite ? hilite_keyword : "") << " FROM " << (settings.hilite ? hilite_none : "") << DB::quote << from;
    }
    else if (type == ASTAlterCommand::FREEZE_PARTITION)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "FREEZE PARTITION " << (settings.hilite ? hilite_none : "");
        partition->format(ostr, settings, state, frame);

        if (!with_name.empty())
        {
            ostr << " " << (settings.hilite ? hilite_keyword : "") << "WITH NAME" << (settings.hilite ? hilite_none : "") << " "
                          << DB::quote << with_name;
        }
    }
    else if (type == ASTAlterCommand::FREEZE_ALL)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "FREEZE" << (settings.hilite ? hilite_none : "");

        if (!with_name.empty())
        {
            ostr << " " << (settings.hilite ? hilite_keyword : "") << "WITH NAME" << (settings.hilite ? hilite_none : "") << " "
                          << DB::quote << with_name;
        }
    }
    else if (type == ASTAlterCommand::UNFREEZE_PARTITION)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "UNFREEZE PARTITION " << (settings.hilite ? hilite_none : "");
        partition->format(ostr, settings, state, frame);

        if (!with_name.empty())
        {
            ostr << " " << (settings.hilite ? hilite_keyword : "") << "WITH NAME" << (settings.hilite ? hilite_none : "") << " "
                          << DB::quote << with_name;
        }
    }
    else if (type == ASTAlterCommand::UNFREEZE_ALL)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "UNFREEZE" << (settings.hilite ? hilite_none : "");

        if (!with_name.empty())
        {
            ostr << " " << (settings.hilite ? hilite_keyword : "") << "WITH NAME" << (settings.hilite ? hilite_none : "") << " "
                          << DB::quote << with_name;
        }
    }
    else if (type == ASTAlterCommand::DELETE)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "DELETE" << (settings.hilite ? hilite_none : "");

        if (partition)
        {
            ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->format(ostr, settings, state, frame);
        }

        ostr << (settings.hilite ? hilite_keyword : "") << " WHERE " << (settings.hilite ? hilite_none : "");
        predicate->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::UPDATE)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "UPDATE " << (settings.hilite ? hilite_none : "");
        update_assignments->format(ostr, settings, state, frame);

        if (partition)
        {
            ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->format(ostr, settings, state, frame);
        }

        ostr << (settings.hilite ? hilite_keyword : "") << " WHERE " << (settings.hilite ? hilite_none : "");
        predicate->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_TTL)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "MODIFY TTL " << (settings.hilite ? hilite_none : "");
        ttl->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::REMOVE_TTL)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "REMOVE TTL" << (settings.hilite ? hilite_none : "");
    }
    else if (type == ASTAlterCommand::MATERIALIZE_TTL)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "MATERIALIZE TTL" << (settings.hilite ? hilite_none : "");
        if (partition)
        {
            ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->format(ostr, settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::MODIFY_SETTING)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "MODIFY SETTING " << (settings.hilite ? hilite_none : "");
        settings_changes->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::RESET_SETTING)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "RESET SETTING " << (settings.hilite ? hilite_none : "");
        settings_resets->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_DATABASE_SETTING)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "MODIFY SETTING " << (settings.hilite ? hilite_none : "");
        settings_changes->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_QUERY)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "MODIFY QUERY" << settings.nl_or_ws
                      << (settings.hilite ? hilite_none : "");
        select->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_REFRESH)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "MODIFY" << settings.nl_or_ws
                      << (settings.hilite ? hilite_none : "");
        refresh->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::RENAME_COLUMN)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "RENAME COLUMN " << (if_exists ? "IF EXISTS " : "")
                      << (settings.hilite ? hilite_none : "");
        column->format(ostr, settings, state, frame);

        ostr << (settings.hilite ? hilite_keyword : "") << " TO ";
        rename_to->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_SQL_SECURITY)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "MODIFY " << (settings.hilite ? hilite_none : "");
        sql_security->format(ostr, settings, state, frame);
    }
    else if (type == ASTAlterCommand::APPLY_DELETED_MASK)
    {
        ostr << (settings.hilite ? hilite_keyword : "") << "APPLY DELETED MASK" << (settings.hilite ? hilite_none : "");

        if (partition)
        {
            ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
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
    ostr << (settings.hilite ? hilite_keyword : "") << indent_str;

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

    ostr << (settings.hilite ? hilite_none : "");

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
