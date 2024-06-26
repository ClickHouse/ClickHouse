#include <Parsers/ASTAlterQuery.h>

#include <Core/ServerSettings.h>
#include <IO/Operators.h>
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

void ASTAlterCommand::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (format_alter_commands_with_parentheses)
        settings.ostr << "(";

    if (type == ASTAlterCommand::ADD_COLUMN)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "ADD COLUMN " << (if_not_exists ? "IF NOT EXISTS " : "")
                      << (settings.hilite ? hilite_none : "");
        col_decl->formatImpl(settings, state, frame);

        if (first)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FIRST " << (settings.hilite ? hilite_none : "");
        else if (column) /// AFTER
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " AFTER " << (settings.hilite ? hilite_none : "");
            column->formatImpl(settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::DROP_COLUMN)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << (clear_column ? "CLEAR " : "DROP ") << "COLUMN "
                      << (if_exists ? "IF EXISTS " : "") << (settings.hilite ? hilite_none : "");
        column->formatImpl(settings, state, frame);
        if (partition)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->formatImpl(settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::MODIFY_COLUMN)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "MODIFY COLUMN " << (if_exists ? "IF EXISTS " : "")
                      << (settings.hilite ? hilite_none : "");
        col_decl->formatImpl(settings, state, frame);

        if (!remove_property.empty())
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " REMOVE " << remove_property;
        }
        else if (settings_changes)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " MODIFY SETTING " << (settings.hilite ? hilite_none : "");
            settings_changes->formatImpl(settings, state, frame);
        }
        else if (settings_resets)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " RESET SETTING " << (settings.hilite ? hilite_none : "");
            settings_resets->formatImpl(settings, state, frame);
        }
        else
        {
            if (first)
                settings.ostr << (settings.hilite ? hilite_keyword : "") << " FIRST " << (settings.hilite ? hilite_none : "");
            else if (column) /// AFTER
            {
                settings.ostr << (settings.hilite ? hilite_keyword : "") << " AFTER " << (settings.hilite ? hilite_none : "");
                column->formatImpl(settings, state, frame);
            }
        }
    }
    else if (type == ASTAlterCommand::MATERIALIZE_COLUMN)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "MATERIALIZE COLUMN " << (settings.hilite ? hilite_none : "");
        column->formatImpl(settings, state, frame);
        if (partition)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->formatImpl(settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::MATERIALIZE_COLUMNS)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "MATERIALIZE COLUMNS " << (settings.hilite ? hilite_none : "");
        if (partition)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->formatImpl(settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::COMMENT_COLUMN)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "COMMENT COLUMN " << (if_exists ? "IF EXISTS " : "")
                      << (settings.hilite ? hilite_none : "");
        column->formatImpl(settings, state, frame);
        settings.ostr << " " << (settings.hilite ? hilite_none : "");
        comment->formatImpl(settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_COMMENT)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "MODIFY COMMENT" << (settings.hilite ? hilite_none : "");
        settings.ostr << " " << (settings.hilite ? hilite_none : "");
        comment->formatImpl(settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_ORDER_BY)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "MODIFY ORDER BY " << (settings.hilite ? hilite_none : "");
        order_by->formatImpl(settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_SAMPLE_BY)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "MODIFY SAMPLE BY " << (settings.hilite ? hilite_none : "");
        sample_by->formatImpl(settings, state, frame);
    }
    else if (type == ASTAlterCommand::REMOVE_SAMPLE_BY)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "REMOVE SAMPLE BY" << (settings.hilite ? hilite_none : "");
    }
    else if (type == ASTAlterCommand::ADD_INDEX)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "ADD INDEX " << (if_not_exists ? "IF NOT EXISTS " : "")
                      << (settings.hilite ? hilite_none : "");
        index_decl->formatImpl(settings, state, frame);

        if (first)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FIRST " << (settings.hilite ? hilite_none : "");
        else if (index) /// AFTER
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " AFTER " << (settings.hilite ? hilite_none : "");
            index->formatImpl(settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::DROP_INDEX)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << (clear_index ? "CLEAR " : "DROP ") << "INDEX "
                      << (if_exists ? "IF EXISTS " : "") << (settings.hilite ? hilite_none : "");
        index->formatImpl(settings, state, frame);
        if (partition)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->formatImpl(settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::MATERIALIZE_INDEX)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "MATERIALIZE INDEX " << (settings.hilite ? hilite_none : "");
        index->formatImpl(settings, state, frame);
        if (partition)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->formatImpl(settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::MATERIALIZE_INDEXES)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "MATERIALIZE INDEXES " << (settings.hilite ? hilite_none : "");
        if (partition)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->formatImpl(settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::ADD_STATISTICS)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "ADD STATISTICS " << (if_not_exists ? "IF NOT EXISTS " : "")
                      << (settings.hilite ? hilite_none : "");
        statistics_decl->formatImpl(settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_STATISTICS)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "MODIFY STATISTICS "
                      << (settings.hilite ? hilite_none : "");
        statistics_decl->formatImpl(settings, state, frame);
    }
    else if (type == ASTAlterCommand::DROP_STATISTICS)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << (clear_statistics ? "CLEAR " : "DROP ") << "STATISTICS "
                      << (if_exists ? "IF EXISTS " : "") << (settings.hilite ? hilite_none : "");
        statistics_decl->formatImpl(settings, state, frame);
        if (partition)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->formatImpl(settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::MATERIALIZE_STATISTICS)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "MATERIALIZE STATISTICS " << (settings.hilite ? hilite_none : "");
        statistics_decl->formatImpl(settings, state, frame);
        if (partition)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->formatImpl(settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::ADD_CONSTRAINT)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "ADD CONSTRAINT " << (if_not_exists ? "IF NOT EXISTS " : "")
                      << (settings.hilite ? hilite_none : "");
        constraint_decl->formatImpl(settings, state, frame);
    }
    else if (type == ASTAlterCommand::DROP_CONSTRAINT)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "DROP CONSTRAINT " << (if_exists ? "IF EXISTS " : "")
                      << (settings.hilite ? hilite_none : "");
        constraint->formatImpl(settings, state, frame);
    }
    else if (type == ASTAlterCommand::ADD_PROJECTION)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "ADD PROJECTION " << (if_not_exists ? "IF NOT EXISTS " : "")
                      << (settings.hilite ? hilite_none : "");
        projection_decl->formatImpl(settings, state, frame);

        if (first)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FIRST " << (settings.hilite ? hilite_none : "");
        else if (projection)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " AFTER " << (settings.hilite ? hilite_none : "");
            projection->formatImpl(settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::DROP_PROJECTION)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << (clear_projection ? "CLEAR " : "DROP ") << "PROJECTION "
                      << (if_exists ? "IF EXISTS " : "") << (settings.hilite ? hilite_none : "");
        projection->formatImpl(settings, state, frame);
        if (partition)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->formatImpl(settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::MATERIALIZE_PROJECTION)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "MATERIALIZE PROJECTION " << (settings.hilite ? hilite_none : "");
        projection->formatImpl(settings, state, frame);
        if (partition)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->formatImpl(settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::MATERIALIZE_PROJECTIONS)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "MATERIALIZE PROJECTIONS " << (settings.hilite ? hilite_none : "");
        if (partition)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->formatImpl(settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::DROP_PARTITION)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << (detach ? "DETACH" : "DROP") << (part ? " PART " : " PARTITION ")
                      << (settings.hilite ? hilite_none : "");
        partition->formatImpl(settings, state, frame);
    }
    else if (type == ASTAlterCommand::DROP_DETACHED_PARTITION)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "DROP DETACHED" << (part ? " PART " : " PARTITION ")
                      << (settings.hilite ? hilite_none : "");
        partition->formatImpl(settings, state, frame);
    }
    else if (type == ASTAlterCommand::FORGET_PARTITION)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "FORGET PARTITION "
                      << (settings.hilite ? hilite_none : "");
        partition->formatImpl(settings, state, frame);
    }
    else if (type == ASTAlterCommand::ATTACH_PARTITION)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "ATTACH " << (part ? "PART " : "PARTITION ")
                      << (settings.hilite ? hilite_none : "");
        partition->formatImpl(settings, state, frame);
    }
    else if (type == ASTAlterCommand::MOVE_PARTITION)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "MOVE " << (part ? "PART " : "PARTITION ")
                      << (settings.hilite ? hilite_none : "");
        partition->formatImpl(settings, state, frame);
        settings.ostr << " TO ";
        switch (move_destination_type)
        {
            case DataDestinationType::DISK:
                settings.ostr << "DISK ";
                break;
            case DataDestinationType::VOLUME:
                settings.ostr << "VOLUME ";
                break;
            case DataDestinationType::TABLE:
                settings.ostr << "TABLE ";
                if (!to_database.empty())
                {
                    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(to_database)
                                  << (settings.hilite ? hilite_none : "") << ".";
                }
                settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(to_table)
                              << (settings.hilite ? hilite_none : "");
                return;
            default:
                break;
        }
        if (move_destination_type != DataDestinationType::TABLE)
        {
            settings.ostr << quoteString(move_destination_name);
        }
    }
    else if (type == ASTAlterCommand::REPLACE_PARTITION)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << (replace ? "REPLACE" : "ATTACH") << " PARTITION "
                      << (settings.hilite ? hilite_none : "");
        partition->formatImpl(settings, state, frame);
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " FROM " << (settings.hilite ? hilite_none : "");
        if (!from_database.empty())
        {
            settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(from_database)
                          << (settings.hilite ? hilite_none : "") << ".";
        }
        settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(from_table) << (settings.hilite ? hilite_none : "");
    }
    else if (type == ASTAlterCommand::FETCH_PARTITION)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "FETCH " << (part ? "PART " : "PARTITION ")
                      << (settings.hilite ? hilite_none : "");
        partition->formatImpl(settings, state, frame);
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " FROM " << (settings.hilite ? hilite_none : "") << DB::quote << from;
    }
    else if (type == ASTAlterCommand::FREEZE_PARTITION)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "FREEZE PARTITION " << (settings.hilite ? hilite_none : "");
        partition->formatImpl(settings, state, frame);

        if (!with_name.empty())
        {
            settings.ostr << " " << (settings.hilite ? hilite_keyword : "") << "WITH NAME" << (settings.hilite ? hilite_none : "") << " "
                          << DB::quote << with_name;
        }
    }
    else if (type == ASTAlterCommand::FREEZE_ALL)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "FREEZE" << (settings.hilite ? hilite_none : "");

        if (!with_name.empty())
        {
            settings.ostr << " " << (settings.hilite ? hilite_keyword : "") << "WITH NAME" << (settings.hilite ? hilite_none : "") << " "
                          << DB::quote << with_name;
        }
    }
    else if (type == ASTAlterCommand::UNFREEZE_PARTITION)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "UNFREEZE PARTITION " << (settings.hilite ? hilite_none : "");
        partition->formatImpl(settings, state, frame);

        if (!with_name.empty())
        {
            settings.ostr << " " << (settings.hilite ? hilite_keyword : "") << "WITH NAME" << (settings.hilite ? hilite_none : "") << " "
                          << DB::quote << with_name;
        }
    }
    else if (type == ASTAlterCommand::UNFREEZE_ALL)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "UNFREEZE" << (settings.hilite ? hilite_none : "");

        if (!with_name.empty())
        {
            settings.ostr << " " << (settings.hilite ? hilite_keyword : "") << "WITH NAME" << (settings.hilite ? hilite_none : "") << " "
                          << DB::quote << with_name;
        }
    }
    else if (type == ASTAlterCommand::DELETE)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "DELETE" << (settings.hilite ? hilite_none : "");

        if (partition)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->formatImpl(settings, state, frame);
        }

        settings.ostr << (settings.hilite ? hilite_keyword : "") << " WHERE " << (settings.hilite ? hilite_none : "");
        predicate->formatImpl(settings, state, frame);
    }
    else if (type == ASTAlterCommand::UPDATE)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "UPDATE " << (settings.hilite ? hilite_none : "");
        update_assignments->formatImpl(settings, state, frame);

        if (partition)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->formatImpl(settings, state, frame);
        }

        settings.ostr << (settings.hilite ? hilite_keyword : "") << " WHERE " << (settings.hilite ? hilite_none : "");
        predicate->formatImpl(settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_TTL)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "MODIFY TTL " << (settings.hilite ? hilite_none : "");
        ttl->formatImpl(settings, state, frame);
    }
    else if (type == ASTAlterCommand::REMOVE_TTL)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "REMOVE TTL" << (settings.hilite ? hilite_none : "");
    }
    else if (type == ASTAlterCommand::MATERIALIZE_TTL)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "MATERIALIZE TTL" << (settings.hilite ? hilite_none : "");
        if (partition)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->formatImpl(settings, state, frame);
        }
    }
    else if (type == ASTAlterCommand::MODIFY_SETTING)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "MODIFY SETTING " << (settings.hilite ? hilite_none : "");
        settings_changes->formatImpl(settings, state, frame);
    }
    else if (type == ASTAlterCommand::RESET_SETTING)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "RESET SETTING " << (settings.hilite ? hilite_none : "");
        settings_resets->formatImpl(settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_DATABASE_SETTING)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "MODIFY SETTING " << (settings.hilite ? hilite_none : "");
        settings_changes->formatImpl(settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_QUERY)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "MODIFY QUERY" << settings.nl_or_ws
                      << (settings.hilite ? hilite_none : "");
        select->formatImpl(settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_REFRESH)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "MODIFY" << settings.nl_or_ws
                      << (settings.hilite ? hilite_none : "");
        refresh->formatImpl(settings, state, frame);
    }
    else if (type == ASTAlterCommand::RENAME_COLUMN)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "RENAME COLUMN " << (if_exists ? "IF EXISTS " : "")
                      << (settings.hilite ? hilite_none : "");
        column->formatImpl(settings, state, frame);

        settings.ostr << (settings.hilite ? hilite_keyword : "") << " TO ";
        rename_to->formatImpl(settings, state, frame);
    }
    else if (type == ASTAlterCommand::MODIFY_SQL_SECURITY)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "MODIFY " << (settings.hilite ? hilite_none : "");
        sql_security->formatImpl(settings, state, frame);
    }
    else if (type == ASTAlterCommand::APPLY_DELETED_MASK)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "APPLY DELETED MASK" << (settings.hilite ? hilite_none : "");

        if (partition)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " IN PARTITION " << (settings.hilite ? hilite_none : "");
            partition->formatImpl(settings, state, frame);
        }
    }
    else
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Unexpected type of ALTER");

    if (format_alter_commands_with_parentheses)
        settings.ostr << ")";
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

void ASTAlterQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    std::string indent_str = settings.one_line ? "" : std::string(4u * frame.indent, ' ');
    settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str;

    switch (alter_object)
    {
        case AlterObjectType::TABLE:
            settings.ostr << "ALTER TABLE ";
            break;
        case AlterObjectType::DATABASE:
            settings.ostr << "ALTER DATABASE ";
            break;
        default:
            break;
    }

    settings.ostr << (settings.hilite ? hilite_none : "");

    if (table)
    {
        settings.ostr << indent_str;
        if (database)
        {
            database->formatImpl(settings, state, frame);
            settings.ostr << '.';
        }

        chassert(table);
        table->formatImpl(settings, state, frame);
    }
    else if (alter_object == AlterObjectType::DATABASE && database)
    {
        settings.ostr << indent_str;
        database->formatImpl(settings, state, frame);
    }

    formatOnCluster(settings);

    FormatStateStacked frame_nested = frame;
    frame_nested.need_parens = false;
    if (settings.one_line)
    {
        frame_nested.expression_list_prepend_whitespace = true;
        command_list->formatImpl(settings, state, frame_nested);
    }
    else
    {
        frame_nested.expression_list_always_start_on_new_line = true;
        command_list->as<ASTExpressionList &>().formatImplMultiline(settings, state, frame_nested);
    }
}

void ASTAlterQuery::forEachPointerToChild(std::function<void(void**)> f)
{
    for (const auto & child : command_list->children)
        child->as<ASTAlterCommand &>().forEachPointerToChild(f);
    f(reinterpret_cast<void **>(&command_list));
}

}
