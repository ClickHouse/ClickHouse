#include <iomanip>
#include <IO/Operators.h>
#include <Parsers/ASTAlterQuery.h>
#include <Common/quoteString.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNEXPECTED_AST_STRUCTURE;
}

String ASTAlterCommand::getID(char delim) const
{
    return String("AlterCommand") + delim + typeToString(type);
}

ASTPtr ASTAlterCommand::clone() const
{
    auto res = std::make_shared<ASTAlterCommand>(*this);
    res->children.clear();

    if (col_decl)
    {
        res->col_decl = col_decl->clone();
        res->children.push_back(res->col_decl);
    }
    if (column)
    {
        res->column = column->clone();
        res->children.push_back(res->column);
    }
    if (order_by)
    {
        res->order_by = order_by->clone();
        res->children.push_back(res->order_by);
    }
    if (partition)
    {
        res->partition = partition->clone();
        res->children.push_back(res->partition);
    }
    if (predicate)
    {
        res->predicate = predicate->clone();
        res->children.push_back(res->predicate);
    }
    if (ttl)
    {
        res->ttl = ttl->clone();
        res->children.push_back(res->ttl);
    }
    if (settings_changes)
    {
        res->settings_changes = settings_changes->clone();
        res->children.push_back(res->settings_changes);
    }
    if (settings_resets)
    {
        res->settings_resets = settings_resets->clone();
        res->children.push_back(res->settings_resets);
    }
    if (values)
    {
        res->values = values->clone();
        res->children.push_back(res->values);
    }
    if (rename_to)
    {
        res->rename_to = rename_to->clone();
        res->children.push_back(res->rename_to);
    }
    if (comment)
    {
        res->comment = comment->clone();
        res->children.push_back(res->comment);
    }

    return res;
}

const char * ASTAlterCommand::typeToString(ASTAlterCommand::Type type)
{
    switch (type)
    {
        case ADD_COLUMN: return "ADD_COLUMN";
        case DROP_COLUMN: return "DROP_COLUMN";
        case MODIFY_COLUMN: return "MODIFY_COLUMN";
        case COMMENT_COLUMN: return "COMMENT_COLUMN";
        case RENAME_COLUMN: return "RENAME_COLUMN";
        case MATERIALIZE_COLUMN: return "MATERIALIZE_COLUMN";
        case MODIFY_ORDER_BY: return "MODIFY_ORDER_BY";
        case MODIFY_SAMPLE_BY: return "MODIFY_SAMPLE_BY";
        case MODIFY_TTL: return "MODIFY_TTL";
        case MATERIALIZE_TTL: return "MATERIALIZE_TTL";
        case MODIFY_SETTING: return "MODIFY_SETTING";
        case RESET_SETTING: return "RESET_SETTING";
        case MODIFY_QUERY: return "MODIFY_QUERY";
        case REMOVE_TTL: return "REMOVE_TTL";
        case REMOVE_SAMPLE_BY: return "REMOVE_SAMPLE_BY";
        case ADD_INDEX: return "ADD_INDEX";
        case DROP_INDEX: return "DROP_INDEX";
        case MATERIALIZE_INDEX: return "MATERIALIZE_INDEX";
        case ADD_CONSTRAINT: return "ADD_CONSTRAINT";
        case DROP_CONSTRAINT: return "DROP_CONSTRAINT";
        case ADD_PROJECTION: return "ADD_PROJECTION";
        case DROP_PROJECTION: return "DROP_PROJECTION";
        case MATERIALIZE_PROJECTION: return "MATERIALIZE_PROJECTION";
        case DROP_PARTITION: return "DROP_PARTITION";
        case DROP_DETACHED_PARTITION: return "DROP_DETACHED_PARTITION";
        case ATTACH_PARTITION: return "ATTACH_PARTITION";
        case MOVE_PARTITION: return "MOVE_PARTITION";
        case REPLACE_PARTITION: return "REPLACE_PARTITION";
        case FETCH_PARTITION: return "FETCH_PARTITION";
        case FREEZE_PARTITION: return "FREEZE_PARTITION";
        case FREEZE_ALL: return "FREEZE_ALL";
        case UNFREEZE_PARTITION: return "UNFREEZE_PARTITION";
        case UNFREEZE_ALL: return "UNFREEZE_ALL";
        case DELETE: return "DELETE";
        case UPDATE: return "UPDATE";
        case NO_TYPE: return "NO_TYPE";
        case LIVE_VIEW_REFRESH: return "LIVE_VIEW_REFRESH";
        case MODIFY_DATABASE_SETTING: return "MODIFY_DATABASE_SETTING";
        case MODIFY_COMMENT: return "MODIFY_COMMENT";
    }
    __builtin_unreachable();
}

void ASTAlterCommand::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
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
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "FREEZE";

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
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "UNFREEZE";

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
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "MODIFY QUERY " << settings.nl_or_ws
                      << (settings.hilite ? hilite_none : "");
        select->formatImpl(settings, state, frame);
    }
    else if (type == ASTAlterCommand::LIVE_VIEW_REFRESH)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "REFRESH " << (settings.hilite ? hilite_none : "");
    }
    else if (type == ASTAlterCommand::RENAME_COLUMN)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "RENAME COLUMN " << (if_exists ? "IF EXISTS " : "")
                      << (settings.hilite ? hilite_none : "");
        column->formatImpl(settings, state, frame);

        settings.ostr << (settings.hilite ? hilite_keyword : "") << " TO ";
        rename_to->formatImpl(settings, state, frame);
    }
    else
        throw Exception("Unexpected type of ALTER", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
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
    return isOneCommandTypeOnly(ASTAlterCommand::MODIFY_SETTING);
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
        case AlterObjectType::LIVE_VIEW:
            settings.ostr << "ALTER LIVE VIEW ";
            break;
        default:
            break;
    }

    settings.ostr << (settings.hilite ? hilite_none : "");

    if (table)
    {
        if (database)
        {
            settings.ostr << indent_str << backQuoteIfNeed(getDatabase());
            settings.ostr << ".";
        }
        settings.ostr << indent_str << backQuoteIfNeed(getTable());
    }
    else if (alter_object == AlterObjectType::DATABASE && database)
    {
        settings.ostr << indent_str << backQuoteIfNeed(getDatabase());
    }

    formatOnCluster(settings);

    FormatStateStacked frame_nested = frame;
    frame_nested.need_parens = false;
    frame_nested.expression_list_always_start_on_new_line = true;
    static_cast<ASTExpressionList *>(command_list)->formatImplMultiline(settings, state, frame_nested);
}

}
