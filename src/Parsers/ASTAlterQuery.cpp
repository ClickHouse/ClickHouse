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
    UNREACHABLE();
}

void ASTAlterCommand::formatImpl(const FormattingBuffer & out) const
{
    if (type == ASTAlterCommand::ADD_COLUMN)
    {
        out.writeKeyword("ADD COLUMN ");
        out.writeKeyword(if_not_exists ? "IF NOT EXISTS " : "");
        col_decl->formatImpl(out);

        if (first)
            out.writeKeyword(" FIRST ");
        else if (column) /// AFTER
        {
            out.writeKeyword(" AFTER ");
            column->formatImpl(out);
        }
    }
    else if (type == ASTAlterCommand::DROP_COLUMN)
    {
        out.writeKeyword(clear_column ? "CLEAR " : "DROP ");
        out.writeKeyword("COLUMN ");
        out.writeKeyword(if_exists ? "IF EXISTS " : "");
        column->formatImpl(out);
        if (partition)
        {
            out.writeKeyword(" IN PARTITION ");
            partition->formatImpl(out);
        }
    }
    else if (type == ASTAlterCommand::MODIFY_COLUMN)
    {
        out.writeKeyword("MODIFY COLUMN ");
        out.writeKeyword(if_exists ? "IF EXISTS " : "");
        col_decl->formatImpl(out);

        if (!remove_property.empty())
        {
            out.writeKeyword(" REMOVE ");
            out.writeKeyword(remove_property);
        }
        else
        {
            if (first)
                out.writeKeyword(" FIRST ");
            else if (column) /// AFTER
            {
                out.writeKeyword(" AFTER ");
                column->formatImpl(out);
            }
        }
    }
    else if (type == ASTAlterCommand::MATERIALIZE_COLUMN)
    {
        out.writeKeyword("MATERIALIZE COLUMN ");
        column->formatImpl(out);
        if (partition)
        {
            out.writeKeyword(" IN PARTITION ");
            partition->formatImpl(out);
        }
    }
    else if (type == ASTAlterCommand::COMMENT_COLUMN)
    {
        out.writeKeyword("COMMENT COLUMN ");
        out.writeKeyword(if_exists ? "IF EXISTS " : "");
        column->formatImpl(out);
        out.ostr << " ";
        comment->formatImpl(out);
    }
    else if (type == ASTAlterCommand::MODIFY_COMMENT)
    {
        out.writeKeyword("MODIFY COMMENT");
        out.ostr << " ";
        comment->formatImpl(out);
    }
    else if (type == ASTAlterCommand::MODIFY_ORDER_BY)
    {
        out.writeKeyword("MODIFY ORDER BY ");
        order_by->formatImpl(out);
    }
    else if (type == ASTAlterCommand::MODIFY_SAMPLE_BY)
    {
        out.writeKeyword("MODIFY SAMPLE BY ");
        sample_by->formatImpl(out);
    }
    else if (type == ASTAlterCommand::REMOVE_SAMPLE_BY)
    {
        out.writeKeyword("REMOVE SAMPLE BY");
    }
    else if (type == ASTAlterCommand::ADD_INDEX)
    {
        out.writeKeyword("ADD INDEX ");
        out.writeKeyword(if_not_exists ? "IF NOT EXISTS " : "");
        index_decl->formatImpl(out);

        if (first)
            out.writeKeyword(" FIRST ");
        else if (index) /// AFTER
        {
            out.writeKeyword(" AFTER ");
            index->formatImpl(out);
        }
    }
    else if (type == ASTAlterCommand::DROP_INDEX)
    {
        out.writeKeyword(clear_index ? "CLEAR " : "DROP ");
        out.writeKeyword("INDEX ");
        out.writeKeyword(if_exists ? "IF EXISTS " : "");
        index->formatImpl(out);
        if (partition)
        {
            out.writeKeyword(" IN PARTITION ");
            partition->formatImpl(out);
        }
    }
    else if (type == ASTAlterCommand::MATERIALIZE_INDEX)
    {
        out.writeKeyword("MATERIALIZE INDEX ");
        index->formatImpl(out);
        if (partition)
        {
            out.writeKeyword(" IN PARTITION ");
            partition->formatImpl(out);
        }
    }
    else if (type == ASTAlterCommand::ADD_CONSTRAINT)
    {
        out.writeKeyword("ADD CONSTRAINT ");
        out.writeKeyword(if_not_exists ? "IF NOT EXISTS " : "");
        constraint_decl->formatImpl(out);
    }
    else if (type == ASTAlterCommand::DROP_CONSTRAINT)
    {
        out.writeKeyword("DROP CONSTRAINT ");
        out.writeKeyword(if_exists ? "IF EXISTS " : "");
        constraint->formatImpl(out);
    }
    else if (type == ASTAlterCommand::ADD_PROJECTION)
    {
        out.writeKeyword("ADD PROJECTION ");
        out.writeKeyword(if_not_exists ? "IF NOT EXISTS " : "");
        projection_decl->formatImpl(out);

        if (first)
            out.writeKeyword(" FIRST ");
        else if (projection)
        {
            out.writeKeyword(" AFTER ");
            projection->formatImpl(out);
        }
    }
    else if (type == ASTAlterCommand::DROP_PROJECTION)
    {
        out.writeKeyword(clear_projection ? "CLEAR " : "DROP ");
        out.writeKeyword("PROJECTION ");
        out.writeKeyword(if_exists ? "IF EXISTS " : "");
        projection->formatImpl(out);
        if (partition)
        {
            out.writeKeyword(" IN PARTITION ");
            partition->formatImpl(out);
        }
    }
    else if (type == ASTAlterCommand::MATERIALIZE_PROJECTION)
    {
        out.writeKeyword("MATERIALIZE PROJECTION ");
        projection->formatImpl(out);
        if (partition)
        {
            out.writeKeyword(" IN PARTITION ");
            partition->formatImpl(out);
        }
    }
    else if (type == ASTAlterCommand::DROP_PARTITION)
    {
        out.writeKeyword(detach ? "DETACH" : "DROP");
        out.writeKeyword(part ? " PART " : " PARTITION ");
        partition->formatImpl(out);
    }
    else if (type == ASTAlterCommand::DROP_DETACHED_PARTITION)
    {
        out.writeKeyword("DROP DETACHED");
        out.writeKeyword(part ? " PART " : " PARTITION ");
        partition->formatImpl(out);
    }
    else if (type == ASTAlterCommand::ATTACH_PARTITION)
    {
        out.writeKeyword("ATTACH ");
        out.writeKeyword(part ? "PART " : "PARTITION ");
        partition->formatImpl(out);
    }
    else if (type == ASTAlterCommand::MOVE_PARTITION)
    {
        out.writeKeyword("MOVE ");
        out.writeKeyword(part ? "PART " : "PARTITION ");
        partition->formatImpl(out);
        out.ostr << " TO ";
        switch (move_destination_type)
        {
            case DataDestinationType::DISK:
                out.ostr << "DISK ";
                break;
            case DataDestinationType::VOLUME:
                out.ostr << "VOLUME ";
                break;
            case DataDestinationType::TABLE:
                out.ostr << "TABLE ";
                if (!to_database.empty())
                {
                    out.writeProbablyBackQuotedIdentifier(to_database);
                    out.ostr << ".";
                }
                out.writeProbablyBackQuotedIdentifier(to_table);
                return;
            default:
                break;
        }
        if (move_destination_type != DataDestinationType::TABLE)
        {
            out.ostr << quoteString(move_destination_name);
        }
    }
    else if (type == ASTAlterCommand::REPLACE_PARTITION)
    {
        out.writeKeyword(replace ? "REPLACE" : "ATTACH");
        out.writeKeyword(" PARTITION ");
        partition->formatImpl(out);
        out.writeKeyword(" FROM ");
        if (!from_database.empty())
        {
            out.writeProbablyBackQuotedIdentifier(from_database);
            out.ostr << ".";
        }
        out.writeProbablyBackQuotedIdentifier(from_table);
    }
    else if (type == ASTAlterCommand::FETCH_PARTITION)
    {
        out.writeKeyword("FETCH ");
        out.writeKeyword(part ? "PART " : "PARTITION ");
        partition->formatImpl(out);
        out.writeKeyword(" FROM ");
        out.ostr << DB::quote << from;
    }
    else if (type == ASTAlterCommand::FREEZE_PARTITION)
    {
        out.writeKeyword("FREEZE PARTITION ");
        partition->formatImpl(out);

        if (!with_name.empty())
        {
            out.writeKeyword(" WITH NAME ");
            out.ostr << DB::quote << with_name;
        }
    }
    else if (type == ASTAlterCommand::FREEZE_ALL)
    {
        out.writeKeyword("FREEZE");

        if (!with_name.empty())
        {
            out.writeKeyword(" WITH NAME ");
            out.ostr << DB::quote << with_name;
        }
    }
    else if (type == ASTAlterCommand::UNFREEZE_PARTITION)
    {
        out.writeKeyword("UNFREEZE PARTITION ");
        partition->formatImpl(out);

        if (!with_name.empty())
        {
            out.writeKeyword(" WITH NAME ");
            out.ostr << DB::quote << with_name;
        }
    }
    else if (type == ASTAlterCommand::UNFREEZE_ALL)
    {
        out.writeKeyword("UNFREEZE");

        if (!with_name.empty())
        {
            out.writeKeyword(" WITH NAME ");
            out.ostr << DB::quote << with_name;
        }
    }
    else if (type == ASTAlterCommand::DELETE)
    {
        out.writeKeyword("DELETE");

        if (partition)
        {
            out.writeKeyword(" IN PARTITION ");
            partition->formatImpl(out);
        }

        out.writeKeyword(" WHERE ");
        predicate->formatImpl(out);
    }
    else if (type == ASTAlterCommand::UPDATE)
    {
        out.writeKeyword("UPDATE ");
        update_assignments->formatImpl(out);

        if (partition)
        {
            out.writeKeyword(" IN PARTITION ");
            partition->formatImpl(out);
        }

        out.writeKeyword(" WHERE ");
        predicate->formatImpl(out);
    }
    else if (type == ASTAlterCommand::MODIFY_TTL)
    {
        out.writeKeyword("MODIFY TTL ");
        ttl->formatImpl(out);
    }
    else if (type == ASTAlterCommand::REMOVE_TTL)
    {
        out.writeKeyword("REMOVE TTL");
    }
    else if (type == ASTAlterCommand::MATERIALIZE_TTL)
    {
        out.writeKeyword("MATERIALIZE TTL");
        if (partition)
        {
            out.writeKeyword(" IN PARTITION ");
            partition->formatImpl(out);
        }
    }
    else if (type == ASTAlterCommand::MODIFY_SETTING)
    {
        out.writeKeyword("MODIFY SETTING ");
        settings_changes->formatImpl(out);
    }
    else if (type == ASTAlterCommand::RESET_SETTING)
    {
        out.writeKeyword("RESET SETTING ");
        settings_resets->formatImpl(out);
    }
    else if (type == ASTAlterCommand::MODIFY_DATABASE_SETTING)
    {
        out.writeKeyword("MODIFY SETTING ");
        settings_changes->formatImpl(out);
    }
    else if (type == ASTAlterCommand::MODIFY_QUERY)
    {
        out.writeKeyword("MODIFY QUERY ");
        out.nlOrWs();
        select->formatImpl(out);
    }
    else if (type == ASTAlterCommand::LIVE_VIEW_REFRESH)
    {
        out.writeKeyword("REFRESH ");
    }
    else if (type == ASTAlterCommand::RENAME_COLUMN)
    {
        out.writeKeyword("RENAME COLUMN ");
        out.writeKeyword(if_exists ? "IF EXISTS " : "");
        column->formatImpl(out);

        out.writeKeyword(" TO ");
        rename_to->formatImpl(out);
    }
    else
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Unexpected type of ALTER");
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

void ASTAlterQuery::formatQueryImpl(const FormattingBuffer & out) const
{
    out.setNeedsParens(false);

    out.writeIndent();

    switch (alter_object)
    {
        case AlterObjectType::TABLE:
            out.writeKeyword("ALTER TABLE ");
            break;
        case AlterObjectType::DATABASE:
            out.writeKeyword("ALTER DATABASE ");
            break;
        case AlterObjectType::LIVE_VIEW:
            out.writeKeyword("ALTER LIVE VIEW ");
            break;
        default:
            break;
    }

    if (table)
    {
        if (database)
        {
            out.writeIndent();
            out.ostr << backQuoteIfNeed(getDatabase());
            out.ostr << ".";
        }
        out.writeIndent();
        out.ostr << backQuoteIfNeed(getTable());
    }
    else if (alter_object == AlterObjectType::DATABASE && database)
    {
        out.writeIndent();
        out.ostr << backQuoteIfNeed(getDatabase());
    }

    formatOnCluster(out);

    static_cast<ASTExpressionList *>(command_list)->formatImplMultiline(
        out.copyWithoutNeedParensAndWithExpressionListAlwaysStartOnNewLine());
}

}
