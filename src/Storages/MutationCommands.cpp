#include <Storages/MutationCommands.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTStatisticsDeclaration.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/typeid_cast.h>
#include <Common/quoteString.h>
#include <Core/Defines.h>
#include <DataTypes/DataTypeFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_MUTATION_COMMAND;
    extern const int MULTIPLE_ASSIGNMENTS_TO_COLUMN;
    extern const int LOGICAL_ERROR;
}


bool MutationCommand::isBarrierCommand() const
{
    return type == RENAME_COLUMN;
}

bool MutationCommand::isPureMetadataCommand() const
{
    return type == ALTER_WITHOUT_MUTATION;
}

bool MutationCommand::isEmptyCommand() const
{
    return type == EMPTY;
}

bool MutationCommand::isDropOrRename() const
{
    return type == Type::DROP_COLUMN
        || type == Type::DROP_INDEX
        || type == Type::DROP_PROJECTION
        || type == Type::DROP_STATISTICS
        || type == Type::RENAME_COLUMN;
}

bool MutationCommand::affectsAllColumns() const
{
    return type == DELETE
        || type == APPLY_DELETED_MASK
        || type == REWRITE_PARTS;
}

ASTPtr MutationCommand::ast() const
{
    if (ast_text.empty())
        return nullptr;

    ParserAlterCommandList p_alter_commands;
    auto commands_ast = parseQuery(
        p_alter_commands, ast_text.data(), ast_text.data() + ast_text.length(),
        "mutation command", 0, max_parser_depth, max_parser_backtracks);

    if (commands_ast->children.size() != 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected exactly one ALTER command parsed from mutation command text, got {}",
            commands_ast->children.size());

    return commands_ast->children[0];
}

ASTPtr MutationCommand::predicate() const
{
    auto command_ast = ast();
    if (!command_ast)
        return nullptr;
    const auto * alter = command_ast->as<ASTAlterCommand>();
    if (!alter || !alter->predicate)
        return nullptr;
    return alter->predicate->clone();
}

ASTPtr MutationCommand::partition() const
{
    auto command_ast = ast();
    if (!command_ast)
        return nullptr;
    const auto * alter = command_ast->as<ASTAlterCommand>();
    if (!alter || !alter->partition)
        return nullptr;
    return alter->partition->clone();
}

std::unordered_map<String, ASTPtr> MutationCommand::columnToUpdateExpression() const
{
    std::unordered_map<String, ASTPtr> result;
    auto command_ast = ast();
    if (!command_ast)
        return result;
    const auto * alter = command_ast->as<ASTAlterCommand>();
    if (!alter || !alter->update_assignments)
        return result;
    for (const auto & child : alter->update_assignments->children)
    {
        const auto & assignment = child->as<ASTAssignment &>();
        result.emplace(assignment.column_name, assignment.expression()->clone());
    }
    return result;
}

std::optional<MutationCommand> MutationCommand::parse(
    const ASTAlterCommand & command,
    bool parse_alter_commands,
    bool with_pure_metadata_commands,
    UInt64 max_parser_depth,
    UInt64 max_parser_backtracks)
{
    MutationCommand res;
    res.ast_text = command.formatWithSecretsOneLine();
    res.max_parser_depth = max_parser_depth;
    res.max_parser_backtracks = max_parser_backtracks;
    if (with_pure_metadata_commands)
    {
        res.type = ALTER_WITHOUT_MUTATION;
        return res;
    }

    if (command.type == ASTAlterCommand::DELETE)
    {
        res.type = DELETE;
        return res;
    }
    if (command.type == ASTAlterCommand::UPDATE)
    {
        res.type = UPDATE;
        NameSet seen_columns;
        for (const ASTPtr & assignment_ast : command.update_assignments->children)
        {
            const auto & assignment = assignment_ast->as<ASTAssignment &>();
            if (!seen_columns.insert(assignment.column_name).second)
                throw Exception(
                    ErrorCodes::MULTIPLE_ASSIGNMENTS_TO_COLUMN,
                    "Multiple assignments in the single statement to column {}",
                    backQuote(assignment.column_name));
        }
        return res;
    }
    if (command.type == ASTAlterCommand::APPLY_DELETED_MASK)
    {
        res.type = APPLY_DELETED_MASK;
        return res;
    }
    else if (command.type == ASTAlterCommand::APPLY_PATCHES)
    {
        res.type = APPLY_PATCHES;
        return res;
    }
    if (command.type == ASTAlterCommand::MATERIALIZE_INDEX)
    {
        res.type = MATERIALIZE_INDEX;
        res.index_name = command.index->as<ASTIdentifier &>().name();
        return res;
    }
    if (command.type == ASTAlterCommand::MATERIALIZE_STATISTICS)
    {
        res.type = MATERIALIZE_STATISTICS;
        if (command.statistics_decl)
        {
            res.statistics_columns = command.statistics_decl->as<ASTStatisticsDeclaration &>().getColumnNames();
        }
        return res;
    }
    if (command.type == ASTAlterCommand::MATERIALIZE_PROJECTION)
    {
        res.type = MATERIALIZE_PROJECTION;
        res.projection_name = command.projection->as<ASTIdentifier &>().name();
        return res;
    }
    if (command.type == ASTAlterCommand::MATERIALIZE_COLUMN)
    {
        res.type = MATERIALIZE_COLUMN;
        res.column_name = getIdentifierName(command.column);
        return res;
    }
    /// MODIFY COLUMN x REMOVE MATERIALIZED/RESET SETTING/MODIFY SETTING is a valid alter command, but doesn't have any specified column type,
    /// thus no mutation is needed
    if (parse_alter_commands && command.type == ASTAlterCommand::MODIFY_COLUMN && command.remove_property.empty()
        && nullptr == command.settings_changes && nullptr == command.settings_resets)
    {
        const auto & ast_col_decl = command.col_decl->as<ASTColumnDeclaration &>();

        if (ast_col_decl.getType() != nullptr)
        {
            res.type = MutationCommand::Type::READ_COLUMN;
            res.column_name = ast_col_decl.name;
            res.data_type = DataTypeFactory::instance().get(ast_col_decl.getType());
            return res;
        }

        const bool metadata_only_modification
            = ast_col_decl.getDefaultExpression() != nullptr
            || ast_col_decl.getComment() != nullptr
            || ast_col_decl.getCodec() != nullptr
            || ast_col_decl.getTTL() != nullptr;

        if (!metadata_only_modification)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "MODIFY COLUMN mutation command doesn't specify type: {}", command.formatForErrorMessage());
    }
    if (parse_alter_commands && command.type == ASTAlterCommand::DROP_COLUMN)
    {
        res.type = MutationCommand::Type::DROP_COLUMN;
        res.column_name = getIdentifierName(command.column);
        if (command.clear_column)
            res.clear = true;

        return res;
    }
    if (parse_alter_commands && command.type == ASTAlterCommand::DROP_INDEX)
    {
        res.type = MutationCommand::Type::DROP_INDEX;
        res.column_name = command.index->as<ASTIdentifier &>().name();
        if (command.clear_index)
            res.clear = true;
        return res;
    }
    if (parse_alter_commands && command.type == ASTAlterCommand::DROP_STATISTICS)
    {
        res.type = MutationCommand::Type::DROP_STATISTICS;
        if (command.clear_statistics)
            res.clear = true;
        if (command.statistics_decl)
            res.statistics_columns = command.statistics_decl->as<ASTStatisticsDeclaration &>().getColumnNames();
        return res;
    }
    if (parse_alter_commands && command.type == ASTAlterCommand::DROP_PROJECTION)
    {
        res.type = MutationCommand::Type::DROP_PROJECTION;
        res.column_name = command.projection->as<ASTIdentifier &>().name();
        if (command.clear_projection)
            res.clear = true;
        return res;
    }
    if (parse_alter_commands && command.type == ASTAlterCommand::RENAME_COLUMN)
    {
        res.type = MutationCommand::Type::RENAME_COLUMN;
        res.column_name = command.column->as<ASTIdentifier &>().name();
        res.rename_to = command.rename_to->as<ASTIdentifier &>().name();
        return res;
    }
    if (command.type == ASTAlterCommand::MATERIALIZE_TTL)
    {
        res.type = MATERIALIZE_TTL;
        return res;
    }
    if (command.type == ASTAlterCommand::REWRITE_PARTS)
    {
        res.type = REWRITE_PARTS;
        return res;
    }

    res.type = ALTER_WITHOUT_MUTATION;
    return res;
}


boost::intrusive_ptr<ASTExpressionList> MutationCommands::ast(bool with_pure_metadata_commands) const
{
    auto res = make_intrusive<ASTExpressionList>();
    for (const MutationCommand & command : *this)
    {
        if (!command.isPureMetadataCommand() || with_pure_metadata_commands)
            res->children.push_back(command.ast());
    }
    return res;
}


void MutationCommands::writeText(WriteBuffer & out, bool with_pure_metadata_commands) const
{
    writeEscapedString(ast(with_pure_metadata_commands)->formatWithSecretsOneLine(), out);
}

void MutationCommands::readText(ReadBuffer & in, bool with_pure_metadata_commands)
{
    String commands_str;
    readEscapedString(commands_str, in);

    ParserAlterCommandList p_alter_commands;
    auto commands_ast = parseQuery(
        p_alter_commands, commands_str.data(), commands_str.data() + commands_str.length(), "mutation commands list", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    for (const auto & child : commands_ast->children)
    {
        auto * command_ast = child->as<ASTAlterCommand>();
        auto command = MutationCommand::parse(*command_ast, true, with_pure_metadata_commands);
        if (!command)
            throw Exception(ErrorCodes::UNKNOWN_MUTATION_COMMAND, "Unknown mutation command type: {}", DB::toString<int>(command_ast->type));
        push_back(std::move(*command));
    }
}

std::string MutationCommands::toString(bool with_pure_metadata_commands) const
{
    return ast(with_pure_metadata_commands)->formatWithSecretsOneLine();
}


bool MutationCommands::hasNonEmptyMutationCommands() const
{
    for (const auto & command : *this)
    {
        if (!command.isEmptyCommand() && !command.isPureMetadataCommand())
            return true;
    }
    return false;
}

bool MutationCommands::hasAnyUpdateCommand() const
{
    return std::ranges::any_of(*this, [](const auto & command) { return command.type == MutationCommand::Type::UPDATE; });
}

bool MutationCommands::hasOnlyUpdateCommands() const
{
    return std::ranges::all_of(*this, [](const auto & command) { return command.type == MutationCommand::Type::UPDATE; });
}

bool MutationCommands::containBarrierCommand() const
{
    for (const auto & command : *this)
    {
        if (command.isBarrierCommand())
            return true;
    }
    return false;
}

NameSet MutationCommands::getAllUpdatedColumns() const
{
    NameSet res;
    for (const auto & command : *this)
        for (const auto & [column_name, _] : command.columnToUpdateExpression())
            res.insert(column_name);
    return res;
}

}
