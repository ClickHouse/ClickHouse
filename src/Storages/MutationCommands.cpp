#include <Storages/MutationCommands.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/TokenIterator.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTStatisticsDeclaration.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <string_view>

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

namespace
{

boost::intrusive_ptr<ASTAlterCommand> parseAlterCommand(const String & ast_text, UInt64 max_parser_depth, UInt64 max_parser_backtracks)
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

    auto child = std::move(commands_ast->children[0]);
    auto * alter = child->as<ASTAlterCommand>();
    if (!alter)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Parsed mutation command is not an ASTAlterCommand: {}",
            child->formatForErrorMessage());

    return boost::intrusive_ptr<ASTAlterCommand>(alter);
}

}

boost::intrusive_ptr<const ASTAlterCommand> MutationCommand::ast() const
{
    return parseAlterCommand(ast_text, max_parser_depth, max_parser_backtracks);
}

MutationCommand::MutableAst::MutableAst(MutationCommand & owner_)
    : owner(owner_)
    , ast(parseAlterCommand(owner_.ast_text, owner_.max_parser_depth, owner_.max_parser_backtracks))
{
}

MutationCommand::MutableAst::~MutableAst() noexcept
{
    /// Catch forgotten `commit` calls in debug; in release the edits are dropped silently.
    chassert(committed || !ast);
}

void MutationCommand::MutableAst::commit()
{
    committed = true;
    if (!ast)
        return;
    owner.ast_text = ast->formatWithSecretsOneLine();
}

std::unordered_map<String, ASTPtr> getColumnToUpdateExpression(const ASTAlterCommand & alter)
{
    std::unordered_map<String, ASTPtr> result;
    if (!alter.update_assignments)
        return result;
    for (const auto & child : alter.update_assignments->children)
    {
        const auto & assignment = child->as<ASTAssignment &>();
        result.emplace(assignment.column_name, assignment.expression());
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
        {
            auto parsed = command.ast();
            if (parsed)
                res->children.emplace_back(const_cast<ASTAlterCommand *>(parsed.get()));
        }
    }
    return res;
}


namespace
{

ASTPtr parseMutationCommandsList(const String & commands_str)
{
    ParserAlterCommandList p_alter_commands;
    return parseQuery(
        p_alter_commands, commands_str.data(), commands_str.data() + commands_str.length(), "mutation commands list", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
}

/// `/*exact*/` is the first token of an exact command. A parenthesis inside a literal is not a command boundary.
std::vector<char> exactColumnMarks(const String & commands_str)
{
    std::vector<char> marks;
    Tokens tokens(commands_str.data(), commands_str.data() + commands_str.size(), 0, false);

    int depth = 0;
    bool in_command = false;
    bool saw_exact = false;
    bool decided = false;

    for (size_t index = 0;; ++index)
    {
        const Token & token = tokens[index];
        if (token.isEnd())
            break;

        if (token.type == TokenType::OpeningRoundBracket)
        {
            if (depth == 0)
            {
                in_command = true;
                saw_exact = false;
                decided = false;
            }
            ++depth;
            continue;
        }

        if (token.type == TokenType::ClosingRoundBracket)
        {
            if (depth == 0)
                continue;
            --depth;
            if (depth == 0 && in_command)
            {
                marks.push_back(saw_exact);
                in_command = false;
            }
            continue;
        }

        if (depth != 1 || !in_command || decided || token.type == TokenType::Whitespace)
            continue;

        if (token.type == TokenType::Comment && std::string_view(token.begin, token.size()) == "/*exact*/")
            saw_exact = true;
        decided = true;
    }

    return marks;
}

}

void MutationCommands::writeText(WriteBuffer & out, bool with_pure_metadata_commands) const
{
    WriteBufferFromOwnString commands;
    std::vector<char> expected_marks;
    expected_marks.reserve(size());
    bool first = true;
    for (const auto & command : *this)
    {
        if (command.isPureMetadataCommand() && !with_pure_metadata_commands)
            continue;

        auto parsed = command.ast();
        if (!parsed)
            continue;

        if (!first)
            commands << ", ";
        first = false;

        String formatted = parsed->formatWithSecretsOneLine();
        if (command.targets_exact_column)
        {
            if (formatted.empty() || formatted.front() != '(')
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot mark an exact column mutation command: {}", formatted);
            formatted.insert(1, "/*exact*/ ");
        }
        commands << formatted;
        expected_marks.push_back(command.targets_exact_column);
    }

    String commands_str = commands.str();

    /// Refuse to persist commands `readText` could not parse back with the same exact-column marks.
    ASTPtr parsed;
    try
    {
        parsed = parseMutationCommandsList(commands_str);
    }
    catch (Exception & e)
    {
        e.addMessage(
            "Serialized mutation commands cannot be parsed back (this indicates a mismatch between the AST formatter "
            "and the parser; it's a bug); refusing to write them out: {}", commands_str);
        throw;
    }

    const auto marks = exactColumnMarks(commands_str);
    if (!parsed || marks.size() != parsed->children.size() || marks != expected_marks)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot recover exact-column marks from serialized mutation commands: {}",
            commands_str);

    writeEscapedString(commands_str, out);
}

void MutationCommands::readText(ReadBuffer & in, bool with_pure_metadata_commands)
{
    String commands_str;
    readEscapedString(commands_str, in);

    auto commands_ast = parseMutationCommandsList(commands_str);
    const auto marks = exactColumnMarks(commands_str);
    size_t index = 0;

    for (const auto & child : commands_ast->children)
    {
        auto * command_ast = child->as<ASTAlterCommand>();
        auto command = MutationCommand::parse(*command_ast, true, with_pure_metadata_commands);
        if (!command)
            throw Exception(ErrorCodes::UNKNOWN_MUTATION_COMMAND, "Unknown mutation command type: {}", DB::toString<int>(command_ast->type));
        if (index >= marks.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot recover exact-column marks from mutation commands: {}", commands_str);
        command->targets_exact_column = marks[index];
        ++index;
        push_back(std::move(*command));
    }

    if (index != marks.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot recover exact-column marks from mutation commands: {}", commands_str);
}

std::string MutationCommands::toString(bool with_pure_metadata_commands) const
{
    return ast(with_pure_metadata_commands)->formatWithSecretsOneLine();
}


/// Shared with `MergeTreeData::checkMutationIsPossible`, so of the commands that reach the
/// heavyweight mutation path, the ones answering here are exactly the ones it refuses.
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

bool MutationCommands::hasRewritePartsCommand() const
{
    return std::ranges::any_of(*this, [](const auto & command) { return command.type == MutationCommand::Type::REWRITE_PARTS; });
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
    {
        auto alter = command.ast();
        if (!alter || !alter->update_assignments)
            continue;
        for (const auto & child : alter->update_assignments->children)
            res.insert(child->as<ASTAssignment &>().column_name);
    }
    return res;
}

}
