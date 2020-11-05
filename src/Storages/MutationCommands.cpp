#include <Storages/MutationCommands.h>
#include <IO/Operators.h>
#include <Parsers/formatAST.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/typeid_cast.h>
#include <Common/quoteString.h>
#include <Core/Defines.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/queryToString.h>
#include <common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_MUTATION_COMMAND;
    extern const int MULTIPLE_ASSIGNMENTS_TO_COLUMN;
}

std::optional<MutationCommand> MutationCommand::parse(ASTAlterCommand * command, bool parse_alter_commands)
{
    if (command->type == ASTAlterCommand::DELETE)
    {
        MutationCommand res;
        res.ast = command->ptr();
        res.type = DELETE;
        res.predicate = command->predicate;
        return res;
    }
    else if (command->type == ASTAlterCommand::UPDATE)
    {
        MutationCommand res;
        res.ast = command->ptr();
        res.type = UPDATE;
        res.predicate = command->predicate;
        for (const ASTPtr & assignment_ast : command->update_assignments->children)
        {
            const auto & assignment = assignment_ast->as<ASTAssignment &>();
            auto insertion = res.column_to_update_expression.emplace(assignment.column_name, assignment.expression);
            if (!insertion.second)
                throw Exception("Multiple assignments in the single statement to column " + backQuote(assignment.column_name),
                    ErrorCodes::MULTIPLE_ASSIGNMENTS_TO_COLUMN);
        }
        return res;
    }
    else if (command->type == ASTAlterCommand::MATERIALIZE_INDEX)
    {
        MutationCommand res;
        res.ast = command->ptr();
        res.type = MATERIALIZE_INDEX;
        res.partition = command->partition;
        res.predicate = nullptr;
        res.index_name = command->index->as<ASTIdentifier &>().name;
        return res;
    }
    else if (parse_alter_commands && command->type == ASTAlterCommand::MODIFY_COLUMN)
    {
        MutationCommand res;
        res.ast = command->ptr();
        res.type = MutationCommand::Type::READ_COLUMN;
        const auto & ast_col_decl = command->col_decl->as<ASTColumnDeclaration &>();
        res.column_name = ast_col_decl.name;
        res.data_type = DataTypeFactory::instance().get(ast_col_decl.type);
        return res;
    }
    else if (parse_alter_commands && command->type == ASTAlterCommand::DROP_COLUMN)
    {
        MutationCommand res;
        res.ast = command->ptr();
        res.type = MutationCommand::Type::DROP_COLUMN;
        res.column_name = getIdentifierName(command->column);
        if (command->partition)
            res.partition = command->partition;
        if (command->clear_column)
            res.clear = true;

        return res;
    }
    else if (parse_alter_commands && command->type == ASTAlterCommand::DROP_INDEX)
    {
        MutationCommand res;
        res.ast = command->ptr();
        res.type = MutationCommand::Type::DROP_INDEX;
        res.column_name = command->index->as<ASTIdentifier &>().name;
        if (command->partition)
            res.partition = command->partition;
        if (command->clear_index)
            res.clear = true;
        return res;
    }
    else if (parse_alter_commands && command->type == ASTAlterCommand::RENAME_COLUMN)
    {
        MutationCommand res;
        res.ast = command->ptr();
        res.type = MutationCommand::Type::RENAME_COLUMN;
        res.column_name = command->column->as<ASTIdentifier &>().name;
        res.rename_to = command->rename_to->as<ASTIdentifier &>().name;
        return res;
    }
    else if (command->type == ASTAlterCommand::MATERIALIZE_TTL)
    {
        MutationCommand res;
        res.ast = command->ptr();
        res.type = MATERIALIZE_TTL;
        res.partition = command->partition;
        return res;
    }
    return {};
}


std::shared_ptr<ASTAlterCommandList> MutationCommands::ast() const
{
    auto res = std::make_shared<ASTAlterCommandList>();
    for (const MutationCommand & command : *this)
        res->add(command.ast->clone());
    return res;
}

void MutationCommands::writeText(WriteBuffer & out) const
{
    std::stringstream commands_ss;
    formatAST(*ast(), commands_ss, /* hilite = */ false, /* one_line = */ true);
    out << escape << commands_ss.str();
}

void MutationCommands::readText(ReadBuffer & in)
{
    String commands_str;
    in >> escape >> commands_str;

    ParserAlterCommandList p_alter_commands;
    auto commands_ast = parseQuery(
        p_alter_commands, commands_str.data(), commands_str.data() + commands_str.length(), "mutation commands list", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
    for (ASTAlterCommand * command_ast : commands_ast->as<ASTAlterCommandList &>().commands)
    {
        auto command = MutationCommand::parse(command_ast, true);
        if (!command)
            throw Exception("Unknown mutation command type: " + DB::toString<int>(command_ast->type), ErrorCodes::UNKNOWN_MUTATION_COMMAND);
        push_back(std::move(*command));
    }
}

}
