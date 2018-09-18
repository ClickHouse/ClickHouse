#include <Storages/MutationCommands.h>
#include <IO/Operators.h>
#include <Parsers/formatAST.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTAssignment.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_MUTATION_COMMAND;
    extern const int MULTIPLE_ASSIGNMENTS_TO_COLUMN;
}

std::optional<MutationCommand> MutationCommand::parse(ASTAlterCommand * command)
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
            const auto & assignment = typeid_cast<const ASTAssignment &>(*assignment_ast);
            auto insertion = res.column_to_update_expression.emplace(assignment.column_name, assignment.expression);
            if (!insertion.second)
                throw Exception("Multiple assignments in the single statement to column `" + assignment.column_name + "`",
                    ErrorCodes::MULTIPLE_ASSIGNMENTS_TO_COLUMN);
        }
        return res;
    }
    else
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
        p_alter_commands, commands_str.data(), commands_str.data() + commands_str.length(), "mutation commands list", 0);
    for (ASTAlterCommand * command_ast : typeid_cast<const ASTAlterCommandList &>(*commands_ast).commands)
    {
        auto command = MutationCommand::parse(command_ast);
        if (!command)
            throw Exception("Unknown mutation command type: " + DB::toString<int>(command_ast->type), ErrorCodes::UNKNOWN_MUTATION_COMMAND);
        push_back(std::move(*command));
    }
}

}
