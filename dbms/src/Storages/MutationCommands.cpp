#include <Storages/MutationCommands.h>
#include <Storages/IStorage.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Columns/FilterDescription.h>
#include <IO/Operators.h>
#include <Parsers/formatAST.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_MUTATION_COMMAND;
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

void MutationCommands::validate(const IStorage & table, const Context & context) const
{
    auto all_columns = table.getColumns().getAll();

    for (const MutationCommand & command : *this)
    {
        switch (command.type)
        {
            case MutationCommand::DELETE:
            {
                auto actions = ExpressionAnalyzer(command.predicate, context, {}, all_columns).getActions(true);

                /// Try executing the resulting actions on the table sample block to detect malformed queries.
                auto table_sample_block = table.getSampleBlock();
                actions->execute(table_sample_block);

                const ColumnWithTypeAndName & predicate_column = actions->getSampleBlock().getByName(
                    command.predicate->getColumnName());
                checkColumnCanBeUsedAsFilter(predicate_column);
                break;
            }
            default:
                throw Exception("Bad mutation type: " + toString<int>(command.type), ErrorCodes::LOGICAL_ERROR);
        }
    }
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
