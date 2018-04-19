#include <Storages/MutationCommands.h>
#include <Storages/IStorage.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Columns/FilterDescription.h>
#include <IO/Operators.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_MUTATION_COMMAND;
}

static String typeToString(MutationCommand::Type type)
{
    switch (type)
    {
        case MutationCommand::DELETE:     return "DELETE";
        default:
            throw Exception("Bad mutation type: " + toString<int>(type), ErrorCodes::LOGICAL_ERROR);
    }
}

void MutationCommand::writeText(WriteBuffer & out) const
{
    out << typeToString(type) << "\n";

    switch (type)
    {
    case  MutationCommand::DELETE:
        {
            std::stringstream ss;
            formatAST(*predicate, ss, /* hilite = */ false, /* one_line = */ true);
            out << "predicate: " << ss.str() << "\n";
            break;
        }
        default:
            throw Exception("Bad mutation type: " + toString<int>(type), ErrorCodes::LOGICAL_ERROR);
    }
}

void MutationCommand::readText(ReadBuffer & in)
{
    String type_str;
    in >> type_str >> "\n";

    if (type_str == "DELETE")
    {
        type = DELETE;

        String predicate_str;
        in >> "predicate: " >> predicate_str >> "\n";
        ParserExpressionWithOptionalAlias p_expr(false);
        predicate = parseQuery(
            p_expr, predicate_str.data(), predicate_str.data() + predicate_str.length(), "mutation predicate", 0);
    }
    else
        throw Exception("Unknown mutation command: `" + type_str + "'", ErrorCodes::UNKNOWN_MUTATION_COMMAND);
}


void MutationCommands::validate(const IStorage & table, const Context & context)
{
    auto all_columns = table.getColumns().getAll();

    for (const MutationCommand & command : commands)
    {
        switch (command.type)
        {
            case MutationCommand::DELETE:
            {
                auto actions = ExpressionAnalyzer(command.predicate, context, {}, all_columns).getActions(true);
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
    out << "format version: 1\n"
        << "count: " << commands.size() << "\n";
    for (const MutationCommand & command : commands)
    {
        command.writeText(out);
    }
}

void MutationCommands::readText(ReadBuffer & in)
{
    in >> "format version: 1\n";

    size_t count;
    in >> "count: " >> count >> "\n";

    for (size_t i = 0; i < count; ++i)
    {
        MutationCommand command;
        command.readText(in);
        commands.push_back(std::move(command));
    }
}

}
