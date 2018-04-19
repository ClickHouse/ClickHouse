#include <Storages/MutationCommands.h>
#include <IO/Operators.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>


namespace DB
{

static String typeToString(MutationCommand::Type type)
{
    switch (type)
    {
    case MutationCommand::DELETE:     return "DELETE";
    default:
        throw Exception("Unknown mutation type: " + toString<int>(type), ErrorCodes::LOGICAL_ERROR);
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
        break;
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
    {
        /// We wouldn't know how to execute the mutation but at least we will be able to load it
        /// and know that it is there (important for selection of merges).
        type = UNKNOWN;
    }
}


void MutationCommands::writeText(WriteBuffer & out) const
{
    out << "mutation commands count: " << commands.size() << "\n";
    for (const MutationCommand & command : commands)
    {
        command.writeText(out);
    }
}

void MutationCommands::readText(ReadBuffer & in)
{
    size_t count;
    in >> "mutation commands count: " >> count >> "\n";

    for (size_t i = 0; i < count; ++i)
    {
        MutationCommand command;
        command.readText(in);
        commands.push_back(std::move(command));
    }
}

}
