#include <Parsers/ASTFreezeQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserFreezeQuery.h>
#include <Parsers/ParserPartition.h>
#include <Parsers/parseDatabaseAndTableName.h>


namespace DB
{
bool ParserFreezeQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_freeze_table("FREEZE TABLE");
    ParserKeyword s_partition("PARTITION");

    ParserKeyword s_with("WITH");
    ParserKeyword s_name("NAME");

    ParserStringLiteral string_literal_p;
    ParserPartition partition_p;

    auto query = std::make_shared<ASTFreezeQuery>();
    node = query;

    if (!s_freeze_table.ignore(pos, expected))
        return false;

    if (!parseDatabaseAndTableName(pos, expected, query->database, query->table))
        return false;

    if (s_partition.ignore(pos, expected))
    {
        if (!partition_p.parse(pos, query->partition, expected))
            return false;
    }

    /// WITH NAME 'name' - place local backup to directory with specified name
    if (s_with.ignore(pos, expected))
    {
        if (!s_name.ignore(pos, expected))
            return false;

        ASTPtr ast_with_name;
        if (!string_literal_p.parse(pos, ast_with_name, expected))
            return false;

        query->with_name = ast_with_name->as<ASTLiteral &>().value.get<const String &>();
    }

    return true;
}

}
