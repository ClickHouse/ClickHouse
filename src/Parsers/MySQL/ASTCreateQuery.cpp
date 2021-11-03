#include <Parsers/MySQL/ASTCreateQuery.h>

#include <Interpreters/StorageID.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/MySQL/ASTCreateDefines.h>
#include <Parsers/MySQL/ASTDeclarePartitionOptions.h>
#include <Parsers/MySQL/ASTDeclareTableOptions.h>

namespace DB
{

namespace MySQLParser
{

ASTPtr ASTCreateQuery::clone() const
{
    auto res = std::make_shared<ASTCreateQuery>(*this);
    res->children.clear();

    if (columns_list)
    {
        res->columns_list = columns_list->clone();
        res->children.emplace_back(res->columns_list);
    }

    if (table_options)
    {
        res->table_options = table_options->clone();
        res->children.emplace_back(res->table_options);
    }

    if (partition_options)
    {
        res->partition_options = partition_options->clone();
        res->children.emplace_back(res->partition_options);
    }

    return res;
}

bool ParserCreateQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr table;
    ASTPtr like_table;
    ASTPtr columns_list;
    ASTPtr table_options;
    ASTPtr partition_options;
    bool is_temporary = false;
    bool if_not_exists = false;

    if (!ParserKeyword("CREATE").ignore(pos, expected))
        return false;

    if (ParserKeyword("TEMPORARY").ignore(pos, expected))
        is_temporary = true;

    if (!ParserKeyword("TABLE").ignore(pos, expected))
        return false;

    if (ParserKeyword("IF NOT EXISTS").ignore(pos, expected))
        if_not_exists = true;

    if (!ParserCompoundIdentifier(false).parse(pos, table, expected))
        return false;

    if (ParserKeyword("LIKE").ignore(pos, expected))
    {
        if (!ParserCompoundIdentifier(false).parse(pos, like_table, expected))
            return false;
    }
    else if (ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected))
    {
        if (ParserKeyword("LIKE").ignore(pos, expected))
        {
            if (!ParserCompoundIdentifier(false).parse(pos, like_table, expected))
                return false;

            if (!ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
                return false;
        }
        else
        {
            if (!ParserCreateDefines().parse(pos, columns_list, expected))
                return false;

            if (!ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
                return false;

            ParserDeclareTableOptions().parse(pos, table_options, expected);
            ParserDeclarePartitionOptions().parse(pos, partition_options, expected);
        }
    }
    else
        return false;

    auto create_query = std::make_shared<ASTCreateQuery>();

    create_query->temporary = is_temporary;
    create_query->if_not_exists = if_not_exists;

    StorageID table_id = getTableIdentifier(table);
    create_query->table = table_id.table_name;
    create_query->database = table_id.database_name;
    create_query->like_table = like_table;
    create_query->columns_list = columns_list;
    create_query->table_options = table_options;
    create_query->partition_options = partition_options;

    if (create_query->like_table)
        create_query->children.emplace_back(create_query->like_table);

    if (create_query->columns_list)
        create_query->children.emplace_back(create_query->columns_list);

    if (create_query->table_options)
        create_query->children.emplace_back(create_query->table_options);

    if (create_query->partition_options)
        create_query->children.emplace_back(create_query->partition_options);

    node = create_query;
    return true;
}
}

}
