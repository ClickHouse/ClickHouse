#include <Parsers/ParserShowCreateAccessEntityQuery.h>
#include <Parsers/ASTShowCreateAccessEntityQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <assert.h>


namespace DB
{
bool ParserShowCreateAccessEntityQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{"SHOW CREATE"}.ignore(pos, expected))
        return false;

    using Kind = ASTShowCreateAccessEntityQuery::Kind;
    Kind kind;
    if (ParserKeyword{"QUOTA"}.ignore(pos, expected))
        kind = Kind::QUOTA;
    else if (ParserKeyword{"POLICY"}.ignore(pos, expected) || ParserKeyword{"ROW POLICY"}.ignore(pos, expected))
        kind = Kind::ROW_POLICY;
    else
        return false;

    String name;
    bool current_quota = false;
    RowPolicy::FullNameParts row_policy_name;

    if (kind == Kind::ROW_POLICY)
    {
        String & database = row_policy_name.database;
        String & table_name = row_policy_name.table_name;
        String & policy_name = row_policy_name.policy_name;
        if (!parseIdentifierOrStringLiteral(pos, expected, policy_name) || !ParserKeyword{"ON"}.ignore(pos, expected)
            || !parseDatabaseAndTableName(pos, expected, database, table_name))
            return false;
    }
    else
    {
        assert(kind == Kind::QUOTA);
        if (ParserKeyword{"CURRENT"}.ignore(pos, expected))
        {
            /// SHOW CREATE QUOTA CURRENT
            current_quota = true;
        }
        else if (parseIdentifierOrStringLiteral(pos, expected, name))
        {
            /// SHOW CREATE QUOTA name
        }
        else
        {
            /// SHOW CREATE QUOTA
            current_quota = true;
        }
    }

    auto query = std::make_shared<ASTShowCreateAccessEntityQuery>(kind);
    node = query;

    query->name = std::move(name);
    query->current_quota = current_quota;
    query->row_policy_name = std::move(row_policy_name);

    return true;
}
}
