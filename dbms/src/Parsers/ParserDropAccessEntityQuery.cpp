#include <Parsers/ParserDropAccessEntityQuery.h>
#include <Parsers/ASTDropAccessEntityQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Access/Quota.h>


namespace DB
{
namespace
{
    bool parseNames(IParserBase::Pos & pos, Expected & expected, Strings & names)
    {
        do
        {
            String name;
            if (!parseIdentifierOrStringLiteral(pos, expected, name))
                return false;

            names.push_back(std::move(name));
        }
        while (ParserToken{TokenType::Comma}.ignore(pos, expected));
        return true;
    }
}


bool ParserDropAccessEntityQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{"DROP"}.ignore(pos, expected))
        return false;

    using Kind = ASTDropAccessEntityQuery::Kind;
    Kind kind;
    if (ParserKeyword{"QUOTA"}.ignore(pos, expected))
        kind = Kind::QUOTA;
    else if (ParserKeyword{"POLICY"}.ignore(pos, expected) || ParserKeyword{"ROW POLICY"}.ignore(pos, expected))
        kind = Kind::ROW_POLICY;
    else
        return false;

    bool if_exists = false;
    if (ParserKeyword{"IF EXISTS"}.ignore(pos, expected))
        if_exists = true;

    Strings names;
    std::vector<RowPolicy::FullNameParts> row_policies_names;

    if (kind == Kind::ROW_POLICY)
    {
        do
        {
            Strings policy_names;
            if (!parseNames(pos, expected, policy_names))
                return false;
            String database, table_name;
            if (!ParserKeyword{"ON"}.ignore(pos, expected) || !parseDatabaseAndTableName(pos, expected, database, table_name))
                return false;
            for (const String & policy_name : policy_names)
                row_policies_names.push_back({database, table_name, policy_name});
        }
        while (ParserToken{TokenType::Comma}.ignore(pos, expected));
    }
    else
    {
        if (!parseNames(pos, expected, names))
            return false;
    }

    auto query = std::make_shared<ASTDropAccessEntityQuery>(kind);
    node = query;

    query->if_exists = if_exists;
    query->names = std::move(names);
    query->row_policies_names = std::move(row_policies_names);

    return true;
}
}
