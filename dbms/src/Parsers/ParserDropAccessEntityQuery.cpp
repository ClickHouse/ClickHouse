#include <Parsers/ParserDropAccessEntityQuery.h>
#include <Parsers/ASTDropAccessEntityQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/parseUserName.h>


namespace DB
{
namespace
{
    bool parseNames(IParserBase::Pos & pos, Expected & expected, Strings & names)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            Strings res_names;
            do
            {
                String name;
                if (!parseIdentifierOrStringLiteral(pos, expected, name))
                    return false;

                res_names.push_back(std::move(name));
            }
            while (ParserToken{TokenType::Comma}.ignore(pos, expected));

            names = std::move(res_names);
            return true;
        });
    }

    bool parseRowPolicyNames(IParserBase::Pos & pos, Expected & expected, std::vector<RowPolicy::FullNameParts> & names)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            std::vector<RowPolicy::FullNameParts> res_names;
            do
            {
                Strings policy_names;
                if (!parseNames(pos, expected, policy_names))
                    return false;
                String database, table_name;
                if (!ParserKeyword{"ON"}.ignore(pos, expected) || !parseDatabaseAndTableName(pos, expected, database, table_name))
                    return false;
                for (const String & policy_name : policy_names)
                    res_names.push_back({database, table_name, policy_name});
            }
            while (ParserToken{TokenType::Comma}.ignore(pos, expected));

            names = std::move(res_names);
            return true;
        });
    }

    bool parseUserNames(IParserBase::Pos & pos, Expected & expected, Strings & names)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            Strings res_names;
            do
            {
                String name;
                if (!parseUserName(pos, expected, name))
                    return false;

                res_names.emplace_back(std::move(name));
            }
            while (ParserToken{TokenType::Comma}.ignore(pos, expected));
            names = std::move(res_names);
            return true;
        });
    }
}


bool ParserDropAccessEntityQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{"DROP"}.ignore(pos, expected))
        return false;

    using Kind = ASTDropAccessEntityQuery::Kind;
    Kind kind;
    if (ParserKeyword{"USER"}.ignore(pos, expected))
        kind = Kind::USER;
    else if (ParserKeyword{"ROLE"}.ignore(pos, expected))
        kind = Kind::ROLE;
    else if (ParserKeyword{"QUOTA"}.ignore(pos, expected))
        kind = Kind::QUOTA;
    else if (ParserKeyword{"POLICY"}.ignore(pos, expected) || ParserKeyword{"ROW POLICY"}.ignore(pos, expected))
        kind = Kind::ROW_POLICY;
    else if (ParserKeyword{"SETTINGS PROFILE"}.ignore(pos, expected) || ParserKeyword{"PROFILE"}.ignore(pos, expected))
        kind = Kind::SETTINGS_PROFILE;
    else
        return false;

    bool if_exists = false;
    if (ParserKeyword{"IF EXISTS"}.ignore(pos, expected))
        if_exists = true;

    Strings names;
    std::vector<RowPolicy::FullNameParts> row_policies_names;

    if ((kind == Kind::USER) || (kind == Kind::ROLE))
    {
        if (!parseUserNames(pos, expected, names))
            return false;
    }
    else if (kind == Kind::ROW_POLICY)
    {
        if (!parseRowPolicyNames(pos, expected, row_policies_names))
            return false;
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
