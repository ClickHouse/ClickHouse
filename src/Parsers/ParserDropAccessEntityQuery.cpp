#include <Parsers/ParserDropAccessEntityQuery.h>
#include <Parsers/ASTDropAccessEntityQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/parseUserName.h>
#include <ext/range.h>


namespace DB
{
namespace
{
    using EntityType = IAccessEntity::Type;
    using EntityTypeInfo = IAccessEntity::TypeInfo;

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

    bool parseRowPolicyNames(IParserBase::Pos & pos, Expected & expected, std::vector<RowPolicy::NameParts> & name_parts)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            std::vector<RowPolicy::NameParts> res_name_parts;
            do
            {
                Strings short_names;
                if (!parseNames(pos, expected, short_names))
                    return false;
                String database, table_name;
                if (!ParserKeyword{"ON"}.ignore(pos, expected) || !parseDatabaseAndTableName(pos, expected, database, table_name))
                    return false;
                for (String & short_name : short_names)
                    res_name_parts.push_back({std::move(short_name), database, table_name});
            }
            while (ParserToken{TokenType::Comma}.ignore(pos, expected));

            name_parts = std::move(res_name_parts);
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

    std::optional<EntityType> type;
    for (auto type_i : ext::range(EntityType::MAX))
    {
        const auto & type_info = EntityTypeInfo::get(type_i);
        if (ParserKeyword{type_info.name.c_str()}.ignore(pos, expected)
            || (!type_info.alias.empty() && ParserKeyword{type_info.alias.c_str()}.ignore(pos, expected)))
        {
            type = type_i;
        }
    }
    if (!type)
        return false;

    bool if_exists = false;
    if (ParserKeyword{"IF EXISTS"}.ignore(pos, expected))
        if_exists = true;

    Strings names;
    std::vector<RowPolicy::NameParts> row_policies_name_parts;

    if ((type == EntityType::USER) || (type == EntityType::ROLE))
    {
        if (!parseUserNames(pos, expected, names))
            return false;
    }
    else if (type == EntityType::ROW_POLICY)
    {
        if (!parseRowPolicyNames(pos, expected, row_policies_name_parts))
            return false;
    }
    else
    {
        if (!parseNames(pos, expected, names))
            return false;
    }

    String cluster;
    if (ParserKeyword{"ON"}.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster, expected))
            return false;
    }

    auto query = std::make_shared<ASTDropAccessEntityQuery>();
    node = query;

    query->type = *type;
    query->if_exists = if_exists;
    query->cluster = std::move(cluster);
    query->names = std::move(names);
    query->row_policies_name_parts = std::move(row_policies_name_parts);

    return true;
}
}
