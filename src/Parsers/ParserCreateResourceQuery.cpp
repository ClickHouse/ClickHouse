#include <Parsers/ParserCreateResourceQuery.h>

#include <Parsers/ASTCreateResourceQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>


namespace DB
{

namespace
{

bool parseOneOperation(ASTCreateResourceQuery::Operation & operation, IParser::Pos & pos, Expected & expected)
{
    ParserIdentifier disk_name_p;

    ASTCreateResourceQuery::AccessMode mode;
    ASTPtr node;
    std::optional<String> disk;

    if (ParserKeyword(Keyword::WRITE).ignore(pos, expected))
        mode = ASTCreateResourceQuery::AccessMode::Write;
    else if (ParserKeyword(Keyword::READ).ignore(pos, expected))
        mode = ASTCreateResourceQuery::AccessMode::Read;
    else
        return false;

    if (ParserKeyword(Keyword::ANY).ignore(pos, expected))
    {
        if (!ParserKeyword(Keyword::DISK).ignore(pos, expected))
            return false;
    }
    else
    {
        if (!ParserKeyword(Keyword::DISK).ignore(pos, expected))
            return false;

        if (!disk_name_p.parse(pos, node, expected))
            return false;

        disk.emplace();
        if (!tryGetIdentifierNameInto(node, *disk))
            return false;
    }

    operation.mode = mode;
    operation.disk = std::move(disk);

    return true;
}

bool parseOperations(IParser::Pos & pos, Expected & expected, ASTCreateResourceQuery::Operations & operations)
{
    return IParserBase::wrapParseImpl(pos, [&]
    {
        ParserToken s_open(TokenType::OpeningRoundBracket);
        ParserToken s_close(TokenType::ClosingRoundBracket);

        if (!s_open.ignore(pos, expected))
            return false;

        ASTCreateResourceQuery::Operations res_operations;

        auto parse_operation = [&]
        {
            ASTCreateResourceQuery::Operation operation;
            if (!parseOneOperation(operation, pos, expected))
                return false;
            res_operations.push_back(std::move(operation));
            return true;
        };

        if (!ParserList::parseUtil(pos, expected, parse_operation, false))
            return false;

        if (!s_close.ignore(pos, expected))
            return false;

        operations = std::move(res_operations);
        return true;
    });
}

}

bool ParserCreateResourceQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_resource(Keyword::RESOURCE);
    ParserKeyword s_or_replace(Keyword::OR_REPLACE);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserIdentifier resource_name_p;

    ASTPtr resource_name;

    String cluster_str;
    bool or_replace = false;
    bool if_not_exists = false;

    if (!s_create.ignore(pos, expected))
        return false;

    if (s_or_replace.ignore(pos, expected))
        or_replace = true;

    if (!s_resource.ignore(pos, expected))
        return false;

    if (!or_replace && s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!resource_name_p.parse(pos, resource_name, expected))
        return false;

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    ASTCreateResourceQuery::Operations operations;
    if (!parseOperations(pos, expected, operations))
        return false;

    auto create_resource_query = std::make_shared<ASTCreateResourceQuery>();
    node = create_resource_query;

    create_resource_query->resource_name = resource_name;
    create_resource_query->children.push_back(resource_name);

    create_resource_query->or_replace = or_replace;
    create_resource_query->if_not_exists = if_not_exists;
    create_resource_query->cluster = std::move(cluster_str);

    create_resource_query->operations = std::move(operations);

    return true;
}

}
