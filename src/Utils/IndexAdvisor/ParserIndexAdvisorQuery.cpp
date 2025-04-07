#include <Utils/IndexAdvisor/ParserIndexAdvisorQuery.h>
#include <Utils/IndexAdvisor/ASTIndexAdvisorQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/CommonParsers.h>
#include <Common/logger_useful.h>

namespace DB
{

bool ParserIndexAdvisorQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = std::make_shared<ASTIndexAdvisorQuery>();
    node = query;

    LOG_DEBUG(&Poco::Logger::get("ParserIndexAdvisorQuery"), "Starting to parse index advisor query");

    // Check for "ADVISE INDEX"
    if (pos->type != TokenType::BareWord || strncasecmp(pos->begin, "ADVISE", 6) != 0)
    {
        LOG_DEBUG(&Poco::Logger::get("ParserIndexAdvisorQuery"), "Expected 'ADVISE' keyword");
        return false;
    }
    ++pos;

    if (pos->type != TokenType::BareWord || strncasecmp(pos->begin, "INDEX", 5) != 0)
    {
        LOG_DEBUG(&Poco::Logger::get("ParserIndexAdvisorQuery"), "Expected 'INDEX' keyword");
        return false;
    }
    ++pos;

    if (!ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected))
    {
        LOG_DEBUG(&Poco::Logger::get("ParserIndexAdvisorQuery"), "Expected opening bracket");
        return false;
    }

    auto queries = std::make_shared<ASTExpressionList>();
    ParserSelectQuery select_p;

    while (true)
    {
        ASTPtr select_query;
        if (!select_p.parse(pos, select_query, expected))
        {
            LOG_DEBUG(&Poco::Logger::get("ParserIndexAdvisorQuery"), "Failed to parse SELECT query");
            return false;
        }

        LOG_DEBUG(&Poco::Logger::get("ParserIndexAdvisorQuery"), "Successfully parsed SELECT query");
        queries->children.push_back(select_query);

        if (!ParserToken(TokenType::Comma).ignore(pos, expected))
            break;
    }

    if (!ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
    {
        LOG_DEBUG(&Poco::Logger::get("ParserIndexAdvisorQuery"), "Expected closing bracket");
        return false;
    }

    query->queries = queries;
    query->children.push_back(queries);
    LOG_DEBUG(&Poco::Logger::get("ParserIndexAdvisorQuery"), "Successfully parsed index advisor query");
    return true;
}

} 
