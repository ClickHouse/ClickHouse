#include <base/defines.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTStatisticDeclaration.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Poco/Logger.h>
#include <Storages/extractKeyExpressionList.h>
#include <Storages/StatisticsDescription.h>
#include <Storages/ColumnsDescription.h>

#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
};

StatisticDescription & StatisticDescription::operator=(const StatisticDescription & other)
{
    if (this == &other)
        return *this;

    type = other.type;
    column_name = other.column_name;
    ast = other.ast ? other.ast->clone() : nullptr;

    return *this;
}

StatisticDescription & StatisticDescription::operator=(StatisticDescription && other) noexcept
{
    if (this == &other)
        return *this;

    type = std::exchange(other.type, StatisticType{});
    column_name = std::move(other.column_name);
    ast = other.ast ? other.ast->clone() : nullptr;
    other.ast.reset();

    return *this;
}

StatisticType stringToType(String type)
{
    if (type == "tdigest")
        return TDigest;
    throw Exception(ErrorCodes::INCORRECT_QUERY, "Unknown statistic type: {}. We only support statistic type `tdigest` right now.", type);
}

String StatisticDescription::getTypeName() const
{
    if (type == TDigest)
        return "tdigest";
    throw Exception(ErrorCodes::INCORRECT_QUERY, "Unknown statistic type: {}. We only support statistic type `tdigest` right now.", type);
}

std::vector<StatisticDescription> StatisticDescription::getStatisticsFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns)
{
    const auto * stat_definition = definition_ast->as<ASTStatisticDeclaration>();
    if (!stat_definition)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create statistic from non ASTStatisticDeclaration AST");

    std::vector<StatisticDescription> stats;
    stats.reserve(stat_definition->columns->children.size());
    for (const auto & column_ast : stat_definition->columns->children)
    {
        StatisticDescription stat;
        stat.type = stringToType(Poco::toLower(stat_definition->type));
        String column_name = column_ast->as<ASTIdentifier &>().name();

        if (!columns.hasPhysical(column_name))
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Incorrect column name {}", column_name);

        const auto & column = columns.getPhysical(column_name);
        stat.column_name = column.name;
        stat.ast = makeASTFunction("STATISTIC", std::make_shared<ASTIdentifier>(stat_definition->type));
        stats.push_back(stat);
    }

    if (stats.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Empty statistic column list");

    return stats;
}

String queryToString(const IAST & query);

StatisticDescription StatisticDescription::getStatisticFromColumnDeclaration(const ASTColumnDeclaration & column)
{
    const auto & stat_type_list_ast = column.stat_type->as<ASTFunction &>().arguments;
    if (stat_type_list_ast->children.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "We expect only one statistic type for column {}", queryToString(column));

    const auto & stat_type = stat_type_list_ast->children[0]->as<ASTFunction &>().name;

    StatisticDescription stat;
    stat.type = stringToType(Poco::toLower(stat_type));
    stat.column_name = column.name;
    stat.ast = column.stat_type;

    return stat;
}

}
