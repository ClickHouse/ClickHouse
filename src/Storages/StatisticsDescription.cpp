#include <base/defines.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTStatisticsDeclaration.h>
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
    extern const int ILLEGAL_STATISTIC;
    extern const int LOGICAL_ERROR;
};

String queryToString(const IAST & query);

StatisticType stringToType(String type)
{
    if (type == "tdigest")
        return TDigest;
    if (type == "uniq")
        return Uniq;
    throw Exception(ErrorCodes::INCORRECT_QUERY, "Unknown statistic type: {}. We only support statistic type `tdigest` right now.", type);
}

String StatisticDescription::getTypeName() const
{
    if (type == TDigest)
        return "TDigest";
    if (type == Uniq)
        return "Uniq";
    throw Exception(ErrorCodes::INCORRECT_QUERY, "Unknown statistic type: {}. We only support statistic type `tdigest` right now.", type);
}

static ASTPtr getASTForStatisticTypes(const std::unordered_map<StatisticType, StatisticDescription> & statistic_types)
{
        auto function_node = std::make_shared<ASTFunction>();
        function_node->name = "STATISTIC";
        function_node->arguments = std::make_shared<ASTExpressionList>();
        for (const auto & [type, desc] : statistic_types)
        {
            if (desc.ast == nullptr)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown ast");
            function_node->arguments->children.push_back(desc.ast);
        }
        function_node->children.push_back(function_node->arguments);
        return function_node;
}

bool StatisticsDescription::contains(const String & stat_type) const
{
    return stats.contains(stringToType(stat_type));
}

void StatisticsDescription::merge(const StatisticsDescription & other, const ColumnDescription & column, bool if_not_exists)
{
    if (other.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "We are merging empty stats in column {}", column.name);

    if (column_name.empty())
    {
        column_name = column.name;
        data_type = column.type;
    }

    for (const auto & iter: other.stats)
    {
        if (!if_not_exists && stats.contains(iter.first))
        {
            throw Exception(ErrorCodes::ILLEGAL_STATISTIC, "Statistic type name {} has existed in column {}", iter.first, column_name);
        }
    }

    for (const auto & iter: other.stats)
        if (!stats.contains(iter.first))
            stats[iter.first] = iter.second;
}

void StatisticsDescription::modify(const StatisticsDescription & other)
{
    if (other.column_name != column_name)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "unmactched statistic columns {} and {}", column_name, other.column_name);

    stats = other.stats;
}

void StatisticsDescription::clear()
{
    stats.clear();
}

std::vector<StatisticsDescription> StatisticsDescription::getStatisticsFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns)
{
    const auto * stat_definition = definition_ast->as<ASTStatisticsDeclaration>();
    if (!stat_definition)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create statistic from non ASTStatisticDeclaration AST");

    std::vector<StatisticsDescription> result;
    result.reserve(stat_definition->columns->children.size());

    std::unordered_map<StatisticType, StatisticDescription> statistic_types;
    for (const auto & stat_ast : stat_definition->types->children)
    {
        StatisticDescription stat;

        String stat_type_name = stat_ast->as<ASTFunction &>().name;
        if (statistic_types.contains(stat.type))
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Duplicated statistic type name: {} ", stat_type_name);
        stat.type = stringToType(Poco::toLower(stat_type_name));
        stat.ast = stat_ast->clone();
        statistic_types[stat.type] = stat;
    }

    for (const auto & column_ast : stat_definition->columns->children)
    {

        StatisticsDescription stats_desc;
        String physical_column_name = column_ast->as<ASTIdentifier &>().name();

        if (!columns.hasPhysical(physical_column_name))
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Incorrect column name {}", physical_column_name);

        const auto & column = columns.getPhysical(physical_column_name);
        stats_desc.column_name = column.name;
        stats_desc.stats = statistic_types;
        result.push_back(stats_desc);
    }

    if (result.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Empty statistic column list is not allowed.");

    return result;
}

StatisticsDescription StatisticsDescription::getStatisticFromColumnDeclaration(const ASTColumnDeclaration & column)
{
    const auto & stat_type_list_ast = column.stat_type->as<ASTFunction &>().arguments;
    if (stat_type_list_ast->children.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "We expect at least one statistic type for column {}", queryToString(column));
    StatisticsDescription stats;
    stats.column_name = column.name;
    for (const auto & ast : stat_type_list_ast->children)
    {
        const auto & stat_type = ast->as<const ASTFunction &>().name;

        StatisticDescription stat;
        stat.type = stringToType(Poco::toLower(stat_type));
        stat.ast = ast->clone();
        stats.add(stat.type, stat);
    }

    return stats;
}

void StatisticsDescription::add(StatisticType stat_type, const StatisticDescription & desc)
{
    if (stats.contains(stat_type))
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Statistic type {} duplicates", stat_type);
    stats[stat_type] = desc;
}

ASTPtr StatisticsDescription::getAST() const
{
    return getASTForStatisticTypes(stats);
}

}
