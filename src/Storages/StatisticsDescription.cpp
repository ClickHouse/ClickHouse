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

#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
};

StatisticType StatisticDescription::stringToType(String type)
{
    if (type.empty())
        return TDigest;
    if (type == "tdigest")
        return TDigest;
    throw Exception(ErrorCodes::INCORRECT_QUERY, "Unknown statistic type: {}", type);
}

namespace
{

String typeToString(StatisticType type)
{
    if (type == TDigest)
        return "tdigest";
    return "unknown";
}

}

StatisticsDescriptions StatisticsDescriptions::getStatisticsFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns, ContextPtr context)
{
    const auto * stat_definition = definition_ast->as<ASTStatisticDeclaration>();
    if (!stat_definition)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create statistic from non ASTStatisticDeclaration AST");

    LOG_INFO(&Poco::Logger::get("stats_desc"), "stat_def is like {}", stat_definition->dumpTree());

    StatisticsDescriptions stats;
    for (const auto & column_ast : stat_definition->columns->children)
    {
        StatisticDescription stat;
        stat.type = StatisticDescription::stringToType(Poco::toLower(stat_definition->type));
        String column_name = column_ast->as<ASTIdentifier &>().name();

        if (!columns.hasPhysical(column_name))
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Incorrect column name {}", column_name);

        const auto & column = columns.getPhysical(column_name);
        stat.column_name = column.name;
        /// TODO: check if it is numeric.
        stat.data_type = column.type;
        stats.push_back(stat);
    }

    if (stats.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Empty statistic column list");

    LOG_INFO(&Poco::Logger::get("stats_desc"), "there are {} stats", stats.size());

    UNUSED(context);

    return stats;
}

bool StatisticsDescriptions::has(const String & name) const
{
    for (const auto & statistic : *this)
        if (statistic.column_name == name)
            return true;
    return false;
}

void StatisticsDescriptions::merge(const StatisticsDescriptions & other)
{
    insert(end(), other.begin(), other.end());
    /// definition_asts.insert(definition_asts.end(), other.definition_asts.begin(), other.definition_asts.end());
}

ASTPtr StatisticsDescriptions::getAST() const
{

    auto list = std::make_shared<ASTExpressionList>();

    for (const auto & stat : *this)
    {
        auto stat_ast = std::make_shared<ASTStatisticDeclaration>();
        auto cols_ast  = std::make_shared<ASTExpressionList>();
        auto col_ast  = std::make_shared<ASTIdentifier>(stat.column_name);
        cols_ast->children.push_back(col_ast);
        stat_ast->set(stat_ast->columns, cols_ast);
        stat_ast->type = typeToString(stat.type);

        list->children.push_back(stat_ast);
    }
    return list;
}

String StatisticsDescriptions::toString() const
{
    if (empty())
        return {};

    return serializeAST(*getAST());
}

}
