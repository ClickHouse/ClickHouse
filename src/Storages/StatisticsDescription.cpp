#include <Storages/StatisticsDescription.h>

#include <base/defines.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTStatisticsDeclaration.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Parsers/ParserCreateQuery.h>
#include <Poco/Logger.h>
#include <Storages/extractKeyExpressionList.h>
#include <Storages/ColumnsDescription.h>

#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int ILLEGAL_STATISTICS;
    extern const int LOGICAL_ERROR;
};

static StatisticsType stringToStatisticType(String type)
{
    if (type == "tdigest")
        return StatisticsType::TDigest;
    if (type == "uniq")
        return StatisticsType::Uniq;
    throw Exception(ErrorCodes::INCORRECT_QUERY, "Unknown statistic type: {}. Supported statistic types are `tdigest` and `uniq`.", type);
}

String SingleStatisticsDescription::getTypeName() const
{
    switch (type)
    {
        case StatisticsType::TDigest:
            return "TDigest";
        case StatisticsType::Uniq:
            return "Uniq";
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown statistic type: {}. Supported statistic types are `tdigest` and `uniq`.", type);
    }
}

SingleStatisticsDescription::SingleStatisticsDescription(StatisticsType type_, ASTPtr ast_)
    : type(type_), ast(ast_)
{}

bool SingleStatisticsDescription::operator==(const SingleStatisticsDescription & other) const
{
    return type == other.type;
}

bool ColumnStatisticsDescription::operator==(const ColumnStatisticsDescription & other) const
{
    if (types_to_desc.size() != other.types_to_desc.size())
        return false;

    for (const auto & s : types_to_desc)
    {
        StatisticsType stats_type = s.first;
        if (!other.types_to_desc.contains(stats_type))
            return false;
        if (!(s.second == other.types_to_desc.at(stats_type)))
            return false;
    }

    return true;
}

bool ColumnStatisticsDescription::empty() const
{
    return types_to_desc.empty();
}

bool ColumnStatisticsDescription::contains(const String & stat_type) const
{
    return types_to_desc.contains(stringToStatisticType(stat_type));
}

void ColumnStatisticsDescription::merge(const ColumnStatisticsDescription & other, const ColumnDescription & column, bool if_not_exists)
{
    if (column_name.empty())
    {
        column_name = column.name;
        data_type = column.type;
    }

    for (const auto & iter: other.types_to_desc)
    {
        if (!if_not_exists && types_to_desc.contains(iter.first))
        {
            throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Statistic type name {} has existed in column {}", iter.first, column_name);
        }
        else if (!types_to_desc.contains(iter.first))
            types_to_desc.emplace(iter.first, iter.second);
    }
}

void ColumnStatisticsDescription::assign(const ColumnStatisticsDescription & other)
{
    if (other.column_name != column_name)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot assign statistics from column {} to {}", column_name, other.column_name);

    types_to_desc = other.types_to_desc;
}

void ColumnStatisticsDescription::clear()
{
    types_to_desc.clear();
}

std::vector<ColumnStatisticsDescription> ColumnStatisticsDescription::getStatisticsDescriptionsFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns)
{
    const auto * stat_definition_ast = definition_ast->as<ASTStatisticsDeclaration>();
    if (!stat_definition_ast)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot AST to ASTStatisticDeclaration");

    std::vector<ColumnStatisticsDescription> result;
    result.reserve(stat_definition_ast->columns->children.size());

    StatisticsTypeDescMap statistic_types;
    for (const auto & stat_ast : stat_definition_ast->types->children)
    {
        String stat_type_name = stat_ast->as<ASTFunction &>().name;
        auto stat_type = stringToStatisticType(Poco::toLower(stat_type_name));
        if (statistic_types.contains(stat_type))
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Statistic type {} was specified more than once", stat_type_name);
        SingleStatisticsDescription stat(stat_type, stat_ast->clone());

        statistic_types.emplace(stat.type, stat);
    }

    for (const auto & column_ast : stat_definition_ast->columns->children)
    {

        ColumnStatisticsDescription types_to_desc_desc;
        String physical_column_name = column_ast->as<ASTIdentifier &>().name();

        if (!columns.hasPhysical(physical_column_name))
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Incorrect column name {}", physical_column_name);

        const auto & column = columns.getPhysical(physical_column_name);
        types_to_desc_desc.column_name = column.name;
        types_to_desc_desc.types_to_desc = statistic_types;
        result.push_back(types_to_desc_desc);
    }

    if (result.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Empty statistic column list is not allowed.");

    return result;
}

ColumnStatisticsDescription ColumnStatisticsDescription::getStatisticFromColumnDeclaration(const ASTColumnDeclaration & column)
{
    const auto & stat_type_list_ast = column.stat_type->as<ASTFunction &>().arguments;
    if (stat_type_list_ast->children.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "We expect at least one statistic type for column {}", queryToString(column));
    ColumnStatisticsDescription stats;
    stats.column_name = column.name;
    for (const auto & ast : stat_type_list_ast->children)
    {
        const auto & stat_type = ast->as<const ASTFunction &>().name;

        SingleStatisticsDescription stat(stringToStatisticType(Poco::toLower(stat_type)), ast->clone());
        stats.add(stat.type, stat);
    }

    return stats;
}

void ColumnStatisticsDescription::add(StatisticsType stat_type, const SingleStatisticsDescription & desc)
{
    if (types_to_desc.contains(stat_type))
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Column {} already contains statistic type {}", column_name, stat_type);
    types_to_desc.emplace(stat_type, desc);
}

ASTPtr ColumnStatisticsDescription::getAST() const
{
    auto function_node = std::make_shared<ASTFunction>();
    function_node->name = "STATISTICS";
    function_node->arguments = std::make_shared<ASTExpressionList>();
    for (const auto & [type, desc] : types_to_desc)
    {
        if (desc.ast == nullptr)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown ast");
        function_node->arguments->children.push_back(desc.ast);
    }
    function_node->children.push_back(function_node->arguments);
    return function_node;
}

}
