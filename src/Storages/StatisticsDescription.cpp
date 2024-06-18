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

SingleStatisticsDescription & SingleStatisticsDescription::operator=(const SingleStatisticsDescription & other)
{
    if (this == &other)
        return *this;

    type = other.type;
    ast = other.ast ? other.ast->clone() : nullptr;

    return *this;
}

SingleStatisticsDescription & SingleStatisticsDescription::operator=(SingleStatisticsDescription && other) noexcept
{
    if (this == &other)
        return *this;

    type = std::exchange(other.type, StatisticsType{});
    ast = other.ast ? other.ast->clone() : nullptr;
    other.ast.reset();

    return *this;
}

static StatisticsType stringToStatisticsType(String type)
{
    if (type == "tdigest")
        return StatisticsType::TDigest;
    if (type == "uniq")
        return StatisticsType::Uniq;
    throw Exception(ErrorCodes::INCORRECT_QUERY, "Unknown statistics type: {}. Supported statistics types are `tdigest` and `uniq`.", type);
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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown statistics type: {}. Supported statistics types are `tdigest` and `uniq`.", type);
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
    return types_to_desc == other.types_to_desc;
}

bool ColumnStatisticsDescription::empty() const
{
    return types_to_desc.empty();
}

bool ColumnStatisticsDescription::contains(const String & stat_type) const
{
    return types_to_desc.contains(stringToStatisticsType(stat_type));
}

void ColumnStatisticsDescription::merge(const ColumnStatisticsDescription & other, const String & merging_column_name, DataTypePtr merging_column_type, bool if_not_exists)
{
    chassert(merging_column_type);

    if (column_name.empty())
    {
        column_name = merging_column_name;
        data_type = merging_column_type;
    }

    for (const auto & [stats_type, stats_desc]: other.types_to_desc)
    {
        if (!if_not_exists && types_to_desc.contains(stats_type))
        {
            throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Statistics type name {} has existed in column {}", stats_type, column_name);
        }
        else if (!types_to_desc.contains(stats_type))
            types_to_desc.emplace(stats_type, stats_desc);
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

std::vector<ColumnStatisticsDescription> ColumnStatisticsDescription::fromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns)
{
    const auto * stat_definition_ast = definition_ast->as<ASTStatisticsDeclaration>();
    if (!stat_definition_ast)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot cast AST to ASTSingleStatisticsDeclaration");

    StatisticsTypeDescMap statistics_types;
    for (const auto & stat_ast : stat_definition_ast->types->children)
    {
        String stat_type_name = stat_ast->as<ASTFunction &>().name;
        auto stat_type = stringToStatisticsType(Poco::toLower(stat_type_name));
        if (statistics_types.contains(stat_type))
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Statistics type {} was specified more than once", stat_type_name);
        SingleStatisticsDescription stat(stat_type, stat_ast->clone());

        statistics_types.emplace(stat.type, stat);
    }

    std::vector<ColumnStatisticsDescription> result;
    result.reserve(stat_definition_ast->columns->children.size());

    for (const auto & column_ast : stat_definition_ast->columns->children)
    {
        ColumnStatisticsDescription stats;
        String physical_column_name = column_ast->as<ASTIdentifier &>().name();

        if (!columns.hasPhysical(physical_column_name))
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Incorrect column name {}", physical_column_name);

        const auto & column = columns.getPhysical(physical_column_name);
        stats.column_name = column.name;
        stats.types_to_desc = statistics_types;
        result.push_back(stats);
    }

    if (result.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Empty statistics column list is not allowed.");

    return result;
}

ColumnStatisticsDescription ColumnStatisticsDescription::fromColumnDeclaration(const ASTColumnDeclaration & column, DataTypePtr data_type)
{
    const auto & stat_type_list_ast = column.statistics_desc->as<ASTFunction &>().arguments;
    if (stat_type_list_ast->children.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "We expect at least one statistics type for column {}", queryToString(column));
    ColumnStatisticsDescription stats;
    stats.column_name = column.name;
    for (const auto & ast : stat_type_list_ast->children)
    {
        const auto & stat_type = ast->as<const ASTFunction &>().name;

        SingleStatisticsDescription stat(stringToStatisticsType(Poco::toLower(stat_type)), ast->clone());
        if (stats.types_to_desc.contains(stat.type))
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Column {} already contains statistics type {}", stats.column_name, stat_type);
        stats.types_to_desc.emplace(stat.type, std::move(stat));
    }
    stats.data_type = data_type;
    return stats;
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
