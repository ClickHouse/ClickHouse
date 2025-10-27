#include <Storages/StatisticsDescription.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTStatisticsDeclaration.h>
#include <Parsers/ParserCreateQuery.h>
#include <Storages/ColumnsDescription.h>


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
    is_implicit = other.is_implicit;

    return *this;
}

SingleStatisticsDescription & SingleStatisticsDescription::operator=(SingleStatisticsDescription && other) noexcept
{
    if (this == &other)
        return *this;

    type = std::exchange(other.type, StatisticsType{});
    ast = other.ast ? other.ast->clone() : nullptr;
    is_implicit = other.is_implicit;
    other.ast.reset();

    return *this;
}

StatisticsType stringToStatisticsType(String type)
{
    if (type == "tdigest")
        return StatisticsType::TDigest;
    if (type == "uniq")
        return StatisticsType::Uniq;
    if (type == "countmin")
        return StatisticsType::CountMinSketch;
    if (type == "minmax")
        return StatisticsType::MinMax;

    throw Exception(ErrorCodes::INCORRECT_QUERY, "Unknown statistics type: {}. Supported statistics types are 'countmin', 'minmax', 'tdigest' and 'uniq'.", type);
}

String SingleStatisticsDescription::getTypeName() const
{
    switch (type)
    {
        case StatisticsType::TDigest:
            return "TDigest";
        case StatisticsType::Uniq:
            return "Uniq";
        case StatisticsType::CountMinSketch:
            return "countmin";
        case StatisticsType::MinMax:
            return "minmax";
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown statistics type: {}. Supported statistics types are 'countmin', 'minmax', 'tdigest' and 'uniq'.", type);
    }
}

SingleStatisticsDescription::SingleStatisticsDescription(StatisticsType type_, ASTPtr ast_, bool is_implicit_)
    : type(type_), ast(ast_), is_implicit(is_implicit_)
{}

bool SingleStatisticsDescription::operator==(const SingleStatisticsDescription & other) const
{
    return type == other.type && is_implicit == other.is_implicit;
}

bool ColumnStatisticsDescription::operator==(const ColumnStatisticsDescription & other) const
{
    return types_to_desc == other.types_to_desc;
}

bool ColumnStatisticsDescription::empty() const
{
    return types_to_desc.empty();
}

bool ColumnStatisticsDescription::hasExplicitStatistics() const
{
    return std::any_of(types_to_desc.begin(), types_to_desc.end(), [](const auto & desc) { return !desc.second.is_implicit; });
}

bool ColumnStatisticsDescription::contains(const String & stat_type) const
{
    return types_to_desc.contains(stringToStatisticsType(stat_type));
}

void ColumnStatisticsDescription::merge(const ColumnStatisticsDescription & other, const String & merging_column_name, DataTypePtr merging_column_type, bool if_not_exists)
{
    chassert(merging_column_type);

    data_type = merging_column_type;

    for (const auto & [stats_type, stats_desc]: other.types_to_desc)
    {
        if (!if_not_exists && types_to_desc.contains(stats_type))
        {
            throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Statistics type name {} has existed in column {}", stats_type, merging_column_name);
        }
        if (!types_to_desc.contains(stats_type))
            types_to_desc.emplace(stats_type, stats_desc);
    }
}

void ColumnStatisticsDescription::assign(const ColumnStatisticsDescription & other)
{
    types_to_desc = other.types_to_desc;
    data_type = other.data_type;
}

void ColumnStatisticsDescription::clear()
{
    types_to_desc.clear();
}

std::vector<std::pair<String, ColumnStatisticsDescription>> ColumnStatisticsDescription::fromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns)
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

        SingleStatisticsDescription stat(stat_type, stat_ast->clone(), false);
        statistics_types.emplace(stat.type, std::move(stat));
    }

    std::vector<std::pair<String, ColumnStatisticsDescription>> result;
    result.reserve(stat_definition_ast->columns->children.size());

    for (const auto & column_ast : stat_definition_ast->columns->children)
    {
        ColumnStatisticsDescription stats;
        String physical_column_name = column_ast->as<ASTIdentifier &>().name();

        if (!columns.hasPhysical(physical_column_name))
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Incorrect column name {}", physical_column_name);

        const auto & column = columns.getPhysical(physical_column_name);
        stats.data_type = column.type;
        stats.types_to_desc = statistics_types;
        result.emplace_back(physical_column_name, stats);
    }

    if (result.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Empty statistics column list is not allowed.");

    return result;
}

ColumnStatisticsDescription ColumnStatisticsDescription::fromStatisticsDescriptionAST(const ASTPtr & statistics_desc, const String & column_name, DataTypePtr data_type)
{
    const auto & stat_type_list_ast = statistics_desc->as<ASTFunction &>().arguments;
    if (stat_type_list_ast->children.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "We expect at least one statistics type for column {}", column_name);

    ColumnStatisticsDescription stats;
    for (const auto & ast : stat_type_list_ast->children)
    {
        const auto & stat_type = ast->as<const ASTFunction &>().name;

        SingleStatisticsDescription stat(stringToStatisticsType(Poco::toLower(stat_type)), ast->clone(), false);
        if (stats.types_to_desc.contains(stat.type))
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Column {} already contains statistics type {}", column_name, stat_type);

        stats.types_to_desc.emplace(stat.type, std::move(stat));
    }
    stats.data_type = data_type;
    return stats;
}

ASTPtr ColumnStatisticsDescription::getAST() const
{
    auto function_node = std::make_shared<ASTFunction>();
    function_node->name = "STATISTICS";
    function_node->kind = ASTFunction::Kind::STATISTICS;
    function_node->arguments = std::make_shared<ASTExpressionList>();

    for (const auto & [type, desc] : types_to_desc)
    {
        if (desc.ast == nullptr)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown ast");

        if (!desc.is_implicit)
            function_node->arguments->children.push_back(desc.ast);
    }

    function_node->children.push_back(function_node->arguments);
    return function_node;
}

String ColumnStatisticsDescription::getNameForLogs() const
{
    String ret;
    for (const auto & [tp, desc] : types_to_desc)
    {
        ret += desc.getTypeName();
        if (desc.is_implicit)
            ret += "(auto)";
        ret += ",";
    }
    if (!ret.empty())
        ret.pop_back();
    return ret;
}


}
