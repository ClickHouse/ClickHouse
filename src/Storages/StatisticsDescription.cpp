#include <Storages/StatisticsDescription.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTStatisticsDeclaration.h>
#include <Parsers/ParserCreateQuery.h>
#include <Storages/ColumnsDescription.h>

#include <optional>

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
    materialization = other.materialization;

    return *this;
}

SingleStatisticsDescription & SingleStatisticsDescription::operator=(SingleStatisticsDescription && other) noexcept
{
    if (this == &other)
        return *this;

    type = std::exchange(other.type, StatisticsType{});
    ast = other.ast ? other.ast->clone() : nullptr;
    is_implicit = other.is_implicit;
    materialization = other.materialization;
    other.ast.reset();

    return *this;
}

StatisticsType stringToStatisticsType(String type)
{
    type = Poco::toLower(type);

    if (type == "tdigest")
        return StatisticsType::TDigest;
    if (type == "uniq")
        return StatisticsType::Uniq;
    if (type == "countmin")
        return StatisticsType::CountMinSketch;
    if (type == "minmax")
        return StatisticsType::MinMax;
    if (type == "basic")
        return StatisticsType::Basic;
    if (type == "uniq_v2")
        return StatisticsType::UniqV2;

    throw Exception(ErrorCodes::INCORRECT_QUERY, "Unknown statistics type: {}. Supported statistics types are 'basic', 'countmin', 'minmax', 'tdigest', 'uniq' and 'uniq_v2'", type);
}

String statisticsTypeToString(StatisticsType type)
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
        case StatisticsType::Basic:
            return "basic";
        case StatisticsType::UniqV2:
            return "uniq_v2";
        case StatisticsType::UniqAssumedAllDistinct:
            return "uniq_assumed_all_distinct";
        case StatisticsType::UniqV2AssumedAllDistinct:
            return "uniq_v2_assumed_all_distinct";
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown statistics type: {}. Supported statistics types are 'basic', 'countmin', 'minmax', 'tdigest', 'uniq' and 'uniq_v2'", type);
    }
}

String statisticsMaterializationToString(StatisticsMaterialization materialization)
{
    switch (materialization)
    {
        case StatisticsMaterialization::Default: return "default";
        case StatisticsMaterialization::AssumedAllDistinct: return "assumed_all_distinct";
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown statistics materialization: {}", static_cast<unsigned>(materialization));
}

ASTPtr makeStatisticsTypeAST(StatisticsType type, StatisticsMaterialization materialization)
{
    if (materialization == StatisticsMaterialization::AssumedAllDistinct)
        return makeASTFunction(
            statisticsTypeToString(type), make_intrusive<ASTIdentifier>(statisticsMaterializationToString(materialization)));

    auto function = makeASTFunction(statisticsTypeToString(type));
    function->setNoEmptyArgs(true);
    return function;
}

bool isAssumedAllDistinctSerializedStatisticsType(StatisticsType type)
{
    return type == StatisticsType::UniqAssumedAllDistinct || type == StatisticsType::UniqV2AssumedAllDistinct;
}

StatisticsType getLogicalStatisticsType(StatisticsType type)
{
    switch (type)
    {
        case StatisticsType::UniqAssumedAllDistinct: return StatisticsType::Uniq;
        case StatisticsType::UniqV2AssumedAllDistinct: return StatisticsType::UniqV2;
        default: return type;
    }
}

StatisticsType getAssumedAllDistinctSerializedStatisticsType(StatisticsType type)
{
    switch (type)
    {
        case StatisticsType::Uniq: return StatisticsType::UniqAssumedAllDistinct;
        case StatisticsType::UniqV2: return StatisticsType::UniqV2AssumedAllDistinct;
        default: throw Exception(ErrorCodes::LOGICAL_ERROR, "Statistics type {} cannot be materialized as assumed_all_distinct", type);
    }
}

String SingleStatisticsDescription::getTypeName() const
{
    return statisticsTypeToString(type);
}

SingleStatisticsDescription::SingleStatisticsDescription(
    StatisticsType type_, ASTPtr ast_, bool is_implicit_, StatisticsMaterialization materialization_)
    : type(type_)
    , ast(ast_)
    , is_implicit(is_implicit_)
    , materialization(materialization_)
{
}

bool SingleStatisticsDescription::operator==(const SingleStatisticsDescription & other) const
{
    return type == other.type && is_implicit == other.is_implicit && materialization == other.materialization;
}

bool ColumnStatisticsDescription::operator==(const ColumnStatisticsDescription & other) const
{
    if (!data_type)
        return !other.data_type;

    if (!other.data_type)
        return false;

    return types_to_desc == other.types_to_desc && data_type->equals(*other.data_type);
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

bool isUniqLikeStatisticsType(StatisticsType type)
{
    return type == StatisticsType::Uniq || type == StatisticsType::UniqV2;
}

namespace
{

std::optional<String> getIdentifierOrStringLiteralName(const ASTPtr & ast)
{
    if (const auto * identifier = ast->as<ASTIdentifier>())
        return identifier->name();

    if (const auto * literal = ast->as<ASTLiteral>(); literal && literal->value.getType() == Field::Types::String)
        return literal->value.safeGet<String>();

    return std::nullopt;
}

std::optional<String> getStatisticsMaterializationNameFromArgument(const ASTPtr & ast)
{
    if (auto name = getIdentifierOrStringLiteralName(ast))
        return name;

    const auto * function = ast->as<ASTFunction>();
    if (!function || !function->arguments || function->name != "equals" || function->arguments->children.size() != 2)
        return std::nullopt;

    auto key = getIdentifierOrStringLiteralName(function->arguments->children[0]);
    if (!key || Poco::toLower(*key) != "materialization")
        return std::nullopt;

    return getIdentifierOrStringLiteralName(function->arguments->children[1]);
}

StatisticsMaterialization getStatisticsMaterializationFromAST(const ASTFunction & stat_ast, StatisticsType type)
{
    if (!stat_ast.arguments || stat_ast.arguments->children.empty())
        return StatisticsMaterialization::Default;

    if (!isUniqLikeStatisticsType(type))
    {
        for (const auto & argument : stat_ast.arguments->children)
        {
            auto materialization_name = getStatisticsMaterializationNameFromArgument(argument);
            if (materialization_name
                && Poco::toLower(*materialization_name) == statisticsMaterializationToString(StatisticsMaterialization::AssumedAllDistinct))
                throw Exception(
                    ErrorCodes::INCORRECT_QUERY, "Statistics type '{}' cannot use assumed_all_distinct materialization", stat_ast.name);
        }
        return StatisticsMaterialization::Default;
    }

    if (stat_ast.arguments->children.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Statistics type '{}' expects at most one materialization argument", stat_ast.name);

    auto materialization_name = getStatisticsMaterializationNameFromArgument(stat_ast.arguments->children[0]);
    if (!materialization_name)
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "Invalid materialization argument for statistics type '{}'. Expected 'assumed_all_distinct'",
            stat_ast.name);

    materialization_name = Poco::toLower(*materialization_name);
    if (*materialization_name == statisticsMaterializationToString(StatisticsMaterialization::AssumedAllDistinct))
        return StatisticsMaterialization::AssumedAllDistinct;

    throw Exception(
        ErrorCodes::INCORRECT_QUERY,
        "Unknown materialization '{}' for statistics type '{}'. Supported materialization is 'assumed_all_distinct'",
        *materialization_name,
        stat_ast.name);
}

SingleStatisticsDescription getSingleStatisticsDescriptionFromAST(const ASTPtr & ast, bool is_implicit)
{
    const auto & stat_ast = ast->as<const ASTFunction &>();
    auto type = stringToStatisticsType(Poco::toLower(stat_ast.name));
    auto materialization = getStatisticsMaterializationFromAST(stat_ast, type);
    auto ast_to_store
        = materialization == StatisticsMaterialization::AssumedAllDistinct ? makeStatisticsTypeAST(type, materialization) : ast->clone();
    return SingleStatisticsDescription(type, ast_to_store, is_implicit, materialization);
}

}

void ColumnStatisticsDescription::merge(
    const ColumnStatisticsDescription & other, const String & merging_column_name, DataTypePtr merging_column_type, bool if_not_exists)
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
        auto stat = getSingleStatisticsDescriptionFromAST(stat_ast, false);
        if (statistics_types.contains(stat.type))
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Statistics type {} was specified more than once", stat_type_name);

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

ColumnStatisticsDescription ColumnStatisticsDescription::fromStatisticsDescriptionAST(
    const ASTPtr & statistics_desc, const String & column_name, DataTypePtr data_type, bool is_implicit_)
{
    const auto & stat_type_list_ast = statistics_desc->as<ASTFunction &>().arguments;
    if (stat_type_list_ast->children.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "We expect at least one statistics type for column {}", column_name);

    ColumnStatisticsDescription stats;
    for (const auto & ast : stat_type_list_ast->children)
    {
        const auto & stat_type = ast->as<const ASTFunction &>().name;

        auto stat = getSingleStatisticsDescriptionFromAST(ast, is_implicit_);
        if (stats.types_to_desc.contains(stat.type))
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Column {} already contains statistics type {}", column_name, stat_type);

        stats.types_to_desc.emplace(stat.type, std::move(stat));
    }
    stats.data_type = data_type;
    return stats;
}

ASTPtr ColumnStatisticsDescription::getAST() const
{
    auto function_node = make_intrusive<ASTFunction>();
    function_node->name = "STATISTICS";
    function_node->setKind(ASTFunction::Kind::STATISTICS);
    function_node->arguments = make_intrusive<ASTExpressionList>();

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
