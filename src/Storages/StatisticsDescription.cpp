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

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
};

StatisticDescription StatisticDescription::getStatisticFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns, ContextPtr context)
{
    const auto * stat_definition = definition_ast->as<ASTStatisticDeclaration>();
    if (!stat_definition)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create statistic from non ASTStatisticDeclaration AST");

    StatisticDescription stat;
    stat.definition_ast = definition_ast->clone();
    stat.type = Poco::toLower(stat_definition->type);
    if (stat.type != "tdigest")
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Incorrect type name {}", stat.type);
    String column_name = stat_definition->column_name;

    if (!columns.hasPhysical(column_name))
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Incorrect column name {}", column_name);

    const auto & column = columns.getPhysical(column_name);
    stat.column_name = column.name;
    /// TODO: check if it is numeric.
    stat.data_type = column.type;

    UNUSED(context);

    return stat;
}

StatisticDescription::StatisticDescription(const StatisticDescription & other)
    : definition_ast(other.definition_ast ? other.definition_ast->clone() : nullptr)
    , type(other.type)
    , column_name(other.column_name)
    , data_type(other.data_type)
{
}

StatisticDescription & StatisticDescription::operator=(const StatisticDescription & other)
{
    if (&other == this)
        return *this;

    if (other.definition_ast)
        definition_ast = other.definition_ast->clone();
    else
        definition_ast.reset();

    type = other.type;
    column_name = other.column_name;
    data_type = other.data_type;

    return *this;
}


bool StatisticsDescriptions::has(const String & name) const
{
    for (const auto & statistic : *this)
        if (statistic.column_name == name)
            return true;
    return false;
}

String StatisticsDescriptions::toString() const
{
    if (empty())
        return {};

    ASTExpressionList list;
    for (const auto & statistic : *this)
        list.children.push_back(statistic.definition_ast);

    return serializeAST(list);
}

StatisticsDescriptions StatisticsDescriptions::parse(const String & str, const ColumnsDescription & columns, ContextPtr context)
{
    StatisticsDescriptions result;
    if (str.empty())
        return result;

    ParserStatisticDeclaration parser;
    ASTPtr list = parseQuery(parser, str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

    for (const auto & index : list->children)
        result.emplace_back(StatisticDescription::getStatisticFromAST(index, columns, context));

    return result;
}

}
