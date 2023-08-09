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

    if (stat_definition->name.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Statistic must have name in definition.");

    // type == nullptr => auto
    if (!stat_definition->type)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "TYPE is required for statistics");

    if (stat_definition->type->parameters && !stat_definition->type->parameters->children.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Statistics type cannot have parameters");

    StatisticDescription stat;
    stat.definition_ast = definition_ast->clone();
    stat.name = stat_definition->name;
    stat.type = Poco::toLower(stat_definition->type->name);

    ASTPtr expr_list = extractKeyExpressionList(stat_definition->columns->clone());
    for (const auto & ast : expr_list->children)
    {
        ASTIdentifier* ident = ast->as<ASTIdentifier>();
        if (!ident || !columns.hasPhysical(ident->getColumnName()))
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Incorrect column");
        const auto & column = columns.get(ident->getColumnName());
        stat.column_names.push_back(column.name);
        stat.data_types.push_back(column.type);
    }

    UNUSED(context);

    return stat;
}

StatisticDescription::StatisticDescription(const StatisticDescription & other)
    : definition_ast(other.definition_ast ? other.definition_ast->clone() : nullptr)
    , name(other.name)
    , type(other.type)
    , column_names(other.column_names)
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

    name = other.name;
    type = other.type;
    column_names = other.column_names;

    return *this;
}


bool StatisticsDescriptions::has(const String & name) const
{
    for (const auto & statistic : *this)
        if (statistic.name == name)
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
