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
        throw Exception("Cannot create statistic from non ASTStatisticDeclaration AST", ErrorCodes::LOGICAL_ERROR);

    if (stat_definition->name.empty())
        throw Exception("Statistic must have name in definition.", ErrorCodes::INCORRECT_QUERY);

    // type == nullptr => auto
    if (!stat_definition->type)
        throw Exception("TYPE is required for statistic", ErrorCodes::INCORRECT_QUERY);

    if (stat_definition->type->parameters && !stat_definition->type->parameters->children.empty())
        throw Exception("Statistic type cannot have parameters", ErrorCodes::INCORRECT_QUERY);

    StatisticDescription stat;
    stat.definition_ast = definition_ast->clone();
    stat.name = stat_definition->name;
    stat.type = Poco::toLower(stat_definition->type->name);
    
    ASTPtr expr_list = extractKeyExpressionList(stat_definition->columns->clone());
    for (const auto & ast : expr_list->children)
    {
        ASTIdentifier* ident = ast->as<ASTIdentifier>();
        Poco::Logger::get("TEST").information(ident->name() + " " + ident->getColumnName());
        for (const auto& cl : columns) {
            Poco::Logger::get("TEXT").information(cl.name);
        }
        if (!ident || !columns.hasPhysical(ident->getColumnName()))
            throw Exception("Incorrect column", ErrorCodes::INCORRECT_QUERY);
        const auto & column = columns.get(ident->getColumnName());
        stat.column_names.push_back(column.name);
        stat.data_types.push_back(column.type);
    }

    UNUSED(context);
    Poco::Logger::get("KEK").information(stat.name + " " + stat.type);

    return stat;
}

StatisticDescription::StatisticDescription(const StatisticDescription & other)
    : definition_ast(other.definition_ast ? other.definition_ast->clone() : nullptr)
    //, expression_list_ast(other.expression_list_ast ? other.expression_list_ast->clone() : nullptr)
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
    
    //if (other.expression_list_ast)
    //    expression_list_ast = other.expression_list_ast->clone();
    //else
    //    expression_list_ast.reset();

    name = other.name;
    type = other.type;
    column_names = other.column_names;

    return *this;
}


bool StatisticDescriptions::has(const String & name) const
{
    for (const auto & statistic : *this)
        if (statistic.name == name)
            return true;
    return false;
}

String StatisticDescriptions::toString() const {
    if (empty())
        return {};

    ASTExpressionList list;
    for (const auto & statistic : *this)
        list.children.push_back(statistic.definition_ast);

    return serializeAST(list, true);
}

StatisticDescriptions StatisticDescriptions::parse(const String & str, const ColumnsDescription & columns, ContextPtr context)
{
    StatisticDescriptions result;
    if (str.empty())
        return result;

    ParserStatisticDeclaration parser;
    ASTPtr list = parseQuery(parser, str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

    for (const auto & index : list->children)
        result.emplace_back(StatisticDescription::getStatisticFromAST(index, columns, context));

    return result;
}

}
