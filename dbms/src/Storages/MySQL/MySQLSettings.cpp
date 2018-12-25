#include <Storages/MySQL/MySQLSettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Common/StringUtils/StringUtils.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include "MySQLSettings.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

static const String VARIABLE_PREFIX = "mysql_variable_";

static void getFieldWithLiteralQueryString(Field & field, const String & literal_query_string)
{
    ParserLiteral literal_p;
    std::string error_message;
    auto literal_query_string_data = literal_query_string.data();
    ASTPtr literal_node = tryParseQuery(literal_p, literal_query_string_data, literal_query_string.data() + literal_query_string.size(),
                                        error_message, false, "", false, 0);

    field = literal_node ? static_cast<ASTLiteral *>(literal_node.get())->value : Field(literal_query_string);
}

MySQLSettings::MySQLSettings(const ASTStorage & storage_def, const Context & context, const Poco::Util::AbstractConfiguration & config,
                             const String & default_database, const String & default_table)
{
    if (storage_def.settings)
        evaluateSetQuery(storage_def.settings);

    if (!storage_def.engine->arguments->children.empty())
        evaluateEngineArguments(storage_def.engine->arguments->children, context);

    if (!remote_database.changed || remote_table_name.changed)
        fillDefaultDatabaseAndTableName(default_database, default_table);

    const auto & set_query = convertConfigToSetQuery(config, "mysql");
    evaluateSetQuery(static_cast<const ASTSetQuery *>(set_query.get()));
}

void MySQLSettings::evaluateSetQuery(const ASTSetQuery * set_query)
{
    for (const ASTSetQuery::Change & setting : set_query->changes)
    {
#define MATCH_MySQL_SETTING(TYPE, NAME, DEFAULT, DESCRIPTION) \
        else if (setting.name == #NAME) NAME.set(setting.value);

        if (startsWith(setting.name, VARIABLE_PREFIX))
        {
            set_variables_query->changes.push_back(ASTSetQuery::Change());

            set_variables_query->changes.back().name = "@@SESSION." + setting.name.substr(VARIABLE_PREFIX.size());
            set_variables_query->changes.back().value = setting.value;
        }
        APPLY_FOR_MySQL_SETTINGS(MATCH_MySQL_SETTING)
        else
            throw Exception("Unknown setting " + setting.name + " for storage MySQL.", ErrorCodes::BAD_ARGUMENTS);
#undef MATCH_MySQL_SETTING
    }
}

void MySQLSettings::evaluateEngineArguments(const ASTs & arguments, const Context & context)
{
#define MATCH_ENGINE_ARGUMENT(TYPE, NAME, DEFAULT, DESCRIPTION) \
    if (arguments.size() >= ++args_index) \
        evaluateEngineArguments(#NAME, NAME, arguments[args_index - 1], context);

    size_t args_index = 0;
    APPLY_FOR_MySQL_SETTINGS(MATCH_ENGINE_ARGUMENT)

#undef MATCH_ENGINE_ARGUMENT
}

void MySQLSettings::fillDefaultDatabaseAndTableName(const String & default_database, const String & default_table_name)
{
    if (!remote_database.changed)
        remote_database.set(default_database);

    if (!remote_table_name.changed)
        remote_table_name.set(default_table_name);
}

ASTPtr MySQLSettings::convertConfigToSetQuery(const Poco::Util::AbstractConfiguration & config, const String & config_prefix) const
{
    const auto set_query = std::make_shared<ASTSetQuery>();

    const auto load_config_to_query = [&set_query, &config](const String & config_prefix)
    {
        Poco::Util::AbstractConfiguration::Keys config_keys;
        config.keys(config_prefix, config_keys);

        for (const auto & config_key : config_keys)
        {
            set_query->changes.push_back(ASTSetQuery::Change());
            set_query->changes.back().name = startsWith(config_key, VARIABLE_PREFIX) ? "@@SESSION." + config_key : config_key;
            getFieldWithLiteralQueryString(set_query->changes.back().value, config.getRawString(config_prefix + "." + config_key));
        }
    };

    load_config_to_query(config_prefix);
    load_config_to_query(config_prefix + "_" + remote_database.value);
    load_config_to_query(config_prefix + "_" + remote_database.value + "_" + remote_table_name.value);
    return set_query;
}

template<typename SettingType>
void MySQLSettings::evaluateEngineArguments(const String & name, SettingType & setting, const ASTPtr & argument, const Context & context)
{
    if (setting.changed)
        throw Exception("There is a duplicate " + name + " definition in the engine parameters.", ErrorCodes::BAD_ARGUMENTS);

    ASTPtr evaluated_argument = evaluateConstantExpressionOrIdentifierAsLiteral(argument, context);
    setting.set(static_cast<const ASTLiteral &>(*evaluated_argument).value);
}

String MySQLSettings::genSessionVariablesQuery()
{
    if (set_variables_query->changes.empty())
        return  "";

    std::stringstream initialize_query_stream;
    IAST::FormatSettings format_settings(initialize_query_stream, true);
    format_settings.always_quote_identifiers = true;
    format_settings.identifier_quoting_style = IdentifierQuotingStyle::Backticks;

    set_variables_query->format(format_settings);
    return initialize_query_stream.str();
}

}
