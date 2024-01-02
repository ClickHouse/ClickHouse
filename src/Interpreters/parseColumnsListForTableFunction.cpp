#include <Parsers/ASTExpressionList.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeNullable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY;
    extern const int ILLEGAL_COLUMN;

}

void validateDataType(const DataTypePtr & type, const DataTypeValidationSettings & settings)
{
    if (!settings.allow_suspicious_low_cardinality_types)
    {
        if (const auto * lc_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        {
            if (!isStringOrFixedString(*removeNullable(lc_type->getDictionaryType())))
                throw Exception(
                    ErrorCodes::SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY,
                    "Creating columns of type {} is prohibited by default due to expected negative impact on performance. "
                    "It can be enabled with the \"allow_suspicious_low_cardinality_types\" setting.",
                    lc_type->getName());
        }
    }

    if (!settings.allow_experimental_object_type)
    {
        if (type->hasDynamicSubcolumns())
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Cannot create column with type '{}' because experimental Object type is not allowed. "
                "Set setting allow_experimental_object_type = 1 in order to allow it", type->getName());
        }
    }

    if (!settings.allow_suspicious_fixed_string_types)
    {
        auto basic_type = removeLowCardinality(removeNullable(type));
        if (const auto * fixed_string = typeid_cast<const DataTypeFixedString *>(basic_type.get()))
        {
            if (fixed_string->getN() > MAX_FIXEDSTRING_SIZE_WITHOUT_SUSPICIOUS)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Cannot create column with type '{}' because fixed string with size > {} is suspicious. "
                    "Set setting allow_suspicious_fixed_string_types = 1 in order to allow it",
                    type->getName(),
                    MAX_FIXEDSTRING_SIZE_WITHOUT_SUSPICIOUS);
        }
    }
}

ColumnsDescription parseColumnsListFromString(const std::string & structure, const ContextPtr & context)
{
    ParserColumnDeclarationList parser(true, true);
    const Settings & settings = context->getSettingsRef();

    ASTPtr columns_list_raw = parseQuery(parser, structure, "columns declaration list", settings.max_query_size, settings.max_parser_depth);

    auto * columns_list = dynamic_cast<ASTExpressionList *>(columns_list_raw.get());
    if (!columns_list)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not cast AST to ASTExpressionList");

    auto columns = InterpreterCreateQuery::getColumnsDescription(*columns_list, context, false);
    auto validation_settings = DataTypeValidationSettings(context->getSettingsRef());
    for (const auto & [name, type] : columns.getAll())
        validateDataType(type, validation_settings);
    return columns;
}

bool tryParseColumnsListFromString(const std::string & structure, ColumnsDescription & columns, const ContextPtr & context, String & error)
{
    ParserColumnDeclarationList parser(true, true);
    const Settings & settings = context->getSettingsRef();

    const char * start = structure.data();
    const char * end = structure.data() + structure.size();
    ASTPtr columns_list_raw = tryParseQuery(parser, start, end, error, false, "columns declaration list", false, settings.max_query_size, settings.max_parser_depth);
    if (!columns_list_raw)
        return false;

    auto * columns_list = dynamic_cast<ASTExpressionList *>(columns_list_raw.get());
    if (!columns_list)
    {
        error = fmt::format("Invalid columns declaration list: \"{}\"", structure);
        return false;
    }

    try
    {
        columns = InterpreterCreateQuery::getColumnsDescription(*columns_list, context, false);
        auto validation_settings = DataTypeValidationSettings(context->getSettingsRef());
        for (const auto & [name, type] : columns.getAll())
            validateDataType(type, validation_settings);
        return true;
    }
    catch (...)
    {
        error = getCurrentExceptionMessage(false);
        return false;
    }
}

}
