#include <Core/Settings.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_dynamic_type;
    extern const SettingsBool allow_experimental_json_type;
    extern const SettingsBool allow_experimental_object_type;
    extern const SettingsBool allow_experimental_variant_type;
    extern const SettingsBool allow_suspicious_fixed_string_types;
    extern const SettingsBool allow_suspicious_low_cardinality_types;
    extern const SettingsBool allow_suspicious_variant_types;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
    extern const SettingsBool validate_experimental_and_suspicious_types_inside_nested_types;
}

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY;
extern const int ILLEGAL_COLUMN;

}

DataTypeValidationSettings::DataTypeValidationSettings(const DB::Settings & settings)
    : allow_suspicious_low_cardinality_types(settings[Setting::allow_suspicious_low_cardinality_types])
    , allow_experimental_object_type(settings[Setting::allow_experimental_object_type])
    , allow_suspicious_fixed_string_types(settings[Setting::allow_suspicious_fixed_string_types])
    , allow_experimental_variant_type(settings[Setting::allow_experimental_variant_type])
    , allow_suspicious_variant_types(settings[Setting::allow_suspicious_variant_types])
    , validate_nested_types(settings[Setting::validate_experimental_and_suspicious_types_inside_nested_types])
    , allow_experimental_dynamic_type(settings[Setting::allow_experimental_dynamic_type])
    , allow_experimental_json_type(settings[Setting::allow_experimental_json_type])
{
}


void validateDataType(const DataTypePtr & type_to_check, const DataTypeValidationSettings & settings)
{
    auto validate_callback = [&](const IDataType & data_type)
    {
        if (!settings.allow_suspicious_low_cardinality_types)
        {
            if (const auto * lc_type = typeid_cast<const DataTypeLowCardinality *>(&data_type))
            {
                if (!isStringOrFixedString(*removeNullable(lc_type->getDictionaryType())))
                    throw Exception(
                        ErrorCodes::SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY,
                        "Creating columns of type {} is prohibited by default due to expected negative impact on performance. "
                        "It can be enabled with the \"allow_suspicious_low_cardinality_types\" setting",
                        lc_type->getName());
            }
        }

        if (!settings.allow_experimental_object_type)
        {
            if (data_type.hasDynamicSubcolumnsDeprecated())
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Cannot create column with type '{}' because experimental Object type is not allowed. "
                    "Set setting allow_experimental_object_type = 1 in order to allow it",
                    data_type.getName());
            }
        }

        if (!settings.allow_suspicious_fixed_string_types)
        {
            if (const auto * fixed_string = typeid_cast<const DataTypeFixedString *>(&data_type))
            {
                if (fixed_string->getN() > MAX_FIXEDSTRING_SIZE_WITHOUT_SUSPICIOUS)
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN,
                        "Cannot create column with type '{}' because fixed string with size > {} is suspicious. "
                        "Set setting allow_suspicious_fixed_string_types = 1 in order to allow it",
                        data_type.getName(),
                        MAX_FIXEDSTRING_SIZE_WITHOUT_SUSPICIOUS);
            }
        }

        if (!settings.allow_experimental_variant_type)
        {
            if (isVariant(data_type))
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Cannot create column with type '{}' because experimental Variant type is not allowed. "
                    "Set setting allow_experimental_variant_type = 1 in order to allow it",
                    data_type.getName());
            }
        }

        if (!settings.allow_suspicious_variant_types)
        {
            if (const auto * variant_type = typeid_cast<const DataTypeVariant *>(&data_type))
            {
                const auto & variants = variant_type->getVariants();
                chassert(!variants.empty());
                for (size_t i = 0; i < variants.size() - 1; ++i)
                {
                    for (size_t j = i + 1; j < variants.size(); ++j)
                    {
                        /// Don't consider bool as similar to something (like number).
                        if (isBool(variants[i]) || isBool(variants[j]))
                            continue;

                        if (auto supertype = tryGetLeastSupertype(DataTypes{variants[i], variants[j]}))
                        {
                            throw Exception(
                                ErrorCodes::ILLEGAL_COLUMN,
                                "Cannot create column with type '{}' because variants '{}' and '{}' have similar types and working with values "
                                "of these types may lead to ambiguity. "
                                "Consider using common single variant '{}' instead of these 2 variants or set setting allow_suspicious_variant_types = 1 "
                                "in order to allow it",
                                data_type.getName(),
                                variants[i]->getName(),
                                variants[j]->getName(),
                                supertype->getName());
                        }
                    }
                }
            }
        }

        if (!settings.allow_experimental_dynamic_type)
        {
            if (isDynamic(data_type))
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Cannot create column with type '{}' because experimental Dynamic type is not allowed. "
                    "Set setting allow_experimental_dynamic_type = 1 in order to allow it",
                    data_type.getName());
            }
        }

        if (!settings.allow_experimental_json_type)
        {
            const auto * object_type = typeid_cast<const DataTypeObject *>(&data_type);
            if (object_type && object_type->getSchemaFormat() == DataTypeObject::SchemaFormat::JSON)
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Cannot create column with type '{}' because experimental JSON type is not allowed. "
                    "Set setting allow_experimental_json_type = 1 in order to allow it",
                    data_type.getName());
            }
        }
    };

    validate_callback(*type_to_check);
    if (settings.validate_nested_types)
        type_to_check->forEachChild(validate_callback);
}

ColumnsDescription parseColumnsListFromString(const std::string & structure, const ContextPtr & context)
{
    ParserColumnDeclarationList parser(true, true);
    const Settings & settings = context->getSettingsRef();

    ASTPtr columns_list_raw = parseQuery(
        parser,
        structure,
        "columns declaration list",
        settings[Setting::max_query_size],
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks]);

    auto * columns_list = dynamic_cast<ASTExpressionList *>(columns_list_raw.get());
    if (!columns_list)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not cast AST to ASTExpressionList");

    auto columns = InterpreterCreateQuery::getColumnsDescription(*columns_list, context, LoadingStrictnessLevel::CREATE);
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
    ASTPtr columns_list_raw = tryParseQuery(
        parser,
        start,
        end,
        error,
        false,
        "columns declaration list",
        false,
        settings[Setting::max_query_size],
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks],
        true);
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
        columns = InterpreterCreateQuery::getColumnsDescription(*columns_list, context, LoadingStrictnessLevel::CREATE);
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
