#include <Core/Settings.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/TypeTree.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTCreateQuery.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool enable_time_time64_type;
    extern const SettingsBool enable_nullable_tuple_type;
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
    , allow_suspicious_fixed_string_types(settings[Setting::allow_suspicious_fixed_string_types])
    , allow_suspicious_variant_types(settings[Setting::allow_suspicious_variant_types])
    , validate_nested_types(settings[Setting::validate_experimental_and_suspicious_types_inside_nested_types])
    , enable_time_time64_type(settings[Setting::enable_time_time64_type])
    , enable_nullable_tuple_type(settings[Setting::enable_nullable_tuple_type])
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
                auto unwrapped = removeNullable(lc_type->getDictionaryType());

                /// It is allowed having LowCardinality(UUID) because often times UUIDs are highly repetitive in tables,
                /// and their relatively large size provides opportunity for better performance.

                if (!isStringOrFixedString(unwrapped) && !isUUID(unwrapped))
                    throw Exception(
                        ErrorCodes::SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY,
                        "Creating columns of type {} is prohibited by default due to expected negative impact on performance. "
                        "It can be enabled with the `allow_suspicious_low_cardinality_types` setting",
                        lc_type->getName());
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

                        const auto * custom_name = variant_type->getCustomName();
                        if (custom_name && custom_name->getName() == "Geometry")
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

        if (!settings.enable_time_time64_type)
        {
            if (isTime(data_type))
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Cannot create column with type '{}' because Time type is not allowed. "
                    "Set setting enable_time_time64_type = 1 in order to allow it",
                    data_type.getName());
            }
            if (isTime64(data_type))
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Cannot create column with type '{}' because Time64 type is not allowed. "
                    "Set setting enable_time_time64_type = 1 in order to allow it",
                    data_type.getName());
            }
        }

        if (!settings.enable_nullable_tuple_type)
        {
            if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(&data_type))
            {
                if (isTuple(nullable_type->getNestedType()))
                {
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN,
                        "Cannot create column with type '{}' because Nullable Tuple type is not allowed. "
                        "Set setting enable_nullable_tuple_type = 1 in order to allow it",
                        data_type.getName());
                }
            }
        }
    };

    if (settings.validate_nested_types)
        forEachInTypeTree(*type_to_check, validate_callback);
    else
        validate_callback(*type_to_check);
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

    if (!dynamic_cast<ASTExpressionList *>(columns_list_raw.get()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not cast AST to ASTExpressionList");

    auto columns_list = make_intrusive<ASTColumns>();
    columns_list->set(columns_list->columns, columns_list_raw);
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

    if (!dynamic_cast<ASTExpressionList *>(columns_list_raw.get()))
    {
        error = fmt::format("Invalid columns declaration list: \"{}\"", structure);
        return false;
    }

    try
    {
        auto columns_list = make_intrusive<ASTColumns>();
        columns_list->set(columns_list->columns, columns_list_raw);
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

namespace
{

void replaceObjectIdentifiersWithLiterals(ASTPtr & ast, const ColumnsDescription & columns)
{
    auto replace_if_not_column = [&](ASTPtr & argument)
    {
        if (const auto * identifier = argument->as<ASTIdentifier>();
            identifier && !columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, identifier->name()))
            argument = make_intrusive<ASTLiteral>(identifier->name());
    };

    if (auto * function = ast->as<ASTFunction>(); function && function->arguments)
    {
        auto & arguments = function->arguments->children;
        if (!arguments.empty() && (functionIsDictGet(function->name) || functionIsJoinGet(function->name)))
            replace_if_not_column(arguments[0]);
        else if (arguments.size() == 2 && functionIsInOrGlobalInOperator(function->name))
            replace_if_not_column(arguments[1]);
    }
    for (auto & child : ast->children)
        replaceObjectIdentifiersWithLiterals(child, columns);
}

}

NameSet getStorageColumnsReadByDefault(const ColumnsDescription & columns, const ColumnDescription & column)
{
    RequiredSourceColumnsVisitor::Data data;
    auto expression = column.default_desc.expression->clone();
    replaceObjectIdentifiersWithLiterals(expression, columns);
    RequiredSourceColumnsVisitor(data).visit(expression);
    NameSet storage_reads;
    for (const auto & read : data.requiredColumns())
    {
        auto read_column = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, read);
        storage_reads.insert(read_column ? read_column->getNameInStorage() : read);
    }
    return storage_reads;
}

void removeMisboundDefaultsFromStructureHint(ColumnsDescription & structure_hint, const NameToNameSetMap & insertion_columns_by_hint_column)
{
    std::unordered_map<String, NameSet> reads;
    for (const auto & column : structure_hint)
        if (column.default_desc.expression)
            reads.emplace(column.name, getStorageColumnsReadByDefault(structure_hint, column));

    NameSet removed;
    for (const auto & [name, required] : reads)
    {
        if (std::ranges::any_of(required, [&](const String & read)
            {
                auto it = insertion_columns_by_hint_column.find(read);
                return it != insertion_columns_by_hint_column.end() && !it->second.contains(read);
            }))
            removed.insert(name);
    }

    for (bool changed = !removed.empty(); changed;)
    {
        changed = false;
        for (const auto & [name, required] : reads)
            if (!removed.contains(name) && std::ranges::any_of(required, [&](const String & read) { return removed.contains(read); }))
                changed |= removed.insert(name).second;
    }

    for (const auto & name : removed)
        structure_hint.modify(name, [](ColumnDescription & column) { column.default_desc = {}; });
}

}
