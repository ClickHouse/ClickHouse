#include <Core/Settings.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeCustom.h>
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
    extern const SettingsBool enable_time_time64_type;
    extern const SettingsBool allow_experimental_nullable_tuple_type;
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
    , allow_experimental_nullable_tuple_type(settings[Setting::allow_experimental_nullable_tuple_type])
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

        if (!settings.allow_experimental_nullable_tuple_type)
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

    validate_callback(*type_to_check);
    if (settings.validate_nested_types)
        type_to_check->forEachChild(validate_callback);

    /// Reloading a table parses the stored type name, so a `Variant` whose elements render to the same name
    /// after re-parsing would silently lose all but one of them (the canonical `DataTypeVariant` constructor
    /// deduplicates by name), and reads of a column written with the original discriminators then fail.
    /// Such a type cannot round-trip through its own name, so it must not be created. This check is an
    /// integrity requirement rather than a suspicious-type policy, so unlike the callback above it is not
    /// gated by any setting. The traversal below cannot use `forEachChild` alone, because
    /// `DataTypeAggregateFunction` does not expose its argument types through it, and a `Variant` used as
    /// an argument of an aggregate function is part of the stored name just the same.
    auto check_variant_name_collisions = [](const IDataType & data_type)
    {
        const auto * variant_type = typeid_cast<const DataTypeVariant *>(&data_type);
        if (!variant_type)
            return;

        const auto & variants = variant_type->getVariants();
        if (variants.size() < 2)
            return;

        std::unordered_map<String, String> reparsed_to_original;
        for (const auto & variant : variants)
        {
            const auto original_name = variant->getName();
            String reparsed_name;
            try
            {
                reparsed_name = DataTypeFactory::instance().get(original_name)->getName();
            }
            catch (Exception & e)
            {
                e.addMessage("while checking that the name of variant '{}' of type '{}' can be parsed back",
                             original_name, data_type.getName());
                throw;
            }

            auto [it, inserted] = reparsed_to_original.emplace(reparsed_name, original_name);
            if (!inserted)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Cannot create column with type '{}' because variants '{}' and '{}' have the same name '{}' "
                    "when the type name is parsed back, so the column could not be read after a reload. "
                    "Note that version 0 is omitted from the name of an AggregateFunction type and is then "
                    "resolved to the default version. Consider using a single variant instead of these 2 variants",
                    data_type.getName(),
                    it->second,
                    original_name,
                    reparsed_name);
        }
    };

    /// `forEachChild` already walks a whole subtree, so it is applied to a type once and never from inside
    /// the callback it is given. The recursion below only adds the arguments of an aggregate function, which
    /// `forEachChild` does not reach. Its depth is bounded by the parse depth limit of `DataTypeFactory`, so
    /// it needs no limit of its own.
    IDataType::ChildCallback check_type_and_aggregate_arguments;
    check_type_and_aggregate_arguments = [&](const IDataType & data_type)
    {
        check_variant_name_collisions(data_type);

        const auto * aggregate_type = typeid_cast<const DataTypeAggregateFunction *>(&data_type);
        if (!aggregate_type)
            return;

        for (const auto & argument_type : aggregate_type->getArgumentsDataTypes())
        {
            check_type_and_aggregate_arguments(*argument_type);
            argument_type->forEachChild(check_type_and_aggregate_arguments);
        }
    };

    check_type_and_aggregate_arguments(*type_to_check);
    type_to_check->forEachChild(check_type_and_aggregate_arguments);
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
