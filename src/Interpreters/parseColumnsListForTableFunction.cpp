#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
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
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
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
    extern const SettingsBool allow_suspicious_aggregate_function_version_0;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
    extern const SettingsBool validate_experimental_and_suspicious_types_inside_nested_types;
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY;
extern const int ILLEGAL_COLUMN;

}

DataTypeValidationSettings::DataTypeValidationSettings(const DB::Settings & settings)
    : allow_suspicious_low_cardinality_types(settings[Setting::allow_suspicious_low_cardinality_types])
    , allow_suspicious_fixed_string_types(settings[Setting::allow_suspicious_fixed_string_types])
    , allow_suspicious_variant_types(settings[Setting::allow_suspicious_variant_types])
    , allow_suspicious_aggregate_function_version_0(settings[Setting::allow_suspicious_aggregate_function_version_0])
    , validate_nested_types(settings[Setting::validate_experimental_and_suspicious_types_inside_nested_types])
    , enable_time_time64_type(settings[Setting::enable_time_time64_type])
    , enable_nullable_tuple_type(settings[Setting::enable_nullable_tuple_type])
{
}

namespace
{

/// A type name does not keep an explicit state version 0, so it reads back as the function's default version.
void checkAggregateFunctionStateVersion(const IDataType & type, bool check_arguments)
{
    const auto * aggregate_type = typeid_cast<const DataTypeAggregateFunction *>(&type);
    if (aggregate_type && aggregate_type->getVersionIfExplicit() == 0 && aggregate_type->getFunction()->getDefaultVersion() != 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot use state version 0 of aggregate function {} in a type, because the type name does not keep it "
            "and reads back as the default state version {}. "
            "Set setting allow_suspicious_aggregate_function_version_0 = 1 in order to allow it",
            aggregate_type->getFunctionName(),
            aggregate_type->getFunction()->getDefaultVersion());

    if (!check_arguments)
        return;

    /// The argument types of `AggregateFunction` and `SimpleAggregateFunction` are not children in the type tree.
    auto check_types = [](const DataTypes & types)
    {
        for (const auto & argument_type : types)
            forEachInTypeTree(*argument_type, [](const IDataType & nested) { checkAggregateFunctionStateVersion(nested, true); });
    };
    if (aggregate_type)
        check_types(aggregate_type->getArgumentsDataTypes());
    if (const auto * simple = typeid_cast<const DataTypeCustomSimpleAggregateFunction *>(type.getCustomName()))
        check_types(simple->getArgumentsDataTypes());
}

DataTypeValidationSettings getStructureValidationSettings(const ContextPtr & context)
{
    DataTypeValidationSettings validation_settings(context->getSettingsRef());
    /// A structure is parsed again, with the server's settings, whenever a table defined by a table function is loaded.
    validation_settings.allow_suspicious_aggregate_function_version_0 = true;
    return validation_settings;
}

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

        if (!settings.allow_suspicious_aggregate_function_version_0)
            checkAggregateFunctionStateVersion(data_type, settings.validate_nested_types);

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
    auto validation_settings = getStructureValidationSettings(context);
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
        auto validation_settings = getStructureValidationSettings(context);
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

void removeMisboundDefaultsFromStructureHint(
    ColumnsDescription & structure_hint, const NameToNameSetMap & insertion_columns_by_hint_column, const ColumnsDescription & insertion_columns)
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
                return it != insertion_columns_by_hint_column.end() && !it->second.contains(read)
                    && insertion_columns.hasColumnOrSubcolumn(GetColumnsOptions::All, read);
            }))
            removed.insert(name);
    }

    for (const auto & name : removed)
        structure_hint.modify(name, [](ColumnDescription & column) { column.default_desc = {}; });
}

String getStructureToSend(const ColumnsDescription & columns, const ColumnsDescription * insertion_columns)
{
    const auto names = columns.getAll().getNameSet();
    std::unordered_map<String, NameSet> reads;
    for (const auto & column : columns)
    {
        if (column.default_desc.kind != ColumnDefaultKind::Default || !column.default_desc.expression
            || column.default_desc.expression->hasSecretParts())
            continue;
        reads.emplace(column.name, getStorageColumnsReadByDefault(columns, column));
    }

    /// The least fixed point, so cycles and self-reads are never sent.
    NameSet sent;
    for (bool changed = true; changed;)
    {
        changed = false;
        for (const auto & [name, required] : reads)
        {
            if (!sent.contains(name) && std::ranges::all_of(required, [&](const String & read)
                    {
                        if (names.contains(read))
                            return !columns.hasDefault(read) || sent.contains(read);
                        /// Not a column of the table the `DEFAULT`s come from either: the nodes resolve the name the same way as
                        /// this server, as a function, dictionary, table or set.
                        return !insertion_columns || !insertion_columns->hasColumnOrSubcolumn(GetColumnsOptions::All, read);
                    }))
                changed |= sent.insert(name).second;
        }
    }

    WriteBufferFromOwnString buf;
    bool first = true;
    for (const auto & column : columns)
    {
        if (!std::exchange(first, false))
            writeCString(", ", buf);
        writeBackQuotedString(column.name, buf);
        writeChar(' ', buf);
        writeString(column.type->getName(), buf);
        if (sent.contains(column.name))
        {
            writeCString(" DEFAULT ", buf);
            writeString(column.default_desc.expression->formatWithSecretsOneLine(), buf);
        }
    }
    return buf.str();
}

}
