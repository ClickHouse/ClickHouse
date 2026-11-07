#include <Core/Settings.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/TypeTree.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpandedASTBudget.h>
#include <Interpreters/IdentifierSemantic.h>
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
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/checkStackSize.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool enable_time_time64_type;
    extern const SettingsBool enable_nullable_tuple_type;
    extern const SettingsBool allow_suspicious_fixed_string_types;
    extern const SettingsBool allow_suspicious_low_cardinality_types;
    extern const SettingsBool allow_suspicious_variant_types;
    extern const SettingsUInt64 max_expanded_ast_elements;
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

constexpr std::string_view unwritten_columns_expansion = "unwritten insertion table columns";

bool isOtherScope(const IAST & ast)
{
    return ast.as<ASTSubquery>() || ast.as<ASTSelectQuery>() || ast.as<ASTSelectWithUnionQuery>() || ast.as<ASTTableExpression>();
}

void collectIdentifierHeads(const IAST & ast, NameSet & heads)
{
    checkStackSize();
    if (const auto * identifier = ast.as<ASTIdentifier>())
        heads.insert(identifier->name_parts.at(0));
    for (const auto & child : ast.children)
        collectIdentifierHeads(*child, heads);
}

bool hasAlias(const IAST & ast)
{
    checkStackSize();
    return !ast.tryGetAlias().empty() || std::ranges::any_of(ast.children, [](const ASTPtr & child) { return hasAlias(*child); });
}

bool hasNonDeterministicFunction(const IAST & ast, const ContextPtr & context)
{
    checkStackSize();
    if (const auto * function = ast.as<ASTFunction>(); function && function->name != "lambda")
    {
        auto resolver = FunctionFactory::instance().tryGet(function->name, context);
        if (!resolver || !resolver->isDeterministicInScopeOfQuery())
            return true;
    }
    return std::ranges::any_of(ast.children, [&](const ASTPtr & child) { return hasNonDeterministicFunction(*child, context); });
}

ASTPtr renameIdentifierHead(const ASTIdentifier & identifier, const String & head)
{
    auto parts = identifier.name_parts;
    parts.at(0) = head;
    auto renamed = make_intrusive<ASTIdentifier>(std::move(parts));
    renamed->setAlias(identifier.tryGetAlias());
    return renamed;
}

/// Renames the occurrences of the lambda argument `from` in `ast`, matched by the head of the identifier.
void renameLambdaArgument(ASTPtr & ast, const String & from, const String & to)
{
    checkStackSize();
    if (isOtherScope(*ast))
        return;

    if (const auto * identifier = ast->as<ASTIdentifier>())
    {
        if (identifier->name_parts.at(0) == from)
            ast = renameIdentifierHead(*identifier, to);
        return;
    }

    if (auto * function = ast->as<ASTFunction>(); function && function->name == "lambda")
    {
        if (!std::ranges::contains(RequiredSourceColumnsMatcher::extractNamesFromLambda(*function), from))
            renameLambdaArgument(function->arguments->children[1], from, to);
        return;
    }

    for (auto & child : ast->children)
        renameLambdaArgument(child, from, to);
}

/// Replaces the reads of the insertion table columns that the `INSERT` does not write with the values the insertion
/// table computes for them, like `addDefaultRequiredExpressionsRecursively`.
class UnwrittenColumnsInliner
{
public:
    UnwrittenColumnsInliner(
        const ColumnsDescription & structure_hint_, const ColumnsDescription & insertion_columns_, const Names & written_columns,
        const ContextPtr & context_)
        : structure_hint(structure_hint_)
        , insertion_columns(insertion_columns_)
        , written(written_columns.begin(), written_columns.end())
        , context(context_)
        , max_expanded_ast_elements(context->getSettingsRef()[Setting::max_expanded_ast_elements])
    {
    }

    /// Whether `rewrite` met a read of a column whose value defines an expression alias, and left it as it is.
    bool metValueWithAlias() const { return met_value_with_alias; }

    /// Returns whether `ast` changed. Identifiers whose head is in `lambda_arguments` are lambda arguments.
    bool rewrite(ASTPtr & ast, ExpandedASTBudget & budget, const NameSet & lambda_arguments)
    {
        checkStackSize();
        if (isOtherScope(*ast))
            return false;

        if (const auto * identifier = ast->as<ASTIdentifier>())
        {
            if (lambda_arguments.contains(identifier->name_parts.at(0)))
                return false;
            auto name = IdentifierSemantic::getColumnName(*identifier);
            const ASTPtr * value = name ? getValue(*name) : nullptr;
            if (!value)
                return false;
            String alias = identifier->tryGetAlias();
            ast = budget.replace(ast, *value);
            if (!alias.empty())
                ast->setAlias(alias);
            return true;
        }

        if (auto * function = ast->as<ASTFunction>(); function && function->name == "lambda")
        {
            auto arguments = RequiredSourceColumnsMatcher::extractNamesFromLambda(*function);
            renameCapturingArguments(*function, arguments);
            NameSet masked = lambda_arguments;
            masked.insert(arguments.begin(), arguments.end());
            return rewrite(function->arguments->children[1], budget, masked);
        }

        bool changed = false;
        for (auto & child : ast->children)
            changed |= rewrite(child, budget, lambda_arguments);
        return changed;
    }

private:
    /// The value of `name` if it is an insertion table column that the `INSERT` does not write.
    const ASTPtr * getValue(const String & name)
    {
        if (auto it = values.find(name); it != values.end())
            return &it->second;

        const auto * column = insertion_columns.tryGet(name);
        if (!column || written.contains(name))
            return nullptr;

        if (column->default_desc.expression && hasAlias(*column->default_desc.expression))
        {
            met_value_with_alias = true;
            return nullptr;
        }

        /// A non-deterministic value would differ from the one the insertion table computes for its own columns.
        if (column->default_desc.expression && hasNonDeterministicFunction(*column->default_desc.expression, context))
            return nullptr;

        ASTPtr value;
        if (column->default_desc.expression)
        {
            /// CREATE rejects cyclic `DEFAULT`s, this only bounds the recursion.
            if (!expanding.insert(name).second)
                return nullptr;
            auto cast = makeASTFunction(
                "_CAST", column->default_desc.expression->clone(), make_intrusive<ASTLiteral>(column->type->getName()));
            ExpandedASTBudget value_budget(max_expanded_ast_elements, *cast, unwritten_columns_expansion);
            rewrite(cast->arguments->children[0], value_budget, {});
            expanding.erase(name);
            value = std::move(cast);
        }
        else
            value = make_intrusive<ASTLiteral>(column->type->getDefault());

        return &values.emplace(name, std::move(value)).first->second;
    }

    /// Renames each argument of `lambda` that an inlined value may read, so that the value is not bound to it.
    void renameCapturingArguments(ASTFunction & lambda, Names & arguments)
    {
        if (!capturable)
        {
            capturable.emplace();
            for (const auto & column : insertion_columns)
                if (column.default_desc.expression && !written.contains(column.name))
                    collectIdentifierHeads(*column.default_desc.expression, *capturable);
        }

        auto & formals = lambda.arguments->children[0]->as<ASTFunction &>().arguments->children;
        for (size_t i = 0; i < formals.size(); ++i)
        {
            if (!capturable->contains(arguments[i]))
                continue;

            NameSet used;
            collectIdentifierHeads(lambda, used);
            auto is_used = [&](const String & name)
            {
                return used.contains(name) || capturable->contains(name) || insertion_columns.has(name) || structure_hint.has(name);
            };
            String fresh = fmt::format("__inlined_lambda_argument_{}", next_argument_index++);
            while (is_used(fresh))
                fresh = fmt::format("__inlined_lambda_argument_{}", next_argument_index++);

            renameLambdaArgument(lambda.arguments->children[1], arguments[i], fresh);
            formals[i] = renameIdentifierHead(formals[i]->as<ASTIdentifier &>(), fresh);
            arguments[i] = fresh;
        }
    }

    const ColumnsDescription & structure_hint;
    const ColumnsDescription & insertion_columns;
    const NameSet written;
    const ContextPtr context;
    const size_t max_expanded_ast_elements;

    std::unordered_map<String, ASTPtr> values;
    NameSet expanding;
    /// The heads of the identifiers that an inlined value may read.
    std::optional<NameSet> capturable;
    size_t next_argument_index = 0;
    bool met_value_with_alias = false;
};

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

void inlineUnwrittenColumnsInStructureHint(
    ColumnsDescription & structure_hint, const ColumnsDescription & insertion_columns, const Names & written_columns,
    const ContextPtr & context)
{
    Names names;
    auto expressions = make_intrusive<ASTExpressionList>();
    for (const auto & column : structure_hint)
    {
        if (column.default_desc.expression)
        {
            names.push_back(column.name);
            expressions->children.push_back(column.default_desc.expression->clone());
        }
    }

    /// The `DEFAULT`s evaluated together resolve the expression aliases of each other and of the values they read.
    if (hasAlias(*expressions))
        return;

    UnwrittenColumnsInliner inliner(structure_hint, insertion_columns, written_columns, context);
    ExpandedASTBudget budget(context->getSettingsRef()[Setting::max_expanded_ast_elements], *expressions, unwritten_columns_expansion);
    std::vector<size_t> changed;
    for (size_t i = 0; i < names.size(); ++i)
        if (inliner.rewrite(expressions->children[i], budget, {}))
            changed.push_back(i);

    if (inliner.metValueWithAlias())
        return;
    for (size_t i : changed)
        structure_hint.modify(names[i], [&](ColumnDescription & column) { column.default_desc.expression = expressions->children[i]; });
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
