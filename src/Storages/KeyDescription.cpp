#include <Storages/KeyDescription.h>
#include <Storages/VirtualColumnUtils.h>

#include <Functions/IFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/extractKeyExpressionList.h>
#include <Common/quoteString.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool enable_extended_results_for_datetime_functions;
    extern const SettingsBool cast_keep_nullable;
    extern const SettingsBool geo_distance_returns_float64_on_float64_arguments;
    extern const SettingsBool function_json_value_return_type_allow_nullable;
    extern const SettingsBool least_greatest_legacy_null_behavior;
    extern const SettingsBool h3togeo_lon_lat_result_order;
    extern const SettingsUInt64 function_date_trunc_return_type_behavior;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int DATA_TYPE_CANNOT_BE_USED_IN_KEY;
}

ContextPtr createKeyExpressionContext(const ContextPtr & context)
{
    /// A storage key expression type must be stable and independent of the transient session settings
    /// that happen to be set when someone (re)builds the metadata. A few settings change the RESULT
    /// TYPE (or, for h3togeo_lon_lat_result_order, the tuple element LAYOUT) of a key expression:
    ///   - enable_extended_results_for_datetime_functions: toStartOf*/toMonday/... on Date32/DateTime64
    ///     return an extended type (Date32/DateTime64) instead of the canonical Date/DateTime;
    ///   - cast_keep_nullable: CAST(nullable AS T) returns Nullable(T) instead of the canonical T;
    ///   - geo_distance_returns_float64_on_float64_arguments: geoDistance/greatCircleDistance/
    ///     greatCircleAngle over Float64 arguments return Float64 instead of Float32;
    ///   - function_json_value_return_type_allow_nullable: JSON_VALUE returns Nullable(String) instead
    ///     of the canonical String;
    ///   - function_date_trunc_return_type_behavior: dateTrunc over Date32/DateTime64 returns the
    ///     extended type instead of the canonical Date/DateTime;
    ///   - least_greatest_legacy_null_behavior: least/greatest with a NULL argument returns
    ///     Nullable(T) (the NULL is ignored) instead of the legacy short-circuited type;
    ///   - h3togeo_lon_lat_result_order: h3ToGeo returns Tuple(longitude, latitude) instead of the
    ///     canonical Tuple(latitude, longitude) - same top-level type but the two Float64 elements are
    ///     swapped, so it silently reorders the produced key column rather than aborting with a Bad cast.
    /// If a CREATE/ALTER runs with any of them at a value that differs from the server baseline, the
    /// recomputed KeyDescription::data_types diverge from the column the storage actually produces for
    /// the key (which is analyzed elsewhere with the baseline), aborting the next write with a Bad cast
    /// or (for h3togeo_lon_lat_result_order) silently swapping the key tuple elements.
    /// Pin exactly these type-affecting settings to the baseline so the key type is deterministic. This
    /// is not a general "make everything session-independent" guarantee: only the settings listed above
    /// are neutralized. New type-affecting settings must be added here explicitly.
    ///
    /// The baseline is the server's current global settings (default/system profile, including any
    /// `compatibility`), read from the global context. It is intentionally NOT the built-in literals.
    /// This exactly matches the context the reload path already resolves key types under (table load
    /// creates the storage with the global/default-profile context), so pinning to it keeps recompute
    /// consistent with a fresh reload while stripping only transient per-query/session overrides (the
    /// divergence this fixes). Pinning to the built-in literals instead would recompute a different type
    /// than the reload path for a table under a non-default `compatibility` (e.g.
    /// geo_distance_returns_float64_on_float64_arguments or function_date_trunc_return_type_behavior
    /// differ across versions), turning an upgrade into a Bad-cast read failure for existing parts.
    ///
    /// Note: resolved key/index types are not persisted per table; every recompute (CREATE/ALTER/reload)
    /// derives them from the AST under the then-current global profile, on master too. Truly pinning to
    /// the profile a table was CREATED under would require persisting a per-table baseline (or the
    /// resolved types) in metadata, a separate metadata-format change beyond this session-override fix.
    if (!context)
        return context;

    /// Values to pin to, taken from the server baseline (global context). Fall back to the built-in
    /// defaults only when there is no global context (an unusual context that does not persist metadata).
    bool ext_dt = false;
    bool keep_null = false;
    bool geo = false;
    bool json_null = false;
    bool least_greatest_legacy = false;
    bool h3togeo_lon_lat = false;
    UInt64 date_trunc = 0;
    if (context->hasGlobalContext())
    {
        const auto & baseline = context->getGlobalContext()->getSettingsRef();
        ext_dt = baseline[Setting::enable_extended_results_for_datetime_functions];
        keep_null = baseline[Setting::cast_keep_nullable];
        geo = baseline[Setting::geo_distance_returns_float64_on_float64_arguments];
        json_null = baseline[Setting::function_json_value_return_type_allow_nullable];
        least_greatest_legacy = baseline[Setting::least_greatest_legacy_null_behavior];
        h3togeo_lon_lat = baseline[Setting::h3togeo_lon_lat_result_order];
        date_trunc = baseline[Setting::function_date_trunc_return_type_behavior];
    }
    else
    {
        const Settings default_settings;
        ext_dt = default_settings[Setting::enable_extended_results_for_datetime_functions];
        keep_null = default_settings[Setting::cast_keep_nullable];
        geo = default_settings[Setting::geo_distance_returns_float64_on_float64_arguments];
        json_null = default_settings[Setting::function_json_value_return_type_allow_nullable];
        least_greatest_legacy = default_settings[Setting::least_greatest_legacy_null_behavior];
        h3togeo_lon_lat = default_settings[Setting::h3togeo_lon_lat_result_order];
        date_trunc = default_settings[Setting::function_date_trunc_return_type_behavior];
    }

    const auto & settings = context->getSettingsRef();
    if (static_cast<bool>(settings[Setting::enable_extended_results_for_datetime_functions]) == ext_dt
        && static_cast<bool>(settings[Setting::cast_keep_nullable]) == keep_null
        && static_cast<bool>(settings[Setting::geo_distance_returns_float64_on_float64_arguments]) == geo
        && static_cast<bool>(settings[Setting::function_json_value_return_type_allow_nullable]) == json_null
        && static_cast<bool>(settings[Setting::least_greatest_legacy_null_behavior]) == least_greatest_legacy
        && static_cast<bool>(settings[Setting::h3togeo_lon_lat_result_order]) == h3togeo_lon_lat
        && static_cast<UInt64>(settings[Setting::function_date_trunc_return_type_behavior]) == date_trunc)
        return context;

    auto key_context = Context::createCopy(context);
    key_context->setSetting("enable_extended_results_for_datetime_functions", Field(ext_dt));
    key_context->setSetting("cast_keep_nullable", Field(keep_null));
    key_context->setSetting("geo_distance_returns_float64_on_float64_arguments", Field(geo));
    key_context->setSetting("function_json_value_return_type_allow_nullable", Field(json_null));
    key_context->setSetting("least_greatest_legacy_null_behavior", Field(least_greatest_legacy));
    key_context->setSetting("h3togeo_lon_lat_result_order", Field(h3togeo_lon_lat));
    key_context->setSetting("function_date_trunc_return_type_behavior", Field(date_trunc));
    return key_context;
}

KeyDescription::KeyDescription(const KeyDescription & other)
    : definition_ast(other.definition_ast ? other.definition_ast->clone() : nullptr)
    , expression_list_ast(other.expression_list_ast ? other.expression_list_ast->clone() : nullptr)
    , sample_block(other.sample_block)
    , column_names(other.column_names)
    , reverse_flags(other.reverse_flags)
    , data_types(other.data_types)
    , additional_columns(other.additional_columns)
    , sort_order_id(other.sort_order_id)
    , canonicalize_key_types(other.canonicalize_key_types)
{
    if (other.expression)
        expression = other.expression->clone();
}

KeyDescription & KeyDescription::operator=(const KeyDescription & other)
{
    if (&other == this)
        return *this;

    if (other.definition_ast)
        definition_ast = other.definition_ast->clone();
    else
        definition_ast.reset();

    if (other.expression_list_ast)
        expression_list_ast = other.expression_list_ast->clone();
    else
        expression_list_ast.reset();

    if (other.expression)
        expression = other.expression->clone();
    else
        expression.reset();

    sample_block = other.sample_block;
    column_names = other.column_names;
    reverse_flags = other.reverse_flags;
    data_types = other.data_types;

    if (!additional_columns.empty() && other.additional_columns.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong key assignment, losing additional_columns");

    additional_columns = other.additional_columns;
    sort_order_id = other.sort_order_id;
    canonicalize_key_types = other.canonicalize_key_types;
    return *this;
}

void KeyDescription::recalculateWithNewAST(
    const ASTPtr & new_ast,
    const ColumnsDescription & columns,
    const VirtualColumnsDescription & virtuals,
    const ContextPtr & context)
{
    *this = getKeyFromAST(new_ast, columns, virtuals, context, additional_columns, canonicalize_key_types);
}

void KeyDescription::recalculateWithNewColumns(
    const ColumnsDescription & new_columns,
    const VirtualColumnsDescription & virtuals,
    const ContextPtr & context)
{
    *this = getKeyFromAST(definition_ast, new_columns, virtuals, context, additional_columns, canonicalize_key_types);
}

bool KeyDescription::moduloToModuloLegacyRecursive(ASTPtr node_expr)
{
    if (!node_expr)
        return false;

    auto * function_expr = node_expr->as<ASTFunction>();
    bool modulo_in_ast = false;
    if (function_expr)
    {
        if (function_expr->name == "modulo")
        {
            function_expr->name = "moduloLegacy";
            modulo_in_ast = true;
        }
        if (function_expr->arguments)
        {
            auto children = function_expr->arguments->children;
            for (const auto & child : children)
                modulo_in_ast |= moduloToModuloLegacyRecursive(child);
        }
    }

    return modulo_in_ast;
}

/// Build expression_list_ast, column_names, and reverse_flags from key children and additional columns.
static std::tuple<ASTPtr, Names, std::vector<bool>> buildKeyColumns(
    const ASTPtr & key_expression_list,
    const NamesAndTypesList & additional_columns)
{
    auto expression_list_ast = make_intrusive<ASTExpressionList>();
    Names column_names;
    std::vector<bool> reverse_flags;

    for (const auto & child : key_expression_list->children)
    {
        auto real_key = child;
        if (auto * elem = child->as<ASTStorageOrderByElement>())
        {
            real_key = elem->children.front();
            reverse_flags.emplace_back(elem->direction < 0);
        }

        expression_list_ast->children.push_back(real_key);
        column_names.emplace_back(real_key->getColumnName());
    }

    for (const auto & col : additional_columns)
    {
        if (std::ranges::contains(column_names, col.name))
            continue;

        ASTPtr column_identifier = make_intrusive<ASTIdentifier>(col.name);
        column_names.emplace_back(column_identifier->getColumnName());
        expression_list_ast->children.push_back(column_identifier);

        if (!reverse_flags.empty())
            reverse_flags.emplace_back(false);
    }

    return {expression_list_ast, std::move(column_names), std::move(reverse_flags)};
}

KeyDescription KeyDescription::getKeyFromAST(
    const ASTPtr & definition_ast,
    const ColumnsDescription & columns,
    const VirtualColumnsDescription & virtuals,
    const ContextPtr & context,
    const NamesAndTypesList & additional_columns,
    bool canonicalize_key_types)
{
    KeyDescription result;
    result.definition_ast = definition_ast;
    result.additional_columns = additional_columns;
    result.canonicalize_key_types = canonicalize_key_types;
    auto key_expression_list = extractKeyExpressionList(definition_ast);
    checkExpressionDoesntContainSubqueries(*key_expression_list);

    std::tie(result.expression_list_ast, result.column_names, result.reverse_flags) = buildKeyColumns(key_expression_list, additional_columns);
    if (!result.reverse_flags.empty() && result.reverse_flags.size() != result.expression_list_ast->children.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "The size of reverse_flags ({}) does not match the size of KeyDescription {}",
            result.reverse_flags.size(), result.expression_list_ast->children.size());

    {
        auto key_context = canonicalize_key_types ? createKeyExpressionContext(context) : context;
        auto expr = result.expression_list_ast->clone();
        auto all_columns = VirtualColumnUtils::getColumnsWithVirtualsForAnalysis(columns, virtuals);
        auto syntax_result = TreeRewriter(key_context).analyze(expr, all_columns);
        /// In expression we also need to store source columns
        result.expression = ExpressionAnalyzer(expr, syntax_result, key_context).getActions(false);
        /// In sample block we use just key columns
        result.sample_block = ExpressionAnalyzer(expr, syntax_result, key_context).getActions(true)->getSampleBlock();
    }

    for (size_t i = 0; i < result.sample_block.columns(); ++i)
    {
        result.data_types.emplace_back(result.sample_block.getByPosition(i).type);
        if (!result.data_types.back()->isComparable())
            throw Exception(ErrorCodes::DATA_TYPE_CANNOT_BE_USED_IN_KEY,
                            "Column {} with type {} is not allowed in key expression, it's not comparable",
                            backQuote(result.sample_block.getByPosition(i).name), result.data_types.back()->getName());

        auto check = [&](const IDataType & type)
        {
            if (isDynamic(type) || isVariant(type) || isObject(type))
                throw Exception(
                    ErrorCodes::DATA_TYPE_CANNOT_BE_USED_IN_KEY,
                    "Column with type Variant/Dynamic/JSON is not allowed in key expression. Consider using a subcolumn with a specific data "
                    "type instead (for example 'column.Int64' or 'json.some.path.:Int64' if its a JSON path subcolumn) or casting this column to a specific data type");
        };

        check(*result.data_types.back());
        result.data_types.back()->forEachChild(check);
    }

    return result;
}

ASTPtr KeyDescription::getOriginalExpressionList() const
{
    if (!expression_list_ast || reverse_flags.empty())
        return expression_list_ast;

    auto expr_list = make_intrusive<ASTExpressionList>();
    size_t size = expression_list_ast->children.size();
    for (size_t i = 0; i < size; ++i)
    {
        auto column_ast = make_intrusive<ASTStorageOrderByElement>();
        column_ast->children.push_back(expression_list_ast->children[i]);
        column_ast->direction = (!reverse_flags.empty() && reverse_flags[i]) ? -1 : 1;
        expr_list->children.push_back(std::move(column_ast));
    }

    return expr_list;
}

KeyDescription KeyDescription::getPrimaryKeyFromAST(
    const ASTPtr & definition_ast,
    const KeyDescription & sorting_key,
    const ColumnsDescription & columns,
    const VirtualColumnsDescription & virtuals,
    const ContextPtr & context)
{
    KeyDescription result = getKeyFromAST(definition_ast, columns, virtuals, context);

    /// The primary key is a prefix of the sorting key (validated in MergeTreeData::checkProperties),
    /// so its per-column directions are the corresponding prefix of the sorting key's.
    if (!sorting_key.reverse_flags.empty())
        result.reverse_flags.assign(
            sorting_key.reverse_flags.begin(),
            sorting_key.reverse_flags.begin() + std::min(result.column_names.size(), sorting_key.reverse_flags.size()));

    return result;
}

KeyDescription KeyDescription::buildEmptyKey()
{
    KeyDescription result;
    result.expression_list_ast = make_intrusive<ASTExpressionList>();
    result.expression = std::make_shared<ExpressionActions>(ActionsDAG(), ExpressionActionsSettings{});
    return result;
}

KeyDescription KeyDescription::parse(
    const String & str,
    const ColumnsDescription & columns,
    const VirtualColumnsDescription & virtuals,
    const ContextPtr & context,
    bool allow_order)
{
    KeyDescription result;
    if (str.empty())
        return result;

    ParserStorageOrderByClause parser(allow_order);
    ASTPtr ast = parseQuery(parser, "(" + str + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    FunctionNameNormalizer::visit(ast.get());

    /// The artificial "(" + str + ")" wrapping above causes the parser to mark
    /// the resulting expression as parenthesized when there is exactly one element.
    /// Strip that flag so the formatter does not produce spurious parentheses
    /// (e.g. `x` round-tripping as `(x)` in metadata comparisons).
    if (ast)
        ast->setParenthesized(false);

    return getKeyFromAST(ast, columns, virtuals, context);
}

}
