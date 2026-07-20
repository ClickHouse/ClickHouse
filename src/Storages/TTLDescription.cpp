#include <Storages/TTLDescription.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnVariant.h>
#include <Compression/CompressionFactory.h>
#include <Core/Settings.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTTTLElement.h>
#include <Storages/extractKeyExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTAssignment.h>
#include <Storages/ColumnsDescription.h>
#include <Interpreters/Context.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeVariant.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Common/CurrentThread.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_codecs;
    extern const SettingsBool allow_suspicious_codecs;
    extern const SettingsBool allow_suspicious_ttl_expressions;
    extern const SettingsBool variant_throw_on_type_mismatch;
    extern const SettingsBool dynamic_throw_on_type_mismatch;
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int BAD_TTL_EXPRESSION;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


TTLAggregateDescription::TTLAggregateDescription(const TTLAggregateDescription & other)
    : column_name(other.column_name)
    , expression_result_column_name(other.expression_result_column_name)
{
    if (other.expression)
        expression = other.expression->clone();
}

TTLAggregateDescription & TTLAggregateDescription::operator=(const TTLAggregateDescription & other)
{
    if (&other == this)
        return *this;

    column_name = other.column_name;
    expression_result_column_name = other.expression_result_column_name;
    if (other.expression)
        expression = other.expression->clone();
    else
        expression.reset();
    return *this;
}

namespace
{

/// Reject TTL expressions that feed an AggregateFunction state into a function which cannot consume it
/// (e.g. `toDateTime(state)`), while still accepting state-aware functions like `finalizeAggregation`.
///
/// We only execute the individual functions that directly receive an argument whose type contains an
/// AggregateFunction state (including states nested inside Tuple/Array/Map/etc.). Executing the whole
/// expression instead would make DDL validity depend on synthetic default values: a data-dependent
/// error from an unrelated downstream function - e.g. division by zero in `intDiv(100, finalizeAggregation(state))`
/// when the default state finalizes to 0 - would turn a perfectly valid TTL into a CREATE TABLE failure.
/// Walking nodes individually also makes the check independent of short-circuit evaluation, so an
/// unsupported consumer hidden in a not-taken `if`/`multiIf` branch is still validated.
///
/// Higher-order functions (e.g. `arrayMap`) keep their lambda body in a separate inner DAG owned by a
/// `FunctionCapture`. Executing the outer node on a synthetic empty array would reduce the lambda over
/// zero rows and never reach the body, so we recurse into the lambda DAG instead. Only the type error
/// is translated into a clear message; all other exceptions are rethrown.
///
/// A synthetic default value catches a top-level AggregateFunction argument, but not one that is only an
/// alternative of a `Variant` column: the default `Variant` row is NULL, so the `Variant` function
/// adaptor short-circuits (returns NULL) and never runs the consumer on the AggregateFunction
/// alternative. To exercise it we additionally probe with a single-row `Variant` column whose only value
/// is that alternative (e.g. `toDateTime(v)` with `v Variant(AggregateFunction(max, DateTime64(3)), String)`).
///
/// `Dynamic` erases its value types entirely: the static type never mentions AggregateFunction, yet any
/// row may carry a state (e.g. inserted via CAST to `Dynamic`), and a consumer like `toDateTime` would
/// only fail later, during TTL execution. Since the stored types cannot be enumerated at DDL time, we
/// probe every `Dynamic` argument with a single-row column carrying a representative synthetic state
/// (`AggregateFunction(max, UInt64)`): type-agnostic consumers (`toString`, `dynamicType`, ...) pass,
/// while consumers with type requirements are rejected as suspicious. Such a TTL is one inserted row
/// away from breaking every merge of the table, so rejecting it at CREATE is the safer default; the
/// `allow_suspicious_ttl_expressions` setting and ATTACH remain available as escape hatches.
void checkActionsDAGForAggregateFunctions(const ActionsDAG & actions_dag, std::string_view expression_kind)
{
    for (const auto & node : actions_dag.getNodes())
    {
        if (node.type != ActionsDAG::ActionType::FUNCTION)
            continue;

        /// Descend into lambda bodies of higher-order functions to validate consumers hidden inside them.
        if (const auto * function_capture = dynamic_cast<const FunctionCapture *>(node.function_base.get()))
        {
            checkActionsDAGForAggregateFunctions(function_capture->getAcionsDAG(), expression_kind);
            continue;
        }

        bool consumes_aggregate_state = false;
        bool has_dynamic_argument = false;
        bool has_lambda_argument = false;
        ColumnsWithTypeAndName arguments;
        arguments.reserve(node.children.size());
        for (const auto * child : node.children)
        {
            if (hasAggregateFunctionType(child->result_type))
                consumes_aggregate_state = true;

            if (WhichDataType(child->result_type).isDynamic())
                has_dynamic_argument = true;

            /// A lambda argument cannot be materialized into a column; the higher-order function that
            /// receives it is validated through the captured lambda DAG above, so skip executing it here.
            if (WhichDataType(child->result_type).isFunction())
            {
                has_lambda_argument = true;
                break;
            }

            /// Preserve constant arguments as constants - some functions (e.g. `CAST`) require a
            /// constant argument and otherwise throw an unrelated error during this synthetic execution.
            ColumnPtr column = child->column
                ? child->column->cloneResized(1)
                : child->result_type->createColumnConstWithDefaultValue(1)->convertToFullColumnIfConst();
            arguments.emplace_back(std::move(column), child->result_type, child->result_name);
        }

        if ((!consumes_aggregate_state && !has_dynamic_argument) || has_lambda_argument)
            continue;

        /// Translate the "cannot consume an AggregateFunction state" type error into a clear TTL message;
        /// rethrow anything else (e.g. a data-dependent error raised by a perfectly valid consumer).
        auto probe = [&](const ColumnsWithTypeAndName & probe_arguments, std::string_view hint)
        {
            try
            {
                node.function_base->execute(probe_arguments, node.result_type, /*input_rows_count=*/ 1, /*dry_run=*/ true);
            }
            catch (Exception & e)
            {
                if (e.code() == ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT)
                    throw Exception(ErrorCodes::BAD_TTL_EXPRESSION,
                        "TTL {}expression uses {}: {}", expression_kind, hint, e.message());
                throw;
            }
        };

        constexpr std::string_view aggregate_state_hint =
            "AggregateFunction column in a function that cannot handle it. "
            "Use `finalizeAggregation` to extract the value first";

        if (consumes_aggregate_state)
        {
            /// Default values cover a top-level AggregateFunction argument.
            probe(arguments, aggregate_state_hint);

            /// Additionally exercise every AggregateFunction alternative hidden inside a `Variant` argument,
            /// which the all-NULL default column above would otherwise skip (see the note above the function).
            for (size_t i = 0; i < arguments.size(); ++i)
            {
                if (!WhichDataType(arguments[i].type).isVariant())
                    continue;

                const auto & variant_type = assert_cast<const DataTypeVariant &>(*arguments[i].type);
                const auto & variant_types = variant_type.getVariants();
                for (size_t discr = 0; discr < variant_types.size(); ++discr)
                {
                    if (!hasAggregateFunctionType(variant_types[discr]))
                        continue;

                    auto variant_column = variant_type.createColumn();
                    ColumnPtr alternative = variant_types[discr]->createColumnConstWithDefaultValue(1)->convertToFullColumnIfConst();
                    assert_cast<ColumnVariant &>(*variant_column).insertIntoVariantFrom(
                        static_cast<ColumnVariant::Discriminator>(discr), *alternative, 0);

                    ColumnsWithTypeAndName probe_arguments = arguments;
                    probe_arguments[i].column = std::move(variant_column);
                    probe(probe_arguments, aggregate_state_hint);
                }
            }
        }

        /// Exercise every `Dynamic` argument with a synthetic AggregateFunction state, since the static
        /// type gives no way to rule one out (see the note above the function).
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            if (!WhichDataType(arguments[i].type).isDynamic())
                continue;

            auto aggregate_state_type = DataTypeFactory::instance().get("AggregateFunction(max, UInt64)");
            ColumnPtr aggregate_state = aggregate_state_type->createColumnConstWithDefaultValue(1)->convertToFullColumnIfConst();

            auto dynamic_column = arguments[i].type->createColumn();
            auto & dynamic = assert_cast<ColumnDynamic &>(*dynamic_column);
            if (dynamic.addNewVariant(aggregate_state_type))
            {
                auto discr = dynamic.getVariantInfo().variant_name_to_discriminator.at(aggregate_state_type->getName());
                dynamic.getVariantColumn().insertIntoVariantFrom(discr, *aggregate_state, 0);
            }
            else
            {
                /// The type cannot hold new variants (e.g. `Dynamic(max_types=0)`), so states would be
                /// stored in the shared variant - probe through it as well.
                dynamic.insertValueIntoSharedVariant(*aggregate_state, aggregate_state_type, aggregate_state_type->getName(), 0);
            }

            ColumnsWithTypeAndName probe_arguments = arguments;
            probe_arguments[i].column = std::move(dynamic_column);
            probe(probe_arguments,
                "a Dynamic column in a function that cannot handle all types a Dynamic column can store "
                "(e.g. an AggregateFunction state), so TTL execution could fail depending on the inserted values. "
                "Use a typed subcolumn instead, or set `allow_suspicious_ttl_expressions` to allow it");
        }
    }
}

/// RAII guard setting `variant_throw_on_type_mismatch` / `dynamic_throw_on_type_mismatch` on the query
/// context of the *current thread* - the only place the `Variant`/`Dynamic` function adaptors read them
/// from - and restoring the previous values on scope exit. Note the DDL `context` cannot be used for this:
/// on a server it has no query context, and the adaptors would not see settings changed on it.
class MismatchSettingsGuard
{
public:
    MismatchSettingsGuard(bool variant_throw, bool dynamic_throw)
    {
        if (CurrentThread::isInitialized())
        {
            if (auto thread_query_context = CurrentThread::tryGetQueryContext())
                thread_context = std::const_pointer_cast<Context>(thread_query_context);
        }

        if (!thread_context)
            return;

        const auto & settings = thread_context->getSettingsRef();
        if (settings[Setting::variant_throw_on_type_mismatch] != variant_throw)
        {
            old_variant_throw = settings[Setting::variant_throw_on_type_mismatch];
            thread_context->setSetting("variant_throw_on_type_mismatch", Field(variant_throw));
        }
        if (settings[Setting::dynamic_throw_on_type_mismatch] != dynamic_throw)
        {
            old_dynamic_throw = settings[Setting::dynamic_throw_on_type_mismatch];
            thread_context->setSetting("dynamic_throw_on_type_mismatch", Field(dynamic_throw));
        }
    }

    ~MismatchSettingsGuard()
    {
        if (!thread_context)
            return;

        if (old_variant_throw)
            thread_context->setSetting("variant_throw_on_type_mismatch", Field(*old_variant_throw));
        if (old_dynamic_throw)
            thread_context->setSetting("dynamic_throw_on_type_mismatch", Field(*old_dynamic_throw));
    }

private:
    ContextMutablePtr thread_context;
    std::optional<bool> old_variant_throw;
    std::optional<bool> old_dynamic_throw;
};

void checkTTLExpressionForAggregateFunctions(
    const ExpressionActionsPtr & expression, std::string_view expression_kind, const ContextPtr & context)
{
    /// The synthetic probe in `checkActionsDAGForAggregateFunctions` exercises consumers over `Variant`/`Dynamic`
    /// columns carrying an AggregateFunction state. For consumers wrapped in the `Variant`/`Dynamic` function
    /// adaptors, whether a type mismatch throws or is silently turned into NULL at *execution* is decided by
    /// `variant_throw_on_type_mismatch` / `dynamic_throw_on_type_mismatch`, which the adaptors read from the
    /// query context of the current thread - i.e. from the DDL session here, but from the background context
    /// (whose settings come from the `background_profile` server config, strict by default) during background
    /// TTL merges. Align the probe with that merge runtime: a session that lowered these settings must not
    /// sneak in a TTL that would throw in every background merge, and a server whose background profile
    /// deliberately runs lenient must not get such a `CREATE` rejected.
    /// (Conversion functions such as `toDateTime` handle `Variant`/`Dynamic` natively, ignore both settings
    /// and always throw on a stored type they cannot convert, so for them the probe's verdict is the same
    /// under any settings.)
    const auto & background_settings = context->getGlobalContext()->getBackgroundContext()->getSettingsRef();
    MismatchSettingsGuard probe_guard(
        background_settings[Setting::variant_throw_on_type_mismatch],
        background_settings[Setting::dynamic_throw_on_type_mismatch]);

    checkActionsDAGForAggregateFunctions(expression->getActionsDAG(), expression_kind);
}

void checkTTLExpression(
    const ExpressionActionsPtr & ttl_expression, const String & result_column_name, bool allow_suspicious, const ContextPtr & context)
{
    /// Do not apply this check in ATTACH queries for compatibility reasons and if explicitly allowed.
    if (!allow_suspicious)
    {
        if (ttl_expression->getRequiredColumns().empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "TTL expression {} does not depend on any of the columns of the table", result_column_name);

        for (const auto & action : ttl_expression->getActions())
        {
            if (action.node->type == ActionsDAG::ActionType::FUNCTION)
            {
                const IFunctionBase & func = *action.node->function_base;
                if (!func.isDeterministic())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "TTL expression cannot contain non-deterministic functions, but contains function {}",
                                    func.getName());
            }
        }

        checkTTLExpressionForAggregateFunctions(ttl_expression, /*expression_kind=*/ "", context);
    }

    const auto & result_column = ttl_expression->getSampleBlock().getByName(result_column_name);
    if (!typeid_cast<const DataTypeDateTime *>(result_column.type.get())
        && !typeid_cast<const DataTypeDate *>(result_column.type.get())
        && !typeid_cast<const DataTypeDateTime64 *>(result_column.type.get())
        && !typeid_cast<const DataTypeDate32 *>(result_column.type.get()))
    {
        throw Exception(ErrorCodes::BAD_TTL_EXPRESSION,
                        "TTL expression result column should have Date, Date32, DateTime or DateTime64 type, but has {}",
                        result_column.type->getName());
    }
}

class FindAggregateFunctionData
{
public:
    using TypeToVisit = ASTFunction;
    bool has_aggregate_function = false;

    void visit(const ASTFunction & func, ASTPtr &)
    {
        /// Do not throw if found aggregate function inside another aggregate function,
        /// because it will be checked, while creating expressions.
        if (AggregateUtils::isAggregateFunction(func))
            has_aggregate_function = true;
    }
};

using FindAggregateFunctionFinderMatcher = OneTypeMatcher<FindAggregateFunctionData>;
using FindAggregateFunctionVisitor = InDepthNodeVisitor<FindAggregateFunctionFinderMatcher, true>;

}

TTLDescription::TTLDescription(const TTLDescription & other)
    : mode(other.mode)
    , expression_ast(other.expression_ast ? other.expression_ast->clone() : nullptr)
    , expression_columns(other.expression_columns)
    , result_column(other.result_column)
    , where_expression_ast(other.where_expression_ast ? other.where_expression_ast->clone() : nullptr)
    , where_expression_columns(other.where_expression_columns)
    , where_result_column(other.where_result_column)
    , group_by_keys(other.group_by_keys)
    , set_parts(other.set_parts)
    , aggregate_descriptions(other.aggregate_descriptions)
    , destination_type(other.destination_type)
    , destination_name(other.destination_name)
    , if_exists(other.if_exists)
    , recompression_codec(other.recompression_codec)
{
}

TTLDescription & TTLDescription::operator=(const TTLDescription & other)
{
    if (&other == this)
        return *this;

    mode = other.mode;
    if (other.expression_ast)
        expression_ast = other.expression_ast->clone();
    else
        expression_ast.reset();

    expression_columns = other.expression_columns;
    result_column = other.result_column;

    if (other.where_expression_ast)
        where_expression_ast = other.where_expression_ast->clone();
    else
        where_expression_ast.reset();

    where_expression_columns = other.where_expression_columns;
    where_result_column = other.where_result_column;
    group_by_keys = other.group_by_keys;
    set_parts = other.set_parts;
    aggregate_descriptions = other.aggregate_descriptions;
    destination_type = other.destination_type;
    destination_name = other.destination_name;
    if_exists = other.if_exists;

    if (other.recompression_codec)
        recompression_codec = other.recompression_codec->clone();
    else
        recompression_codec.reset();

    return * this;
}

static ExpressionAndSets buildExpressionAndSets(ASTPtr & ast, const NamesAndTypesList & columns, const ContextPtr & context)
{
    ExpressionAndSets result;
    auto ttl_string = ast->formatWithSecretsOneLine();
    auto syntax_analyzer_result = TreeRewriter(context).analyze(ast, columns);
    ExpressionAnalyzer analyzer(ast, syntax_analyzer_result, context);
    auto dag = analyzer.getActionsDAG(false);

    const auto * col = &dag.findInOutputs(ast->getColumnName());
    if (col->result_name != ttl_string)
        col = &dag.addAlias(*col, ttl_string);

    dag.getOutputs() = {col};
    dag.removeUnusedActions();

    result.expression = std::make_shared<ExpressionActions>(std::move(dag), ExpressionActionsSettings(context));
    result.sets = analyzer.getPreparedSets();

    return result;
}

/// Collect the argument expressions of every aggregate function found in the AST.
static void collectAggregateFunctionArguments(const ASTPtr & ast, ASTs & arguments)
{
    if (const auto * function = ast->as<ASTFunction>(); function && AggregateUtils::isAggregateFunction(*function))
    {
        if (function->arguments)
            for (const auto & argument : function->arguments->children)
                arguments.push_back(argument);
    }

    for (const auto & child : ast->children)
        collectAggregateFunctionArguments(child, arguments);
}

/// Validate the aggregate-function arguments of a `GROUP BY ... SET` assignment. These argument
/// expressions (e.g. `toDateTime(ts)` in `SET out = max(toDateTime(ts))`) are evaluated later by
/// TTLAggregationAlgorithm and are not part of the main TTL expression, so an unsupported
/// AggregateFunction-state consumer there would otherwise pass CREATE TABLE and fail at merge time.
static void checkTTLGroupBySetForAggregateFunctions(
    const ASTPtr & assignment_expression, const NamesAndTypesList & columns, const ContextPtr & context)
{
    ASTs aggregate_arguments;
    collectAggregateFunctionArguments(assignment_expression, aggregate_arguments);

    for (const auto & argument : aggregate_arguments)
    {
        auto argument_ast = argument->clone();
        auto argument_expression = buildExpressionAndSets(argument_ast, columns, context).expression;
        checkTTLExpressionForAggregateFunctions(argument_expression, /*expression_kind=*/ "GROUP BY SET ", context);
    }
}

ExpressionAndSets TTLDescription::buildExpression(const ContextPtr & context) const
{
    auto ast = expression_ast->clone();
    return buildExpressionAndSets(ast, expression_columns, context);
}

ExpressionAndSets TTLDescription::buildWhereExpression(const ContextPtr & context) const
{
    if (where_expression_ast)
    {
        auto ast = where_expression_ast->clone();
        return buildExpressionAndSets(ast, where_expression_columns, context);
    }

    return {};
}

TTLDescription TTLDescription::getTTLFromAST(
    const ASTPtr & definition_ast,
    const ColumnsDescription & columns,
    ContextPtr context,
    const KeyDescription & primary_key,
    bool is_attach)
{
    TTLDescription result;
    const auto * ttl_element = definition_ast->as<ASTTTLElement>();

    /// First child is expression: `TTL expr TO DISK`
    if (ttl_element != nullptr)
        result.expression_ast = ttl_element->children.front()->clone();
    else /// It's columns TTL without any additions, just copy it
        result.expression_ast = definition_ast->clone();

    checkExpressionDoesntContainSubqueries(*result.expression_ast);

    /// Building a TTL expression can itself consult `variant_throw_on_type_mismatch`: the `Variant`
    /// function adaptor throws in its constructor when none of the alternatives is compatible with the
    /// consumer, and under a lenient setting resolves the result to constant NULL instead. Such a lenient
    /// build must not slip through DDL validation regardless of the session (or even the background
    /// profile) settings, because it produces a table that is broken no matter how TTL runs later: the
    /// constant fold prunes the referenced column from the stored TTL column list, so every subsequent
    /// rebuild of the TTL expression fails with "Missing columns", and the table cannot even be re-attached
    /// on server restart (loading has no query context, so the adaptor defaults to strict and throws).
    /// Hence the validation build always runs strict. The escape hatches stay intact: on ATTACH or with
    /// `allow_suspicious_ttl_expressions` the build behaves exactly as the session dictates.
    std::optional<MismatchSettingsGuard> build_guard;
    if (!is_attach && !context->getSettingsRef()[Setting::allow_suspicious_ttl_expressions])
        build_guard.emplace(/*variant_throw=*/ true, /*dynamic_throw=*/ true);

    auto ttl_ast = result.expression_ast->clone();
    auto expression = buildExpressionAndSets(ttl_ast, columns.getAllPhysical(), context).expression;
    result.expression_columns = expression->getRequiredColumnsWithTypes();

    result.result_column = expression->getSampleBlock().safeGetByPosition(0).name;

    ExpressionActionsPtr where_expression;

    if (ttl_element == nullptr) /// columns TTL
    {
        result.destination_type = DataDestinationType::DELETE;
        result.mode = TTLMode::DELETE;
    }
    else /// rows TTL
    {
        result.mode = ttl_element->mode;
        result.destination_type = ttl_element->destination_type;
        result.destination_name = ttl_element->destination_name;
        result.if_exists = ttl_element->if_exists;

        if (ttl_element->mode == TTLMode::DELETE)
        {
            if (ASTPtr where_expr_ast = ttl_element->where())
            {
                result.where_expression_ast = where_expr_ast->clone();

                ASTPtr ast = where_expr_ast->clone();
                where_expression = buildExpressionAndSets(ast, columns.getAllPhysical(), context).expression;
                result.where_expression_columns = where_expression->getRequiredColumnsWithTypes();
                result.where_result_column = where_expression->getSampleBlock().safeGetByPosition(0).name;
            }
        }
        else if (ttl_element->mode == TTLMode::GROUP_BY)
        {
            const auto & pk_columns = primary_key.column_names;

            if (ttl_element->group_by_key.size() > pk_columns.size())
                throw Exception(ErrorCodes::BAD_TTL_EXPRESSION, "TTL Expression GROUP BY key should be a prefix of primary key");

            NameSet aggregation_columns_set;

            for (size_t i = 0; i < ttl_element->group_by_key.size(); ++i)
            {
                if (ttl_element->group_by_key[i]->getColumnName() != pk_columns[i])
                    throw Exception(ErrorCodes::BAD_TTL_EXPRESSION, "TTL Expression GROUP BY key should be a prefix of primary key {} {}", ttl_element->group_by_key[i]->getColumnName(), pk_columns[i]);
            }

            std::vector<std::pair<String, ASTPtr>> aggregations;
            for (const auto & ast : ttl_element->group_by_assignments)
            {
                const auto assignment = ast->as<const ASTAssignment &>();
                auto ass_expression = assignment.expression();

                FindAggregateFunctionVisitor::Data data{false};
                FindAggregateFunctionVisitor(data).visit(ass_expression);

                if (!data.has_aggregate_function)
                    throw Exception(ErrorCodes::BAD_TTL_EXPRESSION,
                    "Invalid expression for assignment of column {}. Should contain an aggregate function", assignment.column_name);

                if (!is_attach && !context->getSettingsRef()[Setting::allow_suspicious_ttl_expressions])
                    checkTTLGroupBySetForAggregateFunctions(ass_expression, columns.getAllPhysical(), context);

                ass_expression = addTypeConversionToAST(std::move(ass_expression), columns.getPhysical(assignment.column_name).type->getName());
                aggregations.emplace_back(assignment.column_name, std::move(ass_expression));
                aggregation_columns_set.insert(assignment.column_name);
            }

            if (aggregation_columns_set.size() != ttl_element->group_by_assignments.size())
                throw Exception(ErrorCodes::BAD_TTL_EXPRESSION, "Multiple aggregations set for one column in TTL Expression");

            result.group_by_keys = Names(pk_columns.begin(), pk_columns.begin() + ttl_element->group_by_key.size());

            for (auto [name, value] : aggregations)
            {
                auto syntax_result = TreeRewriter(context).analyze(value, columns.getAllPhysical(), {}, {}, true);
                auto expr_analyzer = ExpressionAnalyzer(value, syntax_result, context);

                TTLAggregateDescription set_part;
                set_part.column_name = name;
                set_part.expression_result_column_name = value->getColumnName();
                set_part.expression = expr_analyzer.getActions(false);

                /// The post-aggregation expression (including the implicit cast to the target column type)
                /// is executed later by TTLAggregationAlgorithm. When an aggregate returns an AggregateFunction
                /// state itself (e.g. `any(ts)`), casting it to an incompatible target type (e.g. `DateTime`)
                /// must be rejected here instead of failing during the TTL merge.
                if (!is_attach && !context->getSettingsRef()[Setting::allow_suspicious_ttl_expressions])
                    checkTTLExpressionForAggregateFunctions(set_part.expression, /*expression_kind=*/ "GROUP BY SET ", context);

                result.set_parts.emplace_back(set_part);

                for (const auto & descr : expr_analyzer.getAnalyzedData().aggregate_descriptions)
                    result.aggregate_descriptions.push_back(descr);
            }
        }
        else if (ttl_element->mode == TTLMode::RECOMPRESS)
        {
            /// On `ATTACH` (loading stored metadata) the codec checks are relaxed the same way column codecs are:
            /// a table created on an earlier version must still load even if its recompression codec would now be
            /// rejected at `CREATE`, otherwise the server could fail to start after an upgrade. `is_attach` here is
            /// also set for a create with `allow_suspicious_ttl_expressions`, matching `checkTTLExpression` below.
            result.recompression_codec =
                CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(
                    ttl_element->recompression_codec, {},
                    !is_attach && !context->getSettingsRef()[Setting::allow_suspicious_codecs],
                    is_attach || context->getSettingsRef()[Setting::allow_experimental_codecs]);
        }
    }

    checkTTLExpression(expression, result.result_column, is_attach || context->getSettingsRef()[Setting::allow_suspicious_ttl_expressions], context);

    if (where_expression && !is_attach && !context->getSettingsRef()[Setting::allow_suspicious_ttl_expressions])
        checkTTLExpressionForAggregateFunctions(where_expression, /*expression_kind=*/ "WHERE ", context);

    return result;
}


TTLTableDescription::TTLTableDescription(const TTLTableDescription & other)
 : definition_ast(other.definition_ast ? other.definition_ast->clone() : nullptr)
 , rows_ttl(other.rows_ttl)
 , rows_where_ttl(other.rows_where_ttl)
 , move_ttl(other.move_ttl)
 , recompression_ttl(other.recompression_ttl)
 , group_by_ttl(other.group_by_ttl)
{
}

TTLTableDescription & TTLTableDescription::operator=(const TTLTableDescription & other)
{
    if (&other == this)
        return *this;

    if (other.definition_ast)
        definition_ast = other.definition_ast->clone();
    else
        definition_ast.reset();

    rows_ttl = other.rows_ttl;
    rows_where_ttl = other.rows_where_ttl;
    move_ttl = other.move_ttl;
    recompression_ttl = other.recompression_ttl;
    group_by_ttl = other.group_by_ttl;

    return *this;
}

TTLTableDescription TTLTableDescription::getTTLForTableFromAST(
    const ASTPtr & definition_ast,
    const ColumnsDescription & columns,
    ContextPtr context,
    const KeyDescription & primary_key,
    bool is_attach)
{
    TTLTableDescription result;
    if (!definition_ast)
        return result;

    result.definition_ast = definition_ast->clone();

    bool have_unconditional_delete_ttl = false;
    for (const auto & ttl_element_ptr : definition_ast->children)
    {
        auto ttl = TTLDescription::getTTLFromAST(ttl_element_ptr, columns, context, primary_key, is_attach);
        if (ttl.mode == TTLMode::DELETE)
        {
            if (!ttl.where_expression_ast)
            {
                if (have_unconditional_delete_ttl)
                    throw Exception(ErrorCodes::BAD_TTL_EXPRESSION, "More than one DELETE TTL expression without WHERE expression is not allowed");

                have_unconditional_delete_ttl = true;
                result.rows_ttl = ttl;
            }
            else
            {
                result.rows_where_ttl.emplace_back(std::move(ttl));
            }
        }
        else if (ttl.mode == TTLMode::RECOMPRESS)
        {
            result.recompression_ttl.emplace_back(std::move(ttl));
        }
        else if (ttl.mode == TTLMode::GROUP_BY)
        {
            result.group_by_ttl.emplace_back(std::move(ttl));
        }
        else
        {
            result.move_ttl.emplace_back(std::move(ttl));
        }
    }
    return result;
}

TTLTableDescription TTLTableDescription::parse(
    const String & str, const ColumnsDescription & columns, ContextPtr context, const KeyDescription & primary_key, bool is_attach)
{
    TTLTableDescription result;
    if (str.empty())
        return result;

    ParserTTLExpressionList parser;
    ASTPtr ast = parseQuery(parser, str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    FunctionNameNormalizer::visit(ast.get());

    return getTTLForTableFromAST(ast, columns, context, primary_key, is_attach);
}

}
