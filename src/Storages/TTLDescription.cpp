#include <Storages/TTLDescription.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Compression/CompressionFactory.h>
#include <Core/Settings.h>
#include <Functions/IFunction.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTTTLElement.h>
#include <Storages/extractKeyExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTAssignment.h>
#include <Storages/ColumnsDescription.h>
#include <Interpreters/Context.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Common/logger_useful.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Poco/String.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_codecs;
    extern const SettingsBool allow_suspicious_codecs;
    extern const SettingsBool allow_suspicious_ttl_expressions;
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int BAD_TTL_EXPRESSION;
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

void checkTTLExpression(const ExpressionActionsPtr & ttl_expression, const String & result_column_name, bool allow_suspicious)
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

/// Replaces bare column references for Date/DateTime columns with CAST(col, 'WidenedType')
/// so that the TTL expression performs arithmetic in a wider type (Date32 / DateTime64)
/// and cannot silently 16/32-bit wrap on overflow.
///
/// Lambda parameter identifiers are not table columns and must not be wrapped, even when
/// they happen to share a name with a table column (e.g. `arrayExists(day -> ..., arr)`
/// where the table also has a `day` column). The matcher tracks names bound by enclosing
/// lambdas in `private_aliases` and skips identifiers that resolve to those bindings.
struct InjectWidenCastData
{
    const std::unordered_map<String, String> & cast_targets;
    std::unordered_set<String> private_aliases;
};

class InjectWidenCastMatcher
{
public:
    using Data = InjectWidenCastData;
    using Visitor = InDepthNodeVisitor<InjectWidenCastMatcher, /*top_to_bottom=*/false>;

    /// Block auto-descent into a lambda's children — its body is visited manually below
    /// after masking the parameter names. Visiting the parameter tuple itself would also
    /// be wrong (those identifiers are declarations, not uses).
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & /*child*/)
    {
        if (const auto * f = node->as<ASTFunction>())
            return Poco::toLower(f->name) != "lambda";
        return true;
    }

    static void visit(ASTPtr & ast, Data & data)
    {
        if (auto * func = ast->as<ASTFunction>(); func && Poco::toLower(func->name) == "lambda")
        {
            visitLambda(*func, data);
            return;
        }
        if (auto * ident = ast->as<ASTIdentifier>())
            visitIdentifier(ident, ast, data);
    }

private:
    static void visitLambda(ASTFunction & node, Data & data)
    {
        /// Recurse into the body with each lambda parameter name masked, then restore.
        std::vector<String> just_added;
        for (const auto & name : RequiredSourceColumnsMatcher::extractNamesFromLambda(node))
            if (data.private_aliases.insert(name).second)
                just_added.push_back(name);

        Visitor(data).visit(node.arguments->children[1]);

        for (const auto & name : just_added)
            data.private_aliases.erase(name);
    }

    static void visitIdentifier(const ASTIdentifier * ident, ASTPtr & node, Data & data)
    {
        const auto & name = ident->name();
        const auto & short_name = ident->shortName();
        if (data.private_aliases.contains(name) || data.private_aliases.contains(short_name))
            return;

        auto it = data.cast_targets.find(name);
        if (it == data.cast_targets.end())
            it = data.cast_targets.find(short_name);
        if (it == data.cast_targets.end())
            return;

        ASTs args;
        args.push_back(node);
        args.push_back(new ASTLiteral(it->second));
        node = makeASTFunction("CAST", std::move(args));
    }
};

/// Bottom-up traversal prevents the framework from re-descending into the CAST node we
/// just injected (its first child is the original identifier — visiting it again would
/// wrap the literal in another CAST).
using InjectWidenCastVisitor = InjectWidenCastMatcher::Visitor;

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

static ExpressionAndSets buildExpressionAndSets(
    ASTPtr & ast, const NamesAndTypesList & columns, const ContextPtr & context)
{
    /// Capture the formatted TTL string from the *un-mutated* AST so the alias and
    /// downstream `result_column` (visible in `system.parts.*_ttl_info.expression`,
    /// `SHOW CREATE TABLE`, etc.) keep the user's original form rather than the
    /// internally widened one.
    auto ttl_string = ast->formatWithSecretsOneLine();

    /// Replace each `Date`/`DateTime` source-column reference with a CAST to the wider
    /// counterpart (`Date32` / `DateTime64(0, tz)`) so arithmetic in the TTL expression
    /// runs in a 64-bit domain and cannot silently 16/32-bit wrap on overflow. The result
    /// type widens accordingly; downstream code (`extractTimestamp`, `updateTTLInfo`,
    /// `checkTTLExpression`) already accepts the wider temporal types.
    std::unordered_map<String, String> cast_targets;
    for (const auto & col : columns)
    {
        /// Strip LowCardinality first, then Nullable: the reverse order misses
        /// `LowCardinality(Nullable(Date))` because `removeNullable` only touches
        /// top-level Nullable.
        const auto inner = removeLowCardinalityAndNullable(col.type);
        if (isDate(inner))
        {
            cast_targets[col.name] = "Date32";
        }
        else if (isDateTime(inner))
        {
            /// Preserve the original timezone so calendar-based arithmetic (addMonths,
            /// addYears) and DST boundaries produce identical results.
            const auto & dt = typeid_cast<const DataTypeDateTime &>(*inner);
            const String & tz = dt.getTimeZone().getTimeZone();
            cast_targets[col.name] = tz.empty() ? "DateTime64(0)" : "DateTime64(0, '" + tz + "')";
        }
    }
    if (!cast_targets.empty())
    {
        InjectWidenCastData inject_data{cast_targets, {}};
        InjectWidenCastVisitor(inject_data).visit(ast);
    }

    ExpressionAndSets result;
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
            NameSet used_primary_key_columns_set;

            for (size_t i = 0; i < ttl_element->group_by_key.size(); ++i)
            {
                if (ttl_element->group_by_key[i]->getColumnName() != pk_columns[i])
                    throw Exception(ErrorCodes::BAD_TTL_EXPRESSION, "TTL Expression GROUP BY key should be a prefix of primary key {} {}", ttl_element->group_by_key[i]->getColumnName(), pk_columns[i]);

                used_primary_key_columns_set.insert(pk_columns[i]);
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

                ass_expression = addTypeConversionToAST(std::move(ass_expression), columns.getPhysical(assignment.column_name).type->getName());
                aggregations.emplace_back(assignment.column_name, std::move(ass_expression));
                aggregation_columns_set.insert(assignment.column_name);
            }

            if (aggregation_columns_set.size() != ttl_element->group_by_assignments.size())
                throw Exception(ErrorCodes::BAD_TTL_EXPRESSION, "Multiple aggregations set for one column in TTL Expression");

            result.group_by_keys = Names(pk_columns.begin(), pk_columns.begin() + ttl_element->group_by_key.size());

            const auto & primary_key_expressions = primary_key.expression_list_ast->children;

            /// Wrap with 'any' aggregate function primary key columns,
            /// which are not in 'GROUP BY' key and was not set explicitly.
            /// The separate step, because not all primary key columns are ordinary columns.
            for (size_t i = ttl_element->group_by_key.size(); i < primary_key_expressions.size(); ++i)
            {
                if (!aggregation_columns_set.contains(pk_columns[i]))
                {
                    ASTPtr expr = makeASTFunction("any", primary_key_expressions[i]->clone());
                    aggregations.emplace_back(pk_columns[i], std::move(expr));
                    aggregation_columns_set.insert(pk_columns[i]);
                }
            }

            /// Wrap with 'any' aggregate function other columns, which was not set explicitly.
            for (const auto & column : columns.getOrdinary())
            {
                if (!aggregation_columns_set.contains(column.name) && !used_primary_key_columns_set.contains(column.name))
                {
                    ASTPtr expr = makeASTFunction("any", make_intrusive<ASTIdentifier>(column.name));
                    aggregations.emplace_back(column.name, std::move(expr));
                }
            }

            for (auto [name, value] : aggregations)
            {
                auto syntax_result = TreeRewriter(context).analyze(value, columns.getAllPhysical(), {}, {}, true);
                auto expr_analyzer = ExpressionAnalyzer(value, syntax_result, context);

                TTLAggregateDescription set_part;
                set_part.column_name = name;
                set_part.expression_result_column_name = value->getColumnName();
                set_part.expression = expr_analyzer.getActions(false);

                result.set_parts.emplace_back(set_part);

                for (const auto & descr : expr_analyzer.getAnalyzedData().aggregate_descriptions)
                    result.aggregate_descriptions.push_back(descr);
            }
        }
        else if (ttl_element->mode == TTLMode::RECOMPRESS)
        {
            result.recompression_codec =
                CompressionCodecFactory::instance().validateCodecAndGetPreprocessedAST(
                    ttl_element->recompression_codec, {}, !context->getSettingsRef()[Setting::allow_suspicious_codecs], context->getSettingsRef()[Setting::allow_experimental_codecs]);
        }
    }

    checkTTLExpression(expression, result.result_column, is_attach || context->getSettingsRef()[Setting::allow_suspicious_ttl_expressions]);
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
