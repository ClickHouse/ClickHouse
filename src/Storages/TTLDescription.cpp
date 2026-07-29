#include <Storages/TTLDescription.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Compression/CompressionFactory.h>
#include <Core/Settings.h>
#include <Functions/IFunction.h>
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
#include <Common/assert_cast.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Parsers/ASTLiteral.h>
#include <base/arithmeticOverflow.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>


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

namespace
{

/// The result of the structural analysis of a rows-TTL expression: the single date/time column the
/// expression shifts and the total constant shift in seconds. The analysis only accepts the shape
/// `column`, or `column` plus/minus constant intervals of fixed length (week/day/hour/minute/second),
/// so the expression is provably `column + offset_seconds` and nothing else.
struct TTLShiftedColumn
{
    String column_name;
    Int64 offset_seconds = 0;
    /// Day/week intervals are only a fixed number of seconds in a time zone with a fixed offset
    /// (`addDays` preserves the local wall-clock time across DST transitions otherwise).
    bool has_day_or_week_interval = false;
    /// Hour/minute/second intervals change the result type of a `Date`/`Date32` expression, so they
    /// are only accepted for `DateTime`/`DateTime64` columns.
    bool has_sub_day_interval = false;
};

/// Recognize a constant fixed-length interval: `toIntervalDay(N)` etc. with a literal integer argument.
/// Calendar-dependent units (month, quarter, year) and sub-second units are rejected: the former are
/// not a constant number of seconds, the latter cannot be represented in the parts' TTL timestamps.
std::optional<TTLShiftedColumn> tryAnalyzeIntervalConstant(const ASTPtr & node)
{
    const auto * func = node->as<ASTFunction>();
    if (!func || !func->arguments || func->arguments->children.size() != 1)
        return {};

    Int64 multiplier = 0;
    bool day_or_week = false;
    if (func->name == "toIntervalSecond")
        multiplier = 1;
    else if (func->name == "toIntervalMinute")
        multiplier = 60;
    else if (func->name == "toIntervalHour")
        multiplier = 3600;
    else if (func->name == "toIntervalDay")
    {
        multiplier = 86400;
        day_or_week = true;
    }
    else if (func->name == "toIntervalWeek")
    {
        multiplier = 7 * 86400;
        day_or_week = true;
    }
    else
        return {};

    const auto * literal = func->arguments->children.front()->as<ASTLiteral>();
    if (!literal)
        return {};

    Int64 value = 0;
    if (literal->value.getType() == Field::Types::UInt64)
    {
        UInt64 unsigned_value = literal->value.safeGet<UInt64>();
        if (unsigned_value > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
            return {};
        value = static_cast<Int64>(unsigned_value);
    }
    else if (literal->value.getType() == Field::Types::Int64)
        value = literal->value.safeGet<Int64>();
    else
        return {};

    TTLShiftedColumn result;
    if (common::mulOverflow(value, multiplier, result.offset_seconds))
        return {};
    result.has_day_or_week_interval = day_or_week;
    result.has_sub_day_interval = !day_or_week;
    return result;
}

/// Structurally decompose a rows-TTL expression into `column + constant seconds`. Returns std::nullopt
/// for any other shape (several columns, non-literal intervals, calendar-dependent units, arbitrary
/// functions), in which case the caller must fall back to a regular `MATERIALIZE TTL` rewrite.
std::optional<TTLShiftedColumn> tryAnalyzeTTLShift(const ASTPtr & node)
{
    if (const auto * identifier = node->as<ASTIdentifier>())
    {
        TTLShiftedColumn result;
        result.column_name = identifier->name();
        return result;
    }

    const auto * func = node->as<ASTFunction>();
    if (!func || (func->name != "plus" && func->name != "minus") || !func->arguments || func->arguments->children.size() != 2)
        return {};

    const bool is_minus = func->name == "minus";
    const auto & lhs = func->arguments->children[0];
    const auto & rhs = func->arguments->children[1];

    /// `<subtree> + interval`, `<subtree> - interval`, or `interval + <subtree>`.
    std::optional<TTLShiftedColumn> interval = tryAnalyzeIntervalConstant(rhs);
    ASTPtr subtree = lhs;
    if (!interval && !is_minus)
    {
        interval = tryAnalyzeIntervalConstant(lhs);
        subtree = rhs;
    }
    if (!interval)
        return {};

    auto result = tryAnalyzeTTLShift(subtree);
    if (!result)
        return {};

    const Int64 shift = is_minus ? -interval->offset_seconds : interval->offset_seconds;
    if (common::addOverflow(result->offset_seconds, shift, result->offset_seconds))
        return {};

    result->has_day_or_week_interval |= interval->has_day_or_week_interval;
    result->has_sub_day_interval |= interval->has_sub_day_interval;
    return result;
}

}

std::optional<time_t> tryComputeConstantTTLDelta(const TTLDescription & old_ttl, const TTLDescription & new_ttl)
{
    if (!old_ttl.expression_ast || !new_ttl.expression_ast)
        return {};

    /// The optimization is only sound when `new_ttl(row) - old_ttl(row)` is the same constant for every
    /// possible row, so we require both expressions to have a provable shape: the same single date/time
    /// column shifted by constant fixed-length intervals. Everything else - other columns (which make
    /// the delta row-dependent, e.g. `if(id = 0, ...)`), calendar month/year intervals, non-literal
    /// intervals, arbitrary functions - is rejected here, falling back to a regular rewrite.
    auto old_shift = tryAnalyzeTTLShift(old_ttl.expression_ast);
    auto new_shift = tryAnalyzeTTLShift(new_ttl.expression_ast);
    if (!old_shift || !new_shift || old_shift->column_name != new_shift->column_name)
        return {};

    /// The shifted identifier must be the one and only source column of both expressions. This also
    /// rejects an ALIAS column, whose analyzed source columns would carry the underlying names.
    auto get_single_column_type = [&](const TTLDescription & ttl) -> DataTypePtr
    {
        if (ttl.expression_columns.size() != 1 || ttl.expression_columns.front().name != old_shift->column_name)
            return nullptr;
        return ttl.expression_columns.front().type;
    };

    DataTypePtr old_type = get_single_column_type(old_ttl);
    DataTypePtr new_type = get_single_column_type(new_ttl);
    if (!old_type || !new_type || !old_type->equals(*new_type))
        return {};

    WhichDataType which(old_type);
    if (which.isDate() || which.isDate32())
    {
        /// Hour/minute/second intervals turn a `Date` expression into a `DateTime` one, whose time zone
        /// depends on the evaluation context; do not try to reason about that.
        if (old_shift->has_sub_day_interval || new_shift->has_sub_day_interval)
            return {};

        /// A `Date` TTL stores `fromDayNum(date + N)` in the server time zone, so consecutive days are
        /// a fixed 86400 seconds apart only when that time zone has a fixed offset from UTC.
        if (!DateLUT::serverTimezoneInstance().hasFixedOffset())
            return {};
    }
    else if (which.isDateTime() || which.isDateTime64())
    {
        /// `addDays`/`addWeeks` preserve the local wall-clock time, so they add a fixed number of
        /// seconds only in a time zone with a fixed offset from UTC. Hour/minute/second intervals
        /// always add a fixed number of seconds regardless of the time zone.
        if (old_shift->has_day_or_week_interval || new_shift->has_day_or_week_interval)
        {
            const auto & time_zone = which.isDateTime()
                ? assert_cast<const DataTypeDateTime &>(*old_type).getTimeZone()
                : assert_cast<const DataTypeDateTime64 &>(*old_type).getTimeZone();
            if (!time_zone.hasFixedOffset())
                return {};
        }
    }
    else
        return {};

    Int64 delta = 0;
    if (common::subOverflow(new_shift->offset_seconds, old_shift->offset_seconds, delta))
        return {};

    return delta;
}

String getRowsTTLTimeZoneFingerprint(const TTLDescription & rows_ttl)
{
    if (rows_ttl.expression_columns.size() == 1)
    {
        const auto & type = rows_ttl.expression_columns.front().type;
        WhichDataType which(type);
        if (which.isDateTime())
            return assert_cast<const DataTypeDateTime &>(*type).getTimeZone().getTimeZone();
        if (which.isDateTime64())
            return assert_cast<const DataTypeDateTime64 &>(*type).getTimeZone().getTimeZone();
    }

    return DateLUT::serverTimezoneInstance().getTimeZone();
}

std::optional<time_t> tryComputeConstantTTLDelta(
    const String & old_ttl_expression, const TTLDescription & new_ttl,
    const ColumnsDescription & columns, const KeyDescription & primary_key, const ContextPtr & context)
{
    if (old_ttl_expression.empty())
        return {};

    try
    {
        ParserTTLExpressionList parser;
        ASTPtr definition_ast = parseQuery(
            parser, old_ttl_expression.data(), old_ttl_expression.data() + old_ttl_expression.size(),
            "rows TTL expression", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

        /// The stored fingerprint is a single unconditional DELETE TTL expression, so `is_attach = true`
        /// merely skips the suspicious-expression check (the expression was already validated on write).
        TTLTableDescription old_ttl = TTLTableDescription::getTTLForTableFromAST(
            definition_ast, columns, context, primary_key, /*is_attach=*/ true);

        if (!old_ttl.rows_ttl.expression_ast)
            return {};

        return tryComputeConstantTTLDelta(old_ttl.rows_ttl, new_ttl);
    }
    catch (...)
    {
        /// The stored expression may be unparseable or invalid for the current table structure;
        /// it is Ok to swallow the exception here: the caller falls back to a full rewrite.
        return {};
    }
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
