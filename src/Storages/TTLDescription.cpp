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
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsDateTime.h>
#include <Common/assert_cast.h>
#include <Common/intExp.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <base/DayNum.h>
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

/// Representative "seed" values (Unix seconds) used to probe a TTL expression. They span many decades,
/// daylight-saving-time transitions, and leap-year boundaries, plus a few small and negative values.
/// Any dependence of the TTL delta on the row (calendar month/year intervals, DST-sensitive day/week
/// intervals, or column-dependent expressions) shows up as unequal deltas across these probes.
constexpr Int64 ttl_delta_probe_seeds[] = {
    0, 1, -1, 3600, 86400, 90061,
    951782400,   /// 2000-02-29 (leap day)
    1000000000,  /// 2001-09-09
    1234567890,  /// 2009-02-13
    1300000000, 1400000000, 1500000000, 1600000000, 1700000000, 1800000000, 1900000000, 2000000000,
    1583020800,  /// 2020-03-01 (before spring DST in the northern hemisphere)
    1585699200,  /// 2020-04-01 (after spring DST)
    1601510400,  /// 2020-10-01 (before autumn DST)
    1604188800,  /// 2020-11-01 (after autumn DST)
    1614556800,  /// 2021-03-01
    -2208988800, /// 1900-01-01 (negative, exercises the Date32 range)
    4102444800,  /// 2100-01-01
    2145916800,  /// 2038-01-01 (near the 32-bit DateTime limit)
};

constexpr size_t ttl_delta_probe_rows = std::size(ttl_delta_probe_seeds);

/// Fill `column` (which must be of `type`) with one probe value per row, converting each seed into the
/// column's native representation. Only date/time types are supported; the caller guarantees that.
void fillTTLProbeColumn(const DataTypePtr & type, IColumn & column)
{
    WhichDataType which(type);
    for (Int64 seed : ttl_delta_probe_seeds)
    {
        if (which.isDate())
            assert_cast<ColumnUInt16 &>(column).getData().push_back(
                static_cast<UInt16>(std::clamp<Int64>(seed / 86400, 0, DATE_LUT_MAX_DAY_NUM)));
        else if (which.isDate32())
            assert_cast<ColumnInt32 &>(column).getData().push_back(static_cast<Int32>(seed / 86400));
        else if (which.isDateTime())
            assert_cast<ColumnUInt32 &>(column).getData().push_back(
                static_cast<UInt32>(std::clamp<Int64>(seed, 0, static_cast<Int64>(0xFFFFFFFF))));
        else if (which.isDateTime64())
        {
            const UInt32 scale = assert_cast<const DataTypeDateTime64 &>(*type).getScale();
            const Int64 multiplier = intExp10OfSize<Int64>(scale);
            /// Guard against overflow when scaling seconds into the column's sub-second units.
            const Int64 max_seconds = std::numeric_limits<Int64>::max() / multiplier;
            const Int64 seconds = std::clamp<Int64>(seed, -max_seconds, max_seconds);
            assert_cast<ColumnDateTime64 &>(column).getData().push_back(DateTime64(seconds * multiplier));
        }
    }
}

/// Extract the expiry timestamp (Unix seconds) for a single row from a TTL result column. Returns
/// std::nullopt for an unexpected result type, so the caller can fall back instead of failing.
std::optional<Int64> extractTTLTimestamp(const IColumn & column, size_t row)
{
    const auto & date_lut = DateLUT::serverTimezoneInstance();

    if (const auto * col = typeid_cast<const ColumnUInt16 *>(&column))
        return static_cast<Int64>(date_lut.fromDayNum(DayNum(col->getData()[row])));
    if (const auto * col = typeid_cast<const ColumnUInt32 *>(&column))
        return static_cast<Int64>(col->getData()[row]);
    if (const auto * col = typeid_cast<const ColumnInt32 *>(&column))
        return static_cast<Int64>(date_lut.fromDayNum(ExtendedDayNum(static_cast<Int32>(col->getData()[row]))));
    if (const auto * col = typeid_cast<const ColumnDateTime64 *>(&column))
        return col->getData()[row] / intExp10OfSize<Int64>(col->getScale());

    return {};
}

}

std::optional<time_t> tryComputeConstantTTLDelta(
    const TTLDescription & old_ttl, const TTLDescription & new_ttl, const ContextPtr & context)
{
    if (!old_ttl.expression_ast || !new_ttl.expression_ast)
        return {};

    /// Build a probe block holding the union of both expressions' input columns. The optimization is
    /// only sound when the delta is independent of the row, so we require every referenced column to be
    /// a date/time type: an expression that reads any other column (for example `if(id = 0, ...)`) is
    /// treated as potentially row-dependent and rejected here, falling back to a regular rewrite.
    Block probe_block;
    NameSet seen;
    for (const auto * columns : {&old_ttl.expression_columns, &new_ttl.expression_columns})
    {
        for (const auto & column : *columns)
        {
            if (!seen.insert(column.name).second)
                continue;

            WhichDataType which(column.type);
            if (!(which.isDate() || which.isDate32() || which.isDateTime() || which.isDateTime64()))
                return {};

            auto probe_column = column.type->createColumn();
            fillTTLProbeColumn(column.type, *probe_column);
            probe_block.insert({std::move(probe_column), column.type, column.name});
        }
    }

    try
    {
        auto evaluate = [&](const TTLDescription & ttl) -> ColumnPtr
        {
            auto expression = ttl.buildExpression(context).expression;
            Block block_copy;
            for (const auto & name : expression->getRequiredColumns())
                block_copy.insert(probe_block.getByName(name));

            size_t num_rows = ttl_delta_probe_rows;
            expression->execute(block_copy, num_rows);
            return block_copy.getByName(ttl.result_column).column->convertToFullColumnIfConst();
        };

        ColumnPtr old_column = evaluate(old_ttl);
        ColumnPtr new_column = evaluate(new_ttl);

        std::optional<Int64> delta;
        for (size_t row = 0; row < ttl_delta_probe_rows; ++row)
        {
            auto old_timestamp = extractTTLTimestamp(*old_column, row);
            auto new_timestamp = extractTTLTimestamp(*new_column, row);
            if (!old_timestamp || !new_timestamp)
                return {};

            const Int64 row_delta = *new_timestamp - *old_timestamp;
            if (!delta)
                delta = row_delta;
            else if (*delta != row_delta)
                return {};
        }

        return delta;
    }
    catch (...)
    {
        /// Any failure to analyze the expressions means we cannot prove the delta is constant.
        return {};
    }
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

        return tryComputeConstantTTLDelta(old_ttl.rows_ttl, new_ttl, context);
    }
    catch (...)
    {
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
