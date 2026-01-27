#include <Storages/TimeSeries/PrometheusQueryToSQL.h>

#include <algorithm>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Parsers/Prometheus/parseTimeSeriesTypes.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int EMPTY_QUERY;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


namespace
{
    using ResultType = PrometheusQueryResultType;

    /// Increases a timestamp to make it divisible by `step`.
    DateTime64 alignUp(DateTime64 time, Decimal64 step)
    {
        chassert(step > 0);
        auto x = time % step;
        if (!x)
            return time;
        return time + step - x;
    }

    /// Decreases a timestamp to make it divisible by `step`.
    DateTime64 alignDown(DateTime64 time, Decimal64 step)
    {
        chassert(step > 0);
        auto x = time % step;
        if (!x)
            return time;
        return time - x;
    }
}


/// Builder of an AST query to evaluate a promql query.
class PrometheusQueryToSQLConverter::ASTBuilder
{
public:
    explicit ASTBuilder(const PrometheusQueryToSQLConverter & converter_)
        : converter(converter_)
        , timestamp_data_type(getTimeSeriesTableInfo().timestamp_data_type)
        , timestamp_scale((isDecimal(timestamp_data_type) || isDateTime64(timestamp_data_type)) ? getDecimalScale(*timestamp_data_type) : 0)
        , value_data_type(getTimeSeriesTableInfo().value_data_type)
        , lookback_delta(fieldToInterval(converter_.lookback_delta))
        , default_resolution(fieldToInterval(converter_.default_resolution))
        , result_type(converter_.result_type)
    {
        if (!converter_.evaluation_time.isNull())
        {
            evaluation_time = fieldToTimestamp(converter_.evaluation_time);
        }
        else if (!converter_.evaluation_range.isNull())
        {
            evaluation_range.emplace();
            evaluation_range->start_time = fieldToTimestamp(converter_.evaluation_range.start_time);
            evaluation_range->end_time = fieldToTimestamp(converter_.evaluation_range.end_time);
            evaluation_range->step = fieldToInterval(converter_.evaluation_range.step);
        }
    }

    ASTPtr getSQL()
    {
        const auto * root_node = getPromQLTree().getRoot();
        if (!root_node)
            throw Exception(ErrorCodes::EMPTY_QUERY, "Can't evaluate an empty prometheus query.");
        return toAST(finalize(buildPiece(root_node)));
    }

private:
    const PrometheusQueryToSQLConverter & converter;
    DataTypePtr timestamp_data_type;
    UInt32 timestamp_scale;
    DataTypePtr value_data_type;
    Decimal64 lookback_delta;
    Decimal64 default_resolution;
    PrometheusQueryResultType result_type;

    std::optional<DateTime64> evaluation_time;

    struct EvaluationRange
    {
        DateTime64 start_time;
        DateTime64 end_time;
        Decimal64 step;
    };
    std::optional<EvaluationRange> evaluation_range;

    mutable std::vector<const PrometheusQueryTree::Node *> parent_nodes;

    const PrometheusQueryTree & getPromQLTree() const { return converter.promql; }
    std::string_view getPromQLText(const PrometheusQueryTree::Node * node) const { return getPromQLTree().getQuery(node); }
    const TimeSeriesTableInfo & getTimeSeriesTableInfo() const { return converter.time_series_table_info; }

    using NodeType = PrometheusQueryTree::NodeType;

    /// Represents a SELECT query built for a node in a prometheus query tree.
    /// [WITH ...] SELECT ... FROM ... [GROUP BY ...] [WHERE ...]
    struct Piece
    {
        /// Result of the query.
        ResultType result_type;

        /// A window is extracted from a range selector. The window is used only by functions accepting range vectors, e.g. rate().
        Decimal64 window;

        /// Columns to select (nullptr if there is no such column).
        /// The names of these columns are always TimeSeriesColumnNames::Group, TimeSeriesColumnNames::Tags and so on.
        ASTPtr group_column;
        ASTPtr tags_column;
        ASTPtr timestamp_column;
        ASTPtr value_column;
        ASTPtr time_series_column;
        ASTPtr scalar_column;
        ASTPtr string_column;

        /// Whether the "timestamp" column and the "value" column are columns of arrays.
        bool timestamp_column_is_array = false;
        bool value_column_is_array = false;

        size_t num_columns() const
        {
            return (group_column != nullptr) + (tags_column != nullptr) + (timestamp_column != nullptr)
                + (value_column != nullptr) + (time_series_column != nullptr) + (scalar_column != nullptr) + (string_column != nullptr);
        }

        bool empty () const { return num_columns() == 0; }

        /// The "FROM" expression when it's a table function. or the temporary table name denoting a subquery.
        ASTPtr from_table_function;

        /// The "FROM" expression when it's a temporary table name denoting a subquery.
        String from_subquery;

        /// The "GROUP BY" expression.
        ASTs group_by;

        ASTPtr where;
        std::vector<std::pair<String, ASTPtr>> with;
    };

    /// List of collected subqueries.
    /// At the end of the process when we finalize the prepared SELECT query we add such collected subqueries to the "WITH" clause of it.
    std::vector<std::pair<String, Piece>> subqueries;

    /// Adds a piece to the list of collected subqueries.
    /// Returns a generated temporary table name for the new subquery.
    String addSubquery(Piece && piece)
    {
        String name = fmt::format("prom{}", subqueries.size() + 1);
        subqueries.emplace_back(name, std::move(piece));
        return name;
    }

    /// Converts a Piece to AST.
    static ASTPtr toAST(const Piece & piece)
    {
        chassert(!piece.empty());
        auto select_query = make_intrusive<ASTSelectQuery>();

        auto select_list_exp = make_intrusive<ASTExpressionList>();
        auto & select_list = select_list_exp->children;
        if (piece.group_column)
            select_list.push_back(piece.group_column);
        if (piece.tags_column)
            select_list.push_back(piece.tags_column);
        if (piece.timestamp_column)
            select_list.push_back(piece.timestamp_column);
        if (piece.value_column)
            select_list.push_back(piece.value_column);
        if (piece.time_series_column)
            select_list.push_back(piece.time_series_column);
        if (piece.scalar_column)
            select_list.push_back(piece.scalar_column);
        if (piece.string_column)
            select_list.push_back(piece.string_column);
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list_exp);

        if (!piece.from_subquery.empty() || piece.from_table_function)
        {
            auto tables = make_intrusive<ASTTablesInSelectQuery>();
            auto table = make_intrusive<ASTTablesInSelectQueryElement>();
            auto table_exp = make_intrusive<ASTTableExpression>();
            if (!piece.from_subquery.empty())
            {
                table_exp->database_and_table_name = make_intrusive<ASTTableIdentifier>(piece.from_subquery);
                table_exp->children.emplace_back(table_exp->database_and_table_name);
            }
            else if (piece.from_table_function)
            {
                table_exp->table_function = piece.from_table_function;
                table_exp->children.emplace_back(table_exp->table_function);
            }
            table->table_expression = table_exp;
            tables->children.push_back(table);
            select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);
        }

        if (!piece.group_by.empty())
        {
            auto group_by_list = make_intrusive<ASTExpressionList>();
            select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, group_by_list);
            group_by_list->children = piece.group_by;
        }

        if (piece.where)
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, piece.where->clone());

        if (!piece.with.empty())
        {
            auto with_expression_list_ast = make_intrusive<ASTExpressionList>();
            for (const auto & [name, ast] : piece.with)
            {
                auto with_element_ast = make_intrusive<ASTWithElement>();
                with_element_ast->name = name;
                with_element_ast->subquery = make_intrusive<ASTSubquery>(ast);
                with_element_ast->children.push_back(with_element_ast->subquery);
                with_expression_list_ast->children.push_back(std::move(with_element_ast));
            }
            select_query->setExpression(ASTSelectQuery::Expression::WITH, std::move(with_expression_list_ast));
        }

        auto select_with_union_query = make_intrusive<ASTSelectWithUnionQuery>();
        auto list_of_selects = make_intrusive<ASTExpressionList>();
        list_of_selects->children.push_back(std::move(select_query));
        select_with_union_query->list_of_selects = list_of_selects;
        select_with_union_query->children.push_back(list_of_selects);

        return select_with_union_query;
    }

    /// Finalizes a Piece built to evaluate a prometheus query.
    Piece finalize(Piece && piece)
    {
        Piece res;

        /// Finalize depending on the result type.
        switch (result_type)
        {
            case ResultType::STRING: res = finalizeWithStringResult(std::move(piece)); break;
            case ResultType::SCALAR: res = finalizeWithScalarResult(std::move(piece)); break;
            case ResultType::INSTANT_VECTOR: res = finalizeWithInstantVectorResult(std::move(piece)); break;
            case ResultType::RANGE_VECTOR: res = finalizeWithRangeVectorResult(std::move(piece)); break;
        }

        /// Add the collected subqueries to the WITH clause of the final query.
        if (!subqueries.empty())
        {
            res.with.reserve(subqueries.size());
            for (const auto & [name, piece_for_subquery] : subqueries)
                res.with.emplace_back(name, toAST(piece_for_subquery));
        }

        return res;
    }

    /// Finalizes a Piece returning a string.
    Piece finalizeWithStringResult(Piece && piece)
    {
        if (piece.string_column && piece.num_columns() == 1)
            return piece;

        Piece res;
        res.result_type = PrometheusQueryResultType::STRING;
        res.string_column = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::String);

        if (piece.empty())
            res.from_table_function = makeASTFunction("null", make_intrusive<ASTLiteral>(fmt::format("{} String", TimeSeriesColumnNames::String)));
        else
            res.from_subquery = addSubquery(std::move(piece));

        return res;
    }

    /// Finalizes a Piece returning a scalar.
    Piece finalizeWithScalarResult(Piece && piece)
    {
        if (piece.scalar_column && (piece.num_columns() == 1))
            return piece;

        Piece res;
        res.result_type = PrometheusQueryResultType::SCALAR;
        res.scalar_column = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Scalar);

        if (piece.empty())
            res.from_table_function = makeASTFunction("null", make_intrusive<ASTLiteral>(fmt::format("{} {}", TimeSeriesColumnNames::Scalar, getTimeSeriesTableInfo().value_data_type)));
        else
            res.from_subquery = addSubquery(std::move(piece));

        return res;
    }

    /// Finalizes a Piece returning an instant vector.
    Piece finalizeWithInstantVectorResult(Piece && piece)
    {
        if (piece.tags_column && piece.timestamp_column && piece.value_column && (piece.num_columns() == 3))
            return piece;

        Piece res;
        res.result_type = PrometheusQueryResultType::INSTANT_VECTOR;

        if (piece.empty())
        {
            res.tags_column = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags);
            res.timestamp_column = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
            res.value_column = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Value);
            res.from_table_function = makeASTFunction("null", make_intrusive<ASTLiteral>(
                fmt::format("{} Array(Tuple(String, String)), {} {}, {} {}",
                            TimeSeriesColumnNames::Tags, TimeSeriesColumnNames::Timestamp, getTimeSeriesTableInfo().timestamp_data_type,
                            TimeSeriesColumnNames::Value, getTimeSeriesTableInfo().value_data_type)));
            return res;
        }

        if (piece.tags_column)
        {
            res.tags_column = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags);
        }
        else if (piece.group_column)
        {
            res.tags_column = makeASTFunction("timeSeriesTagsGroupToTags", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Group));
            res.tags_column->setAlias(TimeSeriesColumnNames::Tags);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected columns {} or {} while building an SQL query", TimeSeriesColumnNames::Tags, TimeSeriesColumnNames::Group);
        }

        if (piece.timestamp_column_is_array || piece.value_column_is_array)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Columns {} and {} are not expected to be arrays", TimeSeriesColumnNames::Timestamp, TimeSeriesColumnNames::Value);

        if (piece.timestamp_column && piece.value_column)
        {
            res.timestamp_column = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
            res.value_column = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Value);
        }
        else if (piece.time_series_column)
        {
            res.where = makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries));
            auto array_element = makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries),
                                                 make_intrusive<ASTLiteral>(Field{1u}));
            res.timestamp_column = makeASTFunction("tupleElement", array_element, make_intrusive<ASTLiteral>(Field{1u}));
            res.timestamp_column->setAlias(TimeSeriesColumnNames::Timestamp);
            res.value_column = makeASTFunction("tupleElement", array_element, make_intrusive<ASTLiteral>(Field{2u}));
            res.value_column->setAlias(TimeSeriesColumnNames::Value);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected columns ({} and {}) or {} while building an SQL query", TimeSeriesColumnNames::Timestamp, TimeSeriesColumnNames::Value, TimeSeriesColumnNames::TimeSeries);
        }

        res.from_subquery = addSubquery(std::move(piece));
        return res;
    }

    /// Finalizes a Piece returning a range vector.
    Piece finalizeWithRangeVectorResult(Piece && piece)
    {
        if (piece.tags_column && piece.time_series_column && (piece.num_columns() == 2))
            return piece;

        Piece res;
        res.result_type = PrometheusQueryResultType::RANGE_VECTOR;

        if (piece.empty())
        {
            res.tags_column = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags);
            res.time_series_column = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries);
            res.from_table_function = makeASTFunction("null", make_intrusive<ASTLiteral>(
                fmt::format("{} Array(Tuple(String, String)), {} Array(Tuple({}, {}))",
                            TimeSeriesColumnNames::Tags, TimeSeriesColumnNames::TimeSeries,
                            getTimeSeriesTableInfo().timestamp_data_type, getTimeSeriesTableInfo().value_data_type)));
            return res;
        }

        if (piece.tags_column)
        {
            res.tags_column = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags);
        }
        else if (piece.group_column)
        {
            res.tags_column = makeASTFunction("timeSeriesTagsGroupToTags", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Group));
            res.tags_column->setAlias(TimeSeriesColumnNames::Tags);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected columns {} or {} while building an SQL query", TimeSeriesColumnNames::Tags, TimeSeriesColumnNames::Group);
        }

        if (piece.timestamp_column_is_array || piece.value_column_is_array)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Columns {} and {} are not expected to be arrays", TimeSeriesColumnNames::Timestamp, TimeSeriesColumnNames::Value);

        if (piece.time_series_column)
        {
            res.time_series_column = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries);
        }
        else if (piece.timestamp_column && piece.value_column)
        {
            res.time_series_column = makeASTFunction("timeSeriesGroupArray",
                                                     make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
                                                     make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Value));
            res.time_series_column->setAlias(TimeSeriesColumnNames::TimeSeries);
            res.group_by.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Group));
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected columns ({} and {}) or {} while building an SQL query", TimeSeriesColumnNames::Timestamp, TimeSeriesColumnNames::Value, TimeSeriesColumnNames::TimeSeries);
        }

        res.from_subquery = addSubquery(std::move(piece));
        return res;
    }

    /// Builds a piece to evaluate a node in a prometheus query tree.
    Piece buildPiece(const PrometheusQueryTree::Node * node)
    {
        auto node_type = node->node_type;
        switch (node_type)
        {
            case NodeType::InstantSelector:
                return buildPieceForInstantSelector(typeid_cast<const PrometheusQueryTree::InstantSelector *>(node));

            case NodeType::RangeSelector:
                return buildPieceForRangeSelector(typeid_cast<const PrometheusQueryTree::RangeSelector *>(node));

            case NodeType::Subquery:
                return buildPieceForSubquery(typeid_cast<const PrometheusQueryTree::Subquery *>(node));

            case NodeType::Offset:
                return buildPieceForOffset(typeid_cast<const PrometheusQueryTree::Offset *>(node));

            case NodeType::Function:
                return buildPieceForFunction(typeid_cast<const PrometheusQueryTree::Function *>(node));

            case NodeType::BinaryOperator:
                return buildPieceForBinaryOperator(typeid_cast<const PrometheusQueryTree::BinaryOperator *>(node));

            default:
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Prometheus query tree node type {} is not implemented", node_type);
        }
    }

    /// Builds an empty piece.
    static Piece getEmptyPiece(ResultType result_type)
    {
        Piece empty;
        empty.result_type = result_type;
        return empty;
    }

    /// Builds a piece to evaluate an instant selector.
    Piece buildPieceForInstantSelector(const PrometheusQueryTree::InstantSelector * instant_selector) const
    {
        if (lookback_delta <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The lookback delta must be positive, got {}", toString(lookback_delta, timestamp_scale));

        /// Lookback deltas are left-open (and right-closed), so we decrease `window` a little bit to consider both boundaries close.
        auto window = lookback_delta;

        DateTime64 start_time;
        DateTime64 end_time;
        Decimal64 step;
        extractRangeAndStep(instant_selector, start_time, end_time, step);

        /// We can get an empty interval here because of aligning in extractRangeAndStep().
        if (end_time < start_time)
            return getEmptyPiece(ResultType::INSTANT_VECTOR);

        Piece res;

        res.from_table_function = makeASTFunction("timeSeriesSelector",
            make_intrusive<ASTLiteral>(getTimeSeriesTableInfo().storage_id.getDatabaseName()),
            make_intrusive<ASTLiteral>(getTimeSeriesTableInfo().storage_id.getTableName()),
            make_intrusive<ASTLiteral>(getPromQLText(instant_selector)),
            timestampToAST(start_time - window + 1),
            timestampToAST(end_time));

        res.group_column = makeASTFunction("timeSeriesIdToTagsGroup", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
        res.group_column->setAlias(TimeSeriesColumnNames::Group);

        res.time_series_column = makeGridFunction("timeSeriesLastToGrid", start_time, end_time, step, window,
                                                  make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
                                                  make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Value));

        res.time_series_column->setAlias(TimeSeriesColumnNames::TimeSeries);
        res.group_by.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Group));
        res.result_type = ResultType::INSTANT_VECTOR;

        return res;
    }

    /// Builds a piece to evaluate a range selector.
    Piece buildPieceForRangeSelector(const PrometheusQueryTree::RangeSelector * range_selector) const
    {
        const auto * instant_selector = range_selector->getInstantSelector();

        auto range = range_selector->range;
        if (range <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Range specified in a range selector must be positive, got {}", toString(range, timestamp_scale));

        /// Ranges are left-open (and right-closed), so we decrease `window` a little bit to consider both boundaries close.
        auto window = range;

        DateTime64 start_time;
        DateTime64 end_time;
        Decimal64 step;
        extractRangeAndStep(range_selector, start_time, end_time, step);

        /// We can get an empty interval here because of aligning in extractRangeAndStep().
        if (end_time < start_time)
            return getEmptyPiece(ResultType::RANGE_VECTOR);

        Piece res;

        res.from_table_function = makeASTFunction("timeSeriesSelector",
            make_intrusive<ASTLiteral>(getTimeSeriesTableInfo().storage_id.getDatabaseName()),
            make_intrusive<ASTLiteral>(getTimeSeriesTableInfo().storage_id.getTableName()),
            make_intrusive<ASTLiteral>(getPromQLText(instant_selector)),
            timestampToAST(start_time - window + 1),
            timestampToAST(end_time));

        res.group_column = makeASTFunction("timeSeriesIdToTagsGroup", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
        res.group_column->setAlias(TimeSeriesColumnNames::Group);
        res.timestamp_column = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
        res.value_column = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Value);
        res.result_type = ResultType::RANGE_VECTOR;
        res.window = window;

        return res;
    }

    /// Builds a piece for a subquery.
    Piece buildPieceForSubquery(const PrometheusQueryTree::Subquery * subquery)
    {
        auto piece = buildPiece(subquery->getExpression());

        if (piece.empty())
            return getEmptyPiece(ResultType::RANGE_VECTOR);

        piece.result_type = ResultType::RANGE_VECTOR;
        piece.window = subquery->range;
        return piece;
    }

    /// Builds a piece to evaluate a function.
    Piece buildPieceForFunction(const PrometheusQueryTree::Function * func)
    {
        const auto & function_name = func->function_name;
        std::vector<Piece> args = buildPiecesForArguments(func);

        if (function_name == "sin")
            return buildPieceForOrdinaryFunction(func, std::move(args));

        if (function_name == "rate" || function_name == "irate" || function_name == "delta" || function_name == "idelta" || function_name == "last_over_time")
            return buildPieceForRangeFunction(func, std::move(args));

        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} is not implemented", func->function_name);
    }

    /// Builds a piece to evaluate an offset.
    Piece buildPieceForOffset(const PrometheusQueryTree::Offset * offset_node)
    {
        /// Offsets are already taken into account - see extractRangeAndStep(). So here we just ignore them.
        return buildPiece(offset_node->getExpression());
    }

    /// Checks the number of arguments of a promql function.
    static void checkNumberArguments(const PrometheusQueryTree::Function * func, const std::vector<Piece> & arguments, size_t expected)
    {
        if (arguments.size() != expected)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires {} arguments, got {}",
                            func->function_name, expected, arguments.size());
    }

    /// Checks the type of an argument of a promql function.
    static void checkArgumentType(const PrometheusQueryTree::Function * func, const std::vector<Piece> & arguments, size_t index, ResultType expected)
    {
        if (arguments.at(index).result_type != expected)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} of function {} must be {}, got {}",
                index + 1, func->function_name, expected, arguments.at(index).result_type);
    }

    /// Builds pieces to evaluate the arguments of a function.
    std::vector<Piece> buildPiecesForArguments(const PrometheusQueryTree::Function * func)
    {
        std::vector<Piece> res;
        res.reserve(func->getArguments().size());
        for (const auto * argument : func->getArguments())
            res.push_back(buildPiece(argument));
        return res;
    }

    /// Builds a piece to evaluate an ordinary function, i.e. a function accepting an instant vector and returning an instant vector.
    Piece buildPieceForOrdinaryFunction(const PrometheusQueryTree::Function * func, std::vector<Piece> && arguments)
    {
        checkNumberArguments(func, arguments, 1);
        checkArgumentType(func, arguments, 0, ResultType::INSTANT_VECTOR);

        auto & argument = arguments[0];

        if (argument.empty())
            return getEmptyPiece(ResultType::INSTANT_VECTOR);

        std::string_view ch_function_name;
        if (func->function_name == "sin")
            ch_function_name = "sin";
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} is not implemented", func->function_name);

        Piece res;
        res.result_type = ResultType::INSTANT_VECTOR;
        res.group_column = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Group);
        res.timestamp_column = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
        res.value_column = makeASTFunction(ch_function_name, make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Value));
        res.value_column->setAlias(TimeSeriesColumnNames::Value);
        res.from_subquery = addSubquery(splitTimeSeriesColumnToTwoNonArrays(std::move(argument)));
        return res;
    }

    /// Builds a piece to evaluate a range function, i.e. a function accepting a range vector and returning an instant vector.
    Piece buildPieceForRangeFunction(const PrometheusQueryTree::Function * func, std::vector<Piece> && arguments)
    {
        checkNumberArguments(func, arguments, 1);
        checkArgumentType(func, arguments, 0, ResultType::RANGE_VECTOR);

        auto & argument = arguments[0];

        if (argument.empty())
            return getEmptyPiece(ResultType::INSTANT_VECTOR);

        std::string_view grid_function_name;
        if (func->function_name == "rate")
            grid_function_name = "timeSeriesRateToGrid";
        else if (func->function_name == "irate")
            grid_function_name = "timeSeriesInstantRateToGrid";
        else if (func->function_name == "delta")
            grid_function_name = "timeSeriesDeltaToGrid";
        else if (func->function_name == "idelta")
            grid_function_name = "timeSeriesInstantDeltaToGrid";
        else if (func->function_name == "last_over_time")
            grid_function_name = "timeSeriesLastToGrid";
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} is not implemented", func->function_name);

        auto window = argument.window;

        DateTime64 start_time;
        DateTime64 end_time;
        Decimal64 step;
        extractRangeAndStep(func, start_time, end_time, step);

        /// We can get an empty interval here because of aligning in extractRangeAndStep().
        if (end_time < start_time)
            return getEmptyPiece(ResultType::INSTANT_VECTOR);

        Piece res;
        res.result_type = ResultType::INSTANT_VECTOR;
        res.group_column = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Group);

        res.time_series_column = makeGridFunction(grid_function_name, start_time, end_time, step, window,
                                                  make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
                                                  make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Value));

        res.time_series_column->setAlias(TimeSeriesColumnNames::TimeSeries);
        res.group_by.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Group));
        res.from_subquery = addSubquery(splitTimeSeriesColumnToTwoArrays(std::move(argument)));
        return res;
    }

    /// Builds a piece to evaluate a binary operator.
    Piece buildPieceForBinaryOperator(const PrometheusQueryTree::BinaryOperator * binary_operator)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Binary operator {} is not implemented", binary_operator->operator_name);
    }

    /// Builds a piece splitting the "time_series" column into two columns "timestamp" and "value", both of them are arrays.
    Piece splitTimeSeriesColumnToTwoArrays(Piece && piece)
    {
        if (!piece.time_series_column)
            return piece;

        Piece res;
        res.result_type = piece.result_type;
        res.group_column = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Group);
        res.timestamp_column = makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries),
                                               make_intrusive<ASTLiteral>(Field{1u}));
        res.timestamp_column->setAlias(TimeSeriesColumnNames::Timestamp);
        res.value_column = makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries),
                                           make_intrusive<ASTLiteral>(Field{2u}));
        res.value_column->setAlias(TimeSeriesColumnNames::Value);
        res.from_subquery = addSubquery(std::move(piece));
        res.timestamp_column_is_array = true;
        res.value_column_is_array = true;
        return res;
    }

    /// Builds a piece splitting the "time_series" column into two columns "timestamp" and "value", which are not arrays.
    Piece splitTimeSeriesColumnToTwoNonArrays(Piece && piece)
    {
        if (!piece.time_series_column)
            return piece;

        Piece res;
        res.result_type = piece.result_type;
        res.group_column = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Group);
        res.timestamp_column = makeASTFunction("tupleElement", makeASTFunction("arrayJoin",
                                               make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries)),
                                               make_intrusive<ASTLiteral>(Field{1u}));
        res.timestamp_column->setAlias(TimeSeriesColumnNames::Timestamp);
        res.value_column = makeASTFunction("tupleElement", makeASTFunction("arrayJoin",
                                           make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries)),
                                           make_intrusive<ASTLiteral>(Field{2u}));
        res.value_column->setAlias(TimeSeriesColumnNames::Value);
        res.from_subquery = addSubquery(std::move(piece));
        return res;
    }

    /// Builds an AST to call functions generating time series on a grid.
    /// Returns something like timeSeriesFromGrid(<start_time>, <step>, timeSeries*ToGrid(<start_time>, <end_time>, <step>, <window>)(<timestamp>, <value>)
    ASTPtr makeGridFunction(std::string_view grid_function_name,
                            DateTime64 start_time, DateTime64 end_time,
                            Decimal64 step, Decimal64 window,
                            ASTPtr timestamp_column, ASTPtr value_column) const
    {
        auto aggregate_function = makeASTFunction(grid_function_name, timestamp_column, value_column);
        aggregate_function->parameters = make_intrusive<ASTExpressionList>();
        aggregate_function->parameters->children.push_back(timestampToAST(start_time));
        aggregate_function->parameters->children.push_back(timestampToAST(end_time));
        aggregate_function->parameters->children.push_back(intervalToAST(step));
        aggregate_function->parameters->children.push_back(intervalToAST(window));
        return makeASTFunction("timeSeriesFromGrid", timestampToAST(start_time), timestampToAST(end_time), intervalToAST(step), aggregate_function);
    }

    /// Finds all subqueries and @ and offset operations related to a specific node
    /// and determine the total time range and optionally the step used in the most inner subquery.
    /// The function always set `start_time` and `end_time`. If the node isn't used in any subquery the function sets `step` to 0.
    void extractRangeAndStep(const PrometheusQueryTree::Node * node,
                             DateTime64 & start_time, DateTime64 & end_time, Decimal64 & step) const
    {
        parent_nodes.clear();
        for (const auto * parent = node; parent; parent = parent->parent)
            parent_nodes.push_back(parent);

        if (evaluation_time)
        {
            start_time = *evaluation_time;
            end_time = *evaluation_time;
            step = 0;
        }
        else
        {
            chassert(evaluation_range);
            start_time = evaluation_range->start_time;
            end_time = evaluation_range->end_time;
            step = evaluation_range->step;
        }

        for (const auto * parent : parent_nodes)
        {
            if (parent->node_type == NodeType::Offset)
            {
                const auto * offset_node = typeid_cast<const PrometheusQueryTree::Offset *>(parent);
                if (offset_node->at_timestamp)
                {
                    start_time = *offset_node->at_timestamp;
                    end_time = start_time;
                    step = 0;
                }
                if (offset_node->offset_value)
                {
                    /// The "offset" modifier moves the evaluation time backward.
                    auto offset = *offset_node->offset_value;
                    start_time = start_time - offset;
                    end_time = end_time - offset;
                }
            }
            else if (parent->node_type == NodeType::Subquery)
            {
                const auto * subquery_node = typeid_cast<const PrometheusQueryTree::Subquery *>(parent);
                if (subquery_node->resolution)
                {
                    step = *subquery_node->resolution;
                    if (step <= 0)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Resolution must be positive, got {}", toString(step, timestamp_scale));
                }
                else
                {
                    if (default_resolution <= 0)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The default resolution must be positive, got {}", toString(default_resolution, timestamp_scale));
                    step = default_resolution;
                }
                auto subquery_range = subquery_node->range;
                if (subquery_range <= 0)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Subquery range must be positive, got {}", toString(subquery_range, timestamp_scale));
                start_time = start_time - subquery_range + 1;

                /// We need to align `start_time` and `end_time` by `step` if there is a subquery.
                /// (See https://www.robustperception.io/promql-subqueries-and-alignment/)
                start_time = alignUp(start_time, step);
                end_time = alignDown(end_time, step);
            }
        }
    }

    /// Converts a scalar or an interval value to a timestamp compatible with the data types used in the TimeSeries table.
    DateTime64 fieldToTimestamp(const Field & field) const
    {
        return parseTimeSeriesTimestamp(field, timestamp_scale);
    }

    /// Converts a scalar or an interval value to an interval compatible with the data types used in the TimeSeries table.
    Decimal64 fieldToInterval(const Field & field) const
    {
        return parseTimeSeriesDuration(field, timestamp_scale);
    }

    /// Converts a timestamp to AST.
    ASTPtr timestampToAST(DateTime64 timestamp) const
    {
        return timeSeriesTimestampToAST(timestamp, timestamp_data_type);
    }

    /// Converts a interval to AST.
    ASTPtr intervalToAST(Decimal64 interval) const
    {
        return timeSeriesDurationToAST(interval, timestamp_data_type);
    }
};


PrometheusQueryToSQLConverter::PrometheusQueryToSQLConverter(
    const PrometheusQueryTree & promql_,
    const TimeSeriesTableInfo & time_series_table_info_,
    const Field & lookback_delta_,
    const Field & default_resolution_)
    : promql(promql_)
    , time_series_table_info(time_series_table_info_)
    , lookback_delta(lookback_delta_)
    , default_resolution(default_resolution_)
{
}

void PrometheusQueryToSQLConverter::setEvaluationTime(const Field & time_)
{
    evaluation_time = time_;
    evaluation_range = {};
    result_type = promql.getResultType();
}

void PrometheusQueryToSQLConverter::setEvaluationRange(const PrometheusQueryEvaluationRange & range_)
{
    if (promql.getResultType() != PrometheusQueryResultType::INSTANT_VECTOR &&
        promql.getResultType() != PrometheusQueryResultType::SCALAR)
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Invalid expression type '{}' for range query, must be scalar or instant Vector",
                        promql.getResultType());
    }
    evaluation_range = range_;
    evaluation_time = Field{};
    result_type = PrometheusQueryResultType::RANGE_VECTOR;
}

ASTPtr PrometheusQueryToSQLConverter::getSQL() const
{
    return ASTBuilder{*this}.getSQL();
}

ColumnsDescription PrometheusQueryToSQLConverter::getResultColumns() const
{
    ColumnsDescription columns;

    switch (result_type)
    {
        case ResultType::SCALAR:
        {
            columns.add(ColumnDescription{TimeSeriesColumnNames::Scalar, time_series_table_info.value_data_type});
            break;
        }
        case ResultType::STRING:
        {
            columns.add(ColumnDescription{TimeSeriesColumnNames::String, std::make_shared<DataTypeString>()});
            break;
        }
        case ResultType::INSTANT_VECTOR:
        {
            columns.add(
                ColumnDescription{
                    TimeSeriesColumnNames::Tags,
                    std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
                        DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}))});
            columns.add(
                ColumnDescription{
                    TimeSeriesColumnNames::Timestamp,
                    time_series_table_info.timestamp_data_type});
            columns.add(
                ColumnDescription{
                    TimeSeriesColumnNames::Value,
                    time_series_table_info.value_data_type});
            break;
        }
        case ResultType::RANGE_VECTOR:
        {
            columns.add(
                ColumnDescription{
                    TimeSeriesColumnNames::Tags,
                    std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
                        DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}))});
            columns.add(
                ColumnDescription{
                    TimeSeriesColumnNames::TimeSeries,
                    std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
                        DataTypes{time_series_table_info.timestamp_data_type, time_series_table_info.value_data_type}))});
            break;
        }
    }
    return columns;
}

}
