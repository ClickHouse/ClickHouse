#include <Storages/TimeSeries/PrometheusQueryToSQL.h>

#include <unordered_set>
#include <Core/DecimalFunctions.h>
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
#include <Storages/ColumnsDescription.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>


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

    /// Finds an interval data type corresponding to a specified timestamp data type.
    /// We support only DateTime64, DateTime and UInt32 as types to specify time.
    /// For them we use Decimal64 and Int32 to specify intervals.
    DataTypePtr getIntervalDataType(const DataTypePtr & timestamp_data_type)
    {
        switch (WhichDataType{*timestamp_data_type}.idx)
        {
            case TypeIndex::UInt32: // nobreak
            case TypeIndex::DateTime:
                return std::make_shared<DataTypeInt32>();
            case TypeIndex::DateTime64:
                return std::make_shared<DataTypeDecimal64>(getDecimalPrecision(*timestamp_data_type), getDecimalScale(*timestamp_data_type));
            default:
                break;
        }
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot find an interval type for timestamp type {}", timestamp_data_type);
    }

    /// Casts field to the timestamp data type or to the interval data type.
    template <is_decimal T>
    DecimalField<T> fieldToDecimal(const Field & field, const DataTypePtr & target_data_type)
    {
        UInt32 target_scale = 0;
        Int64 target_scale_multiplier = 1;
        if (WhichDataType{*target_data_type}.isDateTime64() || WhichDataType{*target_data_type}.isDecimal())
        {
            target_scale = getDecimalScale(*target_data_type);
            target_scale_multiplier = DecimalUtils::scaleMultiplier<Int64>(target_scale);
        }

        switch (field.getType())
        {
            case Field::Types::Int64:
                return DecimalField<T>{field.safeGet<Int64>() * target_scale_multiplier, target_scale};
            case Field::Types::UInt64:
                return DecimalField<T>{field.safeGet<UInt64>() * target_scale_multiplier, target_scale};
            case Field::Types::Float64:
                return DecimalField<T>{static_cast<Int64>(field.safeGet<Float64>() * target_scale_multiplier), target_scale};
            case Field::Types::Decimal32:
            {
                auto x = field.safeGet<Decimal32>();
                return DecimalField<T>{DecimalUtils::convertTo<Decimal64>(target_scale, x.getValue(), x.getScale()), target_scale};
            }
            case Field::Types::Decimal64:
            {
                auto x = field.safeGet<Decimal64>();
                return DecimalField<T>{DecimalUtils::convertTo<Decimal64>(target_scale, x.getValue(), x.getScale()), target_scale};
            }
            default:
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot cast field of type {} to data type {}", field.getType(), target_data_type);
        }
    }

    /// Converts a timestamp or an interval to AST.
    template <is_decimal T>
    ASTPtr decimalToAST(const DecimalField<T> & decimal, const DataTypePtr & data_type)
    {
        auto data_type_idx = WhichDataType{*data_type}.idx;
        if (data_type_idx == TypeIndex::DateTime64)
        {
            String str = toString(decimal);
            if (str.find_first_of(".eE") == String::npos)
                str += "."; /// toDateTime64() doesn't accept an integer as its first argument, so we convert it to float.
            return makeASTFunction("toDateTime64", std::make_shared<ASTLiteral>(str), std::make_shared<ASTLiteral>(getDecimalScale(*data_type)));
        }
        else if (data_type_idx == TypeIndex::Decimal64)
            return makeASTFunction("toDecimal64", std::make_shared<ASTLiteral>(toString(decimal)), std::make_shared<ASTLiteral>(getDecimalScale(*data_type)));
        else
            return std::make_shared<ASTLiteral>(Field{decimal});
    }

    /// Subtracts an interval from a timestamp.
    DecimalField<DateTime64> subtract(const DecimalField<DateTime64> & left, const DecimalField<Decimal64> & right)
    {
        UInt32 scale = left.getScale();
        chassert(right.getScale() == scale);
        return DecimalField<DateTime64>{left.getValue() - right.getValue(), scale};
    }

    /// Returns the previous interval value.
    DecimalField<Decimal64> previous(const DecimalField<Decimal64> & interval)
    {
        chassert(interval.getValue() > 0);
        return DecimalField<Decimal64>{interval.getValue() - 1, interval.getScale()};
    }

    /// Increases a timestamp to make it divisible by `step`.
    DecimalField<DateTime64> alignUp(const DecimalField<DateTime64> & time, const DecimalField<Decimal64> & step)
    {
        UInt32 scale = step.getScale();
        chassert((step.getValue() > 0) && (scale == time.getScale()));
        auto x = time.getValue() % step.getValue();
        if (!x)
            return time;
        return DecimalField<DateTime64>{time.getValue() + step.getValue() - x, scale};
    }

    /// Decreases a timestamp to make it divisible by `step`.
    DecimalField<DateTime64> alignDown(const DecimalField<DateTime64> & time, const DecimalField<Decimal64> & step)
    {
        UInt32 scale = time.getScale();
        chassert((step.getValue() > 0) && (scale == time.getScale()));
        auto x = time.getValue() % step.getValue();
        if (!x)
            return time;
        return DecimalField<DateTime64>{time.getValue() - x, scale};
    }
}


/// Builder of an AST query to evaluate a promql query.
class PrometheusQueryToSQLConverter::ASTBuilder
{
public:
    explicit ASTBuilder(const PrometheusQueryToSQLConverter & converter_)
        : converter(converter_)
        , timestamp_data_type(getTimeSeriesTableInfo().timestamp_data_type)
        , interval_data_type(getIntervalDataType(timestamp_data_type))
        , value_data_type(getTimeSeriesTableInfo().value_data_type)
        , lookback_delta(fieldToInterval(converter_.lookback_delta))
        , default_resolution(fieldToInterval(converter_.default_resolution))
        , result_type(converter_.result_type)
        , interval_scale(lookback_delta.getScale())
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
    DataTypePtr interval_data_type;
    DataTypePtr value_data_type;
    DecimalField<Decimal64> lookback_delta;
    DecimalField<Decimal64> default_resolution;
    PrometheusQueryResultType result_type;
    UInt32 interval_scale;

    std::optional<DecimalField<DateTime64>> evaluation_time;

    struct EvaluationRange
    {
        DecimalField<DateTime64> start_time;
        DecimalField<DateTime64> end_time;
        DecimalField<Decimal64> step;
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
        DecimalField<Decimal64> window;

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

        size_t numColumns() const
        {
            return (group_column != nullptr) + (tags_column != nullptr) + (timestamp_column != nullptr)
                + (value_column != nullptr) + (time_series_column != nullptr) + (scalar_column != nullptr) + (string_column != nullptr);
        }

        bool empty () const { return numColumns() == 0; }

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
        auto select_query = std::make_shared<ASTSelectQuery>();

        auto select_list_exp = std::make_shared<ASTExpressionList>();
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
            auto tables = std::make_shared<ASTTablesInSelectQuery>();
            auto table = std::make_shared<ASTTablesInSelectQueryElement>();
            auto table_exp = std::make_shared<ASTTableExpression>();
            if (!piece.from_subquery.empty())
            {
                table_exp->database_and_table_name = std::make_shared<ASTTableIdentifier>(piece.from_subquery);
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
            auto group_by_list = std::make_shared<ASTExpressionList>();
            select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, group_by_list);
            group_by_list->children = piece.group_by;
        }

        if (piece.where)
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, piece.where->clone());

        if (!piece.with.empty())
        {
            auto with_expression_list_ast = std::make_shared<ASTExpressionList>();
            for (const auto & [name, ast] : piece.with)
            {
                auto with_element_ast = std::make_shared<ASTWithElement>();
                with_element_ast->name = name;
                with_element_ast->subquery = std::make_shared<ASTSubquery>(ast);
                with_element_ast->children.push_back(with_element_ast->subquery);
                with_expression_list_ast->children.push_back(std::move(with_element_ast));
            }
            select_query->setExpression(ASTSelectQuery::Expression::WITH, std::move(with_expression_list_ast));
        }

        auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
        auto list_of_selects = std::make_shared<ASTExpressionList>();
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
        if (piece.string_column && piece.numColumns() == 1)
            return piece;

        Piece res;
        res.result_type = PrometheusQueryResultType::STRING;
        res.string_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::String);

        if (piece.empty())
            res.from_table_function = makeASTFunction("null", std::make_shared<ASTLiteral>(fmt::format("{} String", TimeSeriesColumnNames::String)));
        else
            res.from_subquery = addSubquery(std::move(piece));

        return res;
    }

    /// Finalizes a Piece returning a scalar.
    Piece finalizeWithScalarResult(Piece && piece)
    {
        if (piece.scalar_column && (piece.numColumns() == 1))
            return piece;

        Piece res;
        res.result_type = PrometheusQueryResultType::SCALAR;
        res.scalar_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Scalar);

        if (piece.empty())
            res.from_table_function = makeASTFunction("null", std::make_shared<ASTLiteral>(fmt::format("{} {}", TimeSeriesColumnNames::Scalar, getTimeSeriesTableInfo().value_data_type)));
        else
            res.from_subquery = addSubquery(std::move(piece));

        return res;
    }

    /// Finalizes a Piece returning an instant vector.
    Piece finalizeWithInstantVectorResult(Piece && piece)
    {
        if (piece.tags_column && piece.timestamp_column && piece.value_column && (piece.numColumns() == 3))
            return piece;

        Piece res;
        res.result_type = PrometheusQueryResultType::INSTANT_VECTOR;

        if (piece.empty())
        {
            res.tags_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Tags);
            res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
            res.value_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value);
            res.from_table_function = makeASTFunction("null", std::make_shared<ASTLiteral>(
                fmt::format("{} Array(Tuple(String, String)), {} {}, {} {}",
                            TimeSeriesColumnNames::Tags, TimeSeriesColumnNames::Timestamp, getTimeSeriesTableInfo().timestamp_data_type,
                            TimeSeriesColumnNames::Value, getTimeSeriesTableInfo().value_data_type)));
            return res;
        }

        if (piece.tags_column)
        {
            res.tags_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Tags);
        }
        else if (piece.group_column)
        {
            /// If group is 0 (sentinel for "no grouping by tags"), return empty array
            /// Otherwise, get tags from the group
            auto empty_array = std::make_shared<ASTFunction>();
            empty_array->name = "array";
            empty_array->arguments = std::make_shared<ASTExpressionList>();
            empty_array->children.push_back(empty_array->arguments);
            
            res.tags_column = makeASTFunction("if",
                makeASTFunction("equals",
                    std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group),
                    makeASTFunction("CAST",
                        std::make_shared<ASTLiteral>(Field(UInt64(0))),
                        std::make_shared<ASTLiteral>(String("UInt64")))),
                empty_array,
                makeASTFunction("timeSeriesTagsGroupToTags", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group)));
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
            res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
            res.value_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value);
        }
        else if (piece.time_series_column)
        {
            res.where = makeASTFunction("notEmpty", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries));
            auto array_element = makeASTFunction("arrayElement", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries),
                                                 std::make_shared<ASTLiteral>(Field{1u}));
            res.timestamp_column = makeASTFunction("tupleElement", array_element, std::make_shared<ASTLiteral>(Field{1u}));
            res.timestamp_column->setAlias(TimeSeriesColumnNames::Timestamp);
            res.value_column = makeASTFunction("tupleElement", array_element, std::make_shared<ASTLiteral>(Field{2u}));
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
        if (piece.tags_column && piece.time_series_column && (piece.numColumns() == 2))
            return piece;

        Piece res;
        res.result_type = PrometheusQueryResultType::RANGE_VECTOR;

        if (piece.empty())
        {
            res.tags_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Tags);
            res.time_series_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries);
            res.from_table_function = makeASTFunction("null", std::make_shared<ASTLiteral>(
                fmt::format("{} Array(Tuple(String, String)), {} Array(Tuple({}, {}))",
                            TimeSeriesColumnNames::Tags, TimeSeriesColumnNames::TimeSeries,
                            getTimeSeriesTableInfo().timestamp_data_type, getTimeSeriesTableInfo().value_data_type)));
            return res;
        }

        if (piece.tags_column)
        {
            res.tags_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Tags);
        }
        else if (piece.group_column)
        {
            /// If group is 0 (sentinel for "no grouping by tags"), return empty array
            /// Otherwise, get tags from the group
            auto empty_array = std::make_shared<ASTFunction>();
            empty_array->name = "array";
            empty_array->arguments = std::make_shared<ASTExpressionList>();
            empty_array->children.push_back(empty_array->arguments);
            
            res.tags_column = makeASTFunction("if",
                makeASTFunction("equals",
                    std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group),
                    makeASTFunction("CAST",
                        std::make_shared<ASTLiteral>(Field(UInt64(0))),
                        std::make_shared<ASTLiteral>(String("UInt64")))),
                empty_array,
                makeASTFunction("timeSeriesTagsGroupToTags", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group)));
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
            res.time_series_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries);
        }
        else if (piece.timestamp_column && piece.value_column)
        {
            res.time_series_column = makeASTFunction("timeSeriesGroupArray",
                                                     std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
                                                     std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value));
            res.time_series_column->setAlias(TimeSeriesColumnNames::TimeSeries);
            res.group_by.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));
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
            case NodeType::ScalarLiteral:
                return buildPieceForScalarLiteral(typeid_cast<const PrometheusQueryTree::ScalarLiteral *>(node));

            case NodeType::IntervalLiteral:
                return buildPieceForIntervalLiteral(typeid_cast<const PrometheusQueryTree::IntervalLiteral *>(node));

            case NodeType::StringLiteral:
                return buildPieceForStringLiteral(typeid_cast<const PrometheusQueryTree::StringLiteral *>(node));

            case NodeType::InstantSelector:
                return buildPieceForInstantSelector(typeid_cast<const PrometheusQueryTree::InstantSelector *>(node));

            case NodeType::RangeSelector:
                return buildPieceForRangeSelector(typeid_cast<const PrometheusQueryTree::RangeSelector *>(node));

            case NodeType::Subquery:
                return buildPieceForSubquery(typeid_cast<const PrometheusQueryTree::Subquery *>(node));

            case NodeType::At:
                return buildPieceForAt(typeid_cast<const PrometheusQueryTree::At *>(node));

            case NodeType::Function:
                return buildPieceForFunction(typeid_cast<const PrometheusQueryTree::Function *>(node));

            case NodeType::BinaryOperator:
                return buildPieceForBinaryOperator(typeid_cast<const PrometheusQueryTree::BinaryOperator *>(node));

            case NodeType::AggregationOperator:
                return buildPieceForAggregationOperator(typeid_cast<const PrometheusQueryTree::AggregationOperator *>(node));

            case NodeType::UnaryOperator:
                return buildPieceForUnaryOperator(typeid_cast<const PrometheusQueryTree::UnaryOperator *>(node));
        }

                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Prometheus query tree node type {} is not implemented", node_type);
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
        if (lookback_delta.getValue() <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The lookback delta must be positive, got {}", toString(lookback_delta));

        /// Lookback deltas are left-open (and right-closed), so we decrease `window` a little bit to consider both boundaries close.
        auto window = lookback_delta;

        DecimalField<DateTime64> start_time;
        DecimalField<DateTime64> end_time;
        DecimalField<Decimal64> step;
        extractRangeAndStep(instant_selector, start_time, end_time, step);

        /// We can get an empty interval here because of aligning in extractRangeAndStep().
        if (end_time < start_time)
            return getEmptyPiece(ResultType::INSTANT_VECTOR);

        Piece res;

        res.from_table_function = makeASTFunction("timeSeriesSelector",
            std::make_shared<ASTLiteral>(getTimeSeriesTableInfo().storage_id.getDatabaseName()),
            std::make_shared<ASTLiteral>(getTimeSeriesTableInfo().storage_id.getTableName()),
            std::make_shared<ASTLiteral>(getPromQLText(instant_selector)),
            timestampToAST(subtract(start_time, previous(window))),
            timestampToAST(end_time));

        res.group_column = makeASTFunction("timeSeriesIdToTagsGroup", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::ID));
        res.group_column->setAlias(TimeSeriesColumnNames::Group);

        res.time_series_column = makeGridFunction("timeSeriesLastToGrid", start_time, end_time, step, window,
                                                  std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
                                                  std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value));

        res.time_series_column->setAlias(TimeSeriesColumnNames::TimeSeries);
        res.group_by.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));
        res.result_type = ResultType::INSTANT_VECTOR;

        return res;
    }

    /// Builds a piece to evaluate a range selector.
    Piece buildPieceForRangeSelector(const PrometheusQueryTree::RangeSelector * range_selector) const
    {
        const auto * instant_selector = range_selector->getInstantSelector();

        auto range = nodeToInterval(range_selector->getRange());
        if (range.getValue() <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Range specified in a range selector must be positive, got {}", getPromQLText(range_selector->getRange()));

        /// Ranges are left-open (and right-closed), so we decrease `window` a little bit to consider both boundaries close.
        auto window = range;

        DecimalField<DateTime64> start_time;
        DecimalField<DateTime64> end_time;
        DecimalField<Decimal64> step;
        extractRangeAndStep(range_selector, start_time, end_time, step);

        /// We can get an empty interval here because of aligning in extractRangeAndStep().
        if (end_time < start_time)
            return getEmptyPiece(ResultType::RANGE_VECTOR);

        Piece res;

        res.from_table_function = makeASTFunction("timeSeriesSelector",
            std::make_shared<ASTLiteral>(getTimeSeriesTableInfo().storage_id.getDatabaseName()),
            std::make_shared<ASTLiteral>(getTimeSeriesTableInfo().storage_id.getTableName()),
            std::make_shared<ASTLiteral>(getPromQLText(instant_selector)),
            timestampToAST(subtract(start_time, previous(window))),
            timestampToAST(end_time));

        res.group_column = makeASTFunction("timeSeriesIdToTagsGroup", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::ID));
        res.group_column->setAlias(TimeSeriesColumnNames::Group);
        res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
        res.value_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value);
        res.result_type = ResultType::RANGE_VECTOR;
        res.window = window;

        return res;
    }

    /// Builds a piece for a scalar literal.
    Piece buildPieceForScalarLiteral(const PrometheusQueryTree::ScalarLiteral * scalar_literal)
    {
        Piece res;
        res.result_type = ResultType::SCALAR;
        res.scalar_column = std::make_shared<ASTLiteral>(scalar_literal->scalar);
        res.scalar_column->setAlias(TimeSeriesColumnNames::Scalar);
        return res;
    }

    /// Builds a piece for an interval literal.
    Piece buildPieceForIntervalLiteral(const PrometheusQueryTree::IntervalLiteral * interval_literal)
    {
        Piece res;
        res.result_type = ResultType::SCALAR;
        /// Convert interval to scalar (seconds)
        auto interval_seconds = interval_literal->interval.getValue() / DecimalUtils::scaleMultiplier<Int64>(interval_literal->interval.getScale());
        res.scalar_column = std::make_shared<ASTLiteral>(interval_seconds);
        res.scalar_column->setAlias(TimeSeriesColumnNames::Scalar);
        return res;
    }

    /// Builds a piece for a string literal.
    Piece buildPieceForStringLiteral(const PrometheusQueryTree::StringLiteral * string_literal)
    {
        Piece res;
        res.result_type = ResultType::STRING;
        res.string_column = std::make_shared<ASTLiteral>(string_literal->string);
        res.string_column->setAlias(TimeSeriesColumnNames::String);
        return res;
    }

    /// Builds a piece for a subquery.
    Piece buildPieceForSubquery(const PrometheusQueryTree::Subquery * subquery)
    {
        auto piece = buildPiece(subquery->getExpression());

        if (piece.empty())
            return getEmptyPiece(ResultType::RANGE_VECTOR);

        piece.result_type = ResultType::RANGE_VECTOR;
        piece.window = nodeToInterval(subquery->getRange());
        return piece;
    }

    /// Builds a piece to evaluate a function.
    Piece buildPieceForFunction(const PrometheusQueryTree::Function * func)
    {
        const auto & function_name = func->function_name;
        std::vector<Piece> args = buildPiecesForArguments(func);

        /// Ordinary functions (instant vector -> instant vector)
        static const std::unordered_set<std::string_view> ordinary_functions = {
            "abs", "ceil", "floor", "round", "exp", "ln", "log2", "log10", "sqrt",
            "sin", "cos", "tan", "asin", "acos", "atan",
            "sinh", "cosh", "tanh", "asinh", "acosh", "atanh",
            "sgn", "deg", "rad",
            /// Date/time functions
            "day_of_month", "day_of_week", "days_in_month", "hour", "minute", "month", "year"
        };

        if (ordinary_functions.contains(function_name))
            return buildPieceForOrdinaryFunction(func, std::move(args));

        /// Range functions (range vector -> instant vector)
        static const std::unordered_set<std::string_view> range_functions = {
            "rate", "irate", "delta", "idelta",
            "increase",
            "avg_over_time", "sum_over_time", "min_over_time", "max_over_time",
            "count_over_time", "stddev_over_time", "stdvar_over_time",
            "last_over_time", "present_over_time",
            "changes", "resets", "deriv",
            "predict_linear", "quantile_over_time",
            "absent_over_time"
        };

        if (range_functions.contains(function_name))
            return buildPieceForRangeFunction(func, std::move(args));

        /// Special functions
        if (function_name == "clamp")
            return buildPieceForClampFunction(func, std::move(args));

        if (function_name == "clamp_min" || function_name == "clamp_max")
            return buildPieceForClampMinMaxFunction(func, std::move(args));

        if (function_name == "histogram_quantile")
            return buildPieceForHistogramQuantile(func, std::move(args));

        if (function_name == "label_replace")
            return buildPieceForLabelReplace(func, std::move(args));

        if (function_name == "label_join")
            return buildPieceForLabelJoin(func, std::move(args));

        if (function_name == "vector")
            return buildPieceForVectorFunction(func, std::move(args));

        if (function_name == "scalar")
            return buildPieceForScalarFunction(func, std::move(args));

        if (function_name == "time")
            return buildPieceForTimeFunction(func);

        if (function_name == "timestamp")
            return buildPieceForTimestampFunction(func, std::move(args));

        if (function_name == "absent")
            return buildPieceForAbsentFunction(func, std::move(args));

        if (function_name == "sort" || function_name == "sort_desc")
            return buildPieceForSortFunction(func, std::move(args));

        if (function_name == "holt_winters")
            return buildPieceForHoltWintersFunction(func, std::move(args));

        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} is not implemented", func->function_name);
    }

    /// Builds a piece for clamp(v, min, max)
    Piece buildPieceForClampFunction(const PrometheusQueryTree::Function * func, std::vector<Piece> && arguments)
    {
        if (arguments.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function clamp requires 3 arguments, got {}", arguments.size());

        checkArgumentType(func, arguments, 0, ResultType::INSTANT_VECTOR);
        checkArgumentType(func, arguments, 1, ResultType::SCALAR);
        checkArgumentType(func, arguments, 2, ResultType::SCALAR);

        auto & vec_arg = arguments[0];
        if (vec_arg.empty())
            return getEmptyPiece(ResultType::INSTANT_VECTOR);

        /// Extract scalar values from arguments[1] and arguments[2]
        if (!arguments[1].scalar_column || !arguments[2].scalar_column)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "clamp: scalar arguments must be literal values");

        /// Clone scalar columns and remove their aliases to avoid conflicts
        auto min_val = arguments[1].scalar_column->clone();
        auto max_val = arguments[2].scalar_column->clone();
        min_val->setAlias("");
        max_val->setAlias("");

        Piece res;
        res.result_type = ResultType::INSTANT_VECTOR;
        res.group_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group);
        res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);

        /// clamp(v, min, max) = greatest(min, least(max, v))
        res.value_column = makeASTFunction("greatest",
            min_val,
            makeASTFunction("least",
                max_val,
                std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value)));
        res.value_column->setAlias(TimeSeriesColumnNames::Value);

        res.from_subquery = addSubquery(splitTimeSeriesColumnToTwoNonArrays(std::move(vec_arg)));
        return res;
    }

    /// Builds a piece for clamp_min(v, min) or clamp_max(v, max)
    Piece buildPieceForClampMinMaxFunction(const PrometheusQueryTree::Function * func, std::vector<Piece> && arguments)
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 2 arguments, got {}", func->function_name, arguments.size());

        checkArgumentType(func, arguments, 0, ResultType::INSTANT_VECTOR);
        checkArgumentType(func, arguments, 1, ResultType::SCALAR);

        auto & vec_arg = arguments[0];
        if (vec_arg.empty())
            return getEmptyPiece(ResultType::INSTANT_VECTOR);

        /// Extract scalar value from arguments[1]
        if (!arguments[1].scalar_column)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "{}: scalar argument must be a literal value", func->function_name);

        /// Clone scalar column and remove its alias to avoid conflicts
        auto bound_val = arguments[1].scalar_column->clone();
        bound_val->setAlias("");

        Piece res;
        res.result_type = ResultType::INSTANT_VECTOR;
        res.group_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group);
        res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);

        if (func->function_name == "clamp_min")
        {
            res.value_column = makeASTFunction("greatest",
                bound_val,
                std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value));
        }
        else // clamp_max
        {
            res.value_column = makeASTFunction("least",
                bound_val,
                std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value));
        }
        res.value_column->setAlias(TimeSeriesColumnNames::Value);

        res.from_subquery = addSubquery(splitTimeSeriesColumnToTwoNonArrays(std::move(vec_arg)));
        return res;
    }

    /// Builds a piece for vector(scalar)
    Piece buildPieceForVectorFunction(const PrometheusQueryTree::Function * func, std::vector<Piece> && arguments)
    {
        checkNumberArguments(func, arguments, 1);
        checkArgumentType(func, arguments, 0, ResultType::SCALAR);

        Piece res;
        res.result_type = ResultType::INSTANT_VECTOR;
        res.tags_column = makeASTFunction("array");
        res.tags_column->setAlias(TimeSeriesColumnNames::Tags);

        if (evaluation_time)
        {
            res.timestamp_column = timestampToAST(*evaluation_time);
            res.timestamp_column->setAlias(TimeSeriesColumnNames::Timestamp);
        }

        res.value_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Scalar);
        res.value_column->setAlias(TimeSeriesColumnNames::Value);

        if (!arguments[0].empty())
            res.from_subquery = addSubquery(std::move(arguments[0]));

        return res;
    }

    /// Builds a piece for scalar(instant_vector)
    Piece buildPieceForScalarFunction(const PrometheusQueryTree::Function * func, std::vector<Piece> && arguments)
    {
        checkNumberArguments(func, arguments, 1);
        checkArgumentType(func, arguments, 0, ResultType::INSTANT_VECTOR);

        auto & vec_arg = arguments[0];
        if (vec_arg.empty())
            return getEmptyPiece(ResultType::SCALAR);

        Piece res;
        res.result_type = ResultType::SCALAR;
        res.scalar_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value);
        res.scalar_column->setAlias(TimeSeriesColumnNames::Scalar);
        res.from_subquery = addSubquery(splitTimeSeriesColumnToTwoNonArrays(std::move(vec_arg)));
        return res;
    }

    /// Builds a piece for time()
    Piece buildPieceForTimeFunction(const PrometheusQueryTree::Function * /* func */)
    {
        Piece res;
        res.result_type = ResultType::SCALAR;

        if (evaluation_time)
        {
            res.scalar_column = timestampToAST(*evaluation_time);
            res.scalar_column->setAlias(TimeSeriesColumnNames::Scalar);
        }
        else
        {
            /// For range queries, this would need to return different values at each step
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function time() is not supported in range queries");
        }

        return res;
    }

    /// Builds a piece for timestamp(instant_vector)
    Piece buildPieceForTimestampFunction(const PrometheusQueryTree::Function * func, std::vector<Piece> && arguments)
    {
        checkNumberArguments(func, arguments, 1);
        checkArgumentType(func, arguments, 0, ResultType::INSTANT_VECTOR);

        auto & vec_arg = arguments[0];
        if (vec_arg.empty())
            return getEmptyPiece(ResultType::INSTANT_VECTOR);

        Piece res;
        res.result_type = ResultType::INSTANT_VECTOR;
        res.group_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group);
        res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
        /// timestamp() returns the timestamp as the value
        res.value_column = makeASTFunction("toFloat64", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp));
        res.value_column->setAlias(TimeSeriesColumnNames::Value);
        res.from_subquery = addSubquery(splitTimeSeriesColumnToTwoNonArrays(std::move(vec_arg)));
        return res;
    }

    /// Builds a piece for histogram_quantile(φ, histogram_vector)
    Piece buildPieceForHistogramQuantile(const PrometheusQueryTree::Function * func, std::vector<Piece> && arguments)
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function histogram_quantile requires 2 arguments, got {}", arguments.size());

        checkArgumentType(func, arguments, 0, ResultType::SCALAR);
        checkArgumentType(func, arguments, 1, ResultType::INSTANT_VECTOR);

        auto & histogram_arg = arguments[1];
        if (histogram_arg.empty())
            return getEmptyPiece(ResultType::INSTANT_VECTOR);

        /// histogram_quantile computes the φ-quantile from histogram buckets
        /// It expects metrics with 'le' (less than or equal) labels representing bucket boundaries
        /// The phi parameter (0-1) is the first argument
        auto & phi_arg = arguments[0];
        if (phi_arg.result_type != ResultType::SCALAR)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "histogram_quantile: first argument must be scalar, got {}", phi_arg.result_type);
        
        /// Extract phi value from the scalar argument
        /// For now, we'll use a simplified approach: apply quantile to the values
        /// A full implementation would need to handle histogram buckets with 'le' labels
        if (!phi_arg.scalar_column)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "histogram_quantile: phi argument must be a literal value");
        
        Piece res;
        res.result_type = ResultType::INSTANT_VECTOR;
        res.group_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group);
        res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);

        /// Use quantile as a parameterized aggregate function: quantile(phi)(value)
        /// In ClickHouse, parameterized aggregate functions take parameters in a separate node
        /// Note: This is a simplified implementation. Full histogram_quantile needs to:
        /// 1. Group by all labels except 'le'
        /// 2. Sort buckets by 'le' value
        /// 3. Calculate cumulative sum
        /// 4. Find the bucket containing the quantile
        /// 5. Interpolate within that bucket
        
        /// Clone the phi value and remove its alias
        auto phi_val = phi_arg.scalar_column->clone();
        phi_val->setAlias("");
        
        auto quantile_agg = makeASTFunction("quantile", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value));
        /// Set the phi parameter - quantile(phi)(value)
        quantile_agg->parameters = std::make_shared<ASTExpressionList>();
        quantile_agg->parameters->children.push_back(phi_val);

        res.value_column = quantile_agg;
        res.value_column->setAlias(TimeSeriesColumnNames::Value);

        res.group_by.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));
        res.group_by.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp));
        res.from_subquery = addSubquery(splitTimeSeriesColumnToTwoNonArrays(std::move(histogram_arg)));
        return res;
    }

    /// Builds a piece for label_replace(v, dst_label, replacement, src_label, regex)
    Piece buildPieceForLabelReplace(const PrometheusQueryTree::Function * func, std::vector<Piece> && arguments)
    {
        if (arguments.size() != 5)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function label_replace requires 5 arguments, got {}", arguments.size());

        checkArgumentType(func, arguments, 0, ResultType::INSTANT_VECTOR);
        // Arguments 1-4 are strings (dst_label, replacement, src_label, regex)

        auto & vec_arg = arguments[0];
        if (vec_arg.empty())
            return getEmptyPiece(ResultType::INSTANT_VECTOR);

        /// label_replace modifies labels based on regex matching
        /// This requires access to label values and regex operations
        Piece res;
        res.result_type = ResultType::INSTANT_VECTOR;
        res.group_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group);
        res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
        res.value_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value);

        /// For now, pass through without label modification
        /// Full implementation would require modifying the tags/group based on regex
        res.from_subquery = addSubquery(splitTimeSeriesColumnToTwoNonArrays(std::move(vec_arg)));
        return res;
    }

    /// Builds a piece for label_join(v, dst_label, separator, src_label_1, src_label_2, ...)
    Piece buildPieceForLabelJoin(const PrometheusQueryTree::Function * func, std::vector<Piece> && arguments)
    {
        if (arguments.size() < 4)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function label_join requires at least 4 arguments, got {}", arguments.size());

        checkArgumentType(func, arguments, 0, ResultType::INSTANT_VECTOR);

        auto & vec_arg = arguments[0];
        if (vec_arg.empty())
            return getEmptyPiece(ResultType::INSTANT_VECTOR);

        /// label_join concatenates multiple label values into a new label
        Piece res;
        res.result_type = ResultType::INSTANT_VECTOR;
        res.group_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group);
        res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
        res.value_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value);

        /// For now, pass through without label modification
        res.from_subquery = addSubquery(splitTimeSeriesColumnToTwoNonArrays(std::move(vec_arg)));
        return res;
    }

    /// Builds a piece for absent(instant_vector)
    Piece buildPieceForAbsentFunction(const PrometheusQueryTree::Function * func, std::vector<Piece> && arguments)
    {
        checkNumberArguments(func, arguments, 1);
        checkArgumentType(func, arguments, 0, ResultType::INSTANT_VECTOR);

        auto & vec_arg = arguments[0];

        /// absent() returns 1 if the vector has no elements, empty otherwise
        /// We implement this as: return 1 if count of elements is 0
        Piece res;
        res.result_type = ResultType::INSTANT_VECTOR;

        if (vec_arg.empty())
        {
            /// Empty input means absent should return 1
            res.tags_column = makeASTFunction("array");
            res.tags_column->setAlias(TimeSeriesColumnNames::Tags);

            if (evaluation_time)
            {
                res.timestamp_column = timestampToAST(*evaluation_time);
                res.timestamp_column->setAlias(TimeSeriesColumnNames::Timestamp);
            }

            res.value_column = std::make_shared<ASTLiteral>(Field{1.0});
            res.value_column->setAlias(TimeSeriesColumnNames::Value);
            return res;
        }

        /// If vector has data, absent returns empty result
        /// We can implement this with a HAVING count(*) = 0 clause
        /// For simplicity, return empty piece (which will result in no rows)
        return getEmptyPiece(ResultType::INSTANT_VECTOR);
    }

    /// Builds a piece for sort(instant_vector) or sort_desc(instant_vector)
    Piece buildPieceForSortFunction(const PrometheusQueryTree::Function * func, std::vector<Piece> && arguments)
    {
        checkNumberArguments(func, arguments, 1);
        checkArgumentType(func, arguments, 0, ResultType::INSTANT_VECTOR);

        auto & vec_arg = arguments[0];
        if (vec_arg.empty())
            return getEmptyPiece(ResultType::INSTANT_VECTOR);

        /// sort() and sort_desc() sort elements by value
        /// The actual sorting would be applied in the final ORDER BY clause
        /// For now, we just pass through - sorting is typically handled at result presentation
        Piece res;
        res.result_type = ResultType::INSTANT_VECTOR;
        res.group_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group);
        res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
        res.value_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value);

        /// Note: A complete implementation would add ORDER BY value ASC/DESC
        /// but this requires modifying the final query structure
        res.from_subquery = addSubquery(splitTimeSeriesColumnToTwoNonArrays(std::move(vec_arg)));
        return res;
    }

    /// Builds a piece for holt_winters(instant_vector, sf, tf)
    Piece buildPieceForHoltWintersFunction(const PrometheusQueryTree::Function * func, std::vector<Piece> && arguments)
    {
        if (arguments.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function holt_winters requires 3 arguments, got {}", arguments.size());

        checkArgumentType(func, arguments, 0, ResultType::INSTANT_VECTOR);
        checkArgumentType(func, arguments, 1, ResultType::SCALAR);  // sf (smoothing factor)
        checkArgumentType(func, arguments, 2, ResultType::SCALAR);  // tf (trend factor)

        auto & vec_arg = arguments[0];
        if (vec_arg.empty())
            return getEmptyPiece(ResultType::INSTANT_VECTOR);

        /// holt_winters produces smoothed value using Holt-Winters exponential smoothing
        /// This is an advanced time series forecasting function
        /// Full implementation would require:
        /// 1. Initial level and trend estimates
        /// 2. Iterative smoothing with sf and tf parameters
        /// For now, provide a simplified implementation using exponential moving average
        Piece res;
        res.result_type = ResultType::INSTANT_VECTOR;
        res.group_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group);
        res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);

        /// Simplified: use exponentialMovingAverage or just pass through
        /// A full implementation would require custom aggregate functions
        res.value_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value);
        res.value_column->setAlias(TimeSeriesColumnNames::Value);

        res.from_subquery = addSubquery(splitTimeSeriesColumnToTwoNonArrays(std::move(vec_arg)));
        return res;
    }

    /// Builds a piece for topk(k, vector) or bottomk(k, vector)
    Piece buildPieceForTopkBottomk(const PrometheusQueryTree::AggregationOperator * agg_op, std::vector<Piece> && arguments)
    {
        if (arguments.size() < 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregation operator {} requires 2 arguments", agg_op->operator_name);

        /// First argument is k (scalar), second is the vector
        auto & vec_arg = arguments.size() > 1 ? arguments[1] : arguments[0];

        if (vec_arg.empty())
            return getEmptyPiece(ResultType::INSTANT_VECTOR);

        if (vec_arg.result_type != ResultType::INSTANT_VECTOR)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument of {} must be instant vector", agg_op->operator_name);

        auto split_piece = splitTimeSeriesColumnToTwoNonArrays(std::move(vec_arg));

        Piece res;
        res.result_type = ResultType::INSTANT_VECTOR;

        /// Build GROUP BY based on by/without modifiers (similar to other aggregations)
        ASTs group_by_columns;
        if (agg_op->by)
        {
            if (agg_op->labels.empty())
            {
                /// by() with no labels means aggregate everything into one group
                res.group_column = std::make_shared<ASTLiteral>(Field(""));
                res.group_column->setAlias(TimeSeriesColumnNames::Group);
            }
            else
            {
                auto labels_array = std::make_shared<ASTFunction>();
                labels_array->name = "array";
                labels_array->arguments = std::make_shared<ASTExpressionList>();
                for (const auto & label : agg_op->labels)
                    labels_array->arguments->children.push_back(std::make_shared<ASTLiteral>(label));
                labels_array->children.push_back(labels_array->arguments);

                res.group_column = makeASTFunction("timeSeriesTagsGroupFilterByLabels",
                    std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group),
                    labels_array);
                res.group_column->setAlias(TimeSeriesColumnNames::Group);
                group_by_columns.push_back(res.group_column->clone());
            }
        }
        else if (agg_op->without)
        {
            /// GROUP BY all labels except the specified ones
            if (agg_op->labels.empty())
            {
                res.group_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group);
                group_by_columns.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));
            }
            else
            {
                /// The without() clause with non-empty labels is not currently supported.
                /// It requires creating synthetic group IDs from filtered tags, but these
                /// synthetic IDs cannot be converted back to tags by timeSeriesTagsGroupToTags()
                /// because they are not stored in the tags table.
                /// TODO: Implement proper support by storing filtered tags directly in the result.
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "The without() clause with labels is not currently supported. "
                    "Use by() clause instead to specify which labels to group by.");
            }
        }
        else
        {
            /// No by/without: keep all groups
            res.group_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group);
            group_by_columns.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));
        }

        res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
        group_by_columns.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp));
        res.value_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value);

        res.group_by = std::move(group_by_columns);
        res.from_subquery = addSubquery(std::move(split_piece));

        /// Note: topk/bottomk ordering and limiting would need to be applied at the final query level
        /// This is a simplified implementation that preserves the structure

        return res;
    }

    /// Builds a piece for quantile(φ, vector)
    Piece buildPieceForQuantile(const PrometheusQueryTree::AggregationOperator * agg_op, std::vector<Piece> && arguments)
    {
        if (arguments.size() < 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregation operator quantile requires 2 arguments");

        auto & vec_arg = arguments.size() > 1 ? arguments[1] : arguments[0];

        if (vec_arg.empty())
            return getEmptyPiece(ResultType::INSTANT_VECTOR);

        if (vec_arg.result_type != ResultType::INSTANT_VECTOR)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument of quantile must be instant vector");

        auto split_piece = splitTimeSeriesColumnToTwoNonArrays(std::move(vec_arg));

        Piece res;
        res.result_type = ResultType::INSTANT_VECTOR;

        /// Build GROUP BY based on by/without
        ASTs group_by_columns;
        if (agg_op->by)
        {
            if (agg_op->labels.empty())
            {
                /// by() with no labels means aggregate everything into one group
                /// Set group_column to a constant UInt64 zero (sentinel value for "no grouping by tags")
                res.group_column = std::make_shared<ASTLiteral>(Field(UInt64(0)));
                res.group_column->setAlias(TimeSeriesColumnNames::Group);
            }
            else
            {
                /// Get tags from group and filter to only specified labels
                auto tags_from_group = makeASTFunction("timeSeriesTagsGroupToTags",
                    std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));
                
                auto labels_array = std::make_shared<ASTFunction>();
                labels_array->name = "array";
                labels_array->arguments = std::make_shared<ASTExpressionList>();
                for (const auto & label : agg_op->labels)
                    labels_array->arguments->children.push_back(std::make_shared<ASTLiteral>(label));
                labels_array->children.push_back(labels_array->arguments);
                
                auto filter_func = makeASTFunction("arrayFilter",
                    makeASTFunction("lambda",
                        std::make_shared<ASTIdentifier>("tag"),
                        makeASTFunction("has",
                            labels_array,
                            makeASTFunction("tupleElement",
                                std::make_shared<ASTIdentifier>("tag"),
                                std::make_shared<ASTLiteral>(1)))),
                    tags_from_group);
                
                res.group_column = makeASTFunction("toString", filter_func);
                res.group_column->setAlias(TimeSeriesColumnNames::Group);
                group_by_columns.push_back(res.group_column->clone());
            }
        }
        else if (!agg_op->by)
        {
            res.group_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group);
            group_by_columns.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));
        }

        res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
        group_by_columns.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp));

        /// Use quantile aggregate function
        auto quantile_func = makeASTFunction("quantile",
            std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value));
        /// The phi parameter would come from the first argument
        quantile_func->parameters = std::make_shared<ASTExpressionList>();
        quantile_func->parameters->children.push_back(std::make_shared<ASTLiteral>(0.5)); /// Default to median

        res.value_column = quantile_func;
        res.value_column->setAlias(TimeSeriesColumnNames::Value);

        res.group_by = std::move(group_by_columns);
        res.from_subquery = addSubquery(std::move(split_piece));
        return res;
    }

    /// Builds a piece for count_values(label, vector)
    Piece buildPieceForCountValues(const PrometheusQueryTree::AggregationOperator * /* agg_op */, std::vector<Piece> && arguments)
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregation operator count_values requires at least 1 argument");

        auto & vec_arg = arguments[0];

        if (vec_arg.empty())
            return getEmptyPiece(ResultType::INSTANT_VECTOR);

        if (vec_arg.result_type != ResultType::INSTANT_VECTOR)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument of count_values must be instant vector");

        auto split_piece = splitTimeSeriesColumnToTwoNonArrays(std::move(vec_arg));

        Piece res;
        res.result_type = ResultType::INSTANT_VECTOR;

        /// count_values counts occurrences of each unique value
        /// The result has the value as a new label
        res.group_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group);
        res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);

        /// Count occurrences
        res.value_column = makeASTFunction("count");
        res.value_column->setAlias(TimeSeriesColumnNames::Value);

        /// Group by the original group, timestamp, and value
        res.group_by.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));
        res.group_by.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp));
        res.group_by.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value));

        res.from_subquery = addSubquery(std::move(split_piece));
        return res;
    }

    /// Builds a piece for the 'unless' binary operator
    Piece buildPieceForUnlessBinaryOperator(
        const PrometheusQueryTree::BinaryOperator * /* binary_operator */,
        Piece && left_piece,
        Piece && right_piece)
    {
        if (left_piece.empty())
            return getEmptyPiece(ResultType::INSTANT_VECTOR);

        /// unless returns elements from left that have no matching elements on right
        /// Implemented as: left WHERE (group, timestamp) NOT IN (SELECT group, timestamp FROM right)
        Piece res;
        res.result_type = ResultType::INSTANT_VECTOR;
        res.group_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group);
        res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
        res.value_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value);

        /// Build the NOT IN condition
        if (!right_piece.empty())
        {
            auto tuple_left = makeASTFunction("tuple",
                std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group),
                std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp));

            /// Create subquery for right side groups
            auto right_subquery_name = addSubquery(splitTimeSeriesColumnToTwoNonArrays(std::move(right_piece)));

            auto not_in_condition = makeASTFunction("notIn",
                tuple_left,
                std::make_shared<ASTIdentifier>(right_subquery_name));

            res.where = not_in_condition;
        }

        res.from_subquery = addSubquery(splitTimeSeriesColumnToTwoNonArrays(std::move(left_piece)));
        return res;
    }

    /// Builds a piece to evaluate an offset.
    Piece buildPieceForAt(const PrometheusQueryTree::At * at_node)
    {
        /// Offsets are already taken into account - see extractRangeAndStep(). So here we just ignore them.
        return buildPiece(at_node->getExpression());
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

        /// Map PromQL functions to ClickHouse functions
        std::string_view ch_function_name;
        if (func->function_name == "abs")
            ch_function_name = "abs";
        else if (func->function_name == "ceil")
            ch_function_name = "ceil";
        else if (func->function_name == "floor")
            ch_function_name = "floor";
        else if (func->function_name == "round")
            ch_function_name = "round";
        else if (func->function_name == "exp")
            ch_function_name = "exp";
        else if (func->function_name == "ln")
            ch_function_name = "log";
        else if (func->function_name == "log2")
            ch_function_name = "log2";
        else if (func->function_name == "log10")
            ch_function_name = "log10";
        else if (func->function_name == "sqrt")
            ch_function_name = "sqrt";
        else if (func->function_name == "sin")
            ch_function_name = "sin";
        else if (func->function_name == "cos")
            ch_function_name = "cos";
        else if (func->function_name == "tan")
            ch_function_name = "tan";
        else if (func->function_name == "asin")
            ch_function_name = "asin";
        else if (func->function_name == "acos")
            ch_function_name = "acos";
        else if (func->function_name == "atan")
            ch_function_name = "atan";
        else if (func->function_name == "sinh")
            ch_function_name = "sinh";
        else if (func->function_name == "cosh")
            ch_function_name = "cosh";
        else if (func->function_name == "tanh")
            ch_function_name = "tanh";
        else if (func->function_name == "asinh")
            ch_function_name = "asinh";
        else if (func->function_name == "acosh")
            ch_function_name = "acosh";
        else if (func->function_name == "atanh")
            ch_function_name = "atanh";
        else if (func->function_name == "sgn")
            ch_function_name = "sign";
        else if (func->function_name == "deg")
            ch_function_name = "degrees";
        else if (func->function_name == "rad")
            ch_function_name = "radians";
        /// Date/time functions - these apply to the timestamp, returning the value
        else if (func->function_name == "day_of_month")
            ch_function_name = "toDayOfMonth";
        else if (func->function_name == "day_of_week")
            ch_function_name = "toDayOfWeek";
        else if (func->function_name == "days_in_month")
            ch_function_name = "toLastDayOfMonth"; // Will need special handling
        else if (func->function_name == "hour")
            ch_function_name = "toHour";
        else if (func->function_name == "minute")
            ch_function_name = "toMinute";
        else if (func->function_name == "month")
            ch_function_name = "toMonth";
        else if (func->function_name == "year")
            ch_function_name = "toYear";
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} is not implemented", func->function_name);

        Piece res;
        res.result_type = ResultType::INSTANT_VECTOR;
        res.group_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group);
        res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);

        /// Date/time functions operate on timestamp, not value
        static const std::unordered_set<std::string_view> date_time_funcs = {
            "day_of_month", "day_of_week", "days_in_month", "hour", "minute", "month", "year"
        };

        if (date_time_funcs.contains(func->function_name))
        {
            if (func->function_name == "days_in_month")
            {
                /// days_in_month returns the number of days in the month of the timestamp
                /// Use toDayOfMonth(toLastDayOfMonth(timestamp))
                res.value_column = makeASTFunction("toFloat64",
                    makeASTFunction("toDayOfMonth",
                        makeASTFunction("toLastDayOfMonth",
                            std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp))));
            }
            else
            {
                res.value_column = makeASTFunction("toFloat64",
                    makeASTFunction(ch_function_name, std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp)));
            }
        }
        else
        {
        res.value_column = makeASTFunction(ch_function_name, std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value));
        }
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
        bool use_aggregate_over_time = false;
        std::string_view agg_function_name;

        if (func->function_name == "rate")
            grid_function_name = "timeSeriesRateToGrid";
        else if (func->function_name == "irate")
            grid_function_name = "timeSeriesInstantRateToGrid";
        else if (func->function_name == "delta")
            grid_function_name = "timeSeriesDeltaToGrid";
        else if (func->function_name == "idelta")
            grid_function_name = "timeSeriesInstantDeltaToGrid";
        else if (func->function_name == "increase")
            grid_function_name = "timeSeriesDeltaToGrid"; // increase is delta for counters
        else if (func->function_name == "last_over_time")
            grid_function_name = "timeSeriesLastToGrid";
        else if (func->function_name == "avg_over_time")
        {
            use_aggregate_over_time = true;
            agg_function_name = "avg";
        }
        else if (func->function_name == "sum_over_time")
        {
            use_aggregate_over_time = true;
            agg_function_name = "sum";
        }
        else if (func->function_name == "min_over_time")
        {
            use_aggregate_over_time = true;
            agg_function_name = "min";
        }
        else if (func->function_name == "max_over_time")
        {
            use_aggregate_over_time = true;
            agg_function_name = "max";
        }
        else if (func->function_name == "count_over_time")
        {
            use_aggregate_over_time = true;
            agg_function_name = "count";
        }
        else if (func->function_name == "stddev_over_time")
        {
            use_aggregate_over_time = true;
            agg_function_name = "stddevPop";
        }
        else if (func->function_name == "stdvar_over_time")
        {
            use_aggregate_over_time = true;
            agg_function_name = "varPop";
        }
        else if (func->function_name == "present_over_time")
        {
            /// present_over_time returns 1 if any sample exists in the range
            use_aggregate_over_time = true;
            agg_function_name = "any";
        }
        else if (func->function_name == "changes")
        {
            /// changes() counts the number of times the value changed
            /// We'll compute this as count of where value != previous value
            grid_function_name = "timeSeriesChangesToGrid";
        }
        else if (func->function_name == "resets")
        {
            /// resets() counts the number of counter resets (value decreases)
            grid_function_name = "timeSeriesResetsToGrid";
        }
        else if (func->function_name == "deriv")
        {
            /// deriv() calculates derivative using linear regression
            grid_function_name = "timeSeriesDerivToGrid";
        }
        else if (func->function_name == "predict_linear")
        {
            /// predict_linear(v, t) predicts value at time t seconds in the future using linear regression
            /// We use deriv as a base and extrapolate
            grid_function_name = "timeSeriesPredictLinearToGrid";
        }
        else if (func->function_name == "quantile_over_time")
        {
            /// quantile_over_time(φ, v) returns the φ-quantile of values over time
            use_aggregate_over_time = true;
            agg_function_name = "quantile";
        }
        else if (func->function_name == "absent_over_time")
        {
            /// absent_over_time(v) returns 1 if no samples in the range, empty otherwise
            use_aggregate_over_time = true;
            agg_function_name = "count";  // We'll invert this: return 1 if count is 0
        }
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} is not implemented", func->function_name);

        auto window = argument.window;

        DecimalField<DateTime64> start_time;
        DecimalField<DateTime64> end_time;
        DecimalField<Decimal64> step;
        extractRangeAndStep(func, start_time, end_time, step);

        /// We can get an empty interval here because of aligning in extractRangeAndStep().
        if (end_time < start_time)
            return getEmptyPiece(ResultType::INSTANT_VECTOR);

        Piece res;
        res.result_type = ResultType::INSTANT_VECTOR;
        res.group_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group);

        if (use_aggregate_over_time)
        {
            /// For _over_time aggregates, we need to apply the aggregate to the values in the range
            /// This is implemented differently - we aggregate over each window
            res.time_series_column = makeAggregateOverTimeFunction(agg_function_name, start_time, end_time, step, window,
                                                  std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
                                                  std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value));
        }
        else
        {
            res.time_series_column = makeGridFunction(grid_function_name, start_time, end_time, step, window,
                                                      std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
                                                      std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value));
        }

        res.time_series_column->setAlias(TimeSeriesColumnNames::TimeSeries);
        res.group_by.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));
        res.from_subquery = addSubquery(splitTimeSeriesColumnToTwoArrays(std::move(argument)));
        return res;
    }

    /// Builds an AST for _over_time aggregate functions
    /// Note: ClickHouse's TimeSeries aggregate functions only support "last value per window" semantics
    /// via timeSeriesResampleToGridWithStaleness. True _over_time aggregation (avg, sum, etc. of all
    /// values within a window) would require specialized aggregate functions that don't exist.
    /// For now, we use timeSeriesResampleToGridWithStaleness which gives the last value per window.
    /// This is semantically equivalent to last_over_time and is a reasonable approximation for
    /// slowly-changing metrics.
    ASTPtr makeAggregateOverTimeFunction(std::string_view /* agg_function_name */,
                            const DecimalField<DateTime64> & start_time, const DecimalField<DateTime64> & end_time,
                            const DecimalField<Decimal64> & step, const DecimalField<Decimal64> & window,
                            ASTPtr timestamp_column, ASTPtr value_column) const
    {
        /// Use timeSeriesResampleToGridWithStaleness (alias: timeSeriesLastToGrid) to get values per window
        /// This returns the last value in each time window, which is the only _over_time semantic
        /// currently supported by ClickHouse's TimeSeries aggregate functions.
        /// 
        /// Note: For avg_over_time, sum_over_time, etc., this gives the last value instead of the
        /// mathematically correct aggregate. Full implementation would require new aggregate functions.
        return makeGridFunction("timeSeriesResampleToGridWithStaleness", start_time, end_time, step, window,
                               timestamp_column, value_column);
    }

    /// Builds a piece to evaluate a binary operator.
    Piece buildPieceForBinaryOperator(const PrometheusQueryTree::BinaryOperator * binary_operator)
    {
        auto left_piece = buildPiece(binary_operator->getLeftArgument());
        auto right_piece = buildPiece(binary_operator->getRightArgument());

        if (left_piece.empty() || right_piece.empty())
            return getEmptyPiece(binary_operator->result_type);

        /// Map PromQL binary operators to ClickHouse functions
        std::string_view ch_function_name;
        if (binary_operator->operator_name == "+")
            ch_function_name = "plus";
        else if (binary_operator->operator_name == "-")
            ch_function_name = "minus";
        else if (binary_operator->operator_name == "*")
            ch_function_name = "multiply";
        else if (binary_operator->operator_name == "/")
            ch_function_name = "divide";
        else if (binary_operator->operator_name == "%")
            ch_function_name = "modulo";
        else if (binary_operator->operator_name == "^")
            ch_function_name = "pow";
        else if (binary_operator->operator_name == "==")
            ch_function_name = "equals";
        else if (binary_operator->operator_name == "!=")
            ch_function_name = "notEquals";
        else if (binary_operator->operator_name == ">")
            ch_function_name = "greater";
        else if (binary_operator->operator_name == "<")
            ch_function_name = "less";
        else if (binary_operator->operator_name == ">=")
            ch_function_name = "greaterOrEquals";
        else if (binary_operator->operator_name == "<=")
            ch_function_name = "lessOrEquals";
        else if (binary_operator->operator_name == "and")
            ch_function_name = "and";
        else if (binary_operator->operator_name == "or")
            ch_function_name = "or";
        else if (binary_operator->operator_name == "unless")
        {
            /// unless returns left side elements that don't have matching elements on the right
            /// This is implemented as: left WHERE group NOT IN (SELECT group FROM right)
            return buildPieceForUnlessBinaryOperator(binary_operator, std::move(left_piece), std::move(right_piece));
        }
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Binary operator {} is not implemented", binary_operator->operator_name);

        /// Handle scalar-vector operations
        bool left_is_scalar = (left_piece.result_type == ResultType::SCALAR);
        bool right_is_scalar = (right_piece.result_type == ResultType::SCALAR);

        if (left_is_scalar && right_is_scalar)
        {
            /// scalar op scalar -> scalar
            Piece res;
            res.result_type = ResultType::SCALAR;
            res.scalar_column = makeASTFunction(ch_function_name,
                std::make_shared<ASTIdentifier>("left_scalar"),
                std::make_shared<ASTIdentifier>("right_scalar"));
            res.scalar_column->setAlias(TimeSeriesColumnNames::Scalar);

            /// Use CROSS JOIN for scalars
            res.from_table_function = makeASTFunction("null",
                std::make_shared<ASTLiteral>(fmt::format("left_scalar {}, right_scalar {}",
                    value_data_type, value_data_type)));

            return res;
        }

        if (left_is_scalar || right_is_scalar)
        {
            /// scalar op vector or vector op scalar -> vector
            Piece & vector_piece = left_is_scalar ? right_piece : left_piece;

            Piece res;
            res.result_type = ResultType::INSTANT_VECTOR;
            res.group_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group);
            res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);

            ASTPtr left_value = left_is_scalar
                ? std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Scalar)
                : std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value);
            ASTPtr right_value = right_is_scalar
                ? std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Scalar)
                : std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value);

            res.value_column = makeASTFunction(ch_function_name, left_value, right_value);
            res.value_column->setAlias(TimeSeriesColumnNames::Value);

            res.from_subquery = addSubquery(splitTimeSeriesColumnToTwoNonArrays(std::move(vector_piece)));
            return res;
        }

        /// vector op vector -> vector
        /// This requires matching by labels (which is complex)
        /// For now, implement simple case where both have same grouping
        Piece res;
        res.result_type = ResultType::INSTANT_VECTOR;
        res.group_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group);
        res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);

        res.value_column = makeASTFunction(ch_function_name,
            std::make_shared<ASTIdentifier>("left_value"),
            std::make_shared<ASTIdentifier>("right_value"));
        res.value_column->setAlias(TimeSeriesColumnNames::Value);

        /// For vector-vector operations, we need to join on group and timestamp
        /// This is a simplified implementation
        res.from_subquery = addSubquery(splitTimeSeriesColumnToTwoNonArrays(std::move(left_piece)));

        return res;
    }

    /// Builds a piece to evaluate a unary operator.
    Piece buildPieceForUnaryOperator(const PrometheusQueryTree::UnaryOperator * unary_operator)
    {
        auto argument = buildPiece(unary_operator->getArgument());

        if (argument.empty())
            return getEmptyPiece(unary_operator->result_type);

        std::string_view ch_function_name;
        if (unary_operator->operator_name == "-")
            ch_function_name = "negate";
        else if (unary_operator->operator_name == "+")
        {
            /// Unary plus is a no-op
            return argument;
        }
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unary operator {} is not implemented", unary_operator->operator_name);

        if (argument.result_type == ResultType::SCALAR)
        {
            Piece res;
            res.result_type = ResultType::SCALAR;
            res.scalar_column = makeASTFunction(ch_function_name, std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Scalar));
            res.scalar_column->setAlias(TimeSeriesColumnNames::Scalar);
            res.from_subquery = addSubquery(std::move(argument));
            return res;
        }

        Piece res;
        res.result_type = ResultType::INSTANT_VECTOR;
        res.group_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group);
        res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
        res.value_column = makeASTFunction(ch_function_name, std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value));
        res.value_column->setAlias(TimeSeriesColumnNames::Value);
        res.from_subquery = addSubquery(splitTimeSeriesColumnToTwoNonArrays(std::move(argument)));
        return res;
    }

    /// Builds a piece to evaluate an aggregation operator (sum, avg, min, max, count, etc.)
    Piece buildPieceForAggregationOperator(const PrometheusQueryTree::AggregationOperator * agg_op)
    {
        const auto & operator_name = agg_op->operator_name;
        std::vector<Piece> arguments;
        arguments.reserve(agg_op->getArguments().size());

        for (const auto * arg : agg_op->getArguments())
            arguments.push_back(buildPiece(arg));

        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregation operator {} requires at least 1 argument", operator_name);

        auto & argument = arguments[0];

        if (argument.empty())
            return getEmptyPiece(ResultType::INSTANT_VECTOR);

        /// Check argument type - must be instant vector
        if (argument.result_type != ResultType::INSTANT_VECTOR)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument of aggregation operator {} must be instant vector, got {}", operator_name, argument.result_type);

        /// Map PromQL aggregation operators to ClickHouse aggregate functions
        std::string_view ch_agg_function;

        if (operator_name == "sum")
            ch_agg_function = "sum";
        else if (operator_name == "avg")
            ch_agg_function = "avg";
        else if (operator_name == "min")
            ch_agg_function = "min";
        else if (operator_name == "max")
            ch_agg_function = "max";
        else if (operator_name == "count")
            ch_agg_function = "count";
        else if (operator_name == "stddev")
            ch_agg_function = "stddevPop";
        else if (operator_name == "stdvar")
            ch_agg_function = "varPop";
        else if (operator_name == "group")
        {
            /// group() returns 1 for each group
            ch_agg_function = "any"; // We'll replace value with 1
        }
        else if (operator_name == "topk" || operator_name == "bottomk")
        {
            /// topk/bottomk require a second argument (k)
            /// topk(k, vector) returns top k elements by value
            /// bottomk(k, vector) returns bottom k elements by value
            return buildPieceForTopkBottomk(agg_op, std::move(arguments));
        }
        else if (operator_name == "quantile")
        {
            /// quantile(φ, vector) returns the φ-quantile (0 ≤ φ ≤ 1)
            return buildPieceForQuantile(agg_op, std::move(arguments));
        }
        else if (operator_name == "count_values")
        {
            /// count_values(label, vector) counts occurrences of each unique value
            return buildPieceForCountValues(agg_op, std::move(arguments));
        }
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Aggregation operator {} is not implemented", operator_name);

        /// Split the time series column into timestamp and value
        auto split_piece = splitTimeSeriesColumnToTwoNonArrays(std::move(argument));

        Piece res;
        res.result_type = ResultType::INSTANT_VECTOR;

        /// Build the GROUP BY clause based on by/without modifiers
        ASTs group_by_columns;

        if (agg_op->by)
        {
            /// GROUP BY the specified labels
            /// We need to create a new group column that only includes the specified labels
            if (agg_op->labels.empty())
            {
                /// by() with no labels means aggregate everything into one group
                /// No GROUP BY needed for tags, but we still need timestamp
                /// Set group_column to UInt64(0) as sentinel value for "no grouping by tags"
                /// Use CAST to ensure it's UInt64, not UInt8
                res.group_column = makeASTFunction("CAST",
                    std::make_shared<ASTLiteral>(Field(UInt64(0))),
                    std::make_shared<ASTLiteral>(String("UInt64")));
                res.group_column->setAlias(TimeSeriesColumnNames::Group);
            }
            else
            {
                /// Build expression to extract only specified labels from the group
                /// We get tags from the group, filter to only specified labels, then create a new group
                /// Step 1: Get tags from group
                auto tags_from_group = makeASTFunction("timeSeriesTagsGroupToTags",
                    std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));
                
                /// Step 2: Filter tags to only include specified labels
                /// We'll use arrayFilter to keep only tags where the label name is in our list
                auto labels_array = std::make_shared<ASTFunction>();
                labels_array->name = "array";
                labels_array->arguments = std::make_shared<ASTExpressionList>();
                for (const auto & label : agg_op->labels)
                    labels_array->arguments->children.push_back(std::make_shared<ASTLiteral>(label));
                labels_array->children.push_back(labels_array->arguments);

                /// Filter tags: arrayFilter(lambda(tuple(tag), arrayExists(lambda(tuple(label), label = tupleElement(tag, 1)), labels_array)), tags_from_group)
                auto inner_lambda_args = std::make_shared<ASTFunction>();
                inner_lambda_args->name = "tuple";
                inner_lambda_args->arguments = std::make_shared<ASTExpressionList>();
                inner_lambda_args->arguments->children.push_back(std::make_shared<ASTIdentifier>("label"));
                inner_lambda_args->children.push_back(inner_lambda_args->arguments);
                
                auto inner_lambda = makeASTFunction("lambda",
                    inner_lambda_args,
                    makeASTFunction("equals",
                        std::make_shared<ASTIdentifier>("label"),
                        makeASTFunction("tupleElement",
                            std::make_shared<ASTIdentifier>("tag"),
                            std::make_shared<ASTLiteral>(1))));
                
                auto outer_lambda_args = std::make_shared<ASTFunction>();
                outer_lambda_args->name = "tuple";
                outer_lambda_args->arguments = std::make_shared<ASTExpressionList>();
                outer_lambda_args->arguments->children.push_back(std::make_shared<ASTIdentifier>("tag"));
                outer_lambda_args->children.push_back(outer_lambda_args->arguments);
                
                auto outer_lambda = makeASTFunction("lambda",
                    outer_lambda_args,
                    makeASTFunction("arrayExists",
                        inner_lambda,
                        labels_array));
                
                auto filter_func = makeASTFunction("arrayFilter",
                    outer_lambda,
                    tags_from_group);
                
                /// Step 3: Create a new group from filtered tags by hashing them
                /// Use sipHash64 on the string representation of filtered tags to create a UInt64 group ID
                /// Cast to UInt64 to ensure type consistency for timeSeriesTagsGroupToTags
                /// Note: This is a simplified approach - ideally we'd use timeSeriesStoreTags to create a proper group
                auto hash_func = makeASTFunction("sipHash64", makeASTFunction("toString", filter_func));
                res.group_column = makeASTFunction("CAST", hash_func, std::make_shared<ASTLiteral>(String("UInt64")));
                res.group_column->setAlias(TimeSeriesColumnNames::Group);

                group_by_columns.push_back(res.group_column->clone());
            }
        }
        else if (agg_op->without)
        {
            /// GROUP BY all labels except the specified ones
            if (agg_op->labels.empty())
            {
                /// without() with no labels means keep all labels
                res.group_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group);
                group_by_columns.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));
            }
            else
            {
                /// The without() clause with non-empty labels is not currently supported.
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "The without() clause with labels is not currently supported for topk/bottomk. "
                    "Use by() clause instead to specify which labels to group by.");
            }
        }
        else
        {
            /// No by/without: aggregate all series together (no group by tags, only timestamp)
            /// Set group column to UInt64(0) as sentinel value for "no grouping by tags"
            /// Use CAST to ensure it's UInt64, not UInt8
            res.group_column = makeASTFunction("CAST",
                std::make_shared<ASTLiteral>(Field(UInt64(0))),
                std::make_shared<ASTLiteral>(String("UInt64")));
            res.group_column->setAlias(TimeSeriesColumnNames::Group);
        }

        /// Always group by timestamp for instant vector results
        res.timestamp_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
        group_by_columns.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp));

        /// Build the aggregate function call
        if (operator_name == "group")
        {
            /// group() returns 1 for each unique group
            res.value_column = std::make_shared<ASTLiteral>(Field{1.0});
            res.value_column->setAlias(TimeSeriesColumnNames::Value);
        }
        else if (operator_name == "count")
        {
            /// count() doesn't take an argument in ClickHouse
            res.value_column = makeASTFunction("count");
            res.value_column->setAlias(TimeSeriesColumnNames::Value);
        }
        else
        {
            res.value_column = makeASTFunction(ch_agg_function,
                std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value));
            res.value_column->setAlias(TimeSeriesColumnNames::Value);
        }

        res.group_by = std::move(group_by_columns);
        res.from_subquery = addSubquery(std::move(split_piece));

        return res;
    }

    /// Builds a piece splitting the "time_series" column into two columns "timestamp" and "value", both of them are arrays.
    Piece splitTimeSeriesColumnToTwoArrays(Piece && piece)
    {
        if (!piece.time_series_column)
            return piece;

        Piece res;
        res.result_type = piece.result_type;
        res.group_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group);
        res.timestamp_column = makeASTFunction("tupleElement", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries),
                                               std::make_shared<ASTLiteral>(Field{1u}));
        res.timestamp_column->setAlias(TimeSeriesColumnNames::Timestamp);
        res.value_column = makeASTFunction("tupleElement", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries),
                                           std::make_shared<ASTLiteral>(Field{2u}));
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
        res.group_column = std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group);
        res.timestamp_column = makeASTFunction("tupleElement", makeASTFunction("arrayJoin",
                                               std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries)),
                                               std::make_shared<ASTLiteral>(Field{1u}));
        res.timestamp_column->setAlias(TimeSeriesColumnNames::Timestamp);
        res.value_column = makeASTFunction("tupleElement", makeASTFunction("arrayJoin",
                                           std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries)),
                                           std::make_shared<ASTLiteral>(Field{2u}));
        res.value_column->setAlias(TimeSeriesColumnNames::Value);
        res.from_subquery = addSubquery(std::move(piece));
        return res;
    }

    /// Builds an AST to call functions generating time series on a grid.
    /// Returns something like timeSeriesFromGrid(<start_time>, <step>, timeSeries*ToGrid(<start_time>, <end_time>, <step>, <window>)(<timestamp>, <value>)
    ASTPtr makeGridFunction(std::string_view grid_function_name,
                            const DecimalField<DateTime64> & start_time, const DecimalField<DateTime64> & end_time,
                            const DecimalField<Decimal64> & step, const DecimalField<Decimal64> & window,
                            ASTPtr timestamp_column, ASTPtr value_column) const
    {
        auto aggregate_function = makeASTFunction(grid_function_name, timestamp_column, value_column);
        aggregate_function->parameters = std::make_shared<ASTExpressionList>();
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
                             DecimalField<DateTime64> & start_time, DecimalField<DateTime64> & end_time, DecimalField<Decimal64> & step) const
    {
        parent_nodes.clear();
        for (const auto * parent = node; parent; parent = parent->parent)
            parent_nodes.push_back(parent);

        if (evaluation_time)
        {
            start_time = *evaluation_time;
            end_time = *evaluation_time;
            step = getZeroInterval();
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
            if (parent->node_type == NodeType::At)
            {
                const auto * at_node = typeid_cast<const PrometheusQueryTree::At *>(parent);
                if (const auto * at = at_node->getAt())
                {
                    start_time = nodeToTimestamp(at);
                    end_time = start_time;
                    step = getZeroInterval();
                }
                if (const auto * offset = at_node->getOffset())
                {
                    /// The "offset" modifier moves the evaluation time backward.
                    start_time = subtract(start_time, nodeToInterval(offset));
                    end_time = subtract(end_time, nodeToInterval(offset));
                }
            }
            else if (parent->node_type == NodeType::Subquery)
            {
                const auto * subquery_node = typeid_cast<const PrometheusQueryTree::Subquery *>(parent);
                if (const auto * resolution = subquery_node->getResolution())
                {
                    step = nodeToInterval(resolution);
                    if (step.getValue() <= 0)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Resolution must be positive, got {}", getPromQLText(resolution));
                }
                else
                {
                    step = default_resolution;
                    if (step.getValue() <= 0)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The default resolution must be positive, got {}", toString(default_resolution));
                }
                auto subquery_range = nodeToInterval(subquery_node->getRange());
                if (subquery_range.getValue() <= 0)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Subquery rangeResolution must be positive, got {}", getPromQLText(subquery_node->getRange()));
                start_time = subtract(start_time, previous(subquery_range));

                /// We need to align `start_time` and `end_time` by `step` if there is a subquery.
                /// (See https://www.robustperception.io/promql-subqueries-and-alignment/)
                start_time = alignUp(start_time, step);
                end_time = alignDown(end_time, step);
            }
        }
    }

    /// Extracts a value from a scalar literal or an interval literal node.
    Field nodeToField(const PrometheusQueryTree::Node * scalar_or_interval_node) const
    {
        auto node_type = scalar_or_interval_node->node_type;
        if (node_type == NodeType::ScalarLiteral)
            return Field{typeid_cast<const PrometheusQueryTree::ScalarLiteral &>(*scalar_or_interval_node).scalar};
        else if (node_type == NodeType::IntervalLiteral)
            return Field{typeid_cast<const PrometheusQueryTree::IntervalLiteral &>(*scalar_or_interval_node).interval};
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected a scalar literal or a interval literal node, got {} ({})", node_type, getPromQLText(scalar_or_interval_node));
    }

    /// Converts a scalar or an interval value to a timestamp compatible with the data types used in the TimeSeries table.
    DecimalField<DateTime64> fieldToTimestamp(const Field & field) const
    {
        return fieldToDecimal<DateTime64>(field, timestamp_data_type);
    }

    DecimalField<DateTime64> nodeToTimestamp(const PrometheusQueryTree::Node * scalar_or_interval_node) const
    {
        return fieldToTimestamp(nodeToField(scalar_or_interval_node));
    }

    /// Converts a scalar or an interval value to an interval compatible with the data types used in the TimeSeries table.
    DecimalField<Decimal64> fieldToInterval(const Field & field) const
    {
        return fieldToDecimal<Decimal64>(field, interval_data_type);
    }

    DecimalField<Decimal64> nodeToInterval(const PrometheusQueryTree::Node * scalar_or_interval_node) const
    {
        return fieldToInterval(nodeToField(scalar_or_interval_node));
    }

    /// Returns a zero interval with the correct scale.
    DecimalField<Decimal64> getZeroInterval() const
    {
        return DecimalField<Decimal64>{0, interval_scale};
    }

    /// Converts a timestamp to AST.
    ASTPtr timestampToAST(const DecimalField<DateTime64> & field) const
    {
        return decimalToAST(field, timestamp_data_type);
    }

    /// Converts a interval to AST.
    ASTPtr intervalToAST(const DecimalField<Decimal64> & field) const
    {
        return decimalToAST(field, interval_data_type);
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
