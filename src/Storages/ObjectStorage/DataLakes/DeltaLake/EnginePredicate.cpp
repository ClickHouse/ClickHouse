#include <Storages/ObjectStorage/DataLakes/DeltaLake/EnginePredicate.h>

#if USE_DELTA_KERNEL_RS
#include <Analyzer/Utils.h>
#include <Common/logger_useful.h>
#include <Common/FailPoint.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/IColumn.h>
#include <Common/DateLUTImpl.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/IFunction.h>
#include <Functions/ComparisonNames.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/convertFieldToType.h>
#include <Functions/FunctionsLogical.h>
#include <base/arithmeticOverflow.h>

#include <Interpreters/ActionsDAG.h>
#include <Parsers/ASTHelpers.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>

#include <array>
#include <functional>
#include <limits>
#include <optional>
#include <utility>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int FAULT_INJECTED;
}

namespace DB::FailPoints
{
    extern const char delta_kernel_fail_literal_visitor[];
}

namespace DeltaLake
{

namespace
{
    /// Whether a node is a constant (literal).
    bool isConstNode(const DB::ActionsDAG::Node * node)
    {
        return node->type == DB::ActionsDAG::ActionType::COLUMN;
    }

    /// Whether a node represents a specific column identifier, e.g. column name.
    bool isColumnNode(const DB::ActionsDAG::Node * node)
    {
        return node->type == DB::ActionsDAG::ActionType::INPUT;
    }

    bool isFunctionNode(const DB::ActionsDAG::Node * node)
    {
        return node->type == DB::ActionsDAG::ActionType::FUNCTION;
    }

    DB::TypeIndex getTypeIndex(const DB::ActionsDAG::Node * node)
    {
        if (!node->result_type->isNullable())
            return node->result_type->getTypeId();

        const auto * nullable = assert_cast<const DB::DataTypeNullable *>(node->result_type.get());
        return nullable->getNestedType()->getTypeId();
    }

    DB::DataTypePtr getTypeOrNestedType(const DB::ActionsDAG::Node * node)
    {
        if (!node->result_type->isNullable())
            return node->result_type;

        const auto * nullable = assert_cast<const DB::DataTypeNullable *>(node->result_type.get());
        return nullable->getNestedType();
    }

    enum class ComparisonOperator : uint8_t
    {
        Equals,
        NotEquals,
        Less,
        LessOrEquals,
        Greater,
        GreaterOrEquals,
    };

    std::optional<ComparisonOperator> parseComparisonOperator(const String & name)
    {
        if (name == DB::NameEquals::name)
            return ComparisonOperator::Equals;
        if (name == DB::NameNotEquals::name)
            return ComparisonOperator::NotEquals;
        if (name == DB::NameLess::name)
            return ComparisonOperator::Less;
        if (name == DB::NameLessOrEquals::name)
            return ComparisonOperator::LessOrEquals;
        if (name == DB::NameGreater::name)
            return ComparisonOperator::Greater;
        if (name == DB::NameGreaterOrEquals::name)
            return ComparisonOperator::GreaterOrEquals;
        return std::nullopt;
    }

    /// `literal OP column` read as `column OP' literal`.
    ComparisonOperator mirror(ComparisonOperator op)
    {
        switch (op)
        {
            case ComparisonOperator::Equals:
            case ComparisonOperator::NotEquals:
                return op;
            case ComparisonOperator::Less:
                return ComparisonOperator::Greater;
            case ComparisonOperator::LessOrEquals:
                return ComparisonOperator::GreaterOrEquals;
            case ComparisonOperator::Greater:
                return ComparisonOperator::Less;
            case ComparisonOperator::GreaterOrEquals:
                return ComparisonOperator::LessOrEquals;
        }
        UNREACHABLE();
    }

    /// `column OP x` for `x` strictly between two adjacent column values `q` and `q + 1`, read as a
    /// comparison with `q`. Nullopt for `=` and `!=`: no column value equals such `x`, and the
    /// comparison is left untranslated.
    std::optional<ComparisonOperator> againstFloor(ComparisonOperator op)
    {
        switch (op)
        {
            case ComparisonOperator::Equals:
            case ComparisonOperator::NotEquals:
                return std::nullopt;
            case ComparisonOperator::Less:
            case ComparisonOperator::LessOrEquals:
                return ComparisonOperator::LessOrEquals;
            case ComparisonOperator::Greater:
            case ComparisonOperator::GreaterOrEquals:
                return ComparisonOperator::Greater;
        }
        UNREACHABLE();
    }

    /// Kernel visitor for `left OP right` over two expression IDs.
    using ComparisonVisitor = uintptr_t (*)(ffi::KernelExpressionVisitorState *, uintptr_t, uintptr_t);

    /// The visitor for `column OP literal`.
    ComparisonVisitor getComparisonVisitor(ComparisonOperator op)
    {
        switch (op)
        {
            case ComparisonOperator::Equals:
                return &ffi::visit_predicate_eq;
            case ComparisonOperator::NotEquals:
                return &ffi::visit_predicate_ne;
            case ComparisonOperator::Less:
                return &ffi::visit_predicate_lt;
            case ComparisonOperator::LessOrEquals:
                return &ffi::visit_predicate_le;
            case ComparisonOperator::Greater:
                return &ffi::visit_predicate_gt;
            case ComparisonOperator::GreaterOrEquals:
                return &ffi::visit_predicate_ge;
        }
        UNREACHABLE();
    }

    bool holds(ComparisonOperator op, Int64 value, Int64 bound)
    {
        switch (op)
        {
            case ComparisonOperator::Equals: return value == bound;
            case ComparisonOperator::NotEquals: return value != bound;
            case ComparisonOperator::Less: return value < bound;
            case ComparisonOperator::LessOrEquals: return value <= bound;
            case ComparisonOperator::Greater: return value > bound;
            case ComparisonOperator::GreaterOrEquals: return value >= bound;
        }
        UNREACHABLE();
    }

    /// Whether the comparison `node` agrees with `operand OP bound` at `bound - 1`, `bound` and `bound + 1`,
    /// evaluated by the comparison itself: it was built with the settings of its query (how a string is
    /// parsed, how an out-of-range literal converts), which can differ from those of the read and which
    /// pruning cannot read. A comparison monotonic in the operand that agrees at these points selects the
    /// same operand values as `operand OP bound`. `to_field` gives an operand value as a `Field` of the
    /// operand type, `[min, max]` the values the type holds.
    bool agreesWithComparison(
        const DB::ActionsDAG::Node & node,
        bool swapped,
        ComparisonOperator op,
        Int64 bound,
        Int64 min,
        Int64 max,
        const std::function<DB::Field(Int64)> & to_field)
    {
        const size_t operand_index = swapped ? 1 : 0;
        const auto * operand = node.children[operand_index];
        const auto * literal = node.children[1 - operand_index];

        if (bound < min || bound > max)
            return false;
        std::vector<Int64> values;
        if (bound > min)
            values.push_back(bound - 1);
        values.push_back(bound);
        if (bound < max)
            values.push_back(bound + 1);

        auto column = operand->result_type->createColumn();
        for (Int64 value : values)
            column->insert(to_field(value));

        DB::ColumnsWithTypeAndName arguments(2);
        arguments[operand_index] = {std::move(column), operand->result_type, operand->result_name};
        arguments[1 - operand_index] = {literal->column->cloneResized(values.size()), literal->result_type, literal->result_name};

        DB::ColumnPtr result;
        try
        {
            result = node.function_base->execute(arguments, node.result_type, values.size(), /* dry_run = */ false);
        }
        catch (...)
        {
            return false;
        }
        result = result->convertToFullColumnIfConst();
        const auto * nullable = typeid_cast<const DB::ColumnNullable *>(result.get());
        const auto & matches = nullable ? nullable->getNestedColumn() : *result;

        for (size_t i = 0; i < values.size(); ++i)
            if ((nullable && nullable->isNullAt(i)) || matches.getBool(i) != holds(op, values[i], bound))
                return false;
        return true;
    }

    /// The modes the comparison may have parsed a string literal in: the read's
    /// `cast_string_to_date_time_mode` need not be the one of the query that built the comparison.
    constexpr std::array DATE_TIME_INPUT_FORMATS = {
        DB::FormatSettings::DateTimeInputFormat::Basic,
        DB::FormatSettings::DateTimeInputFormat::BestEffort,
        DB::FormatSettings::DateTimeInputFormat::BestEffortUS,
    };

    struct DateComparison
    {
        const DB::ActionsDAG::Node * column;
        Int32 day;
        bool narrows_to_date;
        ComparisonVisitor visitor;
    };

    /// Matches `CAST`/`toDate`/`toDate32` on a `Date32` column against a literal, in either operand order.
    /// Converts the literal and selects its visitor; returns nullopt when translation is unsafe.
    std::optional<DateComparison> matchDateComparison(
        const DB::ActionsDAG::Node * node,
        ComparisonOperator op,
        DB::FormatSettings format_settings)
    {
        const auto * conversion = node->children[0];
        const auto * literal = node->children[1];
        bool swapped = false;
        if (isConstNode(conversion))
        {
            std::swap(conversion, literal);
            swapped = true;
        }
        if (!isFunctionNode(conversion) || !isConstNode(literal))
            return std::nullopt;

        const auto & name = conversion->function_base->getName();
        const auto & children = conversion->children;
        const bool is_cast = DB::isFunctionCast(name) && children.size() == 2 && isConstNode(children[1]);
        /// The optional constant timezone of `toDate` / `toDate32` is inert for a `Date32` source.
        const bool is_to_date = (name == "toDate" || name == "toDate32")
            && (children.size() == 1 || (children.size() == 2 && isConstNode(children[1])));
        if (!is_cast && !is_to_date)
            return std::nullopt;

        const auto * column = children[0];
        const auto result_type = getTypeOrNestedType(conversion);
        if (!isColumnNode(column) || getTypeIndex(column) != DB::TypeIndex::Date32
            || !DB::isDateOrDate32(result_type->getTypeId()))
            return std::nullopt;

        /// Mixed temporal comparisons use a common type; a `Date`/`Date32` literal converts exactly or not at all (rejected below), anything else stays untranslated.
        const auto literal_type = getTypeOrNestedType(literal);
        if (!literal_type->equals(*result_type) && !isString(literal_type)
            && !DB::isDateOrDate32(literal_type->getTypeId()))
            return std::nullopt;

        /// Convert the literal as the real comparison does, then let the comparison confirm it: e.g.
        /// `toDate(d) = '1969-12-31' SETTINGS date_time_overflow_behavior='throw'` throws, which pruning
        /// must not hide by translating a clamped literal.
        const auto column_op = swapped ? mirror(op) : op;
        const bool is_date = result_type->getTypeId() == DB::TypeIndex::Date;
        const auto to_field = [is_date](Int64 day) { return is_date ? DB::Field(static_cast<UInt64>(day)) : DB::Field(day); };
        const Int64 min_day = is_date ? 0 : std::numeric_limits<Int32>::min();
        const Int64 max_day = is_date ? std::numeric_limits<UInt16>::max() : std::numeric_limits<Int32>::max();
        for (const auto mode : DATE_TIME_INPUT_FORMATS)
        {
            format_settings.date_time_input_format = mode;
            const auto value = DB::tryConvertFieldToType(literal->column->getField(), *result_type, literal_type.get(), format_settings);
            if (value.isNull())
                continue;
            const Int64 day = is_date ? static_cast<Int64>(value.safeGet<UInt64>()) : value.safeGet<Int64>();
            if (agreesWithComparison(*node, swapped, column_op, day, min_day, max_day, to_field))
                return DateComparison{column, static_cast<Int32>(day), is_date, getComparisonVisitor(column_op)};
            if (!isString(literal_type))
                break;
        }
        return std::nullopt;
    }

    /// Scale of the `DateTime64` that a Delta `timestamp` / `timestamp_ntz` is read as: microseconds.
    constexpr UInt32 DELTA_TIMESTAMP_SCALE = 6;

    /// Outside `[TIMESTAMP_FENCE_MIN, TIMESTAMP_FENCE_MAX]` microseconds, the range of `Int64` nanoseconds
    /// (about 1677 to 2262), a row may compare otherwise than its statistics say:
    /// - with `decimal_check_overflow = 0`, a comparison at a finer scale multiplies the column by up to
    ///   1000 and wraps around `Int64`;
    /// - with `date_time_overflow_behavior = 'saturate'`, the Parquet reader clamps an `INT96` timestamp
    ///   to these bounds, while the statistics keep the value as written. The reader takes this setting
    ///   from its own format settings (for a table engine, the table's `SETTINGS` and the server's default
    ///   profile; on a cluster, each replica's), which pruning cannot see. Clamping keeps the order with a
    ///   literal strictly between the bounds, so only a literal on or beyond a bound is fenced for it.
    constexpr Int64 TIMESTAMP_FENCE_MIN = std::numeric_limits<Int64>::min() / 1000;
    constexpr Int64 TIMESTAMP_FENCE_MAX = std::numeric_limits<Int64>::max() / 1000;

    /// Before the epoch, a value finer than microseconds (`INT96`, `TIMESTAMP(NANOS)`) is read rounded
    /// towards zero, that is up, while the statistics hold its floor truncated to milliseconds. The read
    /// value can then exceed the maximum by a full millisecond, one microsecond more than the slack that
    /// delta-kernel allows. Only a negative value is affected, so a literal at or before the epoch keeps
    /// the rows below it undecided.
    constexpr Int64 TIMESTAMP_ROUNDING_FENCE_MIN = 0;

    struct TimestampLiteral
    {
        /// The literal in microseconds, or its floor when the comparison runs at a finer scale.
        Int64 micros;
        /// Whether `micros` is the literal itself rather than its floor.
        bool exact;
        /// The ratio of the literal's ticks to microseconds; 1 when the comparison runs at microseconds.
        Int64 multiplier;
    };

    /// Converts a literal compared with a `DateTime64(6)` column exactly as `FunctionComparison` does,
    /// so that the result can be compared with the raw column microseconds. Returns nullopt when it cannot.
    std::optional<TimestampLiteral> convertTimestampLiteral(
        const DB::DataTypePtr & column_type,
        const DB::ActionsDAG::Node & literal,
        const DB::FormatSettings & format_settings)
    {
        if (!literal.column)
            return std::nullopt;
        const auto field = literal.column->getField();
        if (field.isNull())
            return std::nullopt;

        const auto literal_type = getTypeOrNestedType(&literal);
        const DB::WhichDataType which(literal_type);

        /// `executeWithConstString` parses the string as the column type, in its time zone and under
        /// the comparison's format settings. A string it cannot parse makes it throw or return a constant.
        if (which.isStringOrFixedString())
        {
            const auto value = DB::tryConvertFieldToType(field, *column_type, literal_type.get(), format_settings);
            if (value.isNull())
                return std::nullopt;
            return TimestampLiteral{value.safeGet<DB::DecimalField<DB::DateTime64>>().getValue().value, true, 1};
        }

        /// Two `DateTime64` compare by ticks at the larger scale, whatever their time zones.
        if (which.isDateTime64())
        {
            const auto scale = assert_cast<const DB::DataTypeDateTime64 &>(*literal_type).getScale();
            const Int64 ticks = field.safeGet<DB::DecimalField<DB::DateTime64>>().getValue().value;
            if (scale <= DELTA_TIMESTAMP_SCALE)
            {
                Int64 micros = 0;
                if (common::mulOverflow(ticks, DB::DecimalUtils::scaleMultiplier<Int64>(DELTA_TIMESTAMP_SCALE - scale), micros))
                    return std::nullopt;
                return TimestampLiteral{micros, true, 1};
            }

            /// The column is scaled up instead: `column * multiplier OP ticks`. Round the literal towards
            /// minus infinity; a remainder means no column value equals it.
            const Int64 multiplier = DB::DecimalUtils::scaleMultiplier<Int64>(scale - DELTA_TIMESTAMP_SCALE);
            Int64 quotient = ticks / multiplier;
            Int64 remainder = ticks % multiplier;
            if (remainder < 0)
            {
                --quotient;
                remainder += multiplier;
            }
            return TimestampLiteral{quotient, remainder == 0, multiplier};
        }

        /// A `DateTime`, `Date` or `Date32` is cast to the common type, which is the column type itself
        /// (the `DateTime64` of the larger scale). Cast it the same way: in the column's time zone,
        /// saturation included.
        if (which.isDateTime() || which.isDate() || which.isDate32())
        {
            const auto converted = DB::castColumn({literal_type->createColumnConst(1, field), literal_type, ""}, column_type);
            return TimestampLiteral{(*converted)[0].safeGet<DB::DecimalField<DB::DateTime64>>().getValue().value, true, 1};
        }

        return std::nullopt;
    }

    struct TimestampComparison
    {
        const DB::ActionsDAG::Node * column;
        DeltaTimestampKind kind;
        Int64 micros;
        ComparisonVisitor visitor;
        /// Whether the rows outside `[fence_min, TIMESTAMP_FENCE_MAX]` are kept undecided.
        bool fenced;
        Int64 fence_min;
    };

    /// Matches a comparison of a column backed by a Delta `timestamp` / `timestamp_ntz` with a literal.
    /// Returns nullopt when no kernel predicate keeps every file the comparison can match.
    std::optional<TimestampComparison> matchTimestampComparison(
        const DB::ActionsDAG::Node & node,
        const DB::ActionsDAG::Node * column,
        const DB::ActionsDAG::Node * literal,
        bool swapped,
        const DeltaTimestampColumn & delta_column,
        ComparisonOperator op,
        DB::FormatSettings format_settings)
    {
        /// A column declared with another type is read with a conversion that pruning does not mirror.
        const auto column_type = getTypeOrNestedType(column);
        const auto * date_time64 = typeid_cast<const DB::DataTypeDateTime64 *>(column_type.get());
        if (!date_time64 || date_time64->getScale() != DELTA_TIMESTAMP_SCALE)
            return std::nullopt;

        /// The statistics leave out NULLs, which a column declared not `Nullable` reads as the default value.
        if (delta_column.nullable && !column->result_type->isNullable())
            return std::nullopt;

        const bool is_string = isString(getTypeOrNestedType(literal));
        const auto to_field = [](Int64 micros) { return DB::Field(DB::DecimalField<DB::DateTime64>(micros, DELTA_TIMESTAMP_SCALE)); };
        for (const auto mode : DATE_TIME_INPUT_FORMATS)
        {
            format_settings.date_time_input_format = mode;
            const auto value = convertTimestampLiteral(column_type, *literal, format_settings);
            if (!value)
            {
                if (is_string)
                    continue;
                return std::nullopt;
            }

            auto column_op = swapped ? mirror(op) : op;
            if (!value->exact)
            {
                const auto floor_op = againstFloor(column_op);
                if (!floor_op)
                    return std::nullopt;
                column_op = *floor_op;
            }

            if (!agreesWithComparison(node, swapped, column_op, value->micros,
                    std::numeric_limits<Int64>::min(), std::numeric_limits<Int64>::max(), to_field))
            {
                if (is_string)
                    continue;
                return std::nullopt;
            }

            /// The finest `DateTime64` is nanoseconds, so the comparison scales the column up by at most
            /// 1000: the fence covers the scaled comparison too. Whether it wraps there depends on its
            /// `decimal_check_overflow`, which pruning cannot read, so a scaled comparison is always fenced.
            chassert(value->multiplier <= 1000);
            const bool before_epoch = value->micros <= TIMESTAMP_ROUNDING_FENCE_MIN;
            const bool fenced = before_epoch || value->multiplier > 1 || value->micros >= TIMESTAMP_FENCE_MAX;
            const Int64 fence_min = before_epoch ? TIMESTAMP_ROUNDING_FENCE_MIN : TIMESTAMP_FENCE_MIN;

            return TimestampComparison{column, delta_column.kind, value->micros, getComparisonVisitor(column_op), fenced, fence_min};
        }
        return std::nullopt;
    }
}

std::shared_ptr<EnginePredicate> getEnginePredicate(
    const DB::ActionsDAG & filter, std::exception_ptr & exception, DB::ContextPtr context,
    DeltaTimestampColumns timestamp_columns)
{
    return std::make_unique<EnginePredicate>(filter, exception, context, std::move(timestamp_columns));
}

/// Contains state for EngineIterator
/// (an iterator over DB::ActionsDAG const node ptr's).
struct EngineIteratorData
{
    EngineIteratorData(
        ffi::KernelExpressionVisitorState * state_,
        const DB::ActionsDAG::NodeRawConstPtrs & nodes_,
        EnginePredicate & predicate_)
        : state(state_)
        , predicate(predicate_)
        , nodes(nodes_)
        , it(nodes.begin())
    {
    }

    void setException(std::exception_ptr exception_)
    {
        predicate.setException(exception_);
    }

    bool hasException() const { return predicate.exception != nullptr; }

    const LoggerPtr & log() const { return predicate.log; }

    const DB::ActionsDAG::Node * next()
    {
        if (it == nodes.end())
            return {};
        return *(it++);
    }

    ffi::KernelExpressionVisitorState * state;
    EnginePredicate & predicate;

private:
    const DB::ActionsDAG::NodeRawConstPtrs & nodes;
    DB::ActionsDAG::NodeRawConstPtrs::const_iterator it;
};

/// An iterator over DB::ActionsDAG const node ptr's.
/// Applies corresponding delta-kernel visitors to each node.
class  EngineIterator : public ffi::EngineIterator
{
public:
    static constexpr uint64_t VISITOR_FAILED_OR_UNSUPPORTED = ~0;

    explicit EngineIterator(EngineIteratorData & data_) // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init)
    {
        data = &data_;
        get_next = &getNext;
    }

private:
    /// Name of the `Unknown` predicate handed to delta-kernel for a sub-expression we could not
    /// translate. It must be a fixed ASCII literal: visit_predicate_unknown validates the name as
    /// UTF-8 and reports failure for anything else, while a DAG node's result_name may hold
    /// arbitrary bytes (e.g. a binary string literal appearing in the filter).
    static constexpr std::string_view UNTRANSLATED_PREDICATE_NAME = "clickhouse_untranslated";

    /// Represent "this node could not be translated" to delta-kernel.
    ///
    /// Returning nullptr instead would mean *the iterator is exhausted*, which truncates the
    /// enclosing junction rather than dropping one child. That is only harmless in monotone
    /// position: an empty conjunction normalizes to TRUE, so under NOT it becomes FALSE and every
    /// data file gets skipped; a partially consumed conjunction under NOT yields a predicate
    /// narrower than the truth. An explicit Unknown is "cannot decide" in the kernel's
    /// three-valued logic and never skips a file on its own account, in any polarity.
    static uintptr_t visitUntranslated(EngineIteratorData & iterator_data)
    {
        const std::string name{UNTRANSLATED_PREDICATE_NAME};
        auto unknown = ffi::visit_predicate_unknown(iterator_data.state, KernelUtils::toDeltaString(name));
        if (!unknown)
        {
            /// Unreachable with a compile-time ASCII name unless an invariant broke: 0 is the
            /// kernel's reserved "no id" sentinel, and handing it on would reintroduce the very
            /// truncation this function exists to prevent.
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "delta-kernel rejected the `{}` predicate name", name);
        }
        return unknown;
    }

    static const void * getNext(void * data_)
    {
        auto * iterator_data = static_cast<EngineIteratorData *>(data_);
        try
        {
            if (iterator_data->hasException())
            {
                LOG_TEST(iterator_data->log(), "Exception during processing");
                return nullptr;
            }

            const auto * node = iterator_data->next();
            if (!node)
            {
                /// Real exhaustion, which is what nullptr means to the kernel.
                LOG_TEST(iterator_data->log(), "Iterator finished");
                return nullptr;
            }

            LOG_TEST(
                iterator_data->log(),
                "Node name: {}, node type: {}, column type: {}",
                node->result_name,
                node->type,
                node->column ? toString(node->column->getDataType()) : "None");

            auto result = getNextImpl(*iterator_data, node);
            if (result && result != VISITOR_FAILED_OR_UNSUPPORTED)
            {
                return reinterpret_cast<const void *>(result);
            }

            LOG_TEST(iterator_data->log(), "Node could not be translated, visiting it as unknown");
        }
        catch (...)
        {
            iterator_data->setException(std::current_exception());
        }

        /// Reached when the node was not translated, either because the visitor reported
        /// failure or because it threw (the exception stays recorded on the shared predicate).
        /// This function is invoked from Rust through an `extern "C"` pointer, so an exception
        /// must not leave it.
        try
        {
            return reinterpret_cast<const void *>(visitUntranslated(*iterator_data));
        }
        catch (...)
        {
            iterator_data->setException(std::current_exception());
            return nullptr;
        }
    }

    static uintptr_t getNextImpl(EngineIteratorData & iterator_data, const DB::ActionsDAG::Node * node);

    static uintptr_t visitComparisonOverDateConversion(
        EngineIteratorData & iterator_data,
        const DateComparison & comparison);

    static uintptr_t visitTimestampComparison(
        EngineIteratorData & iterator_data,
        const TimestampComparison & comparison);
};

uintptr_t EnginePredicate::visitPredicate(void * data, ffi::KernelExpressionVisitorState * state)
{
    auto * predicate = static_cast<EnginePredicate *>(data);
    EngineIteratorData iterator_data(state, predicate->filter.getOutputs(), *predicate);
    EngineIterator engine_iterator(iterator_data);
    auto result = ffi::visit_predicate_and(state, &engine_iterator);

    LOG_TEST(iterator_data.log(), "visitPredicate finished (exception: {})", predicate->hasException());
    return result;
}

static void injectLiteralVisitorFault()
{
    fiu_do_on(DB::FailPoints::delta_kernel_fail_literal_visitor,
    {
        throw DB::Exception(DB::ErrorCodes::FAULT_INJECTED, "Injecting fault for visitLiteralValue");
    });
}

static uintptr_t visitLiteralValue(
    const DB::Field & value,
    DB::TypeIndex type_index,
    DB::DataTypePtr data_type,
    ffi::KernelExpressionVisitorState * state)
{
    LOG_TEST(getLogger("EnginePredicate"), "Type index: {}, data type: {}", type_index, data_type->getName());

    injectLiteralVisitorFault();

    switch (type_index)
    {
        case DB::TypeIndex::String:
        case DB::TypeIndex::FixedString:
        {
            const auto & value_str = value.safeGet<String>();
            auto value_delta_str = KernelUtils::toDeltaString(value_str);
            return KernelUtils::unwrapResult(
                ffi::visit_expression_literal_string(
                    state,
                    value_delta_str,
                    &KernelUtils::allocateError), "visit_expression_literal_string");
        }
        case DB::TypeIndex::Int8:
        {
            auto result = value.safeGet<Int8>();
            return ffi::visit_expression_literal_byte(state, static_cast<int8_t>(result));
        }
        case DB::TypeIndex::UInt8:
        {
            if (isBool(data_type))
            {
                bool result = value.safeGet<UInt8>();
                return ffi::visit_expression_literal_bool(state, result); /// Accepts bool
            }
            else
            {
                auto result = value.safeGet<Int16>();
                return ffi::visit_expression_literal_short(state, static_cast<int16_t>(result));
            }
        }
        case DB::TypeIndex::Int16:
        {
            auto result = value.safeGet<Int16>();
            return ffi::visit_expression_literal_short(state, static_cast<int16_t>(result));
        }
        case DB::TypeIndex::UInt16:
        {
            auto result = value.safeGet<Int32>();
            return ffi::visit_expression_literal_int(state, static_cast<int32_t>(result));
        }
        case DB::TypeIndex::Int32:
        {
            auto result = value.safeGet<Int32>();
            return ffi::visit_expression_literal_int(state, static_cast<int32_t>(result));
        }
        case DB::TypeIndex::UInt32:
        {
            auto result = value.safeGet<Int64>();
            return ffi::visit_expression_literal_long(state, result); /// Accepts int64
        }
        case DB::TypeIndex::Int64:
        {
            auto result = value.safeGet<Int64>();
            return ffi::visit_expression_literal_long(state, result); /// Accepts int64
        }
        case DB::TypeIndex::Date:
        {
            auto result = value.safeGet<Int32>();
            return ffi::visit_expression_literal_date(state, static_cast<int32_t>(result));
        }
        case DB::TypeIndex::Date32:
        {
            auto result = value.safeGet<Int32>();
            return ffi::visit_expression_literal_date(state, static_cast<int32_t>(result));
        }
        default:
        {
            return EngineIterator::VISITOR_FAILED_OR_UNSUPPORTED;
        }
    }
}

enum class Junction { And, Or };

/// Builds an AND / OR over two predicate IDs.
static uintptr_t visitJunction(
    ffi::KernelExpressionVisitorState * state,
    Junction junction,
    std::array<uintptr_t, 2> ids)
{
    /// The kernel consumes the IDs synchronously and does not retain the iterator.
    struct Cursor
    {
        const std::array<uintptr_t, 2> & ids;
        size_t next = 0;
    };
    Cursor cursor{ids};

    ffi::EngineIterator iterator
    {
        .data = &cursor,
        .get_next = [](void * data) -> const void *
        {
            auto & current = *static_cast<Cursor *>(data);
            if (current.next == current.ids.size())
                return nullptr;
            return reinterpret_cast<const void *>(current.ids[current.next++]);
        },
    };
    return junction == Junction::And
        ? ffi::visit_predicate_and(state, &iterator)
        : ffi::visit_predicate_or(state, &iterator);
}

/// Visits a kernel literal of the column's type from the integer a comparison reduces to.
using LiteralVisitor = uintptr_t (*)(ffi::KernelExpressionVisitorState *, Int64);

static uintptr_t visitDateLiteral(ffi::KernelExpressionVisitorState * state, Int64 day)
{
    return ffi::visit_expression_literal_date(state, static_cast<int32_t>(day));
}

static uintptr_t visitTimestampLiteral(ffi::KernelExpressionVisitorState * state, Int64 micros)
{
    return ffi::visit_expression_literal_timestamp(state, micros);
}

static uintptr_t visitTimestampNtzLiteral(ffi::KernelExpressionVisitorState * state, Int64 micros)
{
    return ffi::visit_expression_literal_timestamp_ntz(state, micros);
}

/// Builds predicates for one column and validates every kernel ID.
struct ColumnPredicateBuilder
{
    ffi::KernelExpressionVisitorState * visitor_state;
    const DB::ActionsDAG::Node * column_node;
    LiteralVisitor visit_literal;

    uintptr_t compare(ComparisonVisitor visitor, Int64 value) const
    {
        /// Comparisons consume expression IDs, so register a fresh column and literal each time.
        auto column_id = assertValid(
            KernelUtils::unwrapResult(
                ffi::visit_expression_column(
                    visitor_state,
                    KernelUtils::toDeltaString(column_node->result_name),
                    &KernelUtils::allocateError),
                "visit_expression_column"),
            "column");
        injectLiteralVisitorFault();
        auto literal_id = assertValid(visit_literal(visitor_state, value), "literal");
        return assertValid(visitor(visitor_state, column_id, literal_id), "comparison");
    }

    uintptr_t andOf(uintptr_t left, uintptr_t right) const
    {
        return junction(Junction::And, left, right, "AND");
    }

    uintptr_t orOf(uintptr_t left, uintptr_t right) const
    {
        return junction(Junction::Or, left, right, "OR");
    }

    /// `compared` holds for a row only while the column is within `[min, max]`. Keeps the rows outside
    /// undecided: `(min <= column <= max AND compared) OR ((column < min OR column > max) AND unknown)`.
    uintptr_t fenced(uintptr_t compared, Int64 min, Int64 max, uintptr_t unknown) const
    {
        auto in_range = andOf(compare(ffi::visit_predicate_ge, min), compare(ffi::visit_predicate_le, max));
        auto out_of_range = orOf(compare(ffi::visit_predicate_lt, min), compare(ffi::visit_predicate_gt, max));
        return orOf(andOf(in_range, compared), andOf(out_of_range, unknown));
    }

private:
    uintptr_t junction(
        Junction kind, uintptr_t left, uintptr_t right, std::string_view description) const
    {
        /// A zero child would end the iterator early and silently shrink the junction.
        assertValid(left, description);
        assertValid(right, description);
        return assertValid(
            visitJunction(visitor_state, kind, {left, right}), description);
    }

    uintptr_t assertValid(uintptr_t id, std::string_view description) const
    {
        if (!id || id == EngineIterator::VISITOR_FAILED_OR_UNSUPPORTED)
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "delta-kernel rejected {} predicate on column `{}`",
                description, column_node->result_name);
        return id;
    }
};

uintptr_t EngineIterator::visitComparisonOverDateConversion(
    EngineIteratorData & iterator_data,
    const DateComparison & comparison)
{
    ColumnPredicateBuilder predicates{iterator_data.state, comparison.column, &visitDateLiteral};

    auto compared = predicates.compare(comparison.visitor, comparison.day);
    if (!comparison.narrows_to_date)
        return compared;

    /// `Date32` -> `Date` can wrap, saturate, or throw outside the Date domain.
    return predicates.fenced(compared, 0, DATE_LUT_MAX_DAY_NUM, visitUntranslated(iterator_data));
}

uintptr_t EngineIterator::visitTimestampComparison(
    EngineIteratorData & iterator_data,
    const TimestampComparison & comparison)
{
    ColumnPredicateBuilder predicates{
        iterator_data.state,
        comparison.column,
        comparison.kind == DeltaTimestampKind::TimestampNtz ? &visitTimestampNtzLiteral : &visitTimestampLiteral};

    auto compared = predicates.compare(comparison.visitor, comparison.micros);
    if (!comparison.fenced)
        return compared;
    return predicates.fenced(compared, comparison.fence_min, TIMESTAMP_FENCE_MAX, visitUntranslated(iterator_data));
}

uintptr_t EngineIterator::getNextImpl(EngineIteratorData & iterator_data, const DB::ActionsDAG::Node * node)
{
    if (iterator_data.hasException())
    {
        LOG_TEST(iterator_data.log(), "Exception during processing, returning from getNextImpl");
        return VISITOR_FAILED_OR_UNSUPPORTED;
    }

    switch (node->type)
    {
        case DB::ActionsDAG::ActionType::FUNCTION:
        {
            const auto func_name = node->function_base->getName();
            LOG_TEST(iterator_data.log(), "Function: {}", func_name);

            if (func_name == DB::NameAnd::name)
            {
                EngineIteratorData current_iterator_data(
                        iterator_data.state,
                        node->children,
                        iterator_data.predicate);

                EngineIterator current_engine_iterator(current_iterator_data);

                return ffi::visit_predicate_and(iterator_data.state, &current_engine_iterator);
            }
            else if (func_name == DB::NameNot::name)
            {
                if (node->children.size() != 1)
                {
                    throw DB::Exception(
                        DB::ErrorCodes::LOGICAL_ERROR,
                        "Expected function `{}` to have 1 child node, got {}",
                        func_name, node->children.size());
                }

                if (isColumnNode(node->children[0]))
                {
                    const auto column_name = KernelUtils::toDeltaString(node->children[0]->result_name);
                    uintptr_t column = KernelUtils::unwrapResult(
                        ffi::visit_expression_column(iterator_data.state,
                                                     column_name,
                                                     &KernelUtils::allocateError), "visit_expression_column");
                    return ffi::visit_predicate_not(iterator_data.state, column);
                }

                if (isFunctionNode(node->children[0]))
                {
                    EngineIteratorData current_iterator_data(
                            iterator_data.state,
                            node->children,
                            iterator_data.predicate);

                    EngineIterator current_engine_iterator(current_iterator_data);
                    auto column = ffi::visit_predicate_and(iterator_data.state, &current_engine_iterator);
                    return ffi::visit_predicate_not(iterator_data.state, column);
                }
            }
            else if (auto op = parseComparisonOperator(func_name))
            {
                if (node->children.size() != 2)
                {
                    throw DB::Exception(
                        DB::ErrorCodes::LOGICAL_ERROR,
                        "Expected function `{}` to have 2 child nodes, got {}",
                        func_name, node->children.size());
                }

                auto print_node_info = [&](const DB::ActionsDAG::Node * node_)
                {
                    LOG_TEST(getLogger("test"),
                             "Left node type: {}, result name: {}, result type: {}, "
                             "is constant: {}, column: {}, column type: {}",
                             node_->type,
                             node_->result_name,
                             node_->result_type,
                             node_->is_deterministic_constant,
                             bool(node_->column),
                             node_->column ? DB::toString(node_->column->getDataType()) : "None");
                };
                print_node_info(node->children[0]);
                print_node_info(node->children[1]);

                const DB::ActionsDAG::Node * column_node = nullptr;
                const DB::ActionsDAG::Node * literal_node = nullptr;
                if (isConstNode(node->children[0]) && isColumnNode(node->children[1]))
                {
                    literal_node = node->children[0];
                    column_node = node->children[1];
                }
                else if (isConstNode(node->children[1]) && isColumnNode(node->children[0]))
                {
                    literal_node = node->children[1];
                    column_node = node->children[0];
                }

                if (literal_node && column_node)
                {
                    /// A Delta timestamp column never takes the non-accurate cast below,
                    /// which would truncate a literal finer than microseconds.
                    if (const auto * delta_column = iterator_data.predicate.getTimestampColumn(column_node->result_name))
                    {
                        if (auto comparison = matchTimestampComparison(
                                *node, column_node, literal_node, isConstNode(node->children[0]), *delta_column, *op,
                                DB::getFormatSettings(iterator_data.predicate.getContext())))
                            return visitTimestampComparison(iterator_data, *comparison);
                        return VISITOR_FAILED_OR_UNSUPPORTED;
                    }

                    /// If literal node has a different type from column's,
                    /// cast it to column's type.
                    if (!column_node->result_type->equals(*literal_node->result_type))
                    {
                        auto column_name = column_node->result_type->getName();
                        auto column_type = std::make_shared<DB::DataTypeString>();
                        auto column = assert_cast<const DB::ColumnConst &>(*column_type->createColumnConst(0, column_name)).getPtr();

                        /// TODO: get rid of const_cast.
                        DB::ActionsDAG & dag = const_cast<DB::ActionsDAG &>(iterator_data.predicate.getFilterDAG());

                        const auto * right_arg = &dag.addColumn(std::move(column), std::move(column_type), std::move(column_name));
                        const auto * left_arg = literal_node;

                        DB::CastDiagnostic diagnostic = {literal_node->result_name, column_node->result_name};
                        DB::ColumnWithTypeAndName left_column{nullptr, literal_node->result_type, {}};
                        auto func_base_cast = DB::createInternalCast(
                            /* from */std::move(left_column),
                            /* to */column_node->result_type,
                            DB::CastType::nonAccurate,
                            std::move(diagnostic),
                            iterator_data.predicate.getContext());

                        DB::ActionsDAG::NodeRawConstPtrs children = { left_arg, right_arg };
                        literal_node = &dag.addFunction(func_base_cast, std::move(children), {});

                        print_node_info(literal_node);
                    }

                    const auto column_name = KernelUtils::toDeltaString(column_node->result_name);
                    uintptr_t column = KernelUtils::unwrapResult(
                        ffi::visit_expression_column(iterator_data.state,
                                                    column_name,
                                                    &KernelUtils::allocateError), "visit_expression_column");

                    const auto comparison_type_index = getTypeIndex(column_node);

                    DB::Field value = literal_node->column->getField();

                    uintptr_t constant = visitLiteralValue(
                        value,
                        comparison_type_index,
                        getTypeOrNestedType(column_node),
                        iterator_data.state);

                    if (!constant || constant == VISITOR_FAILED_OR_UNSUPPORTED)
                    {
                        LOG_TEST(iterator_data.log(), "Unsupported literal type: {}", comparison_type_index);
                        return VISITOR_FAILED_OR_UNSUPPORTED;
                    }

                    auto visitor = getComparisonVisitor(isConstNode(node->children[0]) ? mirror(*op) : *op);
                    return visitor(iterator_data.state, column, constant);
                }

                if (auto comparison = matchDateComparison(node, *op, DB::getFormatSettings(iterator_data.predicate.getContext())))
                    return visitComparisonOverDateConversion(iterator_data, *comparison);
            }

            break;
        }
        default:
        {
            break;
        }
    }
    return VISITOR_FAILED_OR_UNSUPPORTED;
}

}

#endif
