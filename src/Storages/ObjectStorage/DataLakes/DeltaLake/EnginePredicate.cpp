#include <Storages/ObjectStorage/DataLakes/DeltaLake/EnginePredicate.h>

#if USE_DELTA_KERNEL_RS
#include <Analyzer/Utils.h>
#include <Common/logger_useful.h>
#include <Common/FailPoint.h>

#include <Columns/ColumnConst.h>
#include <Columns/IColumn.h>
#include <Common/DateLUTImpl.h>
#include <Common/FieldAccurateComparison.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <Formats/FormatFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/IFunction.h>
#include <Functions/ComparisonNames.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/convertFieldToType.h>
#include <Functions/FunctionsLogical.h>

#include <Interpreters/ActionsDAG.h>
#include <Parsers/ASTHelpers.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>

#include <array>
#include <optional>
#include <utility>

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

    /// Kernel visitor for `left OP right` over two expression IDs:
    /// `visit_predicate_eq`, `_ne`, `_lt`, `_le`, `_gt`, `_ge`.
    using ComparisonVisitor = uintptr_t (*)(ffi::KernelExpressionVisitorState *, uintptr_t, uintptr_t);

    struct ComparisonVisitors
    {
        ComparisonVisitor direct;
        /// Reads the comparison with its operands swapped (`literal OP column`).
        ComparisonVisitor mirrored;
        /// For a literal strictly between a column value and the next one, the comparison against that value; null for `=` / `!=`.
        ComparisonVisitor direct_inexact = nullptr;
        ComparisonVisitor mirrored_inexact = nullptr;

        ComparisonVisitor forOperands(bool swapped, bool exact) const
        {
            if (exact)
                return swapped ? mirrored : direct;
            return swapped ? mirrored_inexact : direct_inexact;
        }
    };

    std::optional<ComparisonVisitors> getComparisonVisitors(const String & name)
    {
        if (name == DB::NameEquals::name)
            return ComparisonVisitors{&ffi::visit_predicate_eq, &ffi::visit_predicate_eq};
        if (name == DB::NameNotEquals::name)
            return ComparisonVisitors{&ffi::visit_predicate_ne, &ffi::visit_predicate_ne};
        if (name == DB::NameLess::name)
            return ComparisonVisitors{
                &ffi::visit_predicate_lt, &ffi::visit_predicate_gt, &ffi::visit_predicate_le, &ffi::visit_predicate_gt};
        if (name == DB::NameLessOrEquals::name)
            return ComparisonVisitors{
                &ffi::visit_predicate_le, &ffi::visit_predicate_ge, &ffi::visit_predicate_le, &ffi::visit_predicate_gt};
        if (name == DB::NameGreater::name)
            return ComparisonVisitors{
                &ffi::visit_predicate_gt, &ffi::visit_predicate_lt, &ffi::visit_predicate_gt, &ffi::visit_predicate_le};
        if (name == DB::NameGreaterOrEquals::name)
            return ComparisonVisitors{
                &ffi::visit_predicate_ge, &ffi::visit_predicate_le, &ffi::visit_predicate_gt, &ffi::visit_predicate_le};
        return std::nullopt;
    }

    struct ConvertedLiteral
    {
        DB::Field value;
        bool exact;
    };

    /// Converts a literal to `column_type` the way a comparison with such a column compares it.
    /// An inexact result lies strictly between `value` and the next value of the type.
    std::optional<ConvertedLiteral> convertLiteralToType(
        const DB::DataTypePtr & column_type,
        const DB::ActionsDAG::Node & literal_node,
        const DB::FormatSettings & format_settings)
    {
        const auto type = DB::removeLowCardinalityAndNullable(column_type);
        const auto literal_type = DB::removeLowCardinalityAndNullable(literal_node.result_type);
        const auto literal = literal_node.column->getField();
        if (literal.isNull())
            return std::nullopt;

        /// `Bool` equals `UInt8`.
        if (literal_type->equals(*type) && isBool(literal_type) == isBool(type))
            return ConvertedLiteral{literal, true};

        /// Strings of different types compare zero-padded, and no other literal compares with a string.
        if (isStringOrFixedString(type))
            return std::nullopt;

        DB::Field value;
        if (isStringOrFixedString(literal_type))
        {
            value = DB::tryConvertFieldToType(literal, *type, literal_type.get(), format_settings);
        }
        else if (isDateOrDate32(type) && isDateOrDate32OrDateTimeOrDateTime64(literal_type))
        {
            /// Compared in the common type, whose time zone can differ from the literal's.
            const auto common = DB::tryGetLeastSupertype(DB::DataTypes{type, literal_type});
            /// A `Date` compared in a `DateTime` is out of order outside the `DateTime` range.
            if (!common || isDateTime(common))
                return std::nullopt;
            const auto instant = DB::tryConvertFieldToType(literal, *common, literal_type.get(), format_settings, /*strict=*/ true);
            const auto day = DB::tryConvertFieldToType(instant, *type, common.get(), format_settings, /*strict=*/ true);
            const auto day_start = DB::tryConvertFieldToType(day, *common, type.get(), format_settings, /*strict=*/ true);
            if (instant.isNull() || day.isNull() || day_start.isNull() || !DB::accurateLessOrEqual(day_start, instant))
                return std::nullopt;
            /// Days outside the `DateTime64` range compare as its bounds, so an instant at a bound can equal several days.
            if (isDateTime64(common))
            {
                const auto scale_multiplier = DB::DecimalUtils::scaleMultiplier<DB::DateTime64::NativeType>(
                    assert_cast<const DB::DataTypeDateTime64 &>(*common).getScale());
                const Int64 ticks = instant.safeGet<DB::DecimalField<DB::DateTime64>>().getValue().value;
                if (ticks <= DB::minTicksForDateTime64(scale_multiplier) || ticks >= DB::maxTicksForDateTime64(scale_multiplier))
                    return std::nullopt;
            }
            if (!DB::accurateEquals(day_start, instant))
                return ConvertedLiteral{day, false};
            /// A skipped day starts at the same instant as the next one.
            const Int64 day_number = day.safeGet<Int64>();
            if (day_number > (isDate(type) ? 0 : DATE_LUT_MIN_EXTEND_DAY_NUM))
            {
                const auto previous_start = DB::tryConvertFieldToType(
                    DB::Field(day_number - 1), *common, type.get(), format_settings, /*strict=*/ true);
                if (previous_start.isNull() || !DB::accurateLess(previous_start, day_start))
                    return std::nullopt;
            }
            return ConvertedLiteral{day, true};
        }
        else
        {
            value = DB::tryConvertFieldToTypeExact(literal, *type, literal_type.get());
        }

        if (value.isNull())
            return std::nullopt;
        return ConvertedLiteral{value, true};
    }

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
        const ComparisonVisitors & visitors,
        const DB::FormatSettings & format_settings)
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

        /// Removing nullability can throw; pruning must not hide those rows.
        if (column->result_type->isNullable() && !conversion->result_type->isNullable())
            return std::nullopt;

        const auto converted = convertLiteralToType(result_type, *literal, format_settings);
        if (!converted)
            return std::nullopt;
        const auto visitor = visitors.forOperands(swapped, converted->exact);
        if (!visitor)
            return std::nullopt;

        return DateComparison{
            column,
            static_cast<Int32>(converted->value.safeGet<Int32>()),
            result_type->getTypeId() == DB::TypeIndex::Date,
            visitor};
    }
}

std::shared_ptr<EnginePredicate> getEnginePredicate(
    const DB::ActionsDAG & filter, std::exception_ptr & exception, DB::ContextPtr context)
{
    return std::make_unique<EnginePredicate>(filter, exception, context);
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

static uintptr_t visitLiteralValue(
    const DB::Field & value,
    DB::TypeIndex type_index,
    DB::DataTypePtr data_type,
    ffi::KernelExpressionVisitorState * state)
{
    LOG_TEST(getLogger("EnginePredicate"), "Type index: {}, data type: {}", type_index, data_type->getName());

    fiu_do_on(DB::FailPoints::delta_kernel_fail_literal_visitor,
    {
        throw DB::Exception(DB::ErrorCodes::FAULT_INJECTED, "Injecting fault for visitLiteralValue");
    });

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
                /// A `Bool` byte above 1 compares as a number, which a kernel boolean cannot express.
                const auto result = value.safeGet<UInt64>();
                if (result > 1)
                    return EngineIterator::VISITOR_FAILED_OR_UNSUPPORTED;
                return ffi::visit_expression_literal_bool(state, result == 1); /// Accepts bool
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

/// Builds predicates for one column and validates every kernel ID.
struct ColumnPredicateBuilder
{
    ffi::KernelExpressionVisitorState * visitor_state;
    const DB::ActionsDAG::Node * column_node;
    DB::DataTypePtr column_type;

    uintptr_t compare(ComparisonVisitor visitor, Int32 day) const
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
        auto literal_id = assertValid(
            visitLiteralValue(
                DB::Field(Int64(day)), column_type->getTypeId(), column_type, visitor_state),
            "date literal");
        return assertValid(visitor(visitor_state, column_id, literal_id), "date comparison");
    }

    uintptr_t andOf(uintptr_t left, uintptr_t right) const
    {
        return junction(Junction::And, left, right, "AND");
    }

    uintptr_t orOf(uintptr_t left, uintptr_t right) const
    {
        return junction(Junction::Or, left, right, "OR");
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
    ColumnPredicateBuilder predicates{
        iterator_data.state, comparison.column, getTypeOrNestedType(comparison.column)};

    auto compared = predicates.compare(comparison.visitor, comparison.day);
    if (!comparison.narrows_to_date)
        return compared;

    /// `Date32` -> `Date` can wrap, saturate, or throw outside the Date domain.
    /// Preserve outside-domain rows: (inside AND comparison) OR (outside AND Unknown).
    auto is_date_range = predicates.andOf(
        predicates.compare(ffi::visit_predicate_ge, 0),
        predicates.compare(ffi::visit_predicate_le, DATE_LUT_MAX_DAY_NUM));
    auto outside_date_range = predicates.orOf(
        predicates.compare(ffi::visit_predicate_lt, 0),
        predicates.compare(ffi::visit_predicate_gt, DATE_LUT_MAX_DAY_NUM));
    auto unknown_outside_range =
        predicates.andOf(outside_date_range, visitUntranslated(iterator_data));
    return predicates.orOf(
        predicates.andOf(is_date_range, compared), unknown_outside_range);
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
            else if (auto visitors = getComparisonVisitors(func_name))
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
                    const auto column_type = getTypeOrNestedType(column_node);
                    const auto literal = convertLiteralToType(
                        column_type, *literal_node, iterator_data.predicate.getFormatSettings());
                    if (!literal)
                        return VISITOR_FAILED_OR_UNSUPPORTED;
                    const auto visitor = visitors->forOperands(isConstNode(node->children[0]), literal->exact);
                    if (!visitor)
                        return VISITOR_FAILED_OR_UNSUPPORTED;

                    const auto column_name = KernelUtils::toDeltaString(column_node->result_name);
                    uintptr_t column = KernelUtils::unwrapResult(
                        ffi::visit_expression_column(iterator_data.state,
                                                    column_name,
                                                    &KernelUtils::allocateError), "visit_expression_column");

                    const auto comparison_type_index = getTypeIndex(column_node);

                    uintptr_t constant = visitLiteralValue(
                        literal->value,
                        comparison_type_index,
                        column_type,
                        iterator_data.state);

                    if (!constant || constant == VISITOR_FAILED_OR_UNSUPPORTED)
                    {
                        LOG_TEST(iterator_data.log(), "Unsupported literal type: {}", comparison_type_index);
                        return VISITOR_FAILED_OR_UNSUPPORTED;
                    }

                    return visitor(iterator_data.state, column, constant);
                }

                if (auto comparison = matchDateComparison(
                        node, *visitors, iterator_data.predicate.getFormatSettings()))
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
