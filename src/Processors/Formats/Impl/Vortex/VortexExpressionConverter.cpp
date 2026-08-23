#include <Processors/Formats/Impl/Vortex/VortexExpressionConverter.h>

#if USE_VORTEX

#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Set.h>
#include <Interpreters/convertFieldToType.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Common/StringUtils.h>
#include <Common/assert_cast.h>

#include <arrow/type.h>

#include <cmath>

#include <vortex_ffi.h>

namespace DB::Vortex
{

namespace
{

/// A larger `IN` list becomes a chain of ORs that has to be evaluated against every statistics
/// zone, which costs more than the pushdown saves.
constexpr size_t MAX_PUSHED_DOWN_SET_SIZE = 64;

/// The scale a `DateTime64` header must have for its ticks to be the file's ticks. A coarser or
/// finer scale would make the decoder rescale the values, and a comparison on the raw ticks would
/// then mean something else.
std::optional<UInt32> timestampUnitScale(arrow::TimeUnit::type unit)
{
    switch (unit)
    {
        case arrow::TimeUnit::SECOND: return 0;
        case arrow::TimeUnit::MILLI: return 3;
        case arrow::TimeUnit::MICRO: return 6;
        case arrow::TimeUnit::NANO: return 9;
    }
    return std::nullopt;
}

std::optional<FFI_VortexTimeUnit> timestampFFIUnit(arrow::TimeUnit::type unit)
{
    switch (unit)
    {
        case arrow::TimeUnit::SECOND: return FFI_VortexTimeUnit::Seconds;
        case arrow::TimeUnit::MILLI: return FFI_VortexTimeUnit::Milliseconds;
        case arrow::TimeUnit::MICRO: return FFI_VortexTimeUnit::Microseconds;
        case arrow::TimeUnit::NANO: return FFI_VortexTimeUnit::Nanoseconds;
    }
    return std::nullopt;
}

std::optional<FFI_VortexComparisonOperator> comparisonOperatorFromFunctionName(std::string_view name)
{
    if (name == "equals")
        return FFI_VortexComparisonOperator::Eq;
    if (name == "notEquals")
        return FFI_VortexComparisonOperator::NotEq;
    if (name == "less")
        return FFI_VortexComparisonOperator::Lt;
    if (name == "lessOrEquals")
        return FFI_VortexComparisonOperator::Lte;
    if (name == "greater")
        return FFI_VortexComparisonOperator::Gt;
    if (name == "greaterOrEquals")
        return FFI_VortexComparisonOperator::Gte;
    return std::nullopt;
}

/// `5 < n` compares the same rows as `n > 5`.
FFI_VortexComparisonOperator mirrorComparisonOperator(FFI_VortexComparisonOperator op)
{
    switch (op)
    {
        case FFI_VortexComparisonOperator::Lt: return FFI_VortexComparisonOperator::Gt;
        case FFI_VortexComparisonOperator::Lte: return FFI_VortexComparisonOperator::Gte;
        case FFI_VortexComparisonOperator::Gt: return FFI_VortexComparisonOperator::Lt;
        case FFI_VortexComparisonOperator::Gte: return FFI_VortexComparisonOperator::Lte;
        case FFI_VortexComparisonOperator::Eq:
        case FFI_VortexComparisonOperator::NotEq: return op;
    }
}

}

VortexExpressionConverter::VortexExpressionConverter(
    const Block & header_, const arrow::Schema & file_schema_, const FormatSettings & format_settings_)
    : header(header_)
    , file_schema(file_schema_)
    , format_settings(format_settings_)
{
}

const std::unordered_map<std::string_view, VortexExpressionConverter::Handler> & VortexExpressionConverter::handlers()
{
    /// `nullIn`/`globalNullIn` (`transform_null_in = 1`) are absent on purpose: they match NULL
    /// against NULL elements, while a Vortex comparison with NULL never matches.
    static const std::unordered_map<std::string_view, Handler> map = {
        {"and", &VortexExpressionConverter::convertAnd},
        {"or", &VortexExpressionConverter::convertOr},
        {"not", &VortexExpressionConverter::convertNot},
        {"equals", &VortexExpressionConverter::convertComparison},
        {"notEquals", &VortexExpressionConverter::convertComparison},
        {"less", &VortexExpressionConverter::convertComparison},
        {"lessOrEquals", &VortexExpressionConverter::convertComparison},
        {"greater", &VortexExpressionConverter::convertComparison},
        {"greaterOrEquals", &VortexExpressionConverter::convertComparison},
        {"isNull", &VortexExpressionConverter::convertIsNull},
        {"isNotNull", &VortexExpressionConverter::convertIsNotNull},
        {"in", &VortexExpressionConverter::convertIn},
        {"globalIn", &VortexExpressionConverter::convertIn},
        {"notIn", &VortexExpressionConverter::convertIn},
        {"globalNotIn", &VortexExpressionConverter::convertIn},
        {"like", &VortexExpressionConverter::convertLike},
        {"notLike", &VortexExpressionConverter::convertNotLike},
        {"startsWith", &VortexExpressionConverter::convertStartsWith},
    };
    return map;
}

VortexExpressionPtr VortexExpressionConverter::tryConvert(const RPNBuilderTreeNode & node, bool allow_widening) const
{
    if (node.isConstant())
    {
        Field value;
        DataTypePtr value_type;
        if (!node.tryGetConstant(value, value_type))
            return nullptr;
        /// Only a predicate that never holds is useful to push: the scan then skips the whole
        /// file. NULL never passes a WHERE either. An always-true constant is simply no filter.
        bool is_false = value.isNull();
        if (value.getType() == Field::Types::UInt64)
            is_false = value.safeGet<UInt64>() == 0;
        else if (value.getType() == Field::Types::Int64)
            is_false = value.safeGet<Int64>() == 0;
        else if (value.getType() == Field::Types::Bool)
            is_false = !value.safeGet<bool>();
        if (is_false)
            return VortexExpressionPtr(vortex_ffi_expr_literal_bool(false));
        return nullptr;
    }

    if (node.isFunction())
    {
        auto function_node = node.toFunctionNode();
        const auto & map = handlers();
        auto it = map.find(function_node.getFunctionName());
        if (it == map.end())
            return nullptr;
        return (this->*it->second)(function_node, allow_widening);
    }

    return convertBareBooleanColumn(node);
}

std::optional<VortexExpressionConverter::ResolvedColumn>
VortexExpressionConverter::resolveColumn(const RPNBuilderTreeNode & node, TypeMatch type_match) const
{
    if (node.isFunction() || node.isConstant())
        return std::nullopt;

    const String column_name = node.getColumnName();
    auto arrow_field = file_schema.GetFieldByName(column_name);
    const auto * header_column = header.findByName(column_name);
    if (!arrow_field || !header_column)
        return std::nullopt;

    DataTypePtr cmp_type = removeNullable(recursiveRemoveLowCardinality(header_column->type));
    if (type_match == TypeMatch::Required && !typesMatchForFilterPushdown(cmp_type, *arrow_field->type()))
        return std::nullopt;

    /// When a nullable file column is read as non-nullable, its nulls become default values. The
    /// scan does not know about that substitution: it evaluates the atom on null and drops the row.
    const bool header_nullable = header_column->type->isNullable() || header_column->type->isLowCardinalityNullable();
    if (arrow_field->nullable() && !header_nullable)
        return std::nullopt;

    VortexExpressionPtr column(vortex_ffi_expr_column(column_name.c_str()));
    if (!column)
        return std::nullopt;

    return ResolvedColumn{std::move(column), std::move(arrow_field), std::move(cmp_type)};
}

bool VortexExpressionConverter::typesMatchForFilterPushdown(const DataTypePtr & cmp_type, const arrow::DataType & arrow_type) const
{
    WhichDataType which(cmp_type);
    switch (arrow_type.id())
    {
        case arrow::Type::INT8: return which.isInt8();
        case arrow::Type::INT16: return which.isInt16();
        case arrow::Type::INT32: return which.isInt32();
        case arrow::Type::INT64: return which.isInt64();
        /// A `Bool` header over a `U8` file column clamps every non-zero value to 1 when read, so a
        /// comparison on the raw bytes would mean something else.
        case arrow::Type::UINT8: return which.isUInt8() && !isBool(cmp_type);
        /// `Date` is a day number in the same `UInt16` domain, copied 1:1 from a `U16` file column.
        case arrow::Type::UINT16: return which.isUInt16() || which.isDate();
        case arrow::Type::UINT32: return which.isUInt32();
        case arrow::Type::UINT64: return which.isUInt64();
        case arrow::Type::FLOAT: return which.isFloat32();
        case arrow::Type::DOUBLE: return which.isFloat64();
        /// A boolean file column always reads as {0, 1}, whatever UInt8 flavor the header uses.
        case arrow::Type::BOOL: return which.isUInt8();
        /// The day numbers are copied 1:1 - except under `Saturate`, where out-of-range days are
        /// clamped onto the bounds and an equality on a bound would match rows it should not.
        case arrow::Type::DATE32:
            return which.isDate32() && format_settings.date_time_overflow_behavior != FormatSettings::DateTimeOverflowBehavior::Saturate;
        /// The ticks are copied 1:1 when the scales agree; the header's time zone only affects
        /// rendering, not the stored ticks. Any other scale makes the decoder rescale the values.
        case arrow::Type::TIMESTAMP: {
            if (!which.isDateTime64())
                return false;
            auto scale = timestampUnitScale(static_cast<const arrow::TimestampType &>(arrow_type).unit());
            return scale && assert_cast<const DataTypeDateTime64 &>(*cmp_type).getScale() == *scale;
        }
        /// FixedString is excluded: its zero padding makes it order differently from Binary.
        case arrow::Type::STRING:
        case arrow::Type::LARGE_STRING:
        case arrow::Type::STRING_VIEW:
        case arrow::Type::BINARY:
        case arrow::Type::LARGE_BINARY:
        case arrow::Type::BINARY_VIEW: return which.isString();
        default: return false;
    }
}

VortexExpressionPtr VortexExpressionConverter::makeLiteral(
    const arrow::DataType & file_type, const DataTypePtr & cmp_type, const Field & value, const DataTypePtr & value_type) const
{
    /// A comparison with NULL matches nothing; there is no literal to build.
    if (value.isNull())
        return nullptr;

    Field converted = value;
    if (!value_type || !value_type->equals(*cmp_type))
    {
        /// Strict: the value converts exactly or not at all. A rounded bound would change which
        /// rows the comparison matches.
        converted = tryConvertFieldToType(value, *cmp_type, value_type.get(), {}, /* strict */ true);
        if (converted.isNull())
            return nullptr;
    }

    auto make_int = [&](FFI_VortexPrimitiveType ptype) -> VortexExpressionPtr
    {
        if (converted.getType() == Field::Types::Int64)
            return VortexExpressionPtr(vortex_ffi_expr_literal_int(ptype, converted.safeGet<Int64>()));
        if (converted.getType() == Field::Types::UInt64)
        {
            UInt64 uint_value = converted.safeGet<UInt64>();
            if (uint_value > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
                return nullptr;
            return VortexExpressionPtr(vortex_ffi_expr_literal_int(ptype, static_cast<Int64>(uint_value)));
        }
        return nullptr;
    };

    auto make_uint = [&](FFI_VortexPrimitiveType ptype) -> VortexExpressionPtr
    {
        if (converted.getType() == Field::Types::UInt64)
            return VortexExpressionPtr(vortex_ffi_expr_literal_uint(ptype, converted.safeGet<UInt64>()));
        if (converted.getType() == Field::Types::Int64)
        {
            Int64 int_value = converted.safeGet<Int64>();
            if (int_value < 0)
                return nullptr;
            return VortexExpressionPtr(vortex_ffi_expr_literal_uint(ptype, static_cast<UInt64>(int_value)));
        }
        return nullptr;
    };

    auto make_float = [&](FFI_VortexPrimitiveType ptype) -> VortexExpressionPtr
    {
        if (converted.getType() != Field::Types::Float64)
            return nullptr;
        Float64 float_value = converted.safeGet<Float64>();
        if (!std::isfinite(float_value))
            return nullptr;
        return VortexExpressionPtr(vortex_ffi_expr_literal_float(ptype, float_value));
    };

    auto make_string = [&](bool is_utf8) -> VortexExpressionPtr
    {
        if (converted.getType() != Field::Types::String)
            return nullptr;
        const auto & string_value = converted.safeGet<String>();
        return VortexExpressionPtr(
            vortex_ffi_expr_literal_string(reinterpret_cast<const uint8_t *>(string_value.data()), string_value.size(), is_utf8));
    };

    auto make_bool = [&]() -> VortexExpressionPtr
    {
        UInt64 bool_value;
        if (converted.getType() == Field::Types::UInt64 || converted.getType() == Field::Types::Bool)
            bool_value = converted.safeGet<UInt64>();
        else if (converted.getType() == Field::Types::Int64 && converted.safeGet<Int64>() >= 0)
            bool_value = static_cast<UInt64>(converted.safeGet<Int64>());
        else
            return nullptr;
        /// A boolean file column only holds {0, 1}; any other value can never be equal to it, and
        /// as a range bound it degenerates to always/never - not worth a special case.
        if (bool_value > 1)
            return nullptr;
        return VortexExpressionPtr(vortex_ffi_expr_literal_bool(bool_value == 1));
    };

    auto make_date = [&]() -> VortexExpressionPtr
    {
        /// `Date32` values are day numbers in an `Int64` field.
        if (converted.getType() != Field::Types::Int64)
            return nullptr;
        return VortexExpressionPtr(vortex_ffi_expr_literal_date(FFI_VortexTimeUnit::Days, converted.safeGet<Int64>()));
    };

    auto make_timestamp = [&]() -> VortexExpressionPtr
    {
        const auto & timestamp_type = static_cast<const arrow::TimestampType &>(file_type);
        auto unit = timestampFFIUnit(timestamp_type.unit());
        if (!unit || converted.getType() != Field::Types::Decimal64)
            return nullptr;
        /// The ticks are absolute; the literal only has to carry the file column's zone, because
        /// Vortex refuses to compare timestamps whose metadata differs.
        const Int64 ticks = converted.safeGet<DecimalField<DateTime64>>().getValue().value;
        const std::string & timezone = timestamp_type.timezone();
        return VortexExpressionPtr(vortex_ffi_expr_literal_timestamp(*unit, timezone.empty() ? nullptr : timezone.c_str(), ticks));
    };

    switch (file_type.id())
    {
        case arrow::Type::INT8: return make_int(FFI_VortexPrimitiveType::I8);
        case arrow::Type::INT16: return make_int(FFI_VortexPrimitiveType::I16);
        case arrow::Type::INT32: return make_int(FFI_VortexPrimitiveType::I32);
        case arrow::Type::INT64: return make_int(FFI_VortexPrimitiveType::I64);
        case arrow::Type::UINT8: return make_uint(FFI_VortexPrimitiveType::U8);
        case arrow::Type::UINT16: return make_uint(FFI_VortexPrimitiveType::U16);
        case arrow::Type::UINT32: return make_uint(FFI_VortexPrimitiveType::U32);
        case arrow::Type::UINT64: return make_uint(FFI_VortexPrimitiveType::U64);
        case arrow::Type::FLOAT: return make_float(FFI_VortexPrimitiveType::F32);
        case arrow::Type::DOUBLE: return make_float(FFI_VortexPrimitiveType::F64);
        case arrow::Type::BOOL: return make_bool();
        case arrow::Type::DATE32: return make_date();
        case arrow::Type::TIMESTAMP: return make_timestamp();
        case arrow::Type::STRING:
        case arrow::Type::LARGE_STRING:
        case arrow::Type::STRING_VIEW: return make_string(/* is_utf8 */ true);
        case arrow::Type::BINARY:
        case arrow::Type::LARGE_BINARY:
        case arrow::Type::BINARY_VIEW: return make_string(/* is_utf8 */ false);
        default: return nullptr;
    }
}

VortexExpressionPtr VortexExpressionConverter::convertAnd(const RPNBuilderFunctionTreeNode & node, bool allow_widening) const
{
    /// Dropping a conjunct only lets more rows through, so a partially translated AND is still
    /// sound where widening is allowed.
    VortexExpressionPtr result;
    for (size_t i = 0, arguments = node.getArgumentsSize(); i < arguments; ++i)
    {
        auto child = tryConvert(node.getArgumentAt(i), allow_widening);
        if (!child)
        {
            if (allow_widening)
                continue;
            return nullptr;
        }
        if (result)
            result = VortexExpressionPtr(vortex_ffi_expr_and(result.get(), child.get()));
        else
            result = std::move(child);
    }
    return result;
}

VortexExpressionPtr VortexExpressionConverter::convertOr(const RPNBuilderFunctionTreeNode & node, bool allow_widening) const
{
    /// Dropping a disjunct would lose rows, so every one has to translate. Each may itself be
    /// widened: a wider disjunct only widens the OR.
    VortexExpressionPtr result;
    for (size_t i = 0, arguments = node.getArgumentsSize(); i < arguments; ++i)
    {
        auto child = tryConvert(node.getArgumentAt(i), allow_widening);
        if (!child)
            return nullptr;
        if (result)
            result = VortexExpressionPtr(vortex_ffi_expr_or(result.get(), child.get()));
        else
            result = std::move(child);
    }
    return result;
}

VortexExpressionPtr VortexExpressionConverter::convertNot(const RPNBuilderFunctionTreeNode & node, bool allow_widening) const
{
    if (node.getArgumentsSize() != 1)
        return nullptr;
    /// NOT of a widened subtree would lose rows, so below this point everything has to translate
    /// exactly. `allow_widening` never becomes true again: a second NOT underneath would have been
    /// folded away by the inversion push-down.
    (void)allow_widening;
    auto child = tryConvert(node.getArgumentAt(0), /* allow_widening */ false);
    if (!child)
        return nullptr;
    return VortexExpressionPtr(vortex_ffi_expr_not(child.get()));
}

VortexExpressionPtr VortexExpressionConverter::convertComparison(const RPNBuilderFunctionTreeNode & node, bool /* allow_widening */) const
{
    if (node.getArgumentsSize() != 2)
        return nullptr;

    auto op = comparisonOperatorFromFunctionName(node.getFunctionName());
    if (!op)
        return nullptr;

    auto lhs = node.getArgumentAt(0);
    auto rhs = node.getArgumentAt(1);

    Field value;
    DataTypePtr value_type;
    const RPNBuilderTreeNode * column_node = nullptr;
    if (rhs.isConstant() && rhs.tryGetConstant(value, value_type))
        column_node = &lhs;
    else if (lhs.isConstant() && lhs.tryGetConstant(value, value_type))
    {
        column_node = &rhs;
        *op = mirrorComparisonOperator(*op);
    }
    else
        return nullptr;

    auto column = resolveColumn(*column_node, TypeMatch::Required);
    if (!column)
        return nullptr;

    auto literal = makeLiteral(*column->field->type(), column->cmp_type, value, value_type);
    if (!literal)
        return nullptr;

    return VortexExpressionPtr(vortex_ffi_expr_compare(*op, column->expr.get(), literal.get()));
}

VortexExpressionPtr VortexExpressionConverter::convertIsNull(const RPNBuilderFunctionTreeNode & node, bool /* allow_widening */) const
{
    if (node.getArgumentsSize() != 1)
        return nullptr;
    /// Null-ness does not compare values, so the value sets do not have to match.
    auto column = resolveColumn(node.getArgumentAt(0), TypeMatch::NotRequired);
    if (!column)
        return nullptr;
    return VortexExpressionPtr(vortex_ffi_expr_is_null(column->expr.get()));
}

VortexExpressionPtr VortexExpressionConverter::convertIsNotNull(const RPNBuilderFunctionTreeNode & node, bool /* allow_widening */) const
{
    if (node.getArgumentsSize() != 1)
        return nullptr;
    auto column = resolveColumn(node.getArgumentAt(0), TypeMatch::NotRequired);
    if (!column)
        return nullptr;
    VortexExpressionPtr null_check(vortex_ffi_expr_is_null(column->expr.get()));
    return VortexExpressionPtr(vortex_ffi_expr_not(null_check.get()));
}

VortexExpressionPtr VortexExpressionConverter::convertIn(const RPNBuilderFunctionTreeNode & node, bool /* allow_widening */) const
{
    if (node.getArgumentsSize() != 2)
        return nullptr;

    const std::string function_name = node.getFunctionName();
    const bool negated = function_name == "notIn" || function_name == "globalNotIn";

    auto column = resolveColumn(node.getArgumentAt(0), TypeMatch::Required);
    if (!column)
        return nullptr;

    /// The discipline of `KeyCondition`: only inspect a set that is already built - forcing one
    /// here would execute an `IN` subquery from inside a format reader - and only then materialize
    /// its ordered elements, which is cheap for a built set.
    auto future_set = node.getArgumentAt(1).tryGetPreparedSet();
    if (!future_set)
        return nullptr;
    auto built_set = future_set->get();
    if (!built_set)
        return nullptr;
    if (built_set->getTotalRowCount() == 0 || built_set->getTotalRowCount() > MAX_PUSHED_DOWN_SET_SIZE)
        return nullptr;

    auto prepared_set = future_set->buildOrderedSetInplace(node.getTreeContext().getQueryContext());
    if (!prepared_set || !prepared_set->hasExplicitSetElements())
        return nullptr;

    const Columns set_elements = prepared_set->getSetElements();
    const DataTypes & set_types = prepared_set->getElementsTypes();
    /// Only single-column sets: the multi-column `(a, b) IN (...)` shapes have no column to
    /// compare, and their partially-collapsed forms are exactly where widening goes wrong.
    if (set_elements.size() != 1 || set_types.size() != 1)
        return nullptr;

    const IColumn & elements = *set_elements.front();
    VortexExpressionPtr result;
    for (size_t i = 0, elements_size = elements.size(); i < elements_size; ++i)
    {
        const Field element = elements[i];
        /// With `transform_null_in = 0` (`in`, not `nullIn`) a NULL element matches no row on
        /// either side, so it may be skipped exactly.
        if (element.isNull())
            continue;
        auto literal = makeLiteral(*column->field->type(), column->cmp_type, element, set_types.front());
        if (!literal)
            return nullptr;
        VortexExpressionPtr equals(vortex_ffi_expr_compare(FFI_VortexComparisonOperator::Eq, column->expr.get(), literal.get()));
        if (!equals)
            return nullptr;
        if (result)
            result = VortexExpressionPtr(vortex_ffi_expr_or(result.get(), equals.get()));
        else
            result = std::move(equals);
    }
    if (!result)
        return nullptr;
    if (negated)
        return VortexExpressionPtr(vortex_ffi_expr_not(result.get()));
    return result;
}

VortexExpressionPtr
VortexExpressionConverter::makePrefixRange(const ResolvedColumn & column, const String & prefix, bool allow_widening) const
{
    auto left_literal = makeLiteral(*column.field->type(), column.cmp_type, Field(prefix), std::make_shared<DataTypeString>());
    if (!left_literal)
        return nullptr;
    VortexExpressionPtr left_bound(vortex_ffi_expr_compare(FFI_VortexComparisonOperator::Gte, column.expr.get(), left_literal.get()));
    if (!left_bound)
        return nullptr;

    const String right = firstStringThatIsGreaterThanAllStringsWithPrefix(prefix);
    /// No string is greater than this prefix, so the left bound alone is already exact.
    if (right.empty())
        return left_bound;

    auto right_literal = makeLiteral(*column.field->type(), column.cmp_type, Field(right), std::make_shared<DataTypeString>());
    if (right_literal)
    {
        VortexExpressionPtr right_bound(vortex_ffi_expr_compare(FFI_VortexComparisonOperator::Lt, column.expr.get(), right_literal.get()));
        if (!right_bound)
            return nullptr;
        return VortexExpressionPtr(vortex_ffi_expr_and(left_bound.get(), right_bound.get()));
    }

    /// The bumped last byte of the right bound may not be valid UTF-8 for a `Utf8` file column.
    /// Keeping just the left bound widens the range, which is only allowed in a positive position.
    if (allow_widening)
        return left_bound;
    return nullptr;
}

VortexExpressionPtr VortexExpressionConverter::convertLike(const RPNBuilderFunctionTreeNode & node, bool allow_widening) const
{
    if (node.getArgumentsSize() != 2)
        return nullptr;

    Field pattern;
    DataTypePtr pattern_type;
    if (!node.getArgumentAt(1).tryGetConstant(pattern, pattern_type) || pattern.getType() != Field::Types::String)
        return nullptr;

    auto column = resolveColumn(node.getArgumentAt(0), TypeMatch::Required);
    if (!column)
        return nullptr;

    auto prefix = extractFixedPrefixFromLikePattern(pattern.safeGet<String>(), /* requires_perfect_prefix */ false);

    /// A pattern without wildcards is an equality.
    if (prefix.is_exact)
    {
        auto literal = makeLiteral(*column->field->type(), column->cmp_type, Field(prefix.prefix), std::make_shared<DataTypeString>());
        if (!literal)
            return nullptr;
        return VortexExpressionPtr(vortex_ffi_expr_compare(FFI_VortexComparisonOperator::Eq, column->expr.get(), literal.get()));
    }

    if (prefix.prefix.empty())
        return nullptr;

    /// A perfect prefix (`'foo%'`) makes the range exact; an imperfect one (`'foo%bar'`) makes it
    /// an over-approximation, which is only sound where widening is allowed.
    if (!prefix.is_perfect && !allow_widening)
        return nullptr;

    return makePrefixRange(*column, prefix.prefix, allow_widening);
}

VortexExpressionPtr VortexExpressionConverter::convertNotLike(const RPNBuilderFunctionTreeNode & node, bool /* allow_widening */) const
{
    if (node.getArgumentsSize() != 2)
        return nullptr;

    Field pattern;
    DataTypePtr pattern_type;
    if (!node.getArgumentAt(1).tryGetConstant(pattern, pattern_type) || pattern.getType() != Field::Types::String)
        return nullptr;

    auto column = resolveColumn(node.getArgumentAt(0), TypeMatch::Required);
    if (!column)
        return nullptr;

    /// The negation is exact only when the prefix describes the pattern exactly, so an imperfect
    /// prefix yields nothing here.
    auto prefix = extractFixedPrefixFromLikePattern(pattern.safeGet<String>(), /* requires_perfect_prefix */ true);

    if (prefix.is_exact)
    {
        auto literal = makeLiteral(*column->field->type(), column->cmp_type, Field(prefix.prefix), std::make_shared<DataTypeString>());
        if (!literal)
            return nullptr;
        return VortexExpressionPtr(vortex_ffi_expr_compare(FFI_VortexComparisonOperator::NotEq, column->expr.get(), literal.get()));
    }

    if (prefix.prefix.empty())
        return nullptr;
    chassert(prefix.is_perfect);

    auto range = makePrefixRange(*column, prefix.prefix, /* allow_widening */ false);
    if (!range)
        return nullptr;
    return VortexExpressionPtr(vortex_ffi_expr_not(range.get()));
}

VortexExpressionPtr VortexExpressionConverter::convertStartsWith(const RPNBuilderFunctionTreeNode & node, bool allow_widening) const
{
    if (node.getArgumentsSize() != 2)
        return nullptr;

    Field prefix;
    DataTypePtr prefix_type;
    if (!node.getArgumentAt(1).tryGetConstant(prefix, prefix_type) || prefix.getType() != Field::Types::String)
        return nullptr;
    const String & prefix_string = prefix.safeGet<String>();
    if (prefix_string.empty())
        return nullptr;

    auto column = resolveColumn(node.getArgumentAt(0), TypeMatch::Required);
    if (!column)
        return nullptr;

    return makePrefixRange(*column, prefix_string, allow_widening);
}

VortexExpressionPtr VortexExpressionConverter::convertBareBooleanColumn(const RPNBuilderTreeNode & node) const
{
    auto column = resolveColumn(node, TypeMatch::Required);
    /// Only a boolean column may be a filter by itself; for any other type the engine tests
    /// truthiness, which a Vortex filter expression does not.
    if (!column || column->field->type()->id() != arrow::Type::BOOL)
        return nullptr;
    return std::move(column->expr);
}

}

#endif
