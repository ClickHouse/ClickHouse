#include <DataTypes/DataTypeExponentialTimeDecaying.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnBLOB.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnExponentialTimeDecaying.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/SipHash.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <DataTypes/Serializations/SerializationWrapper.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <IO/WriteHelpers.h>

#include <algorithm>
#include <cerrno>
#include <cmath>
#include <limits>
#include <tuple>
#include <fmt/format.h>
#include <utility>
#include <boost/multiprecision/cpp_bin_float.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS;
}

namespace
{

template <typename DataType, typename Callback>
void forEachNestedDataType(const DataType & type, const Callback & callback)
{
    if constexpr (requires { type.forEachChild(callback); })
    {
        type.forEachChild(callback);
    }
    else
    {
        const size_t num_children = type.getNumberOfChildren();
        for (size_t i = 0; i < num_children; ++i)
        {
            const auto & child = *type.getChild(i);
            callback(child);
            forEachNestedDataType(child, callback);
        }
    }
}

using ExtendedTimeDecayFloat = boost::multiprecision::cpp_bin_float_quad;

// Adapted from Boost.Multiprecision `default_ops::eval_log`:
// https://github.com/ClickHouse/boost/blob/e24b1b25e3349236a3c5aac792e3294f62eda1f2/boost/multiprecision/detail/functions/pow.hpp#L366-L456
// Copyright 2011 - 2025 John Maddock.
// Copyright Christopher Kormanyos 2002 - 2025.
// Distributed under the Boost Software License, Version 1.0.
// See https://www.boost.org/LICENSE_1_0.txt or `contrib/boost/LICENSE_1_0.txt`.
// Based on the prior work "Algorithm 910: A Portable C++ Multiple-Precision System for
// Special-Function Calculations", ACM TOMS, Vol. 37, Issue 4, 2011 (C) ACM.
// https://doi.org/10.1145/1916461.1916469
// Keep the pinned range reduction, summation, and rounding order, but use cached `et_off` epsilon.
// The original `et_on` epsilon returns a lazy `ldexp` expression holding a temporary functor.
void evaluateExponentialTimeDecayLog(
    ExtendedTimeDecayFloat::backend_type & result, const ExtendedTimeDecayFloat::backend_type & arg)
{
    using T = ExtendedTimeDecayFloat::backend_type;
    using boost::multiprecision::number;
    using boost::multiprecision::number_category;
    using boost::multiprecision::number_kind_floating_point;
    using boost::multiprecision::default_ops::eval_add;
    using boost::multiprecision::default_ops::eval_divide;
    using boost::multiprecision::default_ops::eval_fpclassify;
    using boost::multiprecision::default_ops::eval_frexp;
    using boost::multiprecision::default_ops::eval_get_sign;
    using boost::multiprecision::default_ops::eval_ldexp;
    using boost::multiprecision::default_ops::eval_multiply;
    using boost::multiprecision::default_ops::eval_signbit;
    using boost::multiprecision::default_ops::eval_subtract;
    using boost::multiprecision::default_ops::get_constant_ln2;
    static_assert(number_category<T>::value == number_kind_floating_point, "The log function is only valid for floating point types.");
    //
    // We use a variation of http://dlmf.nist.gov/4.45#i
    // using `frexp` to reduce the argument to x * 2^n,
    // then let y = x - 1 and compute:
    // log(arg) = log(2) * n + log1p(y)
    //
    using ui_type = typename boost::multiprecision::detail::canonical<unsigned, T>::type;
    using exp_type = typename T::exponent_type;
    using canonical_exp_type = typename boost::multiprecision::detail::canonical<exp_type, T>::type;
    using fp_type = std::tuple_element_t<0, typename T::float_types>;

    const int s = eval_signbit(arg);

    switch (eval_fpclassify(arg))
    {
    case FP_NAN:
        result = arg;
        errno  = EDOM;
        return;
    case FP_INFINITE:
        if (s)
            break;
        result = arg;
        return;
    case FP_ZERO:
        result = std::numeric_limits<number<T>>::has_infinity
            ? std::numeric_limits<number<T>>::infinity().backend()
            : (std::numeric_limits<number<T>>::max)().backend();
        result.negate();
        errno = ERANGE;
        return;
    default:
        break;
    }
    if (s)
    {
        result = std::numeric_limits<number<T> >::quiet_NaN().backend();
        errno  = EDOM;
        return;
    }

    exp_type e = 0;
    T t;
    eval_frexp(t, arg, &e);
    bool alternate = false;

    if (t.compare(fp_type(2) / fp_type(3)) <= 0)
    {
        alternate = true;
        eval_ldexp(t, t, 1);
        --e;
    }

    eval_multiply(result, get_constant_ln2<T>(), canonical_exp_type(e));
    eval_subtract(t, ui_type(1)); /* -0.3 <= t <= 0.3 */
    if (!alternate)
        t.negate(); /* 0 <= t <= 0.33333 */
    T pow = t;
    T lim;
    T t2;

    if (alternate)
        eval_add(result, t);
    else
        eval_subtract(result, t);

    eval_multiply(lim, result, std::numeric_limits<number<T, boost::multiprecision::et_off>>::epsilon().backend());
    if (eval_get_sign(lim) < 0)
        lim.negate();

    ui_type k = 1;
    do
    {
        ++k;
        eval_multiply(pow, t);
        eval_divide(t2, pow, k);
        if (alternate && ((k & 1) != 0))
            eval_add(result, t2);
        else
            eval_subtract(result, t2);
    } while (lim.compare(t2) < 0);
}

ExtendedTimeDecayFloat logarithmExponentialTimeDecay(const ExtendedTimeDecayFloat & argument)
{
    ExtendedTimeDecayFloat result;
    evaluateExponentialTimeDecayLog(result.backend(), argument.backend());
    return result;
}

ExponentialTimeDecayingCanonicalDirectValue normalizeExponentialTimeDecaying128Direct(
    Float64 value, Float64 time, Float64 decay_length)
{
    if (value == 0)
        return {0, 0};

    const bool negative = std::signbit(value);
    const ExtendedTimeDecayFloat unit_timestamp
        = ExtendedTimeDecayFloat(time)
        + ExtendedTimeDecayFloat(decay_length) * logarithmExponentialTimeDecay(ExtendedTimeDecayFloat(std::abs(value)));

    const ExtendedTimeDecayFloat max_anchor(std::numeric_limits<Float64>::max());
    Float64 anchor = 0;
    if (unit_timestamp > max_anchor)
        anchor = std::numeric_limits<Float64>::max();
    else if (unit_timestamp < -max_anchor)
        anchor = -std::numeric_limits<Float64>::max();
    else
    {
        anchor = static_cast<Float64>(unit_timestamp);
        if (ExtendedTimeDecayFloat(anchor) > unit_timestamp)
            anchor = std::nextafter(anchor, -std::numeric_limits<Float64>::infinity());
    }

    auto magnitude_at = [&](Float64 candidate)
    {
        return static_cast<Float64>(
            exp((unit_timestamp - ExtendedTimeDecayFloat(candidate))
                / ExtendedTimeDecayFloat(decay_length)));
    };

    Float64 magnitude = magnitude_at(anchor);
    if ((!std::isfinite(magnitude) || magnitude == 0)
        && unit_timestamp >= -max_anchor
        && unit_timestamp <= max_anchor)
    {
        /// For a very short decay length, the value at the lower Float64
        /// anchor can overflow even though the original Float64 curve is
        /// finite. Move to the adjacent upper anchor; its residual is below
        /// one and the anchor transition preserves lexicographic unit-time
        /// ordering.
        const Float64 upper_anchor
            = std::nextafter(anchor, std::numeric_limits<Float64>::infinity());
        if (std::isfinite(upper_anchor))
        {
            const Float64 upper_magnitude = magnitude_at(upper_anchor);
            if (std::isfinite(upper_magnitude) && upper_magnitude != 0)
            {
                anchor = upper_anchor;
                magnitude = upper_magnitude;
            }
        }
    }

    if (!std::isfinite(magnitude) || magnitude == 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "ExponentialTimeDecaying128 value cannot be represented by a finite canonical boundary value");

    return {negative ? -magnitude : magnitude, anchor};
}

UInt128 packExponentialTimeDecaying128Key(
    Float64 value_at_anchor, Float64 anchor_time)
{
    constexpr UInt128 midpoint = UInt128(1) << 127;
    constexpr UInt64 magnitude_mask = (UInt64(1) << 63) - 1;

    if (value_at_anchor == 0)
        return midpoint;

    const UInt64 sortable_anchor = getExponentialTimeDecayingSortableFloatKey(anchor_time);
    const UInt64 sortable_magnitude
        = getExponentialTimeDecayingSortableFloatKey(std::abs(value_at_anchor));
    const UInt128 timestamp_key
        = (UInt128(sortable_anchor) << 63)
        | UInt128(sortable_magnitude & magnitude_mask);

    if (std::signbit(value_at_anchor))
        return midpoint - 1 - timestamp_key;
    return midpoint + timestamp_key;
}

ExponentialTimeDecayingMode parseModeName(const String & name)
{
    if (name == "compact")
        return ExponentialTimeDecayingMode::Compact;
    if (name == "direct")
        return ExponentialTimeDecayingMode::Direct;

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Unknown ExponentialTimeDecaying mode {}; expected compact or direct",
        name);
}

struct ExponentialTimeDecayingTypeParameters
{
    Float64 decay_length;
    ExponentialTimeDecayingMode mode;
};

ExponentialTimeDecayingTypeParameters getTypeParameters(const ASTPtr & parameters, const char * type_name)
{
    if (!parameters || parameters->children.empty() || parameters->children.size() > 2)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Data type {} takes decay_length and optional mode",
            type_name);

    const auto * literal = parameters->children[0]->as<ASTLiteral>();
    if (!literal)
        throw Exception(
            ErrorCodes::PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS,
            "Decay length of data type {} must be a literal",
            type_name);

    const Float64 decay_length = applyVisitor(FieldVisitorConvertToNumber<Float64>(), literal->value);
    if (!std::isfinite(decay_length) || decay_length <= 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Decay length of data type {} must be finite and positive",
            type_name);

    ExponentialTimeDecayingMode mode = ExponentialTimeDecayingMode::Compact;
    if (parameters->children.size() == 2)
    {
        const auto & mode_ast = parameters->children[1];
        if (const auto * identifier = mode_ast->as<ASTIdentifier>())
            mode = parseModeName(identifier->name());
        else if (const auto * data_type = mode_ast->as<ASTDataType>(); data_type && !data_type->getArguments())
            mode = parseModeName(data_type->name);
        else
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Mode of data type {} must be compact or direct",
                type_name);
    }

    return {decay_length, mode};
}

class SerializationExponentialTimeDecaying final : public SerializationWrapper
{
public:
    SerializationExponentialTimeDecaying(
        SerializationPtr storage_serialization_,
        SerializationPtr logical_serialization_,
        DataTypePtr storage_type_,
        DataTypePtr logical_type_,
        Float64 decay_length_,
        ExponentialTimeDecayingKeyWidth key_width_,
        ExponentialTimeDecayingMode mode_)
        : SerializationWrapper(storage_serialization_)
        , logical_serialization(std::move(logical_serialization_))
        , storage_type(std::move(storage_type_))
        , logical_type(std::move(logical_type_))
        , decay_length(decay_length_)
        , key_width(key_width_)
        , mode(mode_)
    {
    }

    bool supportsPooling() const override { return false; }

    MutableColumnPtr wrapColumnForDeserialization(MutableColumnPtr column) const override
    {
        return column;
    }

    void enumerateStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data) const override
    {
        auto next_data = SubstreamData(nested_serialization)
                             .withType(data.type ? storage_type : nullptr)
                             .withColumn(
                                 data.column
                                     ? assert_cast<const ColumnExponentialTimeDecaying &>(*data.column).getStoragePtr()
                                     : nullptr)
                             .withSerializationInfo(data.serialization_info)
                             .withDeserializeState(data.deserialize_state);
        nested_serialization->enumerateStreams(settings, callback, next_data);
    }

    void serializeBinaryBulkStatePrefix(
        const IColumn & column,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override
    {
        nested_serialization->serializeBinaryBulkStatePrefix(
            assert_cast<const ColumnExponentialTimeDecaying &>(column).getStorageColumn(),
            settings,
            state);
    }

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override
    {
        nested_serialization->serializeBinaryBulkWithMultipleStreams(
            assert_cast<const ColumnExponentialTimeDecaying &>(column).getStorageColumn(),
            offset,
            limit,
            settings,
            state);
    }

    void serializeForHashCalculation(
        const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
    {
        const auto & decaying = assert_cast<const ColumnExponentialTimeDecaying &>(column);
        if (key_width == ExponentialTimeDecayingKeyWidth::Bits64)
        {
            const auto & keys
                = assert_cast<const ColumnUInt64 &>(decaying.getOrderingKeyColumn()).getData();
            writeBinaryLittleEndian(keys[row_num], ostr);
        }
        else
        {
            const auto & keys
                = assert_cast<const ColumnUInt128 &>(decaying.getOrderingKeyColumn()).getData();
            writeBinaryLittleEndian(keys[row_num], ostr);
        }
    }

    void serializeBinaryBulk(
        const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override
    {
        nested_serialization->serializeBinaryBulk(
            assert_cast<const ColumnExponentialTimeDecaying &>(column).getStorageColumn(),
            ostr,
            offset,
            limit);
    }

    void serializeBinary(
        const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        nested_serialization->serializeBinary(
            assert_cast<const ColumnExponentialTimeDecaying &>(column).getStorageColumn(),
            row_num,
            ostr,
            settings);
    }

    void deserializeBinaryBulk(
        IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override
    {
        const size_t previous_size = column.size();
        auto & decaying = assert_cast<ColumnExponentialTimeDecaying &>(column);
        nested_serialization->deserializeBinaryBulk(
            decaying.getStorageColumn(), istr, limit, avg_value_size_hint);
        validateNewRows(column, previous_size);
    }

    void deserializeBinaryBulkWithMultipleStreams(
        IColumn & column,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override
    {
        const size_t previous_size = column.size();
        auto & decaying = assert_cast<ColumnExponentialTimeDecaying &>(column);
        nested_serialization->deserializeBinaryBulkWithMultipleStreams(
            decaying.getStorageColumn(), limit, settings, state, cache);
        validateNewRows(column, previous_size);
    }

    void deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        const size_t previous_size = column.size();
        auto & decaying = assert_cast<ColumnExponentialTimeDecaying &>(column);
        nested_serialization->deserializeBinary(decaying.getStorageColumn(), istr, settings);
        validateNewRows(column, previous_size);
    }

    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        auto logical = materializeExponentialTimeDecayingLogicalColumn(column, decay_length);
        logical_serialization->serializeTextEscaped(*logical, row_num, ostr, settings);
    }
    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        deserializeLogical(column, [&](IColumn & logical) { logical_serialization->deserializeTextEscaped(logical, istr, settings); });
    }
    bool tryDeserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        return tryDeserializeLogical(column, [&](IColumn & logical) { return logical_serialization->tryDeserializeTextEscaped(logical, istr, settings); });
    }

    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        auto logical = materializeExponentialTimeDecayingLogicalColumn(column, decay_length);
        logical_serialization->serializeTextQuoted(*logical, row_num, ostr, settings);
    }
    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        deserializeLogical(column, [&](IColumn & logical) { logical_serialization->deserializeTextQuoted(logical, istr, settings); });
    }
    bool tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        return tryDeserializeLogical(column, [&](IColumn & logical) { return logical_serialization->tryDeserializeTextQuoted(logical, istr, settings); });
    }

    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        auto logical = materializeExponentialTimeDecayingLogicalColumn(column, decay_length);
        logical_serialization->serializeTextCSV(*logical, row_num, ostr, settings);
    }
    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        deserializeLogical(column, [&](IColumn & logical) { logical_serialization->deserializeTextCSV(logical, istr, settings); });
    }
    bool tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        return tryDeserializeLogical(column, [&](IColumn & logical) { return logical_serialization->tryDeserializeTextCSV(logical, istr, settings); });
    }
    void serializeTextHive(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        auto logical = materializeExponentialTimeDecayingLogicalColumn(column, decay_length);
        logical_serialization->serializeTextHive(*logical, row_num, ostr, settings);
    }

    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        auto logical = materializeExponentialTimeDecayingLogicalColumn(column, decay_length);
        logical_serialization->serializeText(*logical, row_num, ostr, settings);
    }
    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        deserializeLogical(column, [&](IColumn & logical) { logical_serialization->deserializeWholeText(logical, istr, settings); });
    }
    bool tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        return tryDeserializeLogical(column, [&](IColumn & logical) { return logical_serialization->tryDeserializeWholeText(logical, istr, settings); });
    }

    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        auto logical = materializeExponentialTimeDecayingLogicalColumn(column, decay_length);
        logical_serialization->serializeTextJSON(*logical, row_num, ostr, settings);
    }
    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        deserializeLogical(column, [&](IColumn & logical) { logical_serialization->deserializeTextJSON(logical, istr, settings); });
    }
    bool tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const override
    {
        return tryDeserializeLogical(column, [&](IColumn & logical) { return logical_serialization->tryDeserializeTextJSON(logical, istr, settings); });
    }
    void serializeTextJSONPretty(
        const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, size_t indent) const override
    {
        auto logical = materializeExponentialTimeDecayingLogicalColumn(column, decay_length);
        logical_serialization->serializeTextJSONPretty(*logical, row_num, ostr, settings, indent);
    }
    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const override
    {
        auto logical = materializeExponentialTimeDecayingLogicalColumn(column, decay_length);
        logical_serialization->serializeTextXML(*logical, row_num, ostr, settings);
    }

private:
    template <typename Deserialize>
    void deserializeLogical(IColumn & column, Deserialize && deserialize) const
    {
        auto logical = logical_type->createColumn();
        deserialize(*logical);
        auto storage = materializeExponentialTimeDecayingStorageColumn(*logical, decay_length, "deserialization", key_width, mode);
        column.insertRangeFrom(*storage, 0, storage->size());
    }

    template <typename Deserialize>
    bool tryDeserializeLogical(IColumn & column, Deserialize && deserialize) const
    {
        auto logical = logical_type->createColumn();
        if (!deserialize(*logical))
            return false;
        auto storage = materializeExponentialTimeDecayingStorageColumn(*logical, decay_length, "deserialization", key_width, mode);
        column.insertRangeFrom(*storage, 0, storage->size());
        return true;
    }

    void validateNewRows(const IColumn & column, size_t previous_size) const
    {
        if (column.size() <= previous_size)
            return;
        const auto new_rows = column.cut(previous_size, column.size() - previous_size);
        validateExponentialTimeDecayingColumn(*new_rows, "deserialization");
    }

    const SerializationPtr logical_serialization;
    const DataTypePtr storage_type;
    const DataTypePtr logical_type;
    const Float64 decay_length;
    const ExponentialTimeDecayingKeyWidth key_width;
    const ExponentialTimeDecayingMode mode;
};

DataTypePtr createFromParameters64(const ASTPtr & parameters)
{
    const auto parsed = getTypeParameters(parameters, "ExponentialTimeDecaying64");
    return std::make_shared<DataTypeExponentialTimeDecaying>(
        parsed.decay_length,
        ExponentialTimeDecayingKeyWidth::Bits64,
        parsed.mode);
}

DataTypePtr createFromParameters128(const ASTPtr & parameters)
{
    const auto parsed = getTypeParameters(parameters, "ExponentialTimeDecaying128");
    return std::make_shared<DataTypeExponentialTimeDecaying>(
        parsed.decay_length,
        ExponentialTimeDecayingKeyWidth::Bits128,
        parsed.mode);
}

}

ExponentialTimeDecayingMode parseExponentialTimeDecayingMode(const String & name)
{
    return parseModeName(name);
}

UInt128 getExponentialTimeDecayingOrderingKey128(
    Float64 value, Float64 time, Float64 decay_length)
{
    const auto direct = normalizeExponentialTimeDecaying128Direct(value, time, decay_length);
    return packExponentialTimeDecaying128Key(direct.value_at_anchor, direct.anchor_time);
}

ExponentialTimeDecayingCanonicalDirectValue
getExponentialTimeDecayingCanonicalDirectValue(UInt128 ordering_key)
{
    constexpr UInt128 midpoint = UInt128(1) << 127;
    constexpr UInt128 residual_mask = (UInt128(1) << 63) - 1;

    if (ordering_key == midpoint)
        return {0, 0};

    const bool negative = ordering_key < midpoint;
    const UInt128 timestamp_key = negative
        ? midpoint - 1 - ordering_key
        : ordering_key - midpoint;

    const UInt64 sortable_anchor = static_cast<UInt64>(timestamp_key >> 63);
    const UInt64 sortable_magnitude
        = (UInt64(1) << 63) | static_cast<UInt64>(timestamp_key & residual_mask);

    const Float64 anchor_time
        = getExponentialTimeDecayingFloatFromSortableKey(sortable_anchor);
    const Float64 magnitude
        = getExponentialTimeDecayingFloatFromSortableKey(sortable_magnitude);

    if (!std::isfinite(anchor_time) || !std::isfinite(magnitude) || magnitude <= 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Serialized ExponentialTimeDecaying128 ordering key is invalid");

    return {negative ? -magnitude : magnitude, anchor_time};
}

int compareExponentialTimeDecayingOrderingKeyDistance(
    UInt128 lhs_ordering_key,
    UInt128 rhs_ordering_key,
    ExponentialTimeDecayingKeyWidth key_width,
    Float64 decay_length,
    Float64 max_distance_in_decay_lengths)
{
    const auto lhs = key_width == ExponentialTimeDecayingKeyWidth::Bits64
        ? getExponentialTimeDecayingCanonicalDirectValue(static_cast<UInt64>(lhs_ordering_key))
        : getExponentialTimeDecayingCanonicalDirectValue(lhs_ordering_key);
    const auto rhs = key_width == ExponentialTimeDecayingKeyWidth::Bits64
        ? getExponentialTimeDecayingCanonicalDirectValue(static_cast<UInt64>(rhs_ordering_key))
        : getExponentialTimeDecayingCanonicalDirectValue(rhs_ordering_key);

    if (lhs.value_at_anchor == 0 || rhs.value_at_anchor == 0)
        return 0;

    const ExtendedTimeDecayFloat extended_decay_length(decay_length);
    const ExtendedTimeDecayFloat distance_in_decay_lengths
        = (ExtendedTimeDecayFloat(lhs.anchor_time) - ExtendedTimeDecayFloat(rhs.anchor_time))
            / extended_decay_length
        + logarithmExponentialTimeDecay(ExtendedTimeDecayFloat(std::abs(lhs.value_at_anchor)))
        - logarithmExponentialTimeDecay(ExtendedTimeDecayFloat(std::abs(rhs.value_at_anchor)));
    const ExtendedTimeDecayFloat cutoff(max_distance_in_decay_lengths);

    if (distance_in_decay_lengths > cutoff)
        return 1;
    if (distance_in_decay_lengths < -cutoff)
        return -1;
    return 0;
}

bool isFiniteExponentialTimeDecayingCurve(
    Float64 value, Float64 time, Float64 decay_length)
{
    if (!std::isfinite(value)
        || !std::isfinite(time)
        || !std::isfinite(decay_length)
        || decay_length <= 0)
        return false;

    return true;
}

ExponentialTimeDecayingValue normalizeExponentialTimeDecaying(
    Float64 value,
    Float64 time,
    Float64 decay_length,
    ExponentialTimeDecayingKeyWidth key_width)
{
    if (value == 0)
    {
        const UInt128 ordering_key = key_width == ExponentialTimeDecayingKeyWidth::Bits64
            ? UInt128(shiftOneBitAndSign(0, 0))
            : (UInt128(1) << 127);
        return {ordering_key, 0, 0};
    }

    if (key_width == ExponentialTimeDecayingKeyWidth::Bits64)
        return {
            UInt128(getExponentialTimeDecayingOrderingKey(value, time, decay_length)),
            value,
            time};

    const auto direct = normalizeExponentialTimeDecaying128Direct(value, time, decay_length);
    return {
        packExponentialTimeDecaying128Key(direct.value_at_anchor, direct.anchor_time),
        direct.value_at_anchor,
        direct.anchor_time};
}

DataTypeExponentialTimeDecaying::DataTypeExponentialTimeDecaying(
    Float64 decay_length_,
    ExponentialTimeDecayingKeyWidth key_width_,
    ExponentialTimeDecayingMode mode_)
    : decay_length(decay_length_)
    , key_width(key_width_)
    , mode(mode_)
    , storage_type(
          mode == ExponentialTimeDecayingMode::Compact
              ? (key_width == ExponentialTimeDecayingKeyWidth::Bits64
                    ? DataTypePtr(std::make_shared<DataTypeUInt64>())
                    : DataTypePtr(std::make_shared<DataTypeUInt128>()))
              : DataTypePtr(std::make_shared<DataTypeTuple>(
                    DataTypes{
                        key_width == ExponentialTimeDecayingKeyWidth::Bits64
                            ? DataTypePtr(std::make_shared<DataTypeUInt64>())
                            : DataTypePtr(std::make_shared<DataTypeUInt128>()),
                        std::make_shared<DataTypeFloat64>(),
                        std::make_shared<DataTypeFloat64>()},
                    Names{"ordering_key", "value", "timestamp"})))
    , direct_type(std::make_shared<DataTypeTuple>(
          DataTypes{
              std::make_shared<DataTypeFloat64>(),
              std::make_shared<DataTypeFloat64>()},
          Names{"value", "timestamp"}))
    , logical_type(std::make_shared<DataTypeTuple>(
          DataTypes{
              std::make_shared<DataTypeFloat64>(),
              std::make_shared<DataTypeFloat64>(),
              std::make_shared<DataTypeFloat64>()},
          Names{"value", "timestamp", "decay_length"}))
{
}

String DataTypeExponentialTimeDecaying::doGetName() const
{
    if (mode == ExponentialTimeDecayingMode::Compact)
        return fmt::format("{}({})", getExponentialTimeDecayingTypeName(key_width), decay_length);
    return fmt::format("{}({}, direct)", getExponentialTimeDecayingTypeName(key_width), decay_length);
}

MutableColumnPtr DataTypeExponentialTimeDecaying::createColumn() const
{
    return ColumnExponentialTimeDecaying::create(storage_type->createColumn(), decay_length, key_width, mode);
}

Field DataTypeExponentialTimeDecaying::getDefault() const
{
    return Tuple{Float64(0), Float64(0), decay_length};
}

void DataTypeExponentialTimeDecaying::insertDefaultInto(IColumn & column) const
{
    column.insert(getDefault());
}

bool DataTypeExponentialTimeDecaying::equals(const IDataType & rhs) const
{
    const auto * other = typeid_cast<const DataTypeExponentialTimeDecaying *>(&rhs);
    return other
        && decay_length == other->decay_length
        && key_width == other->key_width
        && mode == other->mode;
}

bool DataTypeExponentialTimeDecaying::haveMaximumSizeOfValue() const
{
    return storage_type->haveMaximumSizeOfValue();
}

size_t DataTypeExponentialTimeDecaying::getMaximumSizeOfValueInMemory() const
{
    return storage_type->getMaximumSizeOfValueInMemory();
}

size_t DataTypeExponentialTimeDecaying::getSizeOfValueInMemory() const
{
    return storage_type->getSizeOfValueInMemory();
}

void DataTypeExponentialTimeDecaying::updateHashImpl(SipHash & hash) const
{
    hash.update(decay_length);
    hash.update(static_cast<UInt8>(key_width));
    hash.update(static_cast<UInt8>(mode));
}

SerializationPtr DataTypeExponentialTimeDecaying::doGetSerialization(const SerializationInfoSettings &) const
{
    return std::make_shared<SerializationExponentialTimeDecaying>(
        storage_type->getDefaultSerialization(),
        logical_type->getDefaultSerialization(),
        storage_type,
        logical_type,
        decay_length,
        key_width,
        mode);
}

SerializationPtr DataTypeExponentialTimeDecaying::getSerialization(const SerializationInfo & info) const
{
    auto storage_info = info.clone();
    auto storage_kinds = info.getKindStack();
    std::erase(storage_kinds, ISerialization::Kind::DETACHED);
    std::erase(storage_kinds, ISerialization::Kind::REPLICATED);
    storage_info->setKindStack(storage_kinds);

    /// Whole-column wrappers belong outside `SerializationExponentialTimeDecaying`.
    /// Keep sparse storage and tuple-element metadata inside the decaying column.
    auto serialization = std::make_shared<SerializationExponentialTimeDecaying>(
        storage_type->getSerialization(*storage_info),
        logical_type->getDefaultSerialization(),
        storage_type,
        logical_type,
        decay_length,
        key_width,
        mode);
    return wrapSerializationBasedOnKindStack(serialization, info.getKindStack(), info.getSettings());
}

MutableSerializationInfoPtr DataTypeExponentialTimeDecaying::createSerializationInfo(
    const SerializationInfoSettings & settings) const
{
    return storage_type->createSerializationInfo(settings);
}

SerializationInfoPtr DataTypeExponentialTimeDecaying::getSerializationInfo(
    const IColumn & column, const SerializationInfoSettings & settings) const
{
    if (const auto * column_const = checkAndGetColumn<ColumnConst>(&column))
        return getSerializationInfo(column_const->getDataColumn(), settings);

    auto get_wrapped_info = [&](const IColumn & nested_column, ISerialization::Kind kind)
    {
        auto nested_info = getSerializationInfo(nested_column, settings);
        auto info = nested_info->clone();
        auto kinds = nested_info->getKindStack();
        kinds.push_back(kind);
        info->setKindStack(kinds);
        return info;
    };

    if (const auto * column_blob = checkAndGetColumn<ColumnBLOB>(&column))
        return get_wrapped_info(*column_blob->getWrappedColumn(), ISerialization::Kind::DETACHED);

    if (const auto * column_replicated = checkAndGetColumn<ColumnReplicated>(&column))
        return get_wrapped_info(*column_replicated->getNestedColumn(), ISerialization::Kind::REPLICATED);

    const auto & decaying_column = assert_cast<const ColumnExponentialTimeDecaying &>(column);
    return storage_type->getSerializationInfo(decaying_column.getStorageColumn(), settings);
}

DataTypePtr createDataTypeExponentialTimeDecaying(
    Float64 decay_length,
    ExponentialTimeDecayingKeyWidth key_width,
    ExponentialTimeDecayingMode mode)
{
    return std::make_shared<DataTypeExponentialTimeDecaying>(decay_length, key_width, mode);
}

std::optional<Float64> tryGetExponentialTimeDecayingDecayLength(const IDataType & type)
{
    if (const auto * decaying_type = typeid_cast<const DataTypeExponentialTimeDecaying *>(&type))
        return decaying_type->getDecayLength();

    return std::nullopt;
}

std::optional<Float64> tryGetExponentialTimeDecayingDecayLength(const DataTypePtr & type)
{
    return type ? tryGetExponentialTimeDecayingDecayLength(*type) : std::nullopt;
}

std::optional<ExponentialTimeDecayingKeyWidth> tryGetExponentialTimeDecayingKeyWidth(const IDataType & type)
{
    if (const auto * decaying_type = typeid_cast<const DataTypeExponentialTimeDecaying *>(&type))
        return decaying_type->getKeyWidth();

    return std::nullopt;
}

std::optional<ExponentialTimeDecayingKeyWidth> tryGetExponentialTimeDecayingKeyWidth(const DataTypePtr & type)
{
    return type ? tryGetExponentialTimeDecayingKeyWidth(*type) : std::nullopt;
}

std::optional<ExponentialTimeDecayingMode> tryGetExponentialTimeDecayingMode(const IDataType & type)
{
    if (const auto * decaying_type = typeid_cast<const DataTypeExponentialTimeDecaying *>(&type))
        return decaying_type->getMode();

    return std::nullopt;
}

std::optional<ExponentialTimeDecayingMode> tryGetExponentialTimeDecayingMode(const DataTypePtr & type)
{
    return type ? tryGetExponentialTimeDecayingMode(*type) : std::nullopt;
}

bool isExponentialTimeDecaying(const IDataType & type)
{
    return tryGetExponentialTimeDecayingDecayLength(type).has_value();
}

bool isExponentialTimeDecaying(const DataTypePtr & type)
{
    return tryGetExponentialTimeDecayingDecayLength(type).has_value();
}

bool containsExponentialTimeDecaying(const IDataType & type)
{
    if (isExponentialTimeDecaying(type))
        return true;

    bool contains = false;
    forEachNestedDataType(type, [&](const IDataType & child)
    {
        if (!contains)
            contains = isExponentialTimeDecaying(child);
    });
    return contains;
}

bool containsExponentialTimeDecaying(const DataTypePtr & type)
{
    return type && containsExponentialTimeDecaying(*type);
}

namespace
{

DataTypePtr removeExponentialTimeDecayingTransparentWrappers(DataTypePtr type)
{
    while (type)
    {
        if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        {
            type = low_cardinality_type->getDictionaryType();
            continue;
        }

        if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
        {
            type = nullable_type->getNestedType();
            continue;
        }

        break;
    }

    return type;
}

void assertExponentialTimeDecayingTypesCompatibleImpl(
    DataTypePtr left_type, DataTypePtr right_type, const String & operation)
{
    left_type = removeExponentialTimeDecayingTransparentWrappers(std::move(left_type));
    right_type = removeExponentialTimeDecayingTransparentWrappers(std::move(right_type));

    const bool left_contains = containsExponentialTimeDecaying(left_type);
    const bool right_contains = containsExponentialTimeDecaying(right_type);
    if (!left_contains && !right_contains)
        return;

    if (!left_contains || !right_contains)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "{} cannot combine incompatible types {} and {} containing ExponentialTimeDecaying",
            operation,
            left_type->getName(),
            right_type->getName());

    const auto left_decay_length = tryGetExponentialTimeDecayingDecayLength(left_type);
    const auto right_decay_length = tryGetExponentialTimeDecayingDecayLength(right_type);
    if (left_decay_length || right_decay_length)
    {
        if (!left_decay_length || !right_decay_length)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "{} cannot combine ExponentialTimeDecaying with {}",
                operation,
                left_decay_length ? right_type->getName() : left_type->getName());

        if (*left_decay_length != *right_decay_length)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "{} cannot combine ExponentialTimeDecaying values with different decay lengths: {} and {}",
                operation,
                *left_decay_length,
                *right_decay_length);

        if (tryGetExponentialTimeDecayingKeyWidth(left_type)
                != tryGetExponentialTimeDecayingKeyWidth(right_type)
            || tryGetExponentialTimeDecayingMode(left_type)
                != tryGetExponentialTimeDecayingMode(right_type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "{} cannot implicitly combine {} and {}",
                operation,
                left_type->getName(),
                right_type->getName());
        return;
    }

    if (const auto * left_array = typeid_cast<const DataTypeArray *>(left_type.get()))
    {
        const auto * right_array = typeid_cast<const DataTypeArray *>(right_type.get());
        if (right_array)
        {
            assertExponentialTimeDecayingTypesCompatibleImpl(
                left_array->getNestedType(), right_array->getNestedType(), operation);
            return;
        }
    }
    else if (const auto * left_tuple = typeid_cast<const DataTypeTuple *>(left_type.get()))
    {
        const auto * right_tuple = typeid_cast<const DataTypeTuple *>(right_type.get());
        if (right_tuple && left_tuple->getElements().size() == right_tuple->getElements().size())
        {
            for (size_t i = 0; i < left_tuple->getElements().size(); ++i)
                assertExponentialTimeDecayingTypesCompatibleImpl(
                    left_tuple->getElements()[i], right_tuple->getElements()[i], operation);
            return;
        }
    }
    else if (const auto * left_map = typeid_cast<const DataTypeMap *>(left_type.get()))
    {
        const auto * right_map = typeid_cast<const DataTypeMap *>(right_type.get());
        if (right_map)
        {
            assertExponentialTimeDecayingTypesCompatibleImpl(
                left_map->getNestedType(), right_map->getNestedType(), operation);
            return;
        }
    }
    else if (const auto * left_variant = typeid_cast<const DataTypeVariant *>(left_type.get()))
    {
        const auto * right_variant = typeid_cast<const DataTypeVariant *>(right_type.get());
        if (right_variant && left_variant->getVariants().size() == right_variant->getVariants().size())
        {
            for (size_t i = 0; i < left_variant->getVariants().size(); ++i)
                assertExponentialTimeDecayingTypesCompatibleImpl(
                    left_variant->getVariant(i), right_variant->getVariant(i), operation);
            return;
        }
    }

    throw Exception(
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        "{} cannot combine incompatible types {} and {} containing ExponentialTimeDecaying",
        operation,
        left_type->getName(),
        right_type->getName());
}

}

void assertExponentialTimeDecayingTypesCompatible(
    const DataTypePtr & left_type, const DataTypePtr & right_type, const String & operation)
{
    assertExponentialTimeDecayingTypesCompatibleImpl(left_type, right_type, operation);
}

void assertExponentialTimeDecayingConversionTypesCompatible(
    const DataTypePtr & source_type, const DataTypePtr & target_type, const String & operation)
{
    if (!containsExponentialTimeDecaying(source_type) && !containsExponentialTimeDecaying(target_type))
        return;

    const auto nested_source_type = removeExponentialTimeDecayingTransparentWrappers(source_type);
    const auto nested_target_type = removeExponentialTimeDecayingTransparentWrappers(target_type);

    /// Variant conversion examines each active alternative separately. Preserve an exact
    /// semantic alternative, but do not accept a layout-compatible plain Tuple instead.
    if (const auto * variant = typeid_cast<const DataTypeVariant *>(nested_target_type.get()))
    {
        for (const auto & alternative : variant->getVariants())
        {
            if (nested_source_type->equals(*alternative))
                return;
        }
    }

    assertExponentialTimeDecayingTypesCompatible(source_type, target_type, operation);
}

void assertExponentialTimeDecayingSetKeyTypesCompatible(
    const DataTypePtr & probe_type, const DataTypePtr & set_type)
{
    assertExponentialTimeDecayingConversionTypesCompatible(probe_type, set_type, "IN");
}

ColumnPtr materializeExponentialTimeDecayingLogicalColumn(
    const IColumn & storage_column, Float64 decay_length)
{
    ColumnPtr full = storage_column.convertToFullColumnIfConst();
    const auto & decaying = assert_cast<const ColumnExponentialTimeDecaying &>(*full);

    auto values = ColumnFloat64::create();
    auto timestamps = ColumnFloat64::create();
    auto decay_lengths = ColumnFloat64::create(decaying.size(), decay_length);
    values->reserve(decaying.size());
    timestamps->reserve(decaying.size());

    for (size_t row = 0; row < decaying.size(); ++row)
    {
        const auto printable = decaying.getMode() == ExponentialTimeDecayingMode::Direct
            ? decaying.getDirectValueAt(row)
            : decaying.getPrintableValueAt(row);
        values->insertValue(printable.value_at_anchor);
        timestamps->insertValue(printable.anchor_time);
    }

    return ColumnTuple::create(
        Columns{std::move(values), std::move(timestamps), std::move(decay_lengths)});
}

ColumnPtr materializeExponentialTimeDecayingStorageColumn(
    const IColumn & logical_column,
    Float64 decay_length,
    const String & operation,
    ExponentialTimeDecayingKeyWidth key_width,
    ExponentialTimeDecayingMode mode)
{
    ColumnPtr full = logical_column.convertToFullColumnIfConst();
    const auto & tuple = assert_cast<const ColumnTuple &>(*full);
    if (tuple.tupleSize() != 2 && tuple.tupleSize() != 3)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Malformed ExponentialTimeDecaying value in {}: expected (value, timestamp[, decay_length])",
            operation);

    const auto & values = assert_cast<const ColumnFloat64 &>(tuple.getColumn(0)).getData();
    const auto & timestamps = assert_cast<const ColumnFloat64 &>(tuple.getColumn(1)).getData();
    const ColumnFloat64 * decay_lengths = tuple.tupleSize() == 3
        ? &assert_cast<const ColumnFloat64 &>(tuple.getColumn(2))
        : nullptr;

    MutableColumnPtr keys = key_width == ExponentialTimeDecayingKeyWidth::Bits64
        ? MutableColumnPtr(ColumnUInt64::create())
        : MutableColumnPtr(ColumnUInt128::create());
    auto stored_values = ColumnFloat64::create();
    auto stored_timestamps = ColumnFloat64::create();

    if (mode == ExponentialTimeDecayingMode::Direct)
    {
        stored_values->reserve(tuple.size());
        stored_timestamps->reserve(tuple.size());
    }

    for (size_t row = 0; row < tuple.size(); ++row)
    {
        if (decay_lengths)
        {
            const Float64 supplied = decay_lengths->getData()[row];
            if (!std::isfinite(supplied) || supplied != decay_length)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Malformed ExponentialTimeDecaying value in {}: supplied decay length {} does not match type decay length {}",
                    operation,
                    supplied,
                    decay_length);
        }

        const Float64 value = values[row];
        const Float64 timestamp = timestamps[row];
        if (!isFiniteExponentialTimeDecayingCurve(value, timestamp, decay_length))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Malformed ExponentialTimeDecaying value in {}: value and timestamp must be finite",
                operation);

        const auto normalized = normalizeExponentialTimeDecaying(
            value, timestamp, decay_length, key_width);
        if (key_width == ExponentialTimeDecayingKeyWidth::Bits64)
            assert_cast<ColumnUInt64 &>(*keys).insertValue(static_cast<UInt64>(normalized.ordering_key));
        else
            assert_cast<ColumnUInt128 &>(*keys).insertValue(normalized.ordering_key);

        if (mode == ExponentialTimeDecayingMode::Direct)
        {
            stored_values->insertValue(value);
            stored_timestamps->insertValue(timestamp);
        }
    }

    if (mode == ExponentialTimeDecayingMode::Compact)
        return ColumnExponentialTimeDecaying::create(
            std::move(keys), decay_length, key_width, mode);

    Columns columns;
    columns.push_back(std::move(keys));
    columns.push_back(std::move(stored_values));
    columns.push_back(std::move(stored_timestamps));
    return ColumnExponentialTimeDecaying::create(
        ColumnTuple::create(std::move(columns))->assumeMutable(),
        decay_length,
        key_width,
        mode);
}

void validateExponentialTimeDecayingColumn(
    const IColumn & column, const String & operation)
{
    ColumnPtr full_column = column.convertToFullColumnIfConst()->convertToFullColumnIfLowCardinality();
    const ColumnNullable * nullable = typeid_cast<const ColumnNullable *>(full_column.get());

    ColumnPtr nested_holder;
    const IColumn * nested_column = full_column.get();
    if (nullable)
    {
        nested_holder = nullable->getNestedColumnPtr()->convertToFullColumnIfLowCardinality();
        nested_column = nested_holder.get();
    }

    const auto & decaying = assert_cast<const ColumnExponentialTimeDecaying &>(*nested_column);
    for (size_t row = 0; row < decaying.size(); ++row)
    {
        if (nullable && nullable->isNullAt(row))
            continue;

        const auto direct = decaying.getDirectValueAt(row);
        if (!isFiniteExponentialTimeDecayingCurve(
                direct.value_at_anchor, direct.anchor_time, decaying.getDecayLength()))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Malformed ExponentialTimeDecaying value in {}",
                operation);

        if (decaying.getMode() == ExponentialTimeDecayingMode::Direct)
        {
            const auto normalized = normalizeExponentialTimeDecaying(
                direct.value_at_anchor,
                direct.anchor_time,
                decaying.getDecayLength(),
                decaying.getKeyWidth());

            if (decaying.getKeyWidth() == ExponentialTimeDecayingKeyWidth::Bits64)
            {
                const auto & keys
                    = assert_cast<const ColumnUInt64 &>(decaying.getOrderingKeyColumn()).getData();
                if (static_cast<UInt64>(normalized.ordering_key) != keys[row])
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Malformed ExponentialTimeDecaying64 direct value in {}: ordering key does not match interface payload",
                        operation);
            }
            else
            {
                const auto & keys
                    = assert_cast<const ColumnUInt128 &>(decaying.getOrderingKeyColumn()).getData();
                if (normalized.ordering_key != keys[row])
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Malformed ExponentialTimeDecaying128 direct value in {}: ordering key does not match interface payload",
                        operation);
            }
        }
    }
}

namespace
{

void validateExponentialTimeDecayingColumnImpl(
    const IColumn & column, const DataTypePtr & type, const String & operation)
{
    if (!type || !containsExponentialTimeDecaying(type))
        return;

    ColumnPtr full_column = column.convertToFullColumnIfConst()->convertToFullColumnIfLowCardinality();

    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
    {
        validateExponentialTimeDecayingColumnImpl(
            *full_column, low_cardinality_type->getDictionaryType(), operation);
        return;
    }

    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        const auto & nullable_column = assert_cast<const ColumnNullable &>(*full_column);
        validateExponentialTimeDecayingColumnImpl(
            nullable_column.getNestedColumn(), nullable_type->getNestedType(), operation);
        return;
    }

    if (isExponentialTimeDecaying(type))
    {
        validateExponentialTimeDecayingColumn(*full_column, operation);
        return;
    }

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
    {
        const auto & array_column = assert_cast<const ColumnArray &>(*full_column);
        validateExponentialTimeDecayingColumnImpl(
            array_column.getData(), array_type->getNestedType(), operation);
        return;
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        const auto & tuple_column = assert_cast<const ColumnTuple &>(*full_column);
        const auto & element_types = tuple_type->getElements();
        for (size_t i = 0; i < element_types.size(); ++i)
            validateExponentialTimeDecayingColumnImpl(
                tuple_column.getColumn(i), element_types[i], operation);
        return;
    }

    if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
    {
        const auto & map_column = assert_cast<const ColumnMap &>(*full_column);
        validateExponentialTimeDecayingColumnImpl(
            map_column.getNestedColumn(), map_type->getNestedType(), operation);
        return;
    }

    if (const auto * variant_type = typeid_cast<const DataTypeVariant *>(type.get()))
    {
        const auto & variant_column = assert_cast<const ColumnVariant &>(*full_column);
        for (size_t i = 0; i < variant_type->getVariants().size(); ++i)
            validateExponentialTimeDecayingColumnImpl(
                variant_column.getVariantByGlobalDiscriminator(i), variant_type->getVariant(i), operation);
    }
}

}

void validateExponentialTimeDecayingColumn(
    const IColumn & column, const DataTypePtr & type, const String & operation)
{
    validateExponentialTimeDecayingColumnImpl(column, type, operation);
}

void registerDataTypeExponentialTimeDecaying(DataTypeFactory & factory)
{
    const Documentation documentation64{
        .description = R"(
Represents an exponentially time-decaying curve whose complete logical identity is a naturally
ordered `UInt64` key. Curves mapping to the same key compare equal and have the same hash.
`compact` is the default storage mode and stores only that key. `direct` stores the same
authoritative key plus `(value, timestamp)` as non-identity display metadata. Key-only operations
such as `GROUP BY` or `DISTINCT` may choose any metadata representative for equal keys.
Compact values print as `(-1/0/+1, unit_timestamp, decay_length)`.
If the derived unit timestamp exceeds the finite `Float64` range, it is clamped to the
nearest finite boundary before encoding. The key discards one sortable timestamp bit,
so the decoded positive boundary is the `Float64` immediately below the largest finite value.
)",
        .syntax = "ExponentialTimeDecaying64(decay_length[, mode])",
        .examples = {},
        .related = {"SimpleAggregateFunction", "ExponentialTimeDecaying128"},
    };

    const Documentation documentation128{
        .description = R"(
Represents an exponentially time-decaying curve whose complete logical identity is a naturally
ordered `UInt128` key. The upper part stores a canonical Float64 anchor and the remaining
timestamp bits store the positive magnitude at that anchor. This preserves additional unit-time
precision and continues monotonically beyond the finite Float64 timestamp range by holding the
anchor at the nearest finite boundary and using the residual magnitude there.
`compact` stores only the key; `direct` additionally retains `(value, timestamp)` as
non-identity display metadata. Key-only operations may choose any metadata representative for
equal keys. Compact values normally print the curve sign at the representative unit time; extreme
tail keys print the residual value at the finite boundary timestamp.
)",
        .syntax = "ExponentialTimeDecaying128(decay_length[, mode])",
        .examples = {},
        .related = {"SimpleAggregateFunction", "ExponentialTimeDecaying64"},
    };

    factory.registerDataType(
        "ExponentialTimeDecaying64",
        createFromParameters64,
        DataTypeFactory::Case::Sensitive,
        documentation64);

    factory.registerDataType(
        "ExponentialTimeDecaying128",
        createFromParameters128,
        DataTypeFactory::Case::Sensitive,
        documentation128);
}

}
