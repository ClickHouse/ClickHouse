#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <AggregateFunctions/Combinators/AggregateFunctionNull.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>

#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/assert_cast.h>

#include <Core/DecimalFunctions.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <base/arithmeticOverflow.h>
#include <base/defines.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

namespace DB
{

struct Settings;

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int BAD_ARGUMENTS;
extern const int UNSUPPORTED_PARAMETER;
extern const int ARGUMENT_OUT_OF_BOUND;
extern const int LOGICAL_ERROR;
}

namespace
{

enum class HistogramBoundary : UInt8
{
    LEFT_CLOSED_RIGHT_OPEN,
    LEFT_OPEN_RIGHT_CLOSED
};

enum class SpecialValue : UInt8
{
    NULL_VALUE,
    NAN_VALUE,
    ZERO_VALUE
};

const char * specialValueName(SpecialValue value)
{
    switch (value)
    {
        case SpecialValue::NULL_VALUE: return "null";
        case SpecialValue::NAN_VALUE: return "nan";
        case SpecialValue::ZERO_VALUE: return "zero";
    }
    UNREACHABLE();
}

String specialValueResultFieldName(SpecialValue value)
{
    return String(specialValueName(value)) + "_count";
}

using BoundaryList = VectorWithMemoryTracking<Field>;

/// Shared by AggregateFunctionHistogramExplicit and its -Null/-IfNull adapters, which otherwise
/// each repeat this normalization verbatim, differing only in the canonical_parameters source.
DataTypePtr buildNormalizedStateType(const AggregateFunctionPtr & self, const DataTypes & argument_types, const Array & canonical_parameters)
{
    DataTypes normalized_argument_types;
    normalized_argument_types.reserve(argument_types.size());
    for (const auto & arg : argument_types)
        normalized_argument_types.emplace_back(arg->getNormalizedType());

    return std::make_shared<DataTypeAggregateFunction>(self, normalized_argument_types, canonical_parameters);
}

struct HistogramExplicitData
{
    ALWAYS_INLINE static const UInt64 * getCounts(ConstAggregateDataPtr place) { return reinterpret_cast<const UInt64 *>(place); }

    ALWAYS_INLINE static UInt64 * getCounts(AggregateDataPtr place)
    {
        return const_cast<UInt64 *>(getCounts(ConstAggregateDataPtr(place)));
    }

    static void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, size_t total_buckets)
    {
        UInt64 * counts = getCounts(place);
        const UInt64 * rhs_counts = getCounts(rhs);
        for (size_t i = 0; i < total_buckets; ++i)
            counts[i] += rhs_counts[i];
    }

    static void write(ConstAggregateDataPtr place, WriteBuffer & buf, size_t total_buckets)
    {
        const UInt64 * counts = getCounts(place);
        for (size_t i = 0; i < total_buckets; ++i)
            writeVarUInt(counts[i], buf);
    }

    static void read(AggregateDataPtr place, ReadBuffer & buf, size_t total_buckets)
    {
        UInt64 * counts = getCounts(place);
        for (size_t i = 0; i < total_buckets; ++i)
            readVarUInt(counts[i], buf);
    }
};

template <typename NativeType>
ALWAYS_INLINE NativeType divRoundUp(NativeType numerator, NativeType denominator)
{
    const NativeType q = numerator / denominator;
    const NativeType r = numerator % denominator;
    return r > 0 ? q + 1 : q;
}

template <typename NativeType>
ALWAYS_INLINE NativeType divRoundDown(NativeType numerator, NativeType denominator)
{
    const NativeType q = numerator / denominator;
    const NativeType r = numerator % denominator;
    return r < 0 ? q - 1 : q;
}

template <typename NativeType>
std::optional<NativeType> saturateCastToNative(Int256 value)
{
    if constexpr (std::is_same_v<NativeType, UInt256>)
    {
        if (value < 0)
            return std::nullopt;
        return static_cast<NativeType>(value);
    }
    else
    {
        if (value > static_cast<Int256>(std::numeric_limits<NativeType>::max()))
            return std::nullopt;
        if (value < static_cast<Int256>(std::numeric_limits<NativeType>::min()))
            return std::nullopt;
        return static_cast<NativeType>(value);
    }
}

template <typename NativeType, typename RawType>
std::optional<NativeType> rescaleFixedPoint(RawType raw, UInt32 from_scale, UInt32 to_scale, HistogramBoundary boundary_type)
{
    /// Prevent Int256 overflow during scaling if UInt256 exceeds Int256::max()
    if constexpr (std::is_same_v<RawType, UInt256>)
    {
        static constexpr UInt256 int256_max_as_uint256 = static_cast<UInt256>(std::numeric_limits<Int256>::max());
        if (raw > int256_max_as_uint256)
        {
            if constexpr (!std::is_same_v<NativeType, UInt256>)
                return std::nullopt;
            else
            {
                if (from_scale != to_scale)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Unexpected scale mismatch (from_scale={}, to_scale={}) rescaling a UInt256 boundary "
                        "too large for Int256; this case was assumed unreachable and has no rescaling logic",
                        from_scale,
                        to_scale);

                return static_cast<NativeType>(raw);
            }
        }
    }

    const Int256 value = static_cast<Int256>(raw);

    if (from_scale == to_scale)
        return saturateCastToNative<NativeType>(value);

    if (to_scale > from_scale)
    {
        const Int256 multiplier = DecimalUtils::scaleMultiplier<Int256>(to_scale - from_scale);

        Int256 result;
        if (common::mulOverflow(value, multiplier, result))
            return std::nullopt;

        return saturateCastToNative<NativeType>(result);
    }

    const Int256 divisor = DecimalUtils::scaleMultiplier<Int256>(from_scale - to_scale);
    /// Uses floor/ceil and division to check bounds without Int256 overflow.
    const Int256 floor_value = divRoundDown<Int256>(value, divisor);
    const Int256 ceil_value = divRoundUp<Int256>(value, divisor);

    if constexpr (std::is_same_v<NativeType, UInt256>)
    {
        if (floor_value < 0)
            return std::nullopt;
    }
    else
    {
        const Int256 native_min = static_cast<Int256>(std::numeric_limits<NativeType>::min());
        const Int256 native_max = static_cast<Int256>(std::numeric_limits<NativeType>::max());

        if (floor_value < native_min || ceil_value > native_max)
            return std::nullopt;
    }

    const Int256 rescaled = boundary_type == HistogramBoundary::LEFT_CLOSED_RIGHT_OPEN ? ceil_value : floor_value;
    return saturateCastToNative<NativeType>(rescaled);
}

/// Casts double boundary to NativeType with interval rounding ([a, b) UP, (a, b] DOWN)
/// and safe boundary checks against double precision loss on Int64+.
template <typename NativeType>
std::optional<NativeType> saturateRoundToNative(double scaled, HistogramBoundary boundary_type)
{
    if (!std::isfinite(scaled))
        return std::nullopt;

    constexpr double max_native = static_cast<double>(std::numeric_limits<NativeType>::max());
    constexpr double min_native = static_cast<double>(std::numeric_limits<NativeType>::min());
    constexpr bool max_exact_in_double = std::numeric_limits<NativeType>::digits <= std::numeric_limits<double>::digits;

    const bool out_of_range = scaled < min_native || (max_exact_in_double ? scaled > max_native : scaled >= max_native);

    if (out_of_range)
        return std::nullopt;

    const double rounded = boundary_type == HistogramBoundary::LEFT_CLOSED_RIGHT_OPEN ? std::ceil(scaled) : std::floor(scaled);

    return static_cast<NativeType>(rounded);
}

template <typename T>
struct NativeFieldTypeImpl
{
    using type = T;
};

template <typename T>
requires is_decimal<T>
struct NativeFieldTypeImpl<T>
{
    using type = typename T::NativeType;
};

template <typename T>
using NativeFieldType = typename NativeFieldTypeImpl<T>::type;

template <typename T>
constexpr bool is_float_like_v = std::is_floating_point_v<T> || std::is_same_v<T, BFloat16>;

template <typename T>
struct BoundaryStorageTypeImpl
{
    using type = T;
};

template <>
struct BoundaryStorageTypeImpl<Float32>
{
    using type = Float64;
};

/// Store Float32 and BFloat16 boundaries as Float64 to preserve parameter precision.
template <>
struct BoundaryStorageTypeImpl<BFloat16>
{
    using type = Float64;
};

template <typename T>
using BoundaryStorageType = typename BoundaryStorageTypeImpl<T>::type;

template <typename TargetType>
std::optional<TargetType> convertBoundaryToTarget(const Field & field, UInt32 target_scale, HistogramBoundary boundary_type)
{
    using NativeType = NativeFieldType<TargetType>;

    if constexpr (is_float_like_v<TargetType>)
    {
        const auto from_decimal_field = [&]<typename DecimalT>(const DecimalField<DecimalT> & d) -> TargetType
        { return static_cast<TargetType>(DecimalUtils::convertTo<Float64>(d.getValue(), d.getScale())); };

        TargetType val;
        switch (field.getType())
        {
            case Field::Types::UInt64: val = static_cast<TargetType>(field.safeGet<UInt64>()); break;
            case Field::Types::Int64: val = static_cast<TargetType>(field.safeGet<Int64>()); break;
            case Field::Types::Int128: val = static_cast<TargetType>(field.safeGet<Int128>()); break;
            case Field::Types::UInt128: val = static_cast<TargetType>(field.safeGet<UInt128>()); break;
            case Field::Types::Int256: val = static_cast<TargetType>(field.safeGet<Int256>()); break;
            case Field::Types::UInt256: val = static_cast<TargetType>(field.safeGet<UInt256>()); break;
            case Field::Types::Decimal32: val = from_decimal_field(field.safeGet<DecimalField<Decimal32>>()); break;
            case Field::Types::Decimal64: val = from_decimal_field(field.safeGet<DecimalField<Decimal64>>()); break;
            case Field::Types::Decimal128: val = from_decimal_field(field.safeGet<DecimalField<Decimal128>>()); break;
            case Field::Types::Decimal256: val = from_decimal_field(field.safeGet<DecimalField<Decimal256>>()); break;
            case Field::Types::Float64: val = static_cast<TargetType>(field.safeGet<Float64>()); break;
            default: UNREACHABLE();
        }

        if (std::isinf(static_cast<double>(val)))
            return std::nullopt;

        return val;
    }
    else
    {
        const auto finish = [](std::optional<NativeType> native) -> std::optional<TargetType>
        {
            if (!native)
                return std::nullopt;
            if constexpr (is_decimal<TargetType>)
                return TargetType(*native);
            else
                return static_cast<TargetType>(*native);
        };

        const auto from_integral_field = [&]<typename IntT>(IntT raw) -> std::optional<TargetType>
        { return finish(rescaleFixedPoint<NativeType>(raw, 0, target_scale, boundary_type)); };

        const auto from_decimal_field = [&]<typename DecimalT>(const DecimalField<DecimalT> & d) -> std::optional<TargetType>
        { return finish(rescaleFixedPoint<NativeType>(d.getValue().value, d.getScale(), target_scale, boundary_type)); };

        switch (field.getType())
        {
            case Field::Types::Int64: return from_integral_field(field.safeGet<Int64>());

            case Field::Types::UInt64: return from_integral_field(field.safeGet<UInt64>());

            case Field::Types::Int128: return from_integral_field(field.safeGet<Int128>());

            case Field::Types::UInt128: return from_integral_field(field.safeGet<UInt128>());

            case Field::Types::Int256: return from_integral_field(field.safeGet<Int256>());

            case Field::Types::UInt256: return from_integral_field(field.safeGet<UInt256>());

            case Field::Types::Decimal32: return from_decimal_field(field.safeGet<DecimalField<Decimal32>>());
            case Field::Types::Decimal64: return from_decimal_field(field.safeGet<DecimalField<Decimal64>>());
            case Field::Types::Decimal128: return from_decimal_field(field.safeGet<DecimalField<Decimal128>>());
            case Field::Types::Decimal256: return from_decimal_field(field.safeGet<DecimalField<Decimal256>>());

            case Field::Types::Float64: {
                const double boundary = field.safeGet<Float64>();

                if constexpr (is_decimal<TargetType>)
                {
                    const double multiplier = static_cast<double>(DecimalUtils::scaleMultiplier<NativeType>(target_scale));
                    return finish(saturateRoundToNative<NativeType>(boundary * multiplier, boundary_type));
                }
                else
                {
                    return saturateRoundToNative<NativeType>(boundary, boundary_type);
                }
            }

            default: UNREACHABLE();
        }
    }
}

template <typename T>
class AggregateFunctionHistogramExplicit;
/// Custom -If -Null adapter that preserves the value's null map as a distinct histogram bucket,
/// unlike standard AggregateFunctionIfNull which merges nulls with condition mask.
template <typename T>
class AggregateFunctionHistogramExplicitIfNullAdapter final
    : public AggregateFunctionNullBase<false, false, AggregateFunctionHistogramExplicitIfNullAdapter<T>>
{
private:
    using Base = AggregateFunctionNullBase<false, false, AggregateFunctionHistogramExplicitIfNullAdapter<T>>;
    using Base::nested_function;
    using Base::nestedPlace;

    size_t num_arguments;
    bool value_is_nullable = false;
    bool condition_is_nullable = false;
    bool condition_is_only_null = false;

    const AggregateFunctionHistogramExplicit<T> * value_target = nullptr;

    static const AggregateFunctionHistogramExplicit<T> & resolveValueTarget(const IAggregateFunction & function)
    {
        const IAggregateFunction * probe = &function;
        while (probe)
        {
            if (const auto * typed = dynamic_cast<const AggregateFunctionHistogramExplicit<T> *>(probe))
                return *typed;
            AggregateFunctionPtr next = probe->getNestedFunction();
            probe = next.get();
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "AggregateFunctionHistogramExplicitIfNullAdapter: no histogramExplicit found in the nested function chain");
    }

    bool conditionAt(const IColumn * condition_column, size_t row_num) const
    {
        if (condition_is_nullable)
        {
            const auto & nullable_condition = assert_cast<const ColumnNullable &>(*condition_column);
            return !nullable_condition.isNullAt(row_num)
                && assert_cast<const ColumnUInt8 &>(nullable_condition.getNestedColumn()).getData()[row_num];
        }
        return assert_cast<const ColumnUInt8 &>(*condition_column).getData()[row_num];
    }

    void mergeNullableCondition(const IColumn & condition_column, size_t row_begin, size_t row_end, PaddedPODArray<UInt8> & out) const
    {
        const auto & nullable_condition = assert_cast<const ColumnNullable &>(condition_column);
        const auto & condition_data = assert_cast<const ColumnUInt8 &>(nullable_condition.getNestedColumn()).getData();
        const UInt8 * __restrict condition_null_map = nullable_condition.getNullMapData().data();

        out.resize(row_end);
        for (size_t i = row_begin; i < row_end; ++i)
            out[i] = condition_data[i] && !condition_null_map[i];
    }

public:
    AggregateFunctionHistogramExplicitIfNullAdapter(
        AggregateFunctionPtr nested_function_, const DataTypes & arguments, const Array & params)
        : Base(std::move(nested_function_), arguments, params)
        , num_arguments(arguments.size())
    {
        value_target = &resolveValueTarget(*nested_function);
        value_is_nullable = arguments[0]->isNullable();
        condition_is_nullable = arguments[num_arguments - 1]->isNullable();
        condition_is_only_null = arguments[num_arguments - 1]->onlyNull();
    }

    String getName() const override { return nested_function->getName() + "If"; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if (condition_is_only_null)
            return;

        if (!conditionAt(columns[num_arguments - 1], row_num))
            return;

        if (!value_is_nullable)
        {
            nested_function->add(nestedPlace(place), columns, row_num, arena);
            return;
        }

        const auto & nullable_value = assert_cast<const ColumnNullable &>(*columns[0]);
        const IColumn * nested_columns[] = {&nullable_value.getNestedColumn()};

        nested_function->addBatchSinglePlaceNotNull(
            row_num,
            row_num + 1,
            nestedPlace(place),
            nested_columns,
            nullable_value.getNullMapData().data(),
            arena,
            /*if_argument_pos=*/-1);
    }

    void addBatchSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena * arena, ssize_t)
        const override
    {
        if (condition_is_only_null)
            return;

        const IColumn * condition_column = columns[num_arguments - 1];
        MutableColumnPtr merged_condition;

        if (condition_is_nullable)
        {
            merged_condition = ColumnUInt8::create();
            mergeNullableCondition(*condition_column, row_begin, row_end, assert_cast<ColumnUInt8 &>(*merged_condition).getData());
            condition_column = merged_condition.get();
        }

        if (!value_is_nullable)
        {
            const IColumn * nested_columns[] = {columns[0], condition_column};
            nested_function->addBatchSinglePlace(row_begin, row_end, nestedPlace(place), nested_columns, arena, /*if_argument_pos=*/1);
            return;
        }

        const auto & nullable_value = assert_cast<const ColumnNullable &>(*columns[0]);
        const IColumn * nested_columns[] = {&nullable_value.getNestedColumn(), condition_column};

        /// See add() above: forward through nested_function so any wrapping combinator runs its own
        /// addBatchSinglePlaceNotNull first, rather than reaching past it via value_target.
        nested_function->addBatchSinglePlaceNotNull(
            row_begin,
            row_end,
            nestedPlace(place),
            nested_columns,
            nullable_value.getNullMapData().data(),
            arena,
            /*if_argument_pos=*/1);
    }

    bool preservesNulls() const override { return nested_function->preservesNulls(); }

    DataTypePtr getNormalizedStateType() const override
    {
        return buildNormalizedStateType(this->shared_from_this(), this->argument_types, value_target->canonicalParameters());
    }
};

template <typename T>
AggregateFunctionPtr makeIfNullAdapter(const AggregateFunctionPtr & raw_function, const DataTypes & arguments, const Array & params)
{
    if (arguments.size() != 2)
        return nullptr;

    return std::make_shared<AggregateFunctionHistogramExplicitIfNullAdapter<T>>(raw_function, arguments, params);
}

template <typename T>
class AggregateFunctionHistogramExplicitNullAdapter final
    : public AggregateFunctionNullBase<false, false, AggregateFunctionHistogramExplicitNullAdapter<T>>
{
private:
    using Base = AggregateFunctionNullBase<false, false, AggregateFunctionHistogramExplicitNullAdapter<T>>;
    using Base::nested_function;
    using Base::nestedPlace;

    const AggregateFunctionHistogramExplicit<T> & nestedTyped() const
    {
        return assert_cast<const AggregateFunctionHistogramExplicit<T> &>(*nested_function);
    }

public:
    AggregateFunctionHistogramExplicitNullAdapter(AggregateFunctionPtr nested_function_, const DataTypes & arguments, const Array & params)
        : Base(std::move(nested_function_), arguments, params)
    {
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        addBatchSinglePlace(row_num, row_num + 1, place, columns, arena, -1);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        const auto & nullable_column = assert_cast<const ColumnNullable &>(*columns[0]);
        /// Preserve condition column at if_argument_pos when wrapped by -If combinator.
        const IColumn * nested_columns[] = {&nullable_column.getNestedColumn(), if_argument_pos >= 0 ? columns[if_argument_pos] : nullptr};

        nested_function->addBatchSinglePlaceNotNull(
            row_begin, row_end, nestedPlace(place), nested_columns, nullable_column.getNullMapData().data(), arena, if_argument_pos);
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        nested_function->addBatchSinglePlaceNotNull(row_begin, row_end, nestedPlace(place), columns, null_map, arena, if_argument_pos);
    }

    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** /*columns*/, size_t length, Arena * /*arena*/) const override
    {
        nestedTyped().addManyNulls(nestedPlace(place), length);
    }

    void addBatch(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        if (row_begin == row_end)
            return;

        const auto & nullable_column = assert_cast<const ColumnNullable &>(*columns[0]);
        const IColumn * nested_columns[] = {&nullable_column.getNestedColumn(), if_argument_pos >= 0 ? columns[if_argument_pos] : nullptr};

        /// Find first non-null place to calculate nestedPlace offset safely and avoid UB.
        size_t first = row_begin;
        while (first < row_end && !places[first])
            ++first;

        if (first == row_end)
            return;

        const ConstAggregateDataPtr first_place = places[first] + place_offset;
        const size_t adjusted_place_offset = place_offset + static_cast<size_t>(nestedPlace(first_place) - first_place);

        nestedTyped().addBatchNullable(
            row_begin,
            row_end,
            places,
            adjusted_place_offset,
            nested_columns,
            nullable_column.getNullMapData().data(),
            arena,
            if_argument_pos);
    }

    /// Use the raw function when -If wraps this adapter to avoid unwrapping Nullable twice.
    AggregateFunctionPtr getOwnNullAdapterIf(
        const AggregateFunctionPtr & /*function*/,
        const DataTypes & arguments,
        const Array & params,
        const AggregateFunctionProperties & /*properties*/) const override
    {
        return makeIfNullAdapter<T>(nested_function, arguments, params);
    }

    bool preservesNulls() const override { return nested_function->preservesNulls(); }

    DataTypePtr getNormalizedStateType() const override
    {
        return buildNormalizedStateType(this->shared_from_this(), this->argument_types, nestedTyped().canonicalParameters());
    }
};

template <typename T>
class AggregateFunctionHistogramExplicit final
    : public IAggregateFunctionDataHelper<HistogramExplicitData, AggregateFunctionHistogramExplicit<T>>
{
private:
    using Data = HistogramExplicitData;
    using ColVecType = ColumnVectorOrDecimal<T>;
    using BoundaryT = BoundaryStorageType<T>;
    using BoundaryVecType = ColumnVectorOrDecimal<BoundaryT>;

    VectorWithMemoryTracking<BoundaryT> boundaries_native_;
    size_t intervals_count_;
    HistogramBoundary boundary_type_;
    VectorWithMemoryTracking<SpecialValue> special_values_;

    ssize_t null_offset_ = -1;
    ssize_t nan_offset_ = -1;
    ssize_t zero_offset_ = -1;

    static DataTypePtr createResultType(const DataTypes & arg_types, const VectorWithMemoryTracking<SpecialValue> & special_values)
    {
        DataTypePtr nullable_elem_type = makeNullable(boundaryDataType(arg_types));

        auto bucket_tuple = std::make_shared<DataTypeTuple>(
            DataTypes{nullable_elem_type, nullable_elem_type, std::make_shared<DataTypeUInt64>()}, Strings{"lower", "upper", "count"});

        DataTypes result_types{std::make_shared<DataTypeArray>(bucket_tuple)};
        Strings result_names{"buckets"};

        for (const auto & spec : special_values)
        {
            result_types.emplace_back(std::make_shared<DataTypeUInt64>());
            result_names.emplace_back(specialValueResultFieldName(spec));
        }

        return std::make_shared<DataTypeTuple>(result_types, result_names);
    }

    static UInt32 scaleOf(const DataTypes & arg_types)
    {
        if constexpr (is_decimal<T>)
            return assert_cast<const DataTypeDecimal<T> &>(*removeNullable(arg_types.at(0))).getScale();
        else
            return 0;
    }

    static DataTypePtr boundaryDataType(const DataTypes & arg_types)
    {
        if constexpr (!std::is_same_v<BoundaryT, T>)
            return std::make_shared<DataTypeFloat64>();
        else
            return removeNullable(arg_types.at(0));
    }

    static VectorWithMemoryTracking<BoundaryT>
    buildNativeBoundaries(const BoundaryList & boundaries, const DataTypes & arg_types, HistogramBoundary boundary_type)
    {
        const UInt32 scale = scaleOf(arg_types);

        VectorWithMemoryTracking<BoundaryT> native;
        native.reserve(boundaries.size());
        for (const auto & field : boundaries)
        {
            if (auto converted = convertBoundaryToTarget<BoundaryT>(field, scale, boundary_type))
                native.push_back(*converted);
        }

        std::sort(native.begin(), native.end());
        native.erase(std::unique(native.begin(), native.end()), native.end());
        return native;
    }

    size_t getTotalBucketsCount() const { return intervals_count_ + special_values_.size(); }

    struct SpecialOffsets
    {
        ssize_t null_offset;
        ssize_t nan_offset;
        ssize_t zero_offset;
    };

    ALWAYS_INLINE SpecialOffsets getSpecialOffsets() const { return {null_offset_, nan_offset_, zero_offset_}; }

    Field nativeBoundaryToField(BoundaryT native) const
    {
        if constexpr (is_decimal<BoundaryT>)
            return DecimalField<BoundaryT>(native, scaleOf(this->argument_types));
        else
            return Field(native);
    }

    ALWAYS_INLINE static const UInt8 * extractIfFlags(const IColumn ** columns, ssize_t if_argument_pos)
    {
        return if_argument_pos >= 0 ? assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data() : nullptr;
    }

public:

    Array canonicalParameters() const
    {
        Array boundaries_array;
        boundaries_array.reserve(boundaries_native_.size());

        for (const auto & native : boundaries_native_)
            boundaries_array.push_back(nativeBoundaryToField(native));

        std::sort(boundaries_array.begin(), boundaries_array.end());

        boundaries_array.erase(std::unique(boundaries_array.begin(), boundaries_array.end()), boundaries_array.end());

        Array result;
        result.reserve(1 + special_values_.size());
        result.push_back(std::move(boundaries_array));

        for (const auto & spec : special_values_)
            result.emplace_back(String(specialValueName(spec)));
        return result;
    }

    AggregateFunctionHistogramExplicit(
        const DataTypes & argument_types_,
        const Array & params,
        BoundaryList boundaries,
        HistogramBoundary boundary_type,
        VectorWithMemoryTracking<SpecialValue> special_values)
        : IAggregateFunctionDataHelper<HistogramExplicitData, AggregateFunctionHistogramExplicit<T>>(
              argument_types_, params, createResultType(argument_types_, special_values))
        , boundaries_native_(buildNativeBoundaries(boundaries, argument_types_, boundary_type))
        , intervals_count_(boundaries_native_.size() + 1)
        , boundary_type_(boundary_type)
        , special_values_(std::move(special_values))
    {
        const size_t base_offset = intervals_count_;
        for (size_t i = 0; i < special_values_.size(); ++i)
        {
            if (special_values_[i] == SpecialValue::NULL_VALUE)
                null_offset_ = base_offset + i;
            if (special_values_[i] == SpecialValue::NAN_VALUE)
                nan_offset_ = base_offset + i;
            if (special_values_[i] == SpecialValue::ZERO_VALUE)
                zero_offset_ = base_offset + i;
        }
    }

    String getName() const override
    {
        return boundary_type_ == HistogramBoundary::LEFT_CLOSED_RIGHT_OPEN ? "histogramExplicit" : "histogramExplicitOpenClosed";
    }

    bool preservesNulls() const override { return null_offset_ >= 0; }

    bool allocatesMemoryInArena() const override { return false; }

    size_t sizeOfData() const override { return getTotalBucketsCount() * sizeof(UInt64); }

    size_t alignOfData() const override { return alignof(UInt64); }

    void create(AggregateDataPtr __restrict place) const override { memset(place, 0, getTotalBucketsCount() * sizeof(UInt64)); }

    DataTypePtr getNormalizedStateType() const override
    {
        return buildNormalizedStateType(this->shared_from_this(), this->argument_types, canonicalParameters());
    }

private:
    template <typename F>
    ALWAYS_INLINE void dispatchBoundaryType(F && f) const
    {
        if (boundary_type_ == HistogramBoundary::LEFT_CLOSED_RIGHT_OPEN)
            f(std::integral_constant<HistogramBoundary, HistogramBoundary::LEFT_CLOSED_RIGHT_OPEN>{});
        else
            f(std::integral_constant<HistogramBoundary, HistogramBoundary::LEFT_OPEN_RIGHT_CLOSED>{});
    }

    /// Linear scan auto-vectorizes and avoids branch mispredictions for small boundary lists (<= 250 elements).
    template <HistogramBoundary BoundaryType>
    ALWAYS_INLINE static ssize_t
    getBucketIndex(BoundaryT value, std::span<const BoundaryT> boundaries, ssize_t local_nan_offset, ssize_t local_zero_offset)
    {
        if constexpr (std::is_floating_point_v<BoundaryT>)
        {
            if (std::isnan(value))
                return local_nan_offset;
        }

        if (local_zero_offset >= 0 && value == BoundaryT{})
            return local_zero_offset;

        const BoundaryT * __restrict ptr = boundaries.data();
        const size_t count = boundaries.size();

        size_t boundary_index = 0;
        for (size_t j = 0; j < count; ++j)
        {
            if constexpr (BoundaryType == HistogramBoundary::LEFT_CLOSED_RIGHT_OPEN)
                boundary_index += static_cast<size_t>(ptr[j] <= value);
            else
                boundary_index += static_cast<size_t>(ptr[j] < value);
        }

        return static_cast<ssize_t>(boundary_index);
    }

    template <HistogramBoundary BoundaryType, bool HasNullMap>
    ALWAYS_INLINE static void classifyAndIncrement(
        UInt64 * __restrict counts,
        const T * __restrict data,
        const UInt8 * __restrict null_map,
        size_t i,
        const SpecialOffsets & offsets,
        std::span<const BoundaryT> boundaries)
    {
        if constexpr (HasNullMap)
        {
            if (null_map[i])
            {
                if (offsets.null_offset >= 0)
                    ++counts[offsets.null_offset];
                return;
            }
        }

        const BoundaryT value = static_cast<BoundaryT>(data[i]);
        const ssize_t idx = getBucketIndex<BoundaryType>(value, boundaries, offsets.nan_offset, offsets.zero_offset);
        if (idx >= 0)
            ++counts[idx];
    }

    template <HistogramBoundary BoundaryType, bool HasNullMap>
    ALWAYS_INLINE void addBatchImpl(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const T * __restrict data,
        const UInt8 * __restrict null_map,
        const UInt8 * __restrict if_flags,
        std::span<const BoundaryT> boundaries) const
    {
        UInt64 * __restrict counts = Data::getCounts(place);
        const SpecialOffsets offsets = getSpecialOffsets();

        for (size_t i = row_begin; i < row_end; ++i)
        {
            if (if_flags && !if_flags[i])
                continue;

            classifyAndIncrement<BoundaryType, HasNullMap>(counts, data, null_map, i, offsets, boundaries);
        }
    }

    template <bool HasNullMap>
    ALWAYS_INLINE void addBatchSinglePlaceCommon(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        ssize_t if_argument_pos) const
    {
        const auto & col = assert_cast<const ColVecType &>(*columns[0]);
        const T * data = col.getData().data();
        const UInt8 * if_flags = extractIfFlags(columns, if_argument_pos);

        dispatchBoundaryType(
            [&](auto boundary_type)
            {
                addBatchImpl<decltype(boundary_type)::value, HasNullMap>(
                    row_begin, row_end, place, data, null_map, if_flags, boundaries_native_);
            });
    }

public:
    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & col = assert_cast<const ColVecType &>(*columns[0]);
        const BoundaryT value = static_cast<BoundaryT>(col.getData()[row_num]);

        dispatchBoundaryType(
            [&](auto boundary_type)
            {
                const ssize_t idx = getBucketIndex<decltype(boundary_type)::value>(value, boundaries_native_, nan_offset_, zero_offset_);
                if (idx >= 0)
                    ++Data::getCounts(place)[idx];
            });
    }

    void addBatchSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena *, ssize_t if_argument_pos)
        const override
    {
        addBatchSinglePlaceCommon<false>(row_begin, row_end, place, columns, nullptr, if_argument_pos);
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena *,
        ssize_t if_argument_pos) const final
    {
        addBatchSinglePlaceCommon<true>(row_begin, row_end, place, columns, null_map, if_argument_pos);
    }

    ALWAYS_INLINE void addManyNulls(AggregateDataPtr __restrict place, size_t length) const
    {
        if (null_offset_ >= 0)
            Data::getCounts(place)[null_offset_] += length;
    }

private:
    template <bool HasNullMap>
    ALWAYS_INLINE void addBatchCommon(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        const UInt8 * null_map,
        ssize_t if_argument_pos) const
    {
        const auto & col = assert_cast<const ColVecType &>(*columns[0]);
        const T * __restrict data = col.getData().data();
        const UInt8 * __restrict if_flags = extractIfFlags(columns, if_argument_pos);
        const SpecialOffsets offsets = getSpecialOffsets();

        dispatchBoundaryType(
            [&](auto boundary_type)
            {
                for (size_t i = row_begin; i < row_end; ++i)
                {
                    if (!places[i])
                        continue;

                    if (if_flags && !if_flags[i])
                        continue;

                    UInt64 * counts = Data::getCounts(places[i] + place_offset);
                    classifyAndIncrement<decltype(boundary_type)::value, HasNullMap>(
                        counts, data, null_map, i, offsets, boundaries_native_);
                }
            });
    }

public:
    void addBatch(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        addBatchCommon<false>(row_begin, row_end, places, place_offset, columns, nullptr, if_argument_pos);
    }

    void addBatchNullable(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** nested_columns,
        const UInt8 * null_map,
        Arena *,
        ssize_t if_argument_pos) const
    {
        addBatchCommon<true>(row_begin, row_end, places, place_offset, nested_columns, null_map, if_argument_pos);
    }

    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & nested_function,
        const DataTypes & arguments,
        const Array & params,
        const AggregateFunctionProperties & /*properties*/) const override
    {
        return std::make_shared<AggregateFunctionHistogramExplicitNullAdapter<T>>(nested_function, arguments, params);
    }

    AggregateFunctionPtr getOwnNullAdapterIf(
        const AggregateFunctionPtr & nested_function,
        const DataTypes & arguments,
        const Array & params,
        const AggregateFunctionProperties & /*properties*/) const override
    {
        return makeIfNullAdapter<T>(nested_function, arguments, params);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        Data::merge(place, rhs, getTotalBucketsCount());
    }

    void
    serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        Data::write(place, buf, getTotalBucketsCount());
    }

    void
    deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * /* arena */) const override
    {
        Data::read(place, buf, getTotalBucketsCount());
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & tuple_column = assert_cast<ColumnTuple &>(to);
        const auto * counts = Data::getCounts(place);

        auto & array_column = assert_cast<ColumnArray &>(tuple_column.getColumn(0));
        auto & array_offsets = array_column.getOffsets();
        auto & bucket_tuple = assert_cast<ColumnTuple &>(array_column.getData());

        auto & lower_nullable = assert_cast<ColumnNullable &>(bucket_tuple.getColumn(0));
        auto & upper_nullable = assert_cast<ColumnNullable &>(bucket_tuple.getColumn(1));
        auto & count_col = assert_cast<ColumnUInt64 &>(bucket_tuple.getColumn(2)).getData();

        auto & lower_nested = assert_cast<BoundaryVecType &>(lower_nullable.getNestedColumn());
        auto & upper_nested = assert_cast<BoundaryVecType &>(upper_nullable.getNestedColumn());

        auto & lower_null_map = lower_nullable.getNullMapData();
        auto & upper_null_map = upper_nullable.getNullMapData();

        const size_t intervals_count = intervals_count_;
        const size_t native_count = boundaries_native_.size();

        chassert(intervals_count == native_count + 1);

        const size_t current_size = count_col.size();
        lower_nested.reserve(current_size + intervals_count);
        upper_nested.reserve(current_size + intervals_count);
        lower_null_map.reserve(current_size + intervals_count);
        upper_null_map.reserve(current_size + intervals_count);
        count_col.reserve(current_size + intervals_count);

        for (size_t i = 0; i < intervals_count; ++i)
        {
            if (i == 0)
            {
                lower_nested.insertDefault();
                lower_null_map.push_back(UInt8(1));
            }
            else
            {
                lower_nested.insertValue(boundaries_native_[i - 1]);
                lower_null_map.push_back(UInt8(0));
            }

            if (i == native_count)
            {
                upper_nested.insertDefault();
                upper_null_map.push_back(UInt8(1));
            }
            else
            {
                upper_nested.insertValue(boundaries_native_[i]);
                upper_null_map.push_back(UInt8(0));
            }

            count_col.push_back(counts[i]);
        }

        array_offsets.push_back((array_offsets.empty() ? 0 : array_offsets.back()) + intervals_count);

        size_t current_offset = intervals_count;
        for (size_t i = 0; i < special_values_.size(); ++i)
            assert_cast<ColumnUInt64 &>(tuple_column.getColumn(i + 1)).insertValue(counts[current_offset++]);
    }
};

SpecialValue parseSpecialValue(const Field & field, const String & name)
{
    if (field.getType() != Field::Types::String)
    {
        throw Exception(
            ErrorCodes::UNSUPPORTED_PARAMETER, "Special parameter for aggregate function {} must be 'null', 'nan' or 'zero'", name);
    }

    const String & value = field.safeGet<String>();

    if (value == "null")
        return SpecialValue::NULL_VALUE;
    if (value == "nan")
        return SpecialValue::NAN_VALUE;
    if (value == "zero")
        return SpecialValue::ZERO_VALUE;

    throw Exception(
        ErrorCodes::UNSUPPORTED_PARAMETER,
        "Unknown special parameter '{}' for aggregate function {}. Expected 'null', 'nan' or 'zero'",
        value,
        name);
}

void validateBoundaryFieldType(const Field & field, const String & name)
{
    switch (field.getType())
    {
        case Field::Types::Float64:
        case Field::Types::Int64:
        case Field::Types::UInt64:
        case Field::Types::Int128:
        case Field::Types::UInt128:
        case Field::Types::Int256:
        case Field::Types::UInt256:
        case Field::Types::Decimal32:
        case Field::Types::Decimal64:
        case Field::Types::Decimal128:
        case Field::Types::Decimal256: return;
        default:
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Boundary element for aggregate function {} must be numeric or Decimal, got {}",
                name,
                field.getTypeName());
    }
}

AggregateFunctionPtr createHistogramExplicitTyped(
    const String & name,
    const DataTypes & argument_types,
    const Array & parameters,
    BoundaryList boundaries,
    HistogramBoundary boundary_type,
    VectorWithMemoryTracking<SpecialValue> special_values)
{
    const DataTypePtr & data_type = removeNullable(argument_types[0]);

    AggregateFunctionPtr res;
    if (isDecimal(data_type))
    {
        res.reset(createWithDecimalType<AggregateFunctionHistogramExplicit>(
            *data_type, argument_types, parameters, std::move(boundaries), boundary_type, std::move(special_values)));
    }
    else if (isNumber(data_type))
    {
        res.reset(createWithNumericType<AggregateFunctionHistogramExplicit>(
            *data_type, argument_types, parameters, std::move(boundaries), boundary_type, std::move(special_values)));
    }

    if (!res)
    {
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}", data_type->getName(), name);
    }

    return res;
}

AggregateFunctionPtr createAggregateFunctionHistogramExplicit(
    const String & name, const DataTypes & argument_types, const Array & parameters, HistogramBoundary boundary_type)
{
    constexpr size_t MAX_BOUNDARIES = 250;

    if (argument_types.size() != 1)
    {
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires exactly one argument", name);
    }

    const auto & value_type = removeNullable(argument_types[0]);
    if (!isDecimal(value_type) && !isNumber(value_type))
    {
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of argument for aggregate function {}, expected numeric or Decimal type",
            argument_types[0]->getName(),
            name);
    }

    if (parameters.empty())
    {
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires a boundaries array as the first parameter", name);
    }

    if (parameters[0].getType() != Field::Types::Array)
    {
        throw Exception(ErrorCodes::UNSUPPORTED_PARAMETER, "Aggregate function {} requires boundaries as an array (first parameter)", name);
    }

    const Array & boundaries_array = parameters[0].safeGet<Array>();

    if (boundaries_array.empty())
    {
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Aggregate function {} requires a non-empty boundaries array", name);
    }

    if (boundaries_array.size() > MAX_BOUNDARIES)
    {
        throw Exception(
            ErrorCodes::ARGUMENT_OUT_OF_BOUND,
            "Aggregate function {} supports at most {} boundaries, got {}",
            name,
            MAX_BOUNDARIES,
            boundaries_array.size());
    }

    BoundaryList boundaries;
    boundaries.reserve(boundaries_array.size());

    for (const auto & element : boundaries_array)
    {
        if (element.isNull())
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Boundary value for aggregate function {} cannot be NULL", name);
        }

        validateBoundaryFieldType(element, name);

        if (element.getType() == Field::Types::Float64)
        {
            const double value = element.safeGet<Float64>();

            if (std::isnan(value))
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Boundary value for aggregate function {} cannot be NaN", name);
            }
        }

        boundaries.push_back(element);
    }

    VectorWithMemoryTracking<SpecialValue> special_values;
    special_values.reserve(parameters.size() - 1);

    for (size_t i = 1; i < parameters.size(); ++i)
    {
        const SpecialValue special = parseSpecialValue(parameters[i], name);

        if (std::find(special_values.begin(), special_values.end(), special) != special_values.end())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Aggregate function {} contains duplicate special parameter '{}'",
                name,
                specialValueName(special));
        }

        special_values.push_back(special);
    }

    return createHistogramExplicitTyped(name, argument_types, parameters, std::move(boundaries), boundary_type, std::move(special_values));
}

FunctionDocumentation makeHistogramExplicitDocumentation(const String & function_name, const char * interval_notation)
{
    return FunctionDocumentation{
        .description = String(
            "Computes a histogram over a numeric or Decimal column using explicit bucket boundaries. "
            "Intervals are ") + interval_notation + String(
            ". Accepts up to 250 boundaries. A NaN boundary is rejected with an exception. Boundaries that are "
            "+/-Inf, or outside the representable range of the argument's data type, are silently discarded. "
            "The remaining boundaries are converted to the comparison type, sorted, and deduplicated. "
            "The first and last buckets are always unbounded ([-inf, ...) and (..., +inf]), returning missing endpoints as `NULL`.\n\n"
            "Special values ('null', 'nan', 'zero') can optionally be tracked as separate counters. "
            "When not requested, `NULL` and `NaN` are ignored, while zero is placed into its numeric bucket.\n\n"
            "For integer and Decimal inputs, fractional boundaries are rounded to preserve interval semantics: "
            "upward for [a, b) and downward for (a, b]. "
            "When boundaries are Float64, comparisons for Decimal or 128/256-bit integer types are limited to Float64 precision."),
        .syntax = function_name + "(boundaries[, special_value1, ...])(x)",
        .arguments = {
            {
                "x",
                "Value column to place into histogram buckets.",
                {"(U)Int*", "Float*", "BFloat16", "Decimal*"}
            },
        },
        .parameters = {
            {
                "boundaries",
                "Non-empty array of up to 250 explicit bucket boundaries.",
                {"Array(Float64)", "Array(Int64)", "Array(UInt64)", "Array(Decimal*)"}
            },
            {
                "special_value1, special_value2, ...",
                "Optional variadic parameters to track special values: 'null', 'nan', or 'zero'. "
                "Each requested value appends a counter to the result tuple in parameter order.",
                {"String"}
            },
        },
        .returned_value = {
            .description =
                "A tuple containing a 'buckets' array of (lower, upper, count) tuples, "
                "followed by the requested special-value counters. Endpoint types match the type of `x`, "
                "except that Float32 and BFloat16 inputs use Float64 endpoints. "
                "`NULL` endpoints represent unbounded interval sides.",
            .types = {
                "Tuple(buckets Array(Tuple(lower Nullable(T), upper Nullable(T), count UInt64)), ...)"
            },
        },
        .examples = {
            {
                "basic",
                "SELECT " + function_name + "([0, 10, 20], 'null', 'zero')(x) "
                "FROM values('x Nullable(Int64)', "
                "(NULL), (-1), (0), (1), (9), (11), (19), (21))",
                "([(NULL, 0, 1), (0, 10, 2), (10, 20, 2), (20, NULL, 1)], 1, 1)"
            },
        },
        .introduced_in = {26, 9},
        .category = FunctionDocumentation::Category::AggregateFunction,
    };
}

}

void registerAggregateFunctionHistogramExplicit(AggregateFunctionFactory & factory);

void registerAggregateFunctionHistogramExplicit(AggregateFunctionFactory & factory)
{
    static constexpr AggregateFunctionProperties properties{.returns_default_when_only_null = true};

    factory.registerFunction(
        "histogramExplicit",
        AggregateFunctionWithProperties{
            [](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *) {
                return createAggregateFunctionHistogramExplicit(
                    name, argument_types, parameters, HistogramBoundary::LEFT_CLOSED_RIGHT_OPEN);
            },
            makeHistogramExplicitDocumentation("histogramExplicit", "left-closed, right-open: [a, b) (with inclusive infinite bounds)"),
            properties});

    factory.registerFunction(
        "histogramExplicitOpenClosed",
        AggregateFunctionWithProperties{
            [](const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *) {
                return createAggregateFunctionHistogramExplicit(
                    name, argument_types, parameters, HistogramBoundary::LEFT_OPEN_RIGHT_CLOSED);
            },
            makeHistogramExplicitDocumentation(
                "histogramExplicitOpenClosed", "left-open, right-closed: (a, b] (with inclusive infinite bounds)"),
            properties});
}

}
