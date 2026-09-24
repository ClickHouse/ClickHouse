#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>

#include <Common/Exception.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/assert_cast.h>

#include <Core/DecimalFunctions.h>

#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
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
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <type_traits>
#include <utility>

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

constexpr size_t MAX_BOUNDARIES = 250;

enum class HistogramBoundary : UInt8
{
    LEFT_CLOSED_RIGHT_OPEN,
    LEFT_OPEN_RIGHT_CLOSED
};

enum class SpecialValue : UInt8
{
    NAN_VALUE,
    NULL_VALUE,
    ZERO_VALUE
};

const char * specialValueName(SpecialValue value)
{
    switch (value)
    {
        case SpecialValue::NAN_VALUE: return "nan";
        case SpecialValue::NULL_VALUE: return "null";
        case SpecialValue::ZERO_VALUE: return "zero";
    }
    UNREACHABLE();
}

using BoundaryList = VectorWithMemoryTracking<Field>;

struct HistogramExplicitData
{
    template <typename Place>
    ALWAYS_INLINE static auto * getCounts(Place place)
    {
        chassert(reinterpret_cast<uintptr_t>(place) % alignof(UInt64) == 0);
        if constexpr (std::is_const_v<std::remove_pointer_t<Place>>)
            return reinterpret_cast<const UInt64 *>(place);
        else
            return reinterpret_cast<UInt64 *>(place);
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

template <bool HasNullMap>
struct RowMask
{
    const UInt8 * __restrict if_flags;
    const UInt8 * __restrict null_map;

    ALWAYS_INLINE bool skip(size_t i) const { return if_flags && !if_flags[i]; }

    ALWAYS_INLINE bool isNull(size_t i) const
    {
        if constexpr (HasNullMap)
            return null_map[i];
        else
            return false;
    }
};

template <typename NativeType>
std::optional<NativeType> checkedCastToNative(Int256 value)
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
    if constexpr (std::is_same_v<RawType, UInt256>)
    {
        if (raw > static_cast<UInt256>(std::numeric_limits<Int256>::max()))
        {
            if constexpr (std::is_same_v<NativeType, UInt256>)
            {
                chassert(from_scale == to_scale);
                return static_cast<NativeType>(raw);
            }
            else
                return std::nullopt;
        }
    }

    const Int256 value = static_cast<Int256>(raw);

    if (from_scale == to_scale)
        return checkedCastToNative<NativeType>(value);

    if (to_scale > from_scale)
    {
        Int256 result;
        if (common::mulOverflow(value, DecimalUtils::scaleMultiplier<Int256>(to_scale - from_scale), result))
            return std::nullopt;

        return checkedCastToNative<NativeType>(result);
    }

    const Int256 divisor = DecimalUtils::scaleMultiplier<Int256>(from_scale - to_scale);
    const Int256 quotient = value / divisor;
    const Int256 remainder = value % divisor;
    const Int256 floor_value = remainder < 0 ? quotient - 1 : quotient;
    const Int256 ceil_value = remainder > 0 ? quotient + 1 : quotient;

    if constexpr (std::is_same_v<NativeType, UInt256>)
    {
        if (floor_value < 0)
            return std::nullopt;
    }
    else
    {
        if (floor_value < static_cast<Int256>(std::numeric_limits<NativeType>::min())
            || ceil_value > static_cast<Int256>(std::numeric_limits<NativeType>::max()))
            return std::nullopt;
    }

    return static_cast<NativeType>(boundary_type == HistogramBoundary::LEFT_CLOSED_RIGHT_OPEN ? ceil_value : floor_value);
}

template <typename NativeType>
std::optional<NativeType> checkedRoundToNative(double scaled, HistogramBoundary boundary_type)
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
struct NativeFieldTypeImpl { using type = T; };

template <typename T> requires is_decimal<T>
struct NativeFieldTypeImpl<T> { using type = typename T::NativeType; };

template <typename T>
using NativeFieldType = typename NativeFieldTypeImpl<T>::type;

template <typename T>
struct BoundaryStorageTypeImpl { using type = T; };

template <> struct BoundaryStorageTypeImpl<Float32> { using type = Float64; };
template <> struct BoundaryStorageTypeImpl<BFloat16> { using type = Float64; };

template <typename T>
using BoundaryStorageType = typename BoundaryStorageTypeImpl<T>::type;

template <typename F>
auto visitBoundaryField(const Field & field, F && f)
{
    switch (field.getType())
    {
        case Field::Types::Int64: return f(field.safeGet<Int64>());
        case Field::Types::UInt64: return f(field.safeGet<UInt64>());
        case Field::Types::Int128: return f(field.safeGet<Int128>());
        case Field::Types::UInt128: return f(field.safeGet<UInt128>());
        case Field::Types::Int256: return f(field.safeGet<Int256>());
        case Field::Types::UInt256: return f(field.safeGet<UInt256>());
        case Field::Types::Decimal32: return f(field.safeGet<DecimalField<Decimal32>>());
        case Field::Types::Decimal64: return f(field.safeGet<DecimalField<Decimal64>>());
        case Field::Types::Decimal128: return f(field.safeGet<DecimalField<Decimal128>>());
        case Field::Types::Decimal256: return f(field.safeGet<DecimalField<Decimal256>>());
        case Field::Types::Float64: return f(field.safeGet<Float64>());
        default: UNREACHABLE();
    }
}

template <typename TargetType>
std::optional<TargetType> convertBoundaryToTarget(const Field & field, UInt32 target_scale, HistogramBoundary boundary_type)
{
    if constexpr (std::is_floating_point_v<TargetType>)
    {
        const TargetType value = visitBoundaryField(field, [](const auto & v) -> TargetType
        {
            if constexpr (requires { v.getScale(); })
                return static_cast<TargetType>(DecimalUtils::convertTo<Float64>(v.getValue(), v.getScale()));
            else
                return static_cast<TargetType>(v);
        });

        if (std::isinf(value))
            return std::nullopt;

        return value;
    }
    else
    {
        using NativeType = NativeFieldType<TargetType>;

        const auto finish = [](std::optional<NativeType> native) -> std::optional<TargetType>
        {
            if (!native)
                return std::nullopt;
            if constexpr (is_decimal<TargetType>)
                return TargetType(*native);
            else
                return static_cast<TargetType>(*native);
        };

        return visitBoundaryField(field, [&](const auto & v) -> std::optional<TargetType>
        {
            if constexpr (requires { v.getScale(); })
            {
                return finish(rescaleFixedPoint<NativeType>(v.getValue().value, v.getScale(), target_scale, boundary_type));
            }
            else if constexpr (std::is_same_v<std::decay_t<decltype(v)>, Float64>)
            {
                if constexpr (is_decimal<TargetType>)
                {
                    const double multiplier = static_cast<double>(DecimalUtils::scaleMultiplier<NativeType>(target_scale));
                    return finish(checkedRoundToNative<NativeType>(v * multiplier, boundary_type));
                }
                else
                    return checkedRoundToNative<NativeType>(v, boundary_type);
            }
            else
            {
                return finish(rescaleFixedPoint<NativeType>(v, 0, target_scale, boundary_type));
            }
        });
    }
}

template <typename T>
class AggregateFunctionHistogramExplicit final
    : public IAggregateFunctionDataHelper<HistogramExplicitData, AggregateFunctionHistogramExplicit<T>>
{
private:
    using Data = HistogramExplicitData;
    using ColVecType = ColumnVectorOrDecimal<T>;
    using BoundaryT = BoundaryStorageType<T>;
    using BoundaryVecType = ColumnVectorOrDecimal<BoundaryT>;

    struct SpecialOffsets
    {
        ssize_t null_offset = -1;
        ssize_t nan_offset = -1;
        ssize_t zero_offset = -1;
    };

    VectorWithMemoryTracking<BoundaryT> boundaries_native_;
    size_t intervals_count_;
    HistogramBoundary boundary_type_;
    VectorWithMemoryTracking<SpecialValue> special_values_;
    SpecialOffsets offsets_;
    bool value_is_nullable_;

    static DataTypePtr createResultType(const DataTypes & arg_types, const VectorWithMemoryTracking<SpecialValue> & special_values)
    {
        const DataTypePtr bound_type = makeNullable(boundaryDataType(arg_types));

        auto bucket_tuple = std::make_shared<DataTypeTuple>(
            DataTypes{bound_type, bound_type, std::make_shared<DataTypeUInt64>()},
            Strings{"lower", "upper", "count"});

        DataTypes result_types{std::make_shared<DataTypeArray>(bucket_tuple)};
        Strings result_names{"buckets"};

        for (const auto special : special_values)
        {
            result_types.emplace_back(std::make_shared<DataTypeUInt64>());
            result_names.emplace_back(String(specialValueName(special)) + "_count");
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

    static SpecialOffsets computeSpecialOffsets(size_t intervals_count, const VectorWithMemoryTracking<SpecialValue> & special_values)
    {
        SpecialOffsets offsets;
        for (size_t i = 0; i < special_values.size(); ++i)
        {
            const auto offset = static_cast<ssize_t>(intervals_count + i);
            switch (special_values[i])
            {
                case SpecialValue::NAN_VALUE: offsets.nan_offset = offset; break;
                case SpecialValue::NULL_VALUE: offsets.null_offset = offset; break;
                case SpecialValue::ZERO_VALUE: offsets.zero_offset = offset; break;
            }
        }
        return offsets;
    }

    size_t getTotalBucketsCount() const { return intervals_count_ + special_values_.size(); }

    Field nativeBoundaryToField(BoundaryT native) const
    {
        if constexpr (is_decimal<BoundaryT>)
            return DecimalField<BoundaryT>(native, scaleOf(this->argument_types));
        else
            return Field(native);
    }

    Array canonicalParameters() const
    {
        Array boundaries_array;
        boundaries_array.reserve(boundaries_native_.size());

        for (const auto & native : boundaries_native_)
            boundaries_array.push_back(nativeBoundaryToField(native));

        Array result;
        result.reserve(1 + special_values_.size());
        result.push_back(std::move(boundaries_array));

        for (const auto special : special_values_)
            result.emplace_back(String(specialValueName(special)));

        return result;
    }

    ALWAYS_INLINE static const UInt8 * extractIfFlags(const IColumn ** columns, ssize_t if_argument_pos)
    {
        return if_argument_pos >= 0 ? assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data() : nullptr;
    }

    template <typename F>
    ALWAYS_INLINE void dispatch(const IColumn ** columns, const UInt8 * if_flags, F && f) const
    {
        const auto with_boundary_type = [&](const T * data, const auto & mask)
        {
            if (boundary_type_ == HistogramBoundary::LEFT_CLOSED_RIGHT_OPEN)
                f(std::integral_constant<HistogramBoundary, HistogramBoundary::LEFT_CLOSED_RIGHT_OPEN>{}, data, mask);
            else
                f(std::integral_constant<HistogramBoundary, HistogramBoundary::LEFT_OPEN_RIGHT_CLOSED>{}, data, mask);
        };

        if (value_is_nullable_)
        {
            const auto & nullable_column = assert_cast<const ColumnNullable &>(*columns[0]);
            const auto & column = assert_cast<const ColVecType &>(nullable_column.getNestedColumn());
            with_boundary_type(column.getData().data(), RowMask<true>{if_flags, nullable_column.getNullMapData().data()});
        }
        else
        {
            const auto & column = assert_cast<const ColVecType &>(*columns[0]);
            with_boundary_type(column.getData().data(), RowMask<false>{if_flags, nullptr});
        }
    }

    template <HistogramBoundary BoundaryType, bool HasNullMap>
    ALWAYS_INLINE static void classifyAndIncrement(
        UInt64 * __restrict counts,
        const T * __restrict data,
        const RowMask<HasNullMap> & mask,
        size_t i,
        const SpecialOffsets & offsets,
        std::span<const BoundaryT> boundaries)
    {
        if (mask.skip(i))
            return;

        if (mask.isNull(i))
        {
            if (offsets.null_offset >= 0)
                ++counts[offsets.null_offset];
            return;
        }

        const BoundaryT value = static_cast<BoundaryT>(data[i]);

        if constexpr (std::is_floating_point_v<BoundaryT>)
        {
            if (std::isnan(value))
            {
                if (offsets.nan_offset >= 0)
                    ++counts[offsets.nan_offset];
                return;
            }
        }

        if (offsets.zero_offset >= 0 && value == BoundaryT{})
        {
            ++counts[offsets.zero_offset];
            return;
        }

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

        ++counts[boundary_index];
    }

    template <HistogramBoundary BoundaryType, bool HasNullMap>
    ALWAYS_INLINE void addBatchImpl(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const T * __restrict data,
        const RowMask<HasNullMap> & mask) const
    {
        UInt64 * __restrict counts = Data::getCounts(place);
        const SpecialOffsets offsets = offsets_;

        for (size_t i = row_begin; i < row_end; ++i)
            classifyAndIncrement<BoundaryType>(counts, data, mask, i, offsets, boundaries_native_);
    }

public:
    AggregateFunctionHistogramExplicit(
        const DataTypes & argument_types_,
        const Array & params,
        const BoundaryList & boundaries,
        HistogramBoundary boundary_type,
        VectorWithMemoryTracking<SpecialValue> special_values)
        : IAggregateFunctionDataHelper<HistogramExplicitData, AggregateFunctionHistogramExplicit<T>>(
              argument_types_, params, createResultType(argument_types_, special_values))
        , boundaries_native_(buildNativeBoundaries(boundaries, argument_types_, boundary_type))
        , intervals_count_(boundaries_native_.size() + 1)
        , boundary_type_(boundary_type)
        , special_values_(std::move(special_values))
        , offsets_(computeSpecialOffsets(intervals_count_, special_values_))
        , value_is_nullable_(argument_types_[0]->isNullable())
    {
        chassert(std::adjacent_find(
            special_values_.begin(), special_values_.end(), [](SpecialValue l, SpecialValue r) { return l >= r; }) == special_values_.end());
    }

    AggregateFunctionHistogramExplicit(const AggregateFunctionHistogramExplicit & prototype, const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<HistogramExplicitData, AggregateFunctionHistogramExplicit<T>>(
              argument_types_, prototype.parameters, prototype.getResultType())
        , boundaries_native_(prototype.boundaries_native_)
        , intervals_count_(prototype.intervals_count_)
        , boundary_type_(prototype.boundary_type_)
        , special_values_(prototype.special_values_)
        , offsets_(prototype.offsets_)
        , value_is_nullable_(argument_types_[0]->isNullable())
    {
    }

    String getName() const override
    {
        return boundary_type_ == HistogramBoundary::LEFT_CLOSED_RIGHT_OPEN ? "histogramExplicit" : "histogramExplicitOpenClosed";
    }

    bool allocatesMemoryInArena() const override { return false; }

    size_t sizeOfData() const override { return getTotalBucketsCount() * sizeof(UInt64); }

    size_t alignOfData() const override { return alignof(UInt64); }

    void create(AggregateDataPtr __restrict place) const override { memset(place, 0, sizeOfData()); }

    DataTypePtr getNormalizedStateType() const override
    {
        DataTypes normalized_argument_types;
        normalized_argument_types.reserve(this->argument_types.size());
        for (const auto & arg : this->argument_types)
            normalized_argument_types.emplace_back(arg->getNormalizedType());

        return std::make_shared<DataTypeAggregateFunction>(this->shared_from_this(), normalized_argument_types, canonicalParameters());
    }

    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & nested_function,
        const DataTypes & arguments,
        const Array & /*params*/,
        const AggregateFunctionProperties & /*properties*/) const override
    {
        if (offsets_.null_offset < 0)
            return nullptr;

        if (nested_function.get() != this)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "{}::getOwnNullAdapter must be called on the function itself", getName());

        return std::make_shared<AggregateFunctionHistogramExplicit>(*this, arguments);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        UInt64 * __restrict counts = Data::getCounts(place);

        dispatch(columns, nullptr, [&](auto boundary_type, const T * data, const auto & mask)
        {
            classifyAndIncrement<decltype(boundary_type)::value>(counts, data, mask, row_num, offsets_, boundaries_native_);
        });
    }

    void addBatchSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena *, ssize_t if_argument_pos)
        const override
    {
        dispatch(columns, extractIfFlags(columns, if_argument_pos), [&](auto boundary_type, const T * data, const auto & mask)
        {
            addBatchImpl<decltype(boundary_type)::value>(row_begin, row_end, place, data, mask);
        });
    }

    void addBatch(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        dispatch(columns, extractIfFlags(columns, if_argument_pos), [&](auto boundary_type, const T * data, const auto & mask)
        {
            const SpecialOffsets offsets = offsets_;

            for (size_t i = row_begin; i < row_end; ++i)
            {
                if (!places[i])
                    continue;

                classifyAndIncrement<decltype(boundary_type)::value>(
                    Data::getCounts(places[i] + place_offset), data, mask, i, offsets, boundaries_native_);
            }
        });
    }

    void addBatchSinglePlaceNotNull(
        size_t, size_t, AggregateDataPtr __restrict, const IColumn **, const UInt8 *, Arena *, ssize_t) const override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{}::addBatchSinglePlaceNotNull called, NULL rows must not be preprocessed", getName());
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        Data::merge(place, rhs, getTotalBucketsCount());
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        Data::write(place, buf, getTotalBucketsCount());
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * /* arena */) const override
    {
        Data::read(place, buf, getTotalBucketsCount());
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & tuple_column = assert_cast<ColumnTuple &>(to);
        const UInt64 * counts = Data::getCounts(place);

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

        const size_t new_size = count_col.size() + intervals_count_;
        lower_nested.reserve(new_size);
        upper_nested.reserve(new_size);
        lower_null_map.reserve(new_size);
        upper_null_map.reserve(new_size);
        count_col.reserve(new_size);

        const auto push_endpoint = [](auto & nested, auto & null_map, const BoundaryT * value)
        {
            if (value)
            {
                nested.insertValue(*value);
                null_map.push_back(UInt8(0));
            }
            else
            {
                nested.insertDefault();
                null_map.push_back(UInt8(1));
            }
        };

        const size_t boundaries_count = boundaries_native_.size();
        for (size_t i = 0; i < intervals_count_; ++i)
        {
            push_endpoint(lower_nested, lower_null_map, i == 0 ? nullptr : &boundaries_native_[i - 1]);
            push_endpoint(upper_nested, upper_null_map, i == boundaries_count ? nullptr : &boundaries_native_[i]);
            count_col.push_back(counts[i]);
        }

        array_offsets.push_back((array_offsets.empty() ? 0 : array_offsets.back()) + intervals_count_);

        for (size_t i = 0; i < special_values_.size(); ++i)
            assert_cast<ColumnUInt64 &>(tuple_column.getColumn(i + 1)).insertValue(counts[intervals_count_ + i]);
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

    if (value == "null") return SpecialValue::NULL_VALUE;
    if (value == "nan") return SpecialValue::NAN_VALUE;
    if (value == "zero") return SpecialValue::ZERO_VALUE;

    throw Exception(
        ErrorCodes::UNSUPPORTED_PARAMETER,
        "Unknown special parameter '{}' for aggregate function {}. Expected 'null', 'nan' or 'zero'",
        value, name);
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
                name, field.getTypeName());
    }
}

AggregateFunctionPtr createHistogramExplicitTyped(
    const DataTypePtr & value_type,
    const DataTypes & argument_types,
    const Array & parameters,
    const BoundaryList & boundaries,
    HistogramBoundary boundary_type,
    VectorWithMemoryTracking<SpecialValue> special_values)
{
    AggregateFunctionPtr res;
    if (isDecimal(value_type))
    {
        res.reset(createWithDecimalType<AggregateFunctionHistogramExplicit>(
            *value_type, argument_types, parameters, boundaries, boundary_type, std::move(special_values)));
    }
    else
    {
        res.reset(createWithNumericType<AggregateFunctionHistogramExplicit>(
            *value_type, argument_types, parameters, boundaries, boundary_type, std::move(special_values)));
    }

    if (!res)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No histogramExplicit implementation for type {}", value_type->getName());

    return res;
}

AggregateFunctionPtr createAggregateFunctionHistogramExplicit(
    const String & name, const DataTypes & argument_types, const Array & parameters, HistogramBoundary boundary_type)
{
    if (argument_types.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires exactly one argument", name);

    const DataTypePtr value_type = removeNullable(argument_types[0]);
    if (!isDecimal(value_type) && !isNumber(value_type))
    {
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of argument for aggregate function {}, expected numeric or Decimal type",
            argument_types[0]->getName(), name);
    }

    if (parameters.empty())
    {
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires a boundaries array as the first parameter", name);
    }

    if (parameters[0].getType() != Field::Types::Array)
        throw Exception(ErrorCodes::UNSUPPORTED_PARAMETER, "Aggregate function {} requires boundaries as an array (first parameter)", name);

    const Array & boundaries_array = parameters[0].safeGet<Array>();

    if (boundaries_array.empty())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Aggregate function {} requires a non-empty boundaries array", name);

    if (boundaries_array.size() > MAX_BOUNDARIES)
    {
        throw Exception(
            ErrorCodes::ARGUMENT_OUT_OF_BOUND,
            "Aggregate function {} supports at most {} boundaries, got {}",
            name, MAX_BOUNDARIES, boundaries_array.size());
    }

    BoundaryList boundaries;
    boundaries.reserve(boundaries_array.size());

    for (const auto & element : boundaries_array)
    {
        if (element.isNull())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Boundary value for aggregate function {} cannot be NULL", name);

        validateBoundaryFieldType(element, name);

        if (element.getType() == Field::Types::Float64 && std::isnan(element.safeGet<Float64>()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Boundary value for aggregate function {} cannot be NaN", name);

        boundaries.push_back(element);
    }

    VectorWithMemoryTracking<SpecialValue> special_values;
    special_values.reserve(parameters.size() - 1);
    for (size_t i = 1; i < parameters.size(); ++i)
        special_values.push_back(parseSpecialValue(parameters[i], name));

    std::sort(special_values.begin(), special_values.end());

    if (const auto duplicate = std::adjacent_find(special_values.begin(), special_values.end()); duplicate != special_values.end())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Aggregate function {} contains duplicate special parameter '{}'",
            name, specialValueName(*duplicate));
    }

    Array canonical_parameters;
    canonical_parameters.reserve(1 + special_values.size());
    canonical_parameters.push_back(parameters[0]);
    for (const SpecialValue special : special_values)
        canonical_parameters.emplace_back(String(specialValueName(special)));

    return createHistogramExplicitTyped(
        value_type, argument_types, canonical_parameters, boundaries, boundary_type, std::move(special_values));
}

FunctionDocumentation makeHistogramExplicitDocumentation(const String & function_name, const String & interval_notation)
{
    const String max_boundaries = std::to_string(MAX_BOUNDARIES);

    return FunctionDocumentation{
        .description =
            "Computes a histogram over a numeric or Decimal column using explicit bucket boundaries. "
            "Intervals are " + interval_notation + ". "
            "Accepts up to " + max_boundaries + " boundaries. A NaN boundary is rejected with an exception. "
            "Boundaries that are +/-Inf, or that lie outside the range of an integer or Decimal argument type, are silently discarded. "
            "Boundaries that are not representable in an integer or Decimal argument type (for example 2.5 for Int32) "
            "are rounded in the direction that leaves the bucket membership of every value unchanged. "
            "The remaining boundaries are converted to the argument type (Float64 for Float32 and BFloat16 arguments), "
            "sorted, and deduplicated. "
            "The first and last buckets are always unbounded ([-inf, ...) and (..., +inf]), returning missing endpoints as `NULL`.\n\n"
            "Special values ('null', 'nan', 'zero') can optionally be tracked as separate counters. "
            "Repeating a special value is an error. "
            "When not requested, `NULL` and `NaN` are ignored, while zero is placed into its numeric bucket.",
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
                "Non-empty array of up to " + max_boundaries + " explicit bucket boundaries.",
                {"Array(Float64)", "Array(Int64)", "Array(UInt64)", "Array(Decimal*)"}
            },
            {
                "special_value1, special_value2, ...",
                "Optional variadic parameters to track special values: 'null', 'nan', or 'zero'. "
                "Each requested value appends a counter to the result tuple.",
                {"String"}
            },
        },
        .returned_value = {
            .description =
                "A tuple containing a `buckets` array of `(lower, upper, count)` tuples, followed by one `UInt64` counter per requested "
                "special value: `nan_count`, `null_count` and `zero_count`. The counters are always ordered alphabetically by the special "
                "value name, regardless of the order of the parameters. `lower` and `upper` have the argument type "
                "(Float64 for Float32 and BFloat16 arguments).",
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
                "([(NULL,0,1),(0,10,2),(10,20,2),(20,NULL,1)],1,1)"
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
    static constexpr AggregateFunctionProperties properties{.returns_default_when_only_null = true, .is_window_function = true};

    const auto register_variant = [&](const String & name, HistogramBoundary boundary_type, const String & interval_notation)
    {
        factory.registerFunction(
            name,
            AggregateFunctionWithProperties{
                [boundary_type](const String & function_name, const DataTypes & argument_types, const Array & parameters, const Settings *)
                { return createAggregateFunctionHistogramExplicit(function_name, argument_types, parameters, boundary_type); },
                makeHistogramExplicitDocumentation(name, interval_notation),
                properties});
    };

    register_variant(
        "histogramExplicit", HistogramBoundary::LEFT_CLOSED_RIGHT_OPEN, "left-closed, right-open: [a, b) (with inclusive infinite bounds)");
    register_variant(
        "histogramExplicitOpenClosed", HistogramBoundary::LEFT_OPEN_RIGHT_CLOSED, "left-open, right-closed: (a, b] (with inclusive infinite bounds)");
}

}
