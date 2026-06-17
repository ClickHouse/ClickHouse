#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionSumMapTyped.h>
#include <Functions/FunctionHelpers.h>


#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>

#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/FieldVisitorHash.h>
#include <Common/FieldVisitorSum.h>
#include <Common/HashTable/Hash.h>
#include <Common/NaNUtils.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Common/SetWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>

namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

namespace
{

struct FieldHash
{
    size_t operator()(const Field & x) const
    {
        const auto type = x.getType();
        switch (type)
        {
            case Field::Types::UInt64:
            case Field::Types::Bool:
                return intHash64(x.safeGet<UInt64>());
            case Field::Types::Int64:
                return intHash64(static_cast<UInt64>(x.safeGet<Int64>()));
            case Field::Types::Float64:
            {
                Float64 val = x.safeGet<Float64>();
                /// All NaN bit patterns are equal under Field::operator==
                /// (via FloatCompareHelper), so they must hash identically.
                if (std::isnan(val))
                    return intHash64(0x7FF8000000000000ULL);
                /// +0.0 and -0.0 compare equal, so they must hash the same.
                if (val == 0.0)
                    return intHash64(0);
                return intHash64(std::bit_cast<UInt64>(val));
            }
            case Field::Types::UInt128:
                return UInt128Hash()(x.safeGet<UInt128>());
            case Field::Types::UUID:
                return UInt128Hash()(x.safeGet<UUID>().toUnderType());
            case Field::Types::IPv4:
                return intHash64(x.safeGet<IPv4>().toUnderType());
            case Field::Types::IPv6:
                return UInt128Hash()(x.safeGet<IPv6>().toUnderType());
            case Field::Types::String:
            {
                const auto & s = x.safeGet<String>();
                return CityHash_v1_0_2::CityHash64(s.data(), s.size());
            }
            case Field::Types::Int128:
                return UInt128Hash()(std::bit_cast<UInt128>(x.safeGet<Int128>()));
            case Field::Types::UInt256:
                return UInt256Hash()(x.safeGet<UInt256>());
            case Field::Types::Int256:
                return UInt256Hash()(std::bit_cast<UInt256>(x.safeGet<Int256>()));
            default:
            {
                /// Decimals, compound types (Array, Tuple, Map, etc.) — use FieldVisitorHash.
                SipHash hash;
                applyVisitor(FieldVisitorHash(hash), x);
                return hash.get64();
            }
        }
    }
};

struct AggregateFunctionMapData
{
    UnorderedMapWithMemoryTracking<Field, Array, FieldHash> merged_maps;
};

/** Aggregate function, that takes at least two arguments: keys and values, and as a result, builds a tuple of at least 2 arrays -
  * ordered keys and variable number of argument values aggregated by corresponding keys.
  *
  * sumMap function is the most useful when using SummingMergeTree to sum Nested columns, which name ends in "Map".
  *
  * Example: sumMap(k, v...) of:
  *  k           v
  *  [1,2,3]     [10,10,10]
  *  [3,4,5]     [10,10,10]
  *  [4,5,6]     [10,10,10]
  *  [6,7,8]     [10,10,10]
  *  [7,5,3]     [5,15,25]
  *  [8,9,10]    [20,20,20]
  * will return:
  *  ([1,2,3,4,5,6,7,8,9,10],[10,10,45,20,35,20,15,30,20,20])
  *
  * minMap and maxMap share the same idea, but calculate min and max correspondingly.
  *
  * NOTE: The implementation of these functions are "amateur grade" - not efficient and low quality.
  */

template <typename Derived, typename Visitor, bool overflow, bool tuple_argument, bool compact>
class AggregateFunctionMapBase : public IAggregateFunctionDataHelper<
    AggregateFunctionMapData, Derived>
{
private:
    static constexpr auto STATE_VERSION_1_MIN_REVISION = SUMMAP_STATE_VERSION_1_MIN_REVISION;

    DataTypePtr keys_type;
    SerializationPtr keys_serialization;
    DataTypes values_types;
    Serializations values_serializations;
    Serializations promoted_values_serializations;

public:
    using Base = IAggregateFunctionDataHelper<AggregateFunctionMapData, Derived>;

    AggregateFunctionMapBase(const DataTypePtr & keys_type_,
            const DataTypes & values_types_, const DataTypes & argument_types_)
        : Base(argument_types_, {} /* parameters */, createResultType(keys_type_, values_types_))
        , keys_type(keys_type_)
        , keys_serialization(keys_type->getDefaultSerialization())
        , values_types(values_types_)
    {
        values_serializations.reserve(values_types.size());
        promoted_values_serializations.reserve(values_types.size());
        for (const auto & type : values_types)
        {
            values_serializations.emplace_back(type->getDefaultSerialization());
            if (type->canBePromoted())
            {
                if (type->isNullable())
                    promoted_values_serializations.emplace_back(
                         makeNullable(removeNullable(type)->promoteNumericType())->getDefaultSerialization());
                else
                    promoted_values_serializations.emplace_back(type->promoteNumericType()->getDefaultSerialization());
            }
            else
            {
                promoted_values_serializations.emplace_back(type->getDefaultSerialization());
            }
        }
    }

    bool isVersioned() const override { return true; }

    size_t getDefaultVersion() const override { return 1; }

    size_t getVersionFromRevision(size_t revision) const override
    {
        if (revision >= STATE_VERSION_1_MIN_REVISION)
            return 1;
        return 0;
    }

    static DataTypePtr createResultType(
        const DataTypePtr & keys_type_,
        const DataTypes & values_types_)
    {
        if constexpr (std::is_same_v<Visitor, FieldVisitorSum>)
        {
            for (const auto & value_type : values_types_)
            {
                if (!value_type->isSummable())
                    throw Exception{ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Values for -Map cannot be summed, passed type {}",
                        value_type->getName()};
            }
        }

        return createSumMapResultType(keys_type_, values_types_, overflow);
    }

    bool allocatesMemoryInArena() const override { return false; }

    static auto getArgumentColumns(const IColumn ** columns)
    {
        if constexpr (tuple_argument)
        {
            return assert_cast<const ColumnTuple *>(columns[0])->getColumns();
        }
        else
        {
            return columns;
        }
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns_, const size_t row_num, Arena *) const override
    {
        const auto & columns = getArgumentColumns(columns_);
        const size_t num_value_columns = values_types.size();

        // Column 0 contains array of keys of known type
        const ColumnArray & array_column0 = assert_cast<const ColumnArray &>(*columns[0]);
        const IColumn::Offsets & offsets0 = array_column0.getOffsets();
        const IColumn & key_column = array_column0.getData();
        const size_t keys_vec_offset = offsets0[row_num - 1];
        const size_t keys_vec_size = (offsets0[row_num] - keys_vec_offset);

        // Pre-extract value column data and validate sizes
        struct ValColRef { const IColumn * col; size_t offset; };
        ValColRef val_refs_buf[8];
        std::unique_ptr<ValColRef[]> val_refs_heap;
        ValColRef * val_refs = num_value_columns <= 8
            ? val_refs_buf
            : (val_refs_heap = std::make_unique<ValColRef[]>(num_value_columns)).get();

        for (size_t col = 0; col < num_value_columns; ++col)
        {
            const auto & array_column = assert_cast<const ColumnArray &>(*columns[col + 1]);
            const IColumn::Offsets & offsets = array_column.getOffsets();
            const size_t values_vec_offset = offsets[row_num - 1];
            const size_t values_vec_size = (offsets[row_num] - values_vec_offset);

            if (keys_vec_size != values_vec_size)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Sizes of keys and values arrays do not match");

            val_refs[col] = {&array_column.getData(), values_vec_offset};
        }

        // Outer loop over keys, inner loop over value columns.
        // Each key is looked up once instead of once per value column.
        auto & merged_maps = this->data(place).merged_maps;
        for (size_t i = 0; i < keys_vec_size; ++i)
        {
            Field key = key_column[keys_vec_offset + i];

            if (!keepKey(key))
                continue;

            auto [it, inserted] = merged_maps.emplace(key, Array());
            if (inserted)
                it->second.resize(num_value_columns);

            for (size_t col = 0; col < num_value_columns; ++col)
            {
                Field value = (*val_refs[col].col)[val_refs[col].offset + i];

                if (inserted)
                {
                    it->second[col] = value;
                }
                else
                {
                    if (!value.isNull())
                    {
                        if (it->second[col].isNull())
                            it->second[col] = value;
                        else
                            applyVisitor(Visitor(value), it->second[col]);
                    }
                }
            }
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & merged_maps = this->data(place).merged_maps;
        const auto & rhs_maps = this->data(rhs).merged_maps;

        for (const auto & elem : rhs_maps)
        {
            const auto & it = merged_maps.find(elem.first);
            if (it != merged_maps.end())
            {
                for (size_t col = 0; col < values_types.size(); ++col)
                {
                    if (!elem.second[col].isNull())
                    {
                        if (it->second[col].isNull())
                            it->second[col] = elem.second[col];
                        else
                            applyVisitor(Visitor(elem.second[col]), it->second[col]);
                    }
                }
            }
            else
            {
                merged_maps[elem.first] = elem.second;
            }
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        if (!version)
            version = getDefaultVersion();

        const auto & merged_maps = this->data(place).merged_maps;
        size_t size = merged_maps.size();
        writeVarUInt(size, buf);

        std::function<void(size_t, const Array &)> serialize;
        switch (*version)
        {
            case 0:
            {
                serialize = [&](size_t col_idx, const Array & values)
                {
                    values_serializations[col_idx]->serializeBinary(values[col_idx], buf, {});
                };
                break;
            }
            case 1:
            {
                serialize = [&](size_t col_idx, const Array & values)
                {
                    Field value = values[col_idx];

                    /// Compatibility with previous versions.
                    /// Peel off `Nullable` so the underlying `Decimal32`/`Decimal64` is recognised.
                    /// Null values pass through unchanged and are handled by `SerializationNullable`.
                    if (!value.isNull())
                    {
                        WhichDataType value_type(removeNullable(values_types[col_idx]));
                        if (value_type.isDecimal32())
                        {
                            auto source = value.safeGet<DecimalField<Decimal32>>();
                            value = DecimalField<Decimal128>(source.getValue(), source.getScale());
                        }
                        else if (value_type.isDecimal64())
                        {
                            auto source = value.safeGet<DecimalField<Decimal64>>();
                            value = DecimalField<Decimal128>(source.getValue(), source.getScale());
                        }
                    }

                    promoted_values_serializations[col_idx]->serializeBinary(value, buf, {});
                };
                break;
            }
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown version {}, of -Map aggregate function serialization state", *version);
        }

        /// Sort entries by key before serializing to produce a canonical byte
        /// representation, independent of hash-map iteration order.
        constexpr bool sort_serialize = true;

        using MapIter = typename std::decay_t<decltype(merged_maps)>::const_iterator;
        std::vector<MapIter> sorted; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        if constexpr (sort_serialize)
        {
            sorted.reserve(merged_maps.size());
            for (auto it = merged_maps.cbegin(); it != merged_maps.cend(); ++it)
                sorted.push_back(it);
            std::sort(sorted.begin(), sorted.end(),
                [](const MapIter & a, const MapIter & b) { return a->first < b->first; });
        }

        const auto serialize_entry = [&](const Field & key, const Array & values)
        {
            keys_serialization->serializeBinary(key, buf, {});
            for (size_t col = 0; col < values_types.size(); ++col)
                serialize(col, values);
        };

        if constexpr (sort_serialize)
        {
            for (const auto & it : sorted)
                serialize_entry(it->first, it->second);
        }
        else
        {
            for (const auto & elem : merged_maps)
                serialize_entry(elem.first, elem.second);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena *) const override
    {
        if (!version)
            version = getDefaultVersion();

        auto & merged_maps = this->data(place).merged_maps;
        size_t size = 0;
        readVarUInt(size, buf);

        FormatSettings format_settings;
        std::function<void(size_t, Array &)> deserialize;
        switch (*version)
        {
            case 0:
            {
                deserialize = [&](size_t col_idx, Array & values)
                {
                    values_serializations[col_idx]->deserializeBinary(values[col_idx], buf, format_settings);
                };
                break;
            }
            case 1:
            {
                deserialize = [&](size_t col_idx, Array & values)
                {
                    Field & value = values[col_idx];
                    promoted_values_serializations[col_idx]->deserializeBinary(value, buf, format_settings);

                    /// Compatibility with previous versions.
                    /// Peel off `Nullable` so the underlying `Decimal32`/`Decimal64` is recognised.
                    /// Null values come back as `Field::Null` and pass through unchanged.
                    if (value.getType() == Field::Types::Decimal128)
                    {
                        auto source = value.safeGet<DecimalField<Decimal128>>();
                        WhichDataType value_type(removeNullable(values_types[col_idx]));
                        if (value_type.isDecimal32())
                        {
                            value = DecimalField<Decimal32>(source.getValue(), source.getScale());
                        }
                        else if (value_type.isDecimal64())
                        {
                            value = DecimalField<Decimal64>(source.getValue(), source.getScale());
                        }
                    }
                };
                break;
            }
            default:
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected version {} of -Map aggregate function serialization state", *version);
        }

        for (size_t i = 0; i < size; ++i)
        {
            Field key;
            keys_serialization->deserializeBinary(key, buf, format_settings);

            Array values;
            values.resize(values_types.size());

            for (size_t col = 0; col < values_types.size(); ++col)
                deserialize(col, values);

            merged_maps[key] = values;
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const size_t num_columns = values_types.size();
        const auto & merged_maps = this->data(place).merged_maps;

        using MapIter = typename std::decay_t<decltype(merged_maps)>::const_iterator;

        // Collect iterators, skipping compacted entries without mutating state
        std::vector<MapIter> sorted; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        sorted.reserve(merged_maps.size());

        for (auto it = merged_maps.cbegin(); it != merged_maps.cend(); ++it)
        {
            if constexpr (compact)
            {
                bool all_identity = true;
                for (size_t col = 0; col < num_columns; ++col)
                {
                    if (!it->second[col].isNull() && it->second[col] != values_types[col]->getDefault())
                    {
                        all_identity = false;
                        break;
                    }
                }
                if (all_identity)
                    continue;
            }
            sorted.push_back(it);
        }

        // Sort by key to maintain the documented sorted-output property
        std::sort(sorted.begin(), sorted.end(),
            [](const MapIter & a, const MapIter & b) { return a->first < b->first; }); // NOLINT(clang-analyzer-cplusplus.Move)

        const size_t result_size = sorted.size();

        auto & to_tuple = assert_cast<ColumnTuple &>(to);
        auto & to_keys_arr = assert_cast<ColumnArray &>(to_tuple.getColumn(0));
        auto & to_keys_col = to_keys_arr.getData();

        auto & to_keys_offsets = to_keys_arr.getOffsets();
        to_keys_offsets.push_back(to_keys_offsets.back() + result_size);
        to_keys_col.reserve(result_size);

        for (size_t col = 0; col < num_columns; ++col)
        {
            auto & to_values_arr = assert_cast<ColumnArray &>(to_tuple.getColumn(col + 1));
            auto & to_values_offsets = to_values_arr.getOffsets();
            to_values_offsets.push_back(to_values_offsets.back() + result_size);
            to_values_arr.getData().reserve(result_size);
        }

        for (const auto & it : sorted)
        {
            to_keys_col.insert(it->first);

            for (size_t col = 0; col < num_columns; ++col)
            {
                auto & to_values_col = assert_cast<ColumnArray &>(to_tuple.getColumn(col + 1)).getData();
                if (it->second[col].isNull())
                    to_values_col.insertDefault();
                else
                    to_values_col.insert(it->second[col]);
            }
        }
    }

    bool keepKey(const Field & key) const { return static_cast<const Derived &>(*this).keepKey(key); }
    String getName() const override { return Derived::getNameImpl(); }
};

template <bool overflow, bool tuple_argument>
class AggregateFunctionSumMap final :
    public AggregateFunctionMapBase<AggregateFunctionSumMap<overflow, tuple_argument>, FieldVisitorSum, overflow, tuple_argument, true>
{
private:
    using Self = AggregateFunctionSumMap<overflow, tuple_argument>;
    using Base = AggregateFunctionMapBase<Self, FieldVisitorSum, overflow, tuple_argument, true>;

public:
    AggregateFunctionSumMap(const DataTypePtr & keys_type_,
            DataTypes & values_types_, const DataTypes & argument_types_,
            const Array & params_)
        : Base{keys_type_, values_types_, argument_types_}
    {
        // The constructor accepts parameters to have a uniform interface with
        // sumMapFiltered, but this function doesn't have any parameters.
        assertNoParameters(getNameImpl(), params_);
    }

    static String getNameImpl()
    {
        if constexpr (overflow)
        {
            return "sumMapWithOverflow";
        }
        else
        {
            return "sumMap";
        }
    }

    bool keepKey(const Field &) const { return true; }
};


template <bool overflow, bool tuple_argument>
class AggregateFunctionSumMapFiltered final :
    public AggregateFunctionMapBase<
        AggregateFunctionSumMapFiltered<overflow, tuple_argument>,
        FieldVisitorSum,
        overflow,
        tuple_argument,
        true>
{
private:
    using Self = AggregateFunctionSumMapFiltered<overflow, tuple_argument>;
    using Base = AggregateFunctionMapBase<Self, FieldVisitorSum, overflow, tuple_argument, true>;

    using ContainerT = SetWithMemoryTracking<Field>;
    ContainerT keys_to_keep;

public:
    AggregateFunctionSumMapFiltered(const DataTypePtr & keys_type_,
            const DataTypes & values_types_, const DataTypes & argument_types_,
            const Array & params_)
        : Base{keys_type_, values_types_, argument_types_}
    {
        if (params_.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Aggregate function '{}' requires exactly one parameter "
                "of Array type", getNameImpl());

        Array keys_to_keep_values;
        if (!params_.front().tryGet<Array>(keys_to_keep_values))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Aggregate function {} requires an Array as a parameter",
                getNameImpl());

        this->parameters = params_;

        for (const Field & f : keys_to_keep_values)
            keys_to_keep.emplace(f);
    }

    static String getNameImpl()
    {
        if constexpr (overflow)
        {
            return "sumMapFilteredWithOverflow";
        }
        else
        {
            return "sumMapFiltered";
        }
    }

    bool keepKey(const Field & key) const
    {
        if (keys_to_keep.contains(key))
            return true;

        // Determine whether the numerical value of the key can have both types (UInt or Int),
        // and use the other type with the same numerical value for keepKey verification.
        if (key.getType() == Field::Types::UInt64)
        {
            const auto & value = key.safeGet<UInt64>();
            if (value <= std::numeric_limits<Int64>::max())
                return keys_to_keep.contains(Field(Int64(value)));
        }
        else if (key.getType() == Field::Types::Int64)
        {
            const auto & value = key.safeGet<Int64>();
            if (value >= 0)
                return keys_to_keep.contains(Field(UInt64(value)));
        }

        return false;
    }
};


/** Implements `Max` operation.
 *  Returns true if changed
 */
class FieldVisitorMax : public StaticVisitor<bool>
{
private:
    const Field & rhs;

    template <typename FieldType>
    bool compareImpl(FieldType & x) const
    {
        auto val = rhs.safeGet<FieldType>();
        if constexpr (is_floating_point<FieldType>)
        {
            /// Match `max` semantics: NaN is treated as last (i.e. smaller than
            /// any non-NaN value), so it is only kept when every observed value
            /// is NaN. See #100448.
            if (isNaN(val))
                return false;
            if (isNaN(x))
            {
                x = val;
                return true;
            }
        }
        if (val > x)
        {
            x = val;
            return true;
        }

        return false;
    }

public:
    explicit FieldVisitorMax(const Field & rhs_) : rhs(rhs_) {}

    bool operator() (Null &) const
    {
        /// Do not update current value, skip nulls
        return false;
    }

    bool operator() (AggregateFunctionStateData &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot compare AggregateFunctionStates"); }

    bool operator() (Array & x) const { return compareImpl<Array>(x); }
    bool operator() (Tuple & x) const { return compareImpl<Tuple>(x); }
    template <typename T>
    bool operator() (DecimalField<T> & x) const { return compareImpl<DecimalField<T>>(x); }
    template <typename T>
    bool operator() (T & x) const { return compareImpl<T>(x); }
};

/** Implements `Min` operation.
 *  Returns true if changed
 */
class FieldVisitorMin : public StaticVisitor<bool>
{
private:
    const Field & rhs;

    template <typename FieldType>
    bool compareImpl(FieldType & x) const
    {
        auto val = rhs.safeGet<FieldType>();
        if constexpr (is_floating_point<FieldType>)
        {
            /// Match `min` semantics: NaN is treated as last (i.e. larger than
            /// any non-NaN value), so it is only kept when every observed value
            /// is NaN. See #100448.
            if (isNaN(val))
                return false;
            if (isNaN(x))
            {
                x = val;
                return true;
            }
        }
        if (val < x)
        {
            x = val;
            return true;
        }

        return false;
    }

public:
    explicit FieldVisitorMin(const Field & rhs_) : rhs(rhs_) {}


    bool operator() (Null &) const
    {
        /// Do not update current value, skip nulls
        return false;
    }

    bool operator() (AggregateFunctionStateData &) const { throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot sum AggregateFunctionStates"); }

    bool operator() (Array & x) const { return compareImpl<Array>(x); }
    bool operator() (Tuple & x) const { return compareImpl<Tuple>(x); }
    template <typename T>
    bool operator() (DecimalField<T> & x) const { return compareImpl<DecimalField<T>>(x); }
    template <typename T>
    bool operator() (T & x) const { return compareImpl<T>(x); }
};


template <bool tuple_argument>
class AggregateFunctionMinMap final :
    public AggregateFunctionMapBase<AggregateFunctionMinMap<tuple_argument>, FieldVisitorMin, true, tuple_argument, false>
{
private:
    using Self = AggregateFunctionMinMap<tuple_argument>;
    using Base = AggregateFunctionMapBase<Self, FieldVisitorMin, true, tuple_argument, false>;

public:
    AggregateFunctionMinMap(const DataTypePtr & keys_type_,
            DataTypes & values_types_, const DataTypes & argument_types_,
            const Array & params_)
        : Base{keys_type_, values_types_, argument_types_}
    {
        // The constructor accepts parameters to have a uniform interface with
        // sumMapFiltered, but this function doesn't have any parameters.
        assertNoParameters(getNameImpl(), params_);
    }

    static String getNameImpl() { return "minMap"; }

    bool keepKey(const Field &) const { return true; }
};

template <bool tuple_argument>
class AggregateFunctionMaxMap final :
    public AggregateFunctionMapBase<AggregateFunctionMaxMap<tuple_argument>, FieldVisitorMax, true, tuple_argument, false>
{
private:
    using Self = AggregateFunctionMaxMap<tuple_argument>;
    using Base = AggregateFunctionMapBase<Self, FieldVisitorMax, true, tuple_argument, false>;

public:
    AggregateFunctionMaxMap(const DataTypePtr & keys_type_,
            DataTypes & values_types_, const DataTypes & argument_types_,
            const Array & params_)
        : Base{keys_type_, values_types_, argument_types_}
    {
        // The constructor accepts parameters to have a uniform interface with
        // sumMapFiltered, but this function doesn't have any parameters.
        assertNoParameters(getNameImpl(), params_);
    }

    static String getNameImpl() { return "maxMap"; }

    bool keepKey(const Field &) const { return true; }
};


auto parseArguments(const std::string & name, const DataTypes & arguments)
{
    DataTypes args;
    bool tuple_argument = false;

    if (arguments.size() == 1)
    {
        // sumMap state is fully given by its result, so it can be stored in
        // SimpleAggregateFunction columns. There is a caveat: it must support
        // sumMap(sumMap(...)), e.g. it must be able to accept its own output as
        // an input. This is why it also accepts a Tuple(keys, values) argument.
        const auto * tuple_type = checkAndGetDataType<DataTypeTuple>(arguments[0].get());
        if (!tuple_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "When function {} gets one argument it must be a tuple", name);

        const auto elems = tuple_type->getElements();
        args.insert(args.end(), elems.begin(), elems.end());
        tuple_argument = true;
    }
    else
    {
        args.insert(args.end(), arguments.begin(), arguments.end());
        tuple_argument = false;
    }

    if (args.size() < 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} requires at least two arguments of Array type or one argument of tuple of two arrays", name);

    const auto * array_type = checkAndGetDataType<DataTypeArray>(args[0].get());
    if (!array_type)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #1 for function {} must be an array, not {}",
            name, args[0]->getName());

    DataTypePtr keys_type = array_type->getNestedType();

    DataTypes values_types;
    values_types.reserve(args.size() - 1);
    for (size_t i = 1; i < args.size(); ++i)
    {
        array_type = checkAndGetDataType<DataTypeArray>(args[i].get());
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument #{} for function {} must be an array, not {}",
                i + 1, name, args[i]->getName());
        values_types.push_back(array_type->getNestedType());
    }

    return std::tuple<DataTypePtr, DataTypes, bool>{std::move(keys_type), std::move(values_types), tuple_argument};
}

}

void registerAggregateFunctionSumMap(AggregateFunctionFactory & factory);
void registerAggregateFunctionSumMap(AggregateFunctionFactory & factory)
{
    // these functions used to be called *Map, with now these names occupied by
    // Map combinator, which redirects calls here if was called with
    // array or tuple arguments.
    FunctionDocumentation::Description sumMappedArrays_description = R"(
Totals one or more `value` arrays according to the keys specified in the `key` array. Returns a tuple of arrays: keys in sorted order, followed by values summed for the corresponding keys without overflow.

:::note
- Passing a tuple of keys and value arrays is identical to passing an array of keys and an array of values.
- The number of elements in `key` and all `value` arrays must be the same for each row that is totaled.
:::
    )";
    FunctionDocumentation::Syntax sumMappedArrays_syntax = R"(
sumMappedArrays(key, value1 [, value2, ...])
sumMappedArrays(Tuple(key, value1 [, value2, ...]))
    )";
    FunctionDocumentation::Arguments sumMappedArrays_arguments = {
        {"key", "Array of keys.", {"Array"}},
        {"value1, value2, ...", "Arrays of values to sum for each key.", {"Array"}}
    };
    FunctionDocumentation::ReturnedValue sumMappedArrays_returned_value = {"Returns a tuple of arrays: the first array contains keys in sorted order, followed by arrays containing values summed for the corresponding keys.", {"Tuple"}};
    FunctionDocumentation::Examples sumMappedArrays_examples = {
    {
        "Basic usage with Nested type",
        R"(
CREATE TABLE sum_map(
    date Date,
    timeslot DateTime,
    statusMap Nested(
        status UInt16,
        requests UInt64
    ),
    statusMapTuple Tuple(Array(Int32), Array(Int32))
) ENGINE = Memory;

INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10], ([1, 2, 3], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10], ([3, 4, 5], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10], ([4, 5, 6], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10], ([6, 7, 8], [10, 10, 10]));

SELECT
    timeslot,
    sumMappedArrays(statusMap.status, statusMap.requests),
    sumMappedArrays(statusMapTuple)
FROM sum_map
GROUP BY timeslot;
        )",
        R"(
┌────────────timeslot─┬─sumMappedArrays(statusMap.status, statusMap.requests)─┬─sumMappedArrays(statusMapTuple)─────────┐
│ 2000-01-01 00:00:00 │ ([1,2,3,4,5],[10,10,20,10,10])                        │ ([1,2,3,4,5],[10,10,20,10,10])          │
│ 2000-01-01 00:01:00 │ ([4,5,6,7,8],[10,10,20,10,10])                        │ ([4,5,6,7,8],[10,10,20,10,10])          │
└─────────────────────┴───────────────────────────────────────────────────────┴─────────────────────────────────────────┘
        )"
    },
    {
        "Multiple value arrays example",
        R"(
CREATE TABLE multi_metrics(
    date Date,
    browser_metrics Nested(
        browser String,
        impressions UInt32,
        clicks UInt32
    )
)
ENGINE = Memory;

INSERT INTO multi_metrics VALUES
    ('2000-01-01', ['Firefox', 'Chrome'], [100, 200], [10, 25]),
    ('2000-01-01', ['Chrome', 'Safari'], [150, 50], [20, 5]),
    ('2000-01-01', ['Firefox', 'Edge'], [80, 40], [8, 4]);

SELECT
    sumMappedArrays(browser_metrics.browser, browser_metrics.impressions, browser_metrics.clicks) AS result
FROM multi_metrics;
        )",
        R"(
┌─result────────────────────────────────────────────────────────────────────────┐
│ (['Chrome', 'Edge', 'Firefox', 'Safari'], [350, 40, 180, 50], [45, 4, 18, 5]) │
└───────────────────────────────────────────────────────────────────────────────┘
-- In this example:
-- The result tuple contains three arrays
-- First array: keys (browser names) in sorted order
-- Second array: total impressions for each browser
-- Third array: total clicks for each browser
        )"
    }
    };
    FunctionDocumentation::IntroducedIn sumMappedArrays_introduced_in = {1, 1};
    FunctionDocumentation::Category sumMappedArrays_category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation sumMappedArrays_documentation = {sumMappedArrays_description, sumMappedArrays_syntax, sumMappedArrays_arguments, {}, sumMappedArrays_returned_value, sumMappedArrays_examples, sumMappedArrays_introduced_in, sumMappedArrays_category};

    factory.registerFunction("sumMappedArrays", {[](const std::string & name, const DataTypes & arguments, const Array & params, const Settings *) -> AggregateFunctionPtr
    {
        assertNoParameters(name, params);
        auto [keys_type, values_types, tuple_argument] = parseArguments(name, arguments);

        if (isSumMapTypedKeySupported(keys_type->getTypeId()) && areSumMapTypedValuesSupported(values_types)
            && std::all_of(values_types.begin(), values_types.end(), [](const auto & t) { return t->isSummable(); }))
        {
            AggregateFunctionPtr res = createSumMapTyped(keys_type, values_types, arguments, false, tuple_argument, SumMapOp::Sum);
            if (res)
                return res;
        }

        if (tuple_argument)
            return std::make_shared<AggregateFunctionSumMap<false, true>>(keys_type, values_types, arguments, params);
        return std::make_shared<AggregateFunctionSumMap<false, false>>(keys_type, values_types, arguments, params);
    }, sumMappedArrays_documentation});

    FunctionDocumentation::Description minMappedArrays_description = R"(
Calculates the minimum from `value` array according to the keys specified in the `key` array.

:::note
- Passing a tuple of keys and value arrays is identical to passing an array of keys and an array of values.
- The number of elements in `key` and `value` must be the same for each row that is totaled.
:::
    )";
    FunctionDocumentation::Syntax minMappedArrays_syntax = R"(
minMappedArrays(key, value)
minMappedArrays(Tuple(key, value))
    )";
    FunctionDocumentation::Arguments minMappedArrays_arguments = {
        {"key", "Array of keys.", {"Array(T)"}},
        {"value", "Array of values.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue minMappedArrays_returned_value = {"Returns a tuple of two arrays: keys in sorted order, and values calculated for the corresponding keys.", {"Tuple(Array(T), Array(T))"}};
    FunctionDocumentation::Examples minMappedArrays_examples = {
    {
        "Usage example",
        R"(
SELECT minMappedArrays(a, b)
FROM VALUES('a Array(Int32), b Array(Int64)', ([1, 2], [2, 2]), ([2, 3], [1, 1]));
        )",
        R"(
┌─minMappedArrays(a, b)───────────┐
│ ([1, 2, 3], [2, 1, 1])          │
└─────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn minMappedArrays_introduced_in = {20, 5};
    FunctionDocumentation::Category minMappedArrays_category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation minMappedArrays_documentation = {minMappedArrays_description, minMappedArrays_syntax, minMappedArrays_arguments, {}, minMappedArrays_returned_value, minMappedArrays_examples, minMappedArrays_introduced_in, minMappedArrays_category};

    factory.registerFunction("minMappedArrays", {[](const std::string & name, const DataTypes & arguments, const Array & params, const Settings *) -> AggregateFunctionPtr
    {
        assertNoParameters(name, params);
        auto [keys_type, values_types, tuple_argument] = parseArguments(name, arguments);

        if (isSumMapTypedKeySupported(keys_type->getTypeId()) && areSumMapTypedValuesSupported(values_types))
        {
            AggregateFunctionPtr res = createSumMapTyped(keys_type, values_types, arguments, true, tuple_argument, SumMapOp::Min);
            if (res)
                return res;
        }

        if (tuple_argument)
            return std::make_shared<AggregateFunctionMinMap<true>>(keys_type, values_types, arguments, params);
        return std::make_shared<AggregateFunctionMinMap<false>>(keys_type, values_types, arguments, params);
    }, minMappedArrays_documentation, {}});

    FunctionDocumentation::Description maxMappedArrays_description = R"(
Calculates the maximum from `value` array according to the keys specified in the `key` array.

:::note
- Passing a tuple of keys and value arrays is identical to passing an array of keys and an array of values.
- The number of elements in `key` and `value` must be the same for each row that is totaled.
:::
    )";
    FunctionDocumentation::Syntax maxMappedArrays_syntax = R"(
maxMappedArrays(key, value)
maxMappedArrays(Tuple(key, value))
    )";
    FunctionDocumentation::Arguments maxMappedArrays_arguments = {
        {"key", "Array of keys.", {"Array(T)"}},
        {"value", "Array of values.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue maxMappedArrays_returned_value = {"Returns a tuple of two arrays: keys in sorted order, and values calculated for the corresponding keys.", {"Tuple(Array(T), Array(T))"}};
    FunctionDocumentation::Examples maxMappedArrays_examples = {
    {
        "Usage example",
        R"(
SELECT maxMappedArrays(a, b)
FROM VALUES('a Array(Char), b Array(Int64)', (['x', 'y'], [2, 2]), (['y', 'z'], [3, 1]));
        )",
        R"(
┌─maxMappedArrays(a, b)───┐
│ (['x','y','z'],[2,3,1]) │
└─────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn maxMappedArrays_introduced_in = {20, 5};
    FunctionDocumentation::Category maxMappedArrays_category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation maxMappedArrays_documentation = {maxMappedArrays_description, maxMappedArrays_syntax, maxMappedArrays_arguments, {}, maxMappedArrays_returned_value, maxMappedArrays_examples, maxMappedArrays_introduced_in, maxMappedArrays_category};

    factory.registerFunction("maxMappedArrays", {[](const std::string & name, const DataTypes & arguments, const Array & params, const Settings *) -> AggregateFunctionPtr
    {
        assertNoParameters(name, params);
        auto [keys_type, values_types, tuple_argument] = parseArguments(name, arguments);

        if (isSumMapTypedKeySupported(keys_type->getTypeId()) && areSumMapTypedValuesSupported(values_types))
        {
            AggregateFunctionPtr res = createSumMapTyped(keys_type, values_types, arguments, true, tuple_argument, SumMapOp::Max);
            if (res)
                return res;
        }

        if (tuple_argument)
            return std::make_shared<AggregateFunctionMaxMap<true>>(keys_type, values_types, arguments, params);
        return std::make_shared<AggregateFunctionMaxMap<false>>(keys_type, values_types, arguments, params);
    }, maxMappedArrays_documentation});

    // these functions could be renamed to *MappedArrays too, but it would
    // break backward compatibility
    FunctionDocumentation::Description sumMapWithOverflow_description = R"(
Totals a `value` array according to the keys specified in the `key` array. Returns a tuple of two arrays: keys in sorted order, and values summed for the corresponding keys.
It differs from the [`sumMap`](/sql-reference/aggregate-functions/reference/summap) function in that it does summation with overflow - i.e. returns the same data type for the summation as the argument data type.

:::note
- Passing a tuple of key and value arrays is identical to passing an array of keys and an array of values.
- The number of elements in `key` and `value` must be the same for each row that is totaled.
:::
    )";
    FunctionDocumentation::Syntax sumMapWithOverflow_syntax = R"(
sumMapWithOverflow(key, value)
sumMapWithOverflow(Tuple(key, value))
    )";
    FunctionDocumentation::Arguments sumMapWithOverflow_arguments = {
        {"key", "Array of keys.", {"Array"}},
        {"value", "Array of values.", {"Array"}}
    };
    FunctionDocumentation::ReturnedValue sumMapWithOverflow_returned_value = {"Returns a tuple of two arrays: keys in sorted order, and values summed for the corresponding keys.", {"Tuple(Array, Array)"}};
    FunctionDocumentation::Examples sumMapWithOverflow_examples = {
    {
        "Array syntax demonstrating overflow behavior",
        R"(
CREATE TABLE sum_map(
    date Date,
    timeslot DateTime,
    statusMap Nested(
        status UInt8,
        requests UInt8
    ),
    statusMapTuple Tuple(Array(Int8), Array(Int8))
) ENGINE = Memory;

INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10], ([1, 2, 3], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10], ([3, 4, 5], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10], ([4, 5, 6], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10], ([6, 7, 8], [10, 10, 10]));

SELECT
    timeslot,
    toTypeName(sumMap(statusMap.status, statusMap.requests)),
    toTypeName(sumMapWithOverflow(statusMap.status, statusMap.requests))
FROM sum_map
GROUP BY timeslot;
        )",
        R"(
┌────────────timeslot─┬─toTypeName(sumMap⋯usMap.requests))─┬─toTypeName(sumMa⋯usMap.requests))─┐
│ 2000-01-01 00:01:00 │ Tuple(Array(UInt8), Array(UInt64)) │ Tuple(Array(UInt8), Array(UInt8)) │
│ 2000-01-01 00:00:00 │ Tuple(Array(UInt8), Array(UInt64)) │ Tuple(Array(UInt8), Array(UInt8)) │
└─────────────────────┴────────────────────────────────────┴───────────────────────────────────┘
        )"
    },
    {
        "Tuple syntax with same result",
        R"(
SELECT
    timeslot,
    toTypeName(sumMap(statusMapTuple)),
    toTypeName(sumMapWithOverflow(statusMapTuple))
FROM sum_map
GROUP BY timeslot;
        )",
        R"(
┌────────────timeslot─┬─toTypeName(sumMap(statusMapTuple))─┬─toTypeName(sumM⋯tatusMapTuple))─┐
│ 2000-01-01 00:01:00 │ Tuple(Array(Int8), Array(Int64))   │ Tuple(Array(Int8), Array(Int8)) │
│ 2000-01-01 00:00:00 │ Tuple(Array(Int8), Array(Int64))   │ Tuple(Array(Int8), Array(Int8)) │
└─────────────────────┴────────────────────────────────────┴─────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn sumMapWithOverflow_introduced_in = {20, 1};
    FunctionDocumentation::Category sumMapWithOverflow_category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation sumMapWithOverflow_documentation = {sumMapWithOverflow_description, sumMapWithOverflow_syntax, sumMapWithOverflow_arguments, {}, sumMapWithOverflow_returned_value, sumMapWithOverflow_examples, sumMapWithOverflow_introduced_in, sumMapWithOverflow_category};

    factory.registerFunction("sumMapWithOverflow", {[](const std::string & name, const DataTypes & arguments, const Array & params, const Settings *) -> AggregateFunctionPtr
    {
        assertNoParameters(name, params);
        auto [keys_type, values_types, tuple_argument] = parseArguments(name, arguments);

        if (isSumMapTypedKeySupported(keys_type->getTypeId()) && areSumMapTypedValuesSupported(values_types)
            && std::all_of(values_types.begin(), values_types.end(), [](const auto & t) { return t->isSummable(); }))
        {
            AggregateFunctionPtr res = createSumMapTyped(keys_type, values_types, arguments, true, tuple_argument, SumMapOp::Sum);
            if (res)
                return res;
        }

        if (tuple_argument)
            return std::make_shared<AggregateFunctionSumMap<true, true>>(keys_type, values_types, arguments, params);
        return std::make_shared<AggregateFunctionSumMap<true, false>>(keys_type, values_types, arguments, params);
    }, sumMapWithOverflow_documentation});

    factory.registerFunction("sumMapFiltered", {[](const std::string & name, const DataTypes & arguments, const Array & params, const Settings *) -> AggregateFunctionPtr
    {
        auto [keys_type, values_types, tuple_argument] = parseArguments(name, arguments);

        Array keys_filter;
        if (params.size() == 1 && params.front().tryGet<Array>(keys_filter) && !keys_filter.empty()
            && isSumMapTypedKeySupported(keys_type->getTypeId()) && areSumMapTypedValuesSupported(values_types)
            && std::all_of(values_types.begin(), values_types.end(), [](const auto & t) { return t->isSummable(); }))
        {
            AggregateFunctionPtr res = createSumMapTyped(
                keys_type, values_types, arguments, false, tuple_argument, SumMapOp::Sum, keys_filter);
            if (res)
                return res;
        }

        if (tuple_argument)
            return std::make_shared<AggregateFunctionSumMapFiltered<false, true>>(keys_type, values_types, arguments, params);
        return std::make_shared<AggregateFunctionSumMapFiltered<false, false>>(keys_type, values_types, arguments, params);
    }, {}});

    factory.registerFunction("sumMapFilteredWithOverflow", {[](const std::string & name, const DataTypes & arguments, const Array & params, const Settings *) -> AggregateFunctionPtr
    {
        auto [keys_type, values_types, tuple_argument] = parseArguments(name, arguments);

        Array keys_filter;
        if (params.size() == 1 && params.front().tryGet<Array>(keys_filter) && !keys_filter.empty()
            && isSumMapTypedKeySupported(keys_type->getTypeId()) && areSumMapTypedValuesSupported(values_types)
            && std::all_of(values_types.begin(), values_types.end(), [](const auto & t) { return t->isSummable(); }))
        {
            AggregateFunctionPtr res = createSumMapTyped(
                keys_type, values_types, arguments, true, tuple_argument, SumMapOp::Sum, keys_filter);
            if (res)
                return res;
        }

        if (tuple_argument)
            return std::make_shared<AggregateFunctionSumMapFiltered<true, true>>(keys_type, values_types, arguments, params);
        return std::make_shared<AggregateFunctionSumMapFiltered<true, false>>(keys_type, values_types, arguments, params);
    }, {}});
}

}
