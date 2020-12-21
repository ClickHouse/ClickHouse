#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>

#include <Common/FieldVisitors.h>
#include <Common/assert_cast.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <map>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

template <typename T>
struct AggregateFunctionMapData
{
    // Map needs to be ordered to maintain function properties
    std::map<T, Array> merged_maps;
};

/** Aggregate function, that takes at least two arguments: keys and values, and as a result, builds a tuple of of at least 2 arrays -
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
  */

template <typename T, typename Derived, typename Visitor, bool overflow, bool tuple_argument, bool compact>
class AggregateFunctionMapBase : public IAggregateFunctionDataHelper<
    AggregateFunctionMapData<NearestFieldType<T>>, Derived>
{
private:
    DataTypePtr keys_type;
    DataTypes values_types;

public:
    using Base = IAggregateFunctionDataHelper<
        AggregateFunctionMapData<NearestFieldType<T>>, Derived>;

    AggregateFunctionMapBase(const DataTypePtr & keys_type_,
            const DataTypes & values_types_, const DataTypes & argument_types_)
        : Base(argument_types_, {} /* parameters */), keys_type(keys_type_),
          values_types(values_types_)
    {}

    DataTypePtr getReturnType() const override
    {
        DataTypes types;
        types.emplace_back(std::make_shared<DataTypeArray>(keys_type));

        for (const auto & value_type : values_types)
        {
            DataTypePtr result_type;

            if constexpr (overflow)
            {
                // Overflow, meaning that the returned type is the same as
                // the input type.
                result_type = value_type;
            }
            else
            {
                auto value_type_without_nullable = removeNullable(value_type);

                // No overflow, meaning we promote the types if necessary.
                if (!value_type_without_nullable->canBePromoted())
                    throw Exception{ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Values for {} are expected to be Numeric, Float or Decimal, passed type {}",
                        getName(), value_type->getName()};

                result_type = value_type_without_nullable->promoteNumericType();
            }

            types.emplace_back(std::make_shared<DataTypeArray>(result_type));
        }

        return std::make_shared<DataTypeTuple>(types);
    }

    static const auto & getArgumentColumns(const IColumn**& columns)
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

    void add(AggregateDataPtr place, const IColumn** _columns, const size_t row_num, Arena *) const override
    {
        const auto & columns = getArgumentColumns(_columns);

        // Column 0 contains array of keys of known type
        const ColumnArray & array_column0 = assert_cast<const ColumnArray &>(*columns[0]);
        const IColumn::Offsets & offsets0 = array_column0.getOffsets();
        const IColumn & key_column = array_column0.getData();
        const size_t keys_vec_offset = offsets0[row_num - 1];
        const size_t keys_vec_size = (offsets0[row_num] - keys_vec_offset);

        // Columns 1..n contain arrays of numeric values to sum
        auto & merged_maps = this->data(place).merged_maps;
        for (size_t col = 0, size = values_types.size(); col < size; ++col)
        {
            const auto & array_column = assert_cast<const ColumnArray&>(*columns[col + 1]);
            const IColumn & value_column = array_column.getData();
            const IColumn::Offsets & offsets = array_column.getOffsets();
            const size_t values_vec_offset = offsets[row_num - 1];
            const size_t values_vec_size = (offsets[row_num] - values_vec_offset);

            // Expect key and value arrays to be of same length
            if (keys_vec_size != values_vec_size)
                throw Exception("Sizes of keys and values arrays do not match", ErrorCodes::BAD_ARGUMENTS);

            // Insert column values for all keys
            for (size_t i = 0; i < keys_vec_size; ++i)
            {
                auto value = value_column.operator[](values_vec_offset + i);
                auto key = key_column.operator[](keys_vec_offset + i).get<T>();

                if (!keepKey(key))
                    continue;

                if (value.isNull())
                    continue;

                typename std::decay_t<decltype(merged_maps)>::iterator it;
                if constexpr (IsDecimalNumber<T>)
                {
                    // FIXME why is storing NearestFieldType not enough, and we
                    // have to check for decimals again here?
                    UInt32 scale = static_cast<const ColumnDecimal<T> &>(key_column).getData().getScale();
                    it = merged_maps.find(DecimalField<T>(key, scale));
                }
                else
                    it = merged_maps.find(key);

                if (it != merged_maps.end())
                {
                    applyVisitor(Visitor(value), it->second[col]);
                }
                else
                {
                    // Create a value array for this key
                    Array new_values;
                    new_values.resize(values_types.size());
                    for (size_t k = 0; k < new_values.size(); ++k)
                    {
                        new_values[k] = (k == col) ? value : values_types[k]->getDefault();
                    }

                    if constexpr (IsDecimalNumber<T>)
                    {
                        UInt32 scale = static_cast<const ColumnDecimal<T> &>(key_column).getData().getScale();
                        merged_maps.emplace(DecimalField<T>(key, scale), std::move(new_values));
                    }
                    else
                    {
                        merged_maps.emplace(key, std::move(new_values));
                    }
                }
            }
        }
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & merged_maps = this->data(place).merged_maps;
        const auto & rhs_maps = this->data(rhs).merged_maps;

        for (const auto & elem : rhs_maps)
        {
            const auto & it = merged_maps.find(elem.first);
            if (it != merged_maps.end())
            {
                for (size_t col = 0; col < values_types.size(); ++col)
                    applyVisitor(Visitor(elem.second[col]), it->second[col]);
            }
            else
                merged_maps[elem.first] = elem.second;
        }
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        const auto & merged_maps = this->data(place).merged_maps;
        size_t size = merged_maps.size();
        writeVarUInt(size, buf);

        for (const auto & elem : merged_maps)
        {
            keys_type->serializeBinary(elem.first, buf);
            for (size_t col = 0; col < values_types.size(); ++col)
                values_types[col]->serializeBinary(elem.second[col], buf);
        }
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        auto & merged_maps = this->data(place).merged_maps;
        size_t size = 0;
        readVarUInt(size, buf);

        for (size_t i = 0; i < size; ++i)
        {
            Field key;
            keys_type->deserializeBinary(key, buf);

            Array values;
            values.resize(values_types.size());
            for (size_t col = 0; col < values_types.size(); ++col)
                values_types[col]->deserializeBinary(values[col], buf);

            if constexpr (IsDecimalNumber<T>)
                merged_maps[key.get<DecimalField<T>>()] = values;
            else
                merged_maps[key.get<T>()] = values;
        }
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        // Final step does compaction of keys that have zero values, this mutates the state
        auto & merged_maps = this->data(place).merged_maps;

        // Remove keys which are zeros or empty. This should be enabled only for sumMap.
        if constexpr (compact)
        {
            for (auto it = merged_maps.cbegin(); it != merged_maps.cend();)
            {
                // Key is not compacted if it has at least one non-zero value
                bool erase = true;
                for (size_t col = 0; col < values_types.size(); ++col)
                {
                    if (it->second[col] != values_types[col]->getDefault())
                    {
                        erase = false;
                        break;
                    }
                }

                if (erase)
                    it = merged_maps.erase(it);
                else
                    ++it;
            }
        }

        size_t size = merged_maps.size();

        auto & to_tuple = assert_cast<ColumnTuple &>(to);
        auto & to_keys_arr = assert_cast<ColumnArray &>(to_tuple.getColumn(0));
        auto & to_keys_col = to_keys_arr.getData();

        // Advance column offsets
        auto & to_keys_offsets = to_keys_arr.getOffsets();
        to_keys_offsets.push_back(to_keys_offsets.back() + size);
        to_keys_col.reserve(size);

        for (size_t col = 0; col < values_types.size(); ++col)
        {
            auto & to_values_arr = assert_cast<ColumnArray &>(to_tuple.getColumn(col + 1));
            auto & to_values_offsets = to_values_arr.getOffsets();
            to_values_offsets.push_back(to_values_offsets.back() + size);
            to_values_arr.getData().reserve(size);
        }

        // Write arrays of keys and values
        for (const auto & elem : merged_maps)
        {
            // Write array of keys into column
            to_keys_col.insert(elem.first);

            // Write 0..n arrays of values
            for (size_t col = 0; col < values_types.size(); ++col)
            {
                auto & to_values_col = assert_cast<ColumnArray &>(to_tuple.getColumn(col + 1)).getData();
                to_values_col.insert(elem.second[col]);
            }
        }
    }

    bool keepKey(const T & key) const { return static_cast<const Derived &>(*this).keepKey(key); }
    String getName() const override { return static_cast<const Derived &>(*this).getName(); }
};

template <typename T, bool overflow, bool tuple_argument>
class AggregateFunctionSumMap final :
    public AggregateFunctionMapBase<T, AggregateFunctionSumMap<T, overflow, tuple_argument>, FieldVisitorSum, overflow, tuple_argument, true>
{
private:
    using Self = AggregateFunctionSumMap<T, overflow, tuple_argument>;
    using Base = AggregateFunctionMapBase<T, Self, FieldVisitorSum, overflow, tuple_argument, true>;

public:
    AggregateFunctionSumMap(const DataTypePtr & keys_type_,
            DataTypes & values_types_, const DataTypes & argument_types_,
            const Array & params_)
        : Base{keys_type_, values_types_, argument_types_}
    {
        // The constructor accepts parameters to have a uniform interface with
        // sumMapFiltered, but this function doesn't have any parameters.
        assertNoParameters(getName(), params_);
    }

    String getName() const override { return "sumMap"; }

    bool keepKey(const T &) const { return true; }
};

template <typename T, bool overflow, bool tuple_argument>
class AggregateFunctionSumMapFiltered final :
    public AggregateFunctionMapBase<T,
        AggregateFunctionSumMapFiltered<T, overflow, tuple_argument>,
        FieldVisitorSum,
        overflow,
        tuple_argument,
        true>
{
private:
    using Self = AggregateFunctionSumMapFiltered<T, overflow, tuple_argument>;
    using Base = AggregateFunctionMapBase<T, Self, FieldVisitorSum, overflow, tuple_argument, true>;

    /// ARCADIA_BUILD disallow unordered_set for big ints for some reason
    static constexpr const bool allow_hash = !OverBigInt<T>;
    using ContainerT = std::conditional_t<allow_hash, std::unordered_set<T>, std::set<T>>;

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
                "of Array type", getName());

        Array keys_to_keep_;
        if (!params_.front().tryGet<Array>(keys_to_keep_))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Aggregate function {} requires an Array as a parameter",
                getName());

        if constexpr (allow_hash)
            keys_to_keep.reserve(keys_to_keep_.size());

        for (const Field & f : keys_to_keep_)
        {
            keys_to_keep.emplace(f.safeGet<NearestFieldType<T>>());
        }
    }

    String getName() const override
    { return overflow ? "sumMapFilteredWithOverflow" : "sumMapFiltered"; }

    bool keepKey(const T & key) const { return keys_to_keep.count(key); }
};


/** Implements `Max` operation.
 *  Returns true if changed
 */
class FieldVisitorMax : public StaticVisitor<bool>
{
private:
    const Field & rhs;
public:
    explicit FieldVisitorMax(const Field & rhs_) : rhs(rhs_) {}

    bool operator() (Null &) const { throw Exception("Cannot compare Nulls", ErrorCodes::LOGICAL_ERROR); }
    bool operator() (Array &) const { throw Exception("Cannot compare Arrays", ErrorCodes::LOGICAL_ERROR); }
    bool operator() (Tuple &) const { throw Exception("Cannot compare Tuples", ErrorCodes::LOGICAL_ERROR); }
    bool operator() (AggregateFunctionStateData &) const { throw Exception("Cannot compare AggregateFunctionStates", ErrorCodes::LOGICAL_ERROR); }

    template <typename T>
    bool operator() (DecimalField<T> & x) const
    {
        auto val = get<DecimalField<T>>(rhs);
        if (val > x)
        {
            x = val;
            return true;
        }

        return false;
    }

    template <typename T>
    bool operator() (T & x) const
    {
        auto val = get<T>(rhs);
        if (val > x)
        {
            x = val;
            return true;
        }

        return false;
    }
};

/** Implements `Min` operation.
 *  Returns true if changed
 */
class FieldVisitorMin : public StaticVisitor<bool>
{
private:
    const Field & rhs;
public:
    explicit FieldVisitorMin(const Field & rhs_) : rhs(rhs_) {}

    bool operator() (Null &) const { throw Exception("Cannot compare Nulls", ErrorCodes::LOGICAL_ERROR); }
    bool operator() (Array &) const { throw Exception("Cannot sum Arrays", ErrorCodes::LOGICAL_ERROR); }
    bool operator() (Tuple &) const { throw Exception("Cannot sum Tuples", ErrorCodes::LOGICAL_ERROR); }
    bool operator() (AggregateFunctionStateData &) const { throw Exception("Cannot sum AggregateFunctionStates", ErrorCodes::LOGICAL_ERROR); }

    template <typename T>
    bool operator() (DecimalField<T> & x) const
    {
        auto val = get<DecimalField<T>>(rhs);
        if (val < x)
        {
            x = val;
            return true;
        }

        return false;
    }

    template <typename T>
    bool operator() (T & x) const
    {
        auto val = get<T>(rhs);
        if (val < x)
        {
            x = val;
            return true;
        }

        return false;
    }
};


template <typename T, bool tuple_argument>
class AggregateFunctionMinMap final :
    public AggregateFunctionMapBase<T, AggregateFunctionMinMap<T, tuple_argument>, FieldVisitorMin, true, tuple_argument, false>
{
private:
    using Self = AggregateFunctionMinMap<T, tuple_argument>;
    using Base = AggregateFunctionMapBase<T, Self, FieldVisitorMin, true, tuple_argument, false>;

public:
    AggregateFunctionMinMap(const DataTypePtr & keys_type_,
            DataTypes & values_types_, const DataTypes & argument_types_,
            const Array & params_)
        : Base{keys_type_, values_types_, argument_types_}
    {
        // The constructor accepts parameters to have a uniform interface with
        // sumMapFiltered, but this function doesn't have any parameters.
        assertNoParameters(getName(), params_);
    }

    String getName() const override { return "minMap"; }

    bool keepKey(const T &) const { return true; }
};

template <typename T, bool tuple_argument>
class AggregateFunctionMaxMap final :
    public AggregateFunctionMapBase<T, AggregateFunctionMaxMap<T, tuple_argument>, FieldVisitorMax, true, tuple_argument, false>
{
private:
    using Self = AggregateFunctionMaxMap<T, tuple_argument>;
    using Base = AggregateFunctionMapBase<T, Self, FieldVisitorMax, true, tuple_argument, false>;

public:
    AggregateFunctionMaxMap(const DataTypePtr & keys_type_,
            DataTypes & values_types_, const DataTypes & argument_types_,
            const Array & params_)
        : Base{keys_type_, values_types_, argument_types_}
    {
        // The constructor accepts parameters to have a uniform interface with
        // sumMapFiltered, but this function doesn't have any parameters.
        assertNoParameters(getName(), params_);
    }

    String getName() const override { return "maxMap"; }

    bool keepKey(const T &) const { return true; }
};

}
