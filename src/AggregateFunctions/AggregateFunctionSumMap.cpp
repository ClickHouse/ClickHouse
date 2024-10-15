#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Functions/FunctionHelpers.h>

#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>

#include <Common/FieldVisitorSum.h>
#include <Common/assert_cast.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <map>


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

struct AggregateFunctionMapData
{
    // Map needs to be ordered to maintain function properties
    std::map<Field, Array> merged_maps;
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
    static constexpr auto STATE_VERSION_1_MIN_REVISION = 54452;

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
        DataTypes types;
        types.emplace_back(std::make_shared<DataTypeArray>(keys_type_));

        for (const auto & value_type : values_types_)
        {
            if constexpr (std::is_same_v<Visitor, FieldVisitorSum>)
            {
                if (!value_type->isSummable())
                    throw Exception{ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Values for -Map cannot be summed, passed type {}",
                        value_type->getName()};
            }

            DataTypePtr result_type;

            if constexpr (overflow)
            {
                if (value_type->onlyNull())
                    throw Exception{ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Cannot calculate -Map of type {}",
                        value_type->getName()};

                // Overflow, meaning that the returned type is the same as
                // the input type. Nulls are skipped.
                result_type = removeNullable(value_type);
            }
            else
            {
                auto value_type_without_nullable = removeNullable(value_type);

                // No overflow, meaning we promote the types if necessary.
                if (!value_type_without_nullable->canBePromoted())
                    throw Exception{ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Values for -Map are expected to be Numeric, Float or Decimal, passed type {}",
                        value_type->getName()};

                WhichDataType value_type_to_check(value_type_without_nullable);

                /// Do not promote decimal because of implementation issues of this function design
                /// Currently we cannot get result column type in case of decimal we cannot get decimal scale
                /// in method void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
                /// If we decide to make this function more efficient we should promote decimal type during summ
                if (value_type_to_check.isDecimal())
                    result_type = value_type_without_nullable;
                else
                    result_type = value_type_without_nullable->promoteNumericType();
            }

            types.emplace_back(std::make_shared<DataTypeArray>(result_type));
        }

        return std::make_shared<DataTypeTuple>(types);
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
            const auto & array_column = assert_cast<const ColumnArray &>(*columns[col + 1]);
            const IColumn & value_column = array_column.getData();
            const IColumn::Offsets & offsets = array_column.getOffsets();
            const size_t values_vec_offset = offsets[row_num - 1];
            const size_t values_vec_size = (offsets[row_num] - values_vec_offset);

            // Expect key and value arrays to be of same length
            if (keys_vec_size != values_vec_size)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Sizes of keys and values arrays do not match");

            // Insert column values for all keys
            for (size_t i = 0; i < keys_vec_size; ++i)
            {
                Field value = value_column[values_vec_offset + i];
                Field key = key_column[keys_vec_offset + i];

                if (!keepKey(key))
                    continue;

                auto [it, inserted] = merged_maps.emplace(key, Array());

                if (inserted)
                {
                    it->second.resize(size);
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
                    WhichDataType value_type(values_types[col_idx]);
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

                    promoted_values_serializations[col_idx]->serializeBinary(value, buf, {});
                };
                break;
            }
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown version {}, of -Map aggregate function serialization state", *version);
        }

        for (const auto & elem : merged_maps)
        {
            keys_serialization->serializeBinary(elem.first, buf, {});
            for (size_t col = 0; col < values_types.size(); ++col)
                serialize(col, elem.second);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena *) const override
    {
        if (!version)
            version = getDefaultVersion();

        auto & merged_maps = this->data(place).merged_maps;
        size_t size = 0;
        readVarUInt(size, buf);

        std::function<void(size_t, Array &)> deserialize;
        switch (*version)
        {
            case 0:
            {
                deserialize = [&](size_t col_idx, Array & values)
                {
                    values_serializations[col_idx]->deserializeBinary(values[col_idx], buf, {});
                };
                break;
            }
            case 1:
            {
                deserialize = [&](size_t col_idx, Array & values)
                {
                    Field & value = values[col_idx];
                    promoted_values_serializations[col_idx]->deserializeBinary(value, buf, {});

                    /// Compatibility with previous versions.
                    if (value.getType() == Field::Types::Decimal128)
                    {
                        auto source = value.safeGet<DecimalField<Decimal128>>();
                        WhichDataType value_type(values_types[col_idx]);
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
            keys_serialization->deserializeBinary(key, buf, {});

            Array values;
            values.resize(values_types.size());

            for (size_t col = 0; col < values_types.size(); ++col)
                deserialize(col, values);

            merged_maps[key] = values;
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        size_t num_columns = values_types.size();

        // Final step does compaction of keys that have zero values, this mutates the state
        auto & merged_maps = this->data(place).merged_maps;

        // Remove keys which are zeros or empty. This should be enabled only for sumMap.
        if constexpr (compact)
        {
            for (auto it = merged_maps.cbegin(); it != merged_maps.cend();)
            {
                // Key is not compacted if it has at least one non-zero value
                bool erase = true;
                for (size_t col = 0; col < num_columns; ++col)
                {
                    if (!it->second[col].isNull() && it->second[col] != values_types[col]->getDefault())
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

        for (size_t col = 0; col < num_columns; ++col)
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
            for (size_t col = 0; col < num_columns; ++col)
            {
                auto & to_values_col = assert_cast<ColumnArray &>(to_tuple.getColumn(col + 1)).getData();
                if (elem.second[col].isNull())
                    to_values_col.insertDefault();
                else
                    to_values_col.insert(elem.second[col]);
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

    using ContainerT = std::set<Field>;
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
            const auto & value = key.safeGet<const UInt64 &>();
            if (value <= std::numeric_limits<Int64>::max())
                return keys_to_keep.contains(Field(Int64(value)));
        }
        else if (key.getType() == Field::Types::Int64)
        {
            const auto & value = key.safeGet<const Int64 &>();
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

void registerAggregateFunctionSumMap(AggregateFunctionFactory & factory)
{
    // these functions used to be called *Map, with now these names occupied by
    // Map combinator, which redirects calls here if was called with
    // array or tuple arguments.
    factory.registerFunction("sumMappedArrays", [](const std::string & name, const DataTypes & arguments, const Array & params, const Settings *) -> AggregateFunctionPtr
    {
        auto [keys_type, values_types, tuple_argument] = parseArguments(name, arguments);
        if (tuple_argument)
            return std::make_shared<AggregateFunctionSumMap<false, true>>(keys_type, values_types, arguments, params);
        return std::make_shared<AggregateFunctionSumMap<false, false>>(keys_type, values_types, arguments, params);
    });

    factory.registerFunction("minMappedArrays", [](const std::string & name, const DataTypes & arguments, const Array & params, const Settings *) -> AggregateFunctionPtr
    {
        auto [keys_type, values_types, tuple_argument] = parseArguments(name, arguments);
        if (tuple_argument)
            return std::make_shared<AggregateFunctionMinMap<true>>(keys_type, values_types, arguments, params);
        return std::make_shared<AggregateFunctionMinMap<false>>(keys_type, values_types, arguments, params);
    });

    factory.registerFunction("maxMappedArrays", [](const std::string & name, const DataTypes & arguments, const Array & params, const Settings *) -> AggregateFunctionPtr
    {
        auto [keys_type, values_types, tuple_argument] = parseArguments(name, arguments);
        if (tuple_argument)
            return std::make_shared<AggregateFunctionMaxMap<true>>(keys_type, values_types, arguments, params);
        return std::make_shared<AggregateFunctionMaxMap<false>>(keys_type, values_types, arguments, params);
    });

    // these functions could be renamed to *MappedArrays too, but it would
    // break backward compatibility
    factory.registerFunction("sumMapWithOverflow", [](const std::string & name, const DataTypes & arguments, const Array & params, const Settings *) -> AggregateFunctionPtr
    {
        auto [keys_type, values_types, tuple_argument] = parseArguments(name, arguments);
        if (tuple_argument)
            return std::make_shared<AggregateFunctionSumMap<true, true>>(keys_type, values_types, arguments, params);
        return std::make_shared<AggregateFunctionSumMap<true, false>>(keys_type, values_types, arguments, params);
    });

    factory.registerFunction("sumMapFiltered", [](const std::string & name, const DataTypes & arguments, const Array & params, const Settings *) -> AggregateFunctionPtr
    {
        auto [keys_type, values_types, tuple_argument] = parseArguments(name, arguments);
        if (tuple_argument)
            return std::make_shared<AggregateFunctionSumMapFiltered<false, true>>(keys_type, values_types, arguments, params);
        return std::make_shared<AggregateFunctionSumMapFiltered<false, false>>(keys_type, values_types, arguments, params);
    });

    factory.registerFunction("sumMapFilteredWithOverflow", [](const std::string & name, const DataTypes & arguments, const Array & params, const Settings *) -> AggregateFunctionPtr
    {
        auto [keys_type, values_types, tuple_argument] = parseArguments(name, arguments);
        if (tuple_argument)
            return std::make_shared<AggregateFunctionSumMapFiltered<true, true>>(keys_type, values_types, arguments, params);
        return std::make_shared<AggregateFunctionSumMapFiltered<true, false>>(keys_type, values_types, arguments, params);
    });
}

}
