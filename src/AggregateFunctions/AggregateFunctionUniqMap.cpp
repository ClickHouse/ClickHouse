#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Functions/FunctionHelpers.h>

#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>

#include <Common/assert_cast.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <map>
#include <set>


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/** Data structure for uniqMap that tracks unique values per key */
struct AggregateFunctionUniqMapData
{
    // Map needs to be ordered to maintain function properties
    // For each key, we store an array of sets (one set per value column) to track unique values
    std::map<Field, std::vector<std::set<Field>>> merged_maps;
};

/** Aggregate function uniqMap - counts unique values for each key
  * This is a specialized implementation that uses sets to track unique values
  */
template <bool tuple_argument>
class AggregateFunctionUniqMap final : public IAggregateFunctionDataHelper<
    AggregateFunctionUniqMapData, AggregateFunctionUniqMap<tuple_argument>>
{
private:
    static constexpr auto STATE_VERSION_1_MIN_REVISION = 54452;

    DataTypePtr keys_type;
    SerializationPtr keys_serialization;
    DataTypes values_types;
    Serializations values_serializations;

public:
    using Base = IAggregateFunctionDataHelper<AggregateFunctionUniqMapData, AggregateFunctionUniqMap<tuple_argument>>;

    AggregateFunctionUniqMap(const DataTypePtr & keys_type_,
            const DataTypes & values_types_, const DataTypes & argument_types_,
            const Array & params_)
        : Base(argument_types_, {} /* parameters */, createResultType(keys_type_, values_types_))
        , keys_type(keys_type_)
        , keys_serialization(keys_type->getDefaultSerialization())
        , values_types(values_types_)
    {
        assertNoParameters("uniqMap", params_);
        values_serializations.reserve(values_types.size());
        for (const auto & type : values_types)
        {
            values_serializations.emplace_back(type->getDefaultSerialization());
        }
    }

    String getName() const override { return "uniqMap"; }

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

        // Result type for counts is always UInt64
        for (size_t i = 0; i < values_types_.size(); ++i)
        {
            types.emplace_back(std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()));
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
        const auto & array_column0 = assert_cast<const ColumnArray &>(*columns[0]);
        const auto & offsets0 = array_column0.getOffsets();
        const auto & key_column = array_column0.getData();
        const auto keys_vec_offset = offsets0[row_num - 1];
        const auto keys_vec_size = (offsets0[row_num] - keys_vec_offset);

        // Columns 1..n contain arrays of values to track for uniqueness
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

                auto [it, inserted] = merged_maps.emplace(key, std::vector<std::set<Field>>());

                if (inserted)
                {
                    it->second.resize(size);
                }

                // Add value to the set for this key and column (skip nulls)
                if (!value.isNull())
                {
                    it->second[col].insert(value);
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
            auto it = merged_maps.find(elem.first);
            if (it != merged_maps.end())
            {
                // Merge sets for each column
                for (size_t col = 0; col < values_types.size(); ++col)
                {
                    it->second[col].insert(elem.second[col].begin(), elem.second[col].end());
                }
            }
            else
            {
                merged_maps[elem.first] = elem.second;
            }
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & merged_maps = this->data(place).merged_maps;
        size_t size = merged_maps.size();
        writeVarUInt(size, buf);

        for (const auto & elem : merged_maps)
        {
            keys_serialization->serializeBinary(elem.first, buf, {});

            // Serialize each set of unique values
            for (size_t col = 0; col < values_types.size(); ++col)
            {
                const auto & unique_values = elem.second[col];
                writeVarUInt(unique_values.size(), buf);
                for (const auto & val : unique_values)
                {
                    values_serializations[col]->serializeBinary(val, buf, {});
                }
            }
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        auto & merged_maps = this->data(place).merged_maps;
        size_t size = 0;
        readVarUInt(size, buf);

        FormatSettings format_settings;
        for (size_t i = 0; i < size; ++i)
        {
            Field key;
            keys_serialization->deserializeBinary(key, buf, format_settings);

            std::vector<std::set<Field>> value_sets;
            value_sets.resize(values_types.size());

            for (size_t col = 0; col < values_types.size(); ++col)
            {
                size_t set_size = 0;
                readVarUInt(set_size, buf);
                for (size_t j = 0; j < set_size; ++j)
                {
                    Field value;
                    values_serializations[col]->deserializeBinary(value, buf, format_settings);
                    value_sets[col].insert(value);
                }
            }

            merged_maps[key] = std::move(value_sets);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & merged_maps = this->data(place).merged_maps;
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

        // Write arrays of keys and counts
        for (const auto & elem : merged_maps)
        {
            // Write array of keys into column
            to_keys_col.insert(elem.first);

            // Write counts of unique values for each column
            for (size_t col = 0; col < values_types.size(); ++col)
            {
                auto & to_values_col = assert_cast<ColumnArray &>(to_tuple.getColumn(col + 1)).getData();
                // Insert the count of unique values
                to_values_col.insert(UInt64(elem.second[col].size()));
            }
        }
    }
};


auto parseArguments(const std::string & name, const DataTypes & arguments)
{
    DataTypes args;
    bool tuple_argument = false;

    if (arguments.size() == 1)
    {
        // uniqMap state is fully given by its result, so it can be stored in
        // SimpleAggregateFunction columns. There is a caveat: it must support
        // uniqMap(uniqMap(...)), e.g. it must be able to accept its own output as
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

void registerAggregateFunctionUniqMap(AggregateFunctionFactory & factory)
{
    factory.registerFunction("uniqMap", [](const std::string & name, const DataTypes & arguments, const Array & params, const Settings *) -> AggregateFunctionPtr
    {
        auto [keys_type, values_types, tuple_argument] = parseArguments(name, arguments);
        if (tuple_argument)
            return std::make_shared<AggregateFunctionUniqMap<true>>(keys_type, values_types, arguments, params);
        return std::make_shared<AggregateFunctionUniqMap<false>>(keys_type, values_types, arguments, params);
    });
}

}

