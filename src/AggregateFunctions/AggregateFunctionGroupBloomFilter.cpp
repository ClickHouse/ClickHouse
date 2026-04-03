#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupBloomFilterData.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/assert_cast.h>
#include <Core/Field.h>
#include <Common/FieldVisitorConvertToNumber.h>


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Base class for Bloom filter aggregate functions
template <typename Derived>
class AggregateFunctionGroupBloomFilterBase
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupBloomFilterData, Derived>
{
protected:
    size_t filter_size_bytes;
    size_t num_hashes;
    size_t seed;

public:
    AggregateFunctionGroupBloomFilterBase(
        const DataTypePtr & type,
        size_t filter_size_bytes_,
        size_t num_hashes_,
        size_t seed_,
        const Array & user_parameters)
        : IAggregateFunctionDataHelper<AggregateFunctionGroupBloomFilterData, Derived>(
            {type}, user_parameters, std::make_shared<DataTypeNumber<UInt64>>())
        , filter_size_bytes(filter_size_bytes_)
        , num_hashes(num_hashes_)
        , seed(seed_)
    {
    }

    String getName() const override { return AggregateFunctionGroupBloomFilterData::name; }

    bool allocatesMemoryInArena() const override { return false; }

    void create(AggregateDataPtr __restrict place) const override
    {
        new (place) AggregateFunctionGroupBloomFilterData();
        this->data(place).init(filter_size_bytes, num_hashes, seed);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict /* place */, IColumn & to, Arena *) const override
    {
        /// Without -State combinator, return 0 as a placeholder.
        /// The main use case is with -State combinator to get the Bloom filter state.
        assert_cast<ColumnVector<UInt64> &>(to).getData().push_back(0);
    }
};

/// Aggregate function that builds a Bloom filter from numeric column values.
/// Returns UInt64 (approximate count of distinct elements) by default.
/// Use -State combinator to get the Bloom filter state for use with bloomFilterContains.
template <typename T>
class AggregateFunctionGroupBloomFilter final
    : public AggregateFunctionGroupBloomFilterBase<AggregateFunctionGroupBloomFilter<T>>
{
public:
    AggregateFunctionGroupBloomFilter(
        const DataTypePtr & type,
        size_t filter_size_bytes_,
        size_t num_hashes_,
        size_t seed_,
        const Array & user_parameters)
        : AggregateFunctionGroupBloomFilterBase<AggregateFunctionGroupBloomFilter<T>>(
            type, filter_size_bytes_, num_hashes_, seed_, user_parameters)
    {
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const T value = assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num];
        IAggregateFunctionDataHelper<AggregateFunctionGroupBloomFilterData, AggregateFunctionGroupBloomFilter<T>>::data(place).add(reinterpret_cast<const char *>(&value), sizeof(T));
    }
};


/// Specialization for String and FixedString types
class AggregateFunctionGroupBloomFilterString final
    : public AggregateFunctionGroupBloomFilterBase<AggregateFunctionGroupBloomFilterString>
{
public:
    AggregateFunctionGroupBloomFilterString(
        const DataTypePtr & type,
        size_t filter_size_bytes_,
        size_t num_hashes_,
        size_t seed_,
        const Array & user_parameters)
        : AggregateFunctionGroupBloomFilterBase<AggregateFunctionGroupBloomFilterString>(
            type, filter_size_bytes_, num_hashes_, seed_, user_parameters)
    {
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const std::string_view value = columns[0]->getDataAt(row_num);
        IAggregateFunctionDataHelper<AggregateFunctionGroupBloomFilterData, AggregateFunctionGroupBloomFilterString>::data(place).add(value.data(), value.size());
    }
};


/// Specialization for DateTime64 type
class AggregateFunctionGroupBloomFilterDateTime64 final
    : public AggregateFunctionGroupBloomFilterBase<AggregateFunctionGroupBloomFilterDateTime64>
{
public:
    AggregateFunctionGroupBloomFilterDateTime64(
        const DataTypePtr & type,
        size_t filter_size_bytes_,
        size_t num_hashes_,
        size_t seed_,
        const Array & user_parameters)
        : AggregateFunctionGroupBloomFilterBase<AggregateFunctionGroupBloomFilterDateTime64>(
            type, filter_size_bytes_, num_hashes_, seed_, user_parameters)
    {
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto value = assert_cast<const ColumnDecimal<DateTime64> &>(*columns[0]).getData()[row_num];
        IAggregateFunctionDataHelper<AggregateFunctionGroupBloomFilterData, AggregateFunctionGroupBloomFilterDateTime64>::data(place).add(reinterpret_cast<const char *>(&value), sizeof(DateTime64));
    }
};


AggregateFunctionPtr createAggregateFunctionGroupBloomFilter(
    const std::string & name,
    const DataTypes & argument_types,
    const Array & parameters,
    const Settings *)
{
    assertUnary(name, argument_types);

    if (parameters.size() > 3)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} requires 0 to 3 parameters: "
            "([expected_elements[, false_positive_rate[, seed]]]) or "
            "(filter_size_bytes, num_hashes[, seed])",
            name);

    size_t filter_size_bytes = 0;
    size_t num_hashes = 0;
    size_t seed = BLOOM_FILTER_DEFAULT_SEED;

    if (parameters.empty())
    {
        /// No parameters — use all defaults
        filter_size_bytes = bloomFilterOptimalSizeBytes(BLOOM_FILTER_DEFAULT_EXPECTED_ELEMENTS, BLOOM_FILTER_DEFAULT_FALSE_POSITIVE_RATE);
        num_hashes = bloomFilterOptimalHashes(filter_size_bytes, BLOOM_FILTER_DEFAULT_EXPECTED_ELEMENTS);
    }
    else if (parameters.size() == 1)
    {
        /// (expected_elements) — use default false positive rate
        size_t expected_elements = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), parameters[0]);
        filter_size_bytes = bloomFilterOptimalSizeBytes(expected_elements, BLOOM_FILTER_DEFAULT_FALSE_POSITIVE_RATE);
        num_hashes = bloomFilterOptimalHashes(filter_size_bytes, expected_elements);
    }
    else
    {
        /// Check if second parameter looks like a false positive rate (float in (0,1))
        const auto & param2 = parameters[1];
        double param2_as_float = applyVisitor(FieldVisitorConvertToNumber<double>(), param2);
        bool second_is_small_float = param2_as_float > 0.0 && param2_as_float < 1.0
            && (param2.getType() == Field::Types::Float64);

        if (second_is_small_float)
        {
            /// (expected_elements, false_positive_rate[, seed])
            size_t expected_elements = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), parameters[0]);
            double false_positive_rate = param2_as_float;
            filter_size_bytes = bloomFilterOptimalSizeBytes(expected_elements, false_positive_rate);
            num_hashes = bloomFilterOptimalHashes(filter_size_bytes, expected_elements);
        }
        else
        {
            /// (filter_size_bytes, num_hashes[, seed])
            filter_size_bytes = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), parameters[0]);
            num_hashes = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), param2);
        }

        if (parameters.size() == 3)
            seed = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), parameters[2]);
    }

    if (filter_size_bytes == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bloom filter size cannot be zero");
    if (filter_size_bytes > BLOOM_FILTER_MAX_SIZE_BYTES)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Bloom filter size {} exceeds maximum allowed size {}",
            filter_size_bytes, BLOOM_FILTER_MAX_SIZE_BYTES);
    if (num_hashes == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Number of hash functions cannot be zero");

    const DataTypePtr & arg_type = argument_types[0];
    WhichDataType which(arg_type);

    // Integer types
    if (which.isUInt8())
        return std::make_shared<AggregateFunctionGroupBloomFilter<UInt8>>(arg_type, filter_size_bytes, num_hashes, seed, parameters);
    if (which.isUInt16())
        return std::make_shared<AggregateFunctionGroupBloomFilter<UInt16>>(arg_type, filter_size_bytes, num_hashes, seed, parameters);
    if (which.isUInt32())
        return std::make_shared<AggregateFunctionGroupBloomFilter<UInt32>>(arg_type, filter_size_bytes, num_hashes, seed, parameters);
    if (which.isUInt64())
        return std::make_shared<AggregateFunctionGroupBloomFilter<UInt64>>(arg_type, filter_size_bytes, num_hashes, seed, parameters);
    if (which.isUInt128())
        return std::make_shared<AggregateFunctionGroupBloomFilter<UInt128>>(arg_type, filter_size_bytes, num_hashes, seed, parameters);
    if (which.isUInt256())
        return std::make_shared<AggregateFunctionGroupBloomFilter<UInt256>>(arg_type, filter_size_bytes, num_hashes, seed, parameters);
    if (which.isInt8())
        return std::make_shared<AggregateFunctionGroupBloomFilter<Int8>>(arg_type, filter_size_bytes, num_hashes, seed, parameters);
    if (which.isInt16())
        return std::make_shared<AggregateFunctionGroupBloomFilter<Int16>>(arg_type, filter_size_bytes, num_hashes, seed, parameters);
    if (which.isInt32())
        return std::make_shared<AggregateFunctionGroupBloomFilter<Int32>>(arg_type, filter_size_bytes, num_hashes, seed, parameters);
    if (which.isInt64())
        return std::make_shared<AggregateFunctionGroupBloomFilter<Int64>>(arg_type, filter_size_bytes, num_hashes, seed, parameters);
    if (which.isInt128())
        return std::make_shared<AggregateFunctionGroupBloomFilter<Int128>>(arg_type, filter_size_bytes, num_hashes, seed, parameters);
    if (which.isInt256())
        return std::make_shared<AggregateFunctionGroupBloomFilter<Int256>>(arg_type, filter_size_bytes, num_hashes, seed, parameters);
    // Floating point types
    if (which.isFloat32())
        return std::make_shared<AggregateFunctionGroupBloomFilter<Float32>>(arg_type, filter_size_bytes, num_hashes, seed, parameters);
    if (which.isFloat64())
        return std::make_shared<AggregateFunctionGroupBloomFilter<Float64>>(arg_type, filter_size_bytes, num_hashes, seed, parameters);
    // Date and time types
    if (which.isDate())
        return std::make_shared<AggregateFunctionGroupBloomFilter<UInt16>>(arg_type, filter_size_bytes, num_hashes, seed, parameters);
    if (which.isDate32())
        return std::make_shared<AggregateFunctionGroupBloomFilter<Int32>>(arg_type, filter_size_bytes, num_hashes, seed, parameters);
    if (which.isDateTime())
        return std::make_shared<AggregateFunctionGroupBloomFilter<UInt32>>(arg_type, filter_size_bytes, num_hashes, seed, parameters);
    if (which.isDateTime64())
        return std::make_shared<AggregateFunctionGroupBloomFilterDateTime64>(arg_type, filter_size_bytes, num_hashes, seed, parameters);
    // Enum types
    if (which.isEnum8())
        return std::make_shared<AggregateFunctionGroupBloomFilter<Int8>>(arg_type, filter_size_bytes, num_hashes, seed, parameters);
    if (which.isEnum16())
        return std::make_shared<AggregateFunctionGroupBloomFilter<Int16>>(arg_type, filter_size_bytes, num_hashes, seed, parameters);
    // UUID type
    if (which.isUUID())
        return std::make_shared<AggregateFunctionGroupBloomFilter<UUID>>(arg_type, filter_size_bytes, num_hashes, seed, parameters);
    // IP address types
    if (which.isIPv4())
        return std::make_shared<AggregateFunctionGroupBloomFilter<IPv4>>(arg_type, filter_size_bytes, num_hashes, seed, parameters);
    if (which.isIPv6())
        return std::make_shared<AggregateFunctionGroupBloomFilter<IPv6>>(arg_type, filter_size_bytes, num_hashes, seed, parameters);
    // String types
    if (which.isString() || which.isFixedString())
        return std::make_shared<AggregateFunctionGroupBloomFilterString>(arg_type, filter_size_bytes, num_hashes, seed, parameters);

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        "Aggregate function {} does not support type {}",
        name, arg_type->getName());
}

}


void registerAggregateFunctionGroupBloomFilter(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Builds a probabilistic Bloom filter from column values and returns it as an aggregate state.
The Bloom filter can be used with [`bloomFilterContains`](/sql-reference/functions/bloom-filter-functions#bloomFilterContains)
to efficiently check whether a value was present in the aggregated dataset.

This is useful for finding new values that appeared in one time interval but were absent in another,
with low memory usage compared to exact methods like `NOT IN` or `EXCEPT`.

**Parameters:**
- `expected_elements` — expected number of distinct elements to be inserted.
- `false_positive_rate` (optional, default `0.025`) — desired false positive probability in range (0, 1). Lower values require more memory.
- `seed` (optional, default `0`) — seed for hash functions.

Alternatively, you can specify filter parameters directly:
- `filter_size_bytes` — size of the Bloom filter in bytes.
- `num_hashes` — number of hash functions.
    )";
    FunctionDocumentation::Syntax syntax = R"(
groupBloomFilter([expected_elements[, false_positive_rate[, seed]]])(column)
groupBloomFilterState([expected_elements[, false_positive_rate[, seed]]])(column)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"column", "Column values to add to the Bloom filter. Supported types: integers, floats, String, Date, UUID, IP, Enum.", {}}
    };
    FunctionDocumentation::Parameters parameters = {
        {"expected_elements", "Expected number of distinct elements."},
        {"false_positive_rate", "Desired false positive rate in (0, 1). Default: 0.025."},
        {"seed", "Seed for hash functions. Default: 0."}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns `0` by default. Use `-State` combinator to get the Bloom filter state as `AggregateFunction(groupBloomFilter, T)`.",
        {"UInt64"}
    };
    FunctionDocumentation::Examples examples = {
        {
            "Basic usage",
            R"(
SELECT bloomFilterContains(groupBloomFilterState(1000)(number), toUInt64(42)) AS result
FROM numbers(100)
            )",
            R"(
┌─result─┐
│      1 │
└────────┘
            )"
        },
        {
            "Find new values using WITH clause",
            R"(
WITH (
    SELECT groupBloomFilterState(1000)(number)
    FROM numbers(100)
) AS old_bloom
SELECT count() AS new_values_count
FROM numbers(200)
WHERE number >= 100
    AND NOT bloomFilterContains(old_bloom, number)
            )",
            R"(
┌─new_values_count─┐
│              100 │
└──────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction(AggregateFunctionGroupBloomFilterData::name, {createAggregateFunctionGroupBloomFilter, documentation});
}

}
