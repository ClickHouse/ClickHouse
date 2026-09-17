#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupBloomFilterData.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/ProtocolDefines.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeAggregateFunction.h>
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
#include <Common/FieldAccurateComparison.h>
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

UInt64 convertGroupBloomFilterParameterToUInt64(const Field & parameter, std::string_view parameter_name)
{
    try
    {
        UInt64 value = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), parameter);
        if (!accurateEquals(parameter, Field(value)))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Parameter {} for aggregate function {} must be an integer value in the range of UInt64",
                parameter_name, AggregateFunctionGroupBloomFilterData::name);

        return value;
    }
    catch (const Exception &)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Parameter {} for aggregate function {} must be an integer value in the range of UInt64",
            parameter_name, AggregateFunctionGroupBloomFilterData::name);
    }
}

Array getCanonicalGroupBloomFilterParameters(size_t filter_size_bytes, size_t num_hashes, size_t seed)
{
    return {UInt64(filter_size_bytes), UInt64(num_hashes), UInt64(seed)};
}

class IAggregateFunctionGroupBloomFilter
{
public:
    virtual ~IAggregateFunctionGroupBloomFilter() = default;
    virtual size_t getFilterSizeBytes() const = 0;
    virtual size_t getNumHashes() const = 0;
    virtual size_t getSeed() const = 0;
    virtual DataTypes getNormalizedArgumentTypes() const = 0;
};

template <typename Derived>
class AggregateFunctionGroupBloomFilterBase
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupBloomFilterData, Derived>
    , public IAggregateFunctionGroupBloomFilter
{
protected:
    size_t filter_size_bytes;
    size_t num_hashes;
    size_t seed;
    Array canonical_parameters;

public:
    AggregateFunctionGroupBloomFilterBase(
        const DataTypePtr & type,
        size_t filter_size_bytes_,
        size_t num_hashes_,
        size_t seed_,
        const Array & parameters_,
        const Array & canonical_parameters_)
        : IAggregateFunctionDataHelper<AggregateFunctionGroupBloomFilterData, Derived>(
            {type}, parameters_, std::make_shared<DataTypeNumber<UInt64>>())
        , filter_size_bytes(filter_size_bytes_)
        , num_hashes(num_hashes_)
        , seed(seed_)
        , canonical_parameters(canonical_parameters_)
    {
    }

    String getName() const override { return AggregateFunctionGroupBloomFilterData::name; }

    size_t getFilterSizeBytes() const override { return filter_size_bytes; }
    size_t getNumHashes() const override { return num_hashes; }
    size_t getSeed() const override { return seed; }

    DataTypes getNormalizedArgumentTypes() const override
    {
        DataTypes normalized_argument_types;
        normalized_argument_types.reserve(this->argument_types.size());
        for (const auto & argument_type : this->argument_types)
            normalized_argument_types.emplace_back(argument_type->getNormalizedType());
        return normalized_argument_types;
    }

    bool haveSameStateRepresentationImpl(const IAggregateFunction & rhs) const override
    {
        const auto * rhs_bloom = dynamic_cast<const IAggregateFunctionGroupBloomFilter *>(&rhs);
        if (!rhs_bloom
            || filter_size_bytes != rhs_bloom->getFilterSizeBytes()
            || num_hashes != rhs_bloom->getNumHashes()
            || seed != rhs_bloom->getSeed())
            return false;

        const auto lhs_argument_types = getNormalizedArgumentTypes();
        const auto rhs_argument_types = rhs_bloom->getNormalizedArgumentTypes();
        return lhs_argument_types.size() == rhs_argument_types.size()
            && std::equal(
                lhs_argument_types.begin(),
                lhs_argument_types.end(),
                rhs_argument_types.begin(),
                rhs_argument_types.end(),
                [](const auto & lhs, const auto & rhs_) { return lhs->equals(*rhs_); });
    }

    DataTypePtr getNormalizedStateType() const override
    {
        const auto normalized_argument_types = getNormalizedArgumentTypes();

        AggregateFunctionProperties properties;
        return std::make_shared<DataTypeAggregateFunction>(
            AggregateFunctionFactory::instance().get(
                getName(), NullsAction::EMPTY, normalized_argument_types, canonical_parameters, properties),
            normalized_argument_types,
            canonical_parameters);
    }

    bool allocatesMemoryInArena() const override { return false; }

    bool supportsCombinator(const String & combinator_name) const override
    {
        return combinator_name == "Array"
            || combinator_name == "If"
            || combinator_name == "Merge"
            || combinator_name == "Null"
            || combinator_name == "State";
    }

    bool isVersioned() const override { return true; }

    size_t getDefaultVersion() const override { return GROUP_BLOOM_FILTER_STATE_VERSION; }

    size_t getVersionFromRevision(size_t revision) const override
    {
        if (revision >= DBMS_MIN_REVISION_WITH_AGGREGATE_FUNCTIONS_VERSIONING)
            return GROUP_BLOOM_FILTER_STATE_VERSION_V1;
        return 0;
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        new (place) AggregateFunctionGroupBloomFilterData();
        this->data(place).setParameters(filter_size_bytes, num_hashes, seed);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        this->data(place).write(buf, version);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena *) const override
    {
        /// Pass the declared parameters so that read validates the serialized header
        /// against them before constructing the BloomFilter or reading the payload.
        this->data(place).read(buf, filter_size_bytes, num_hashes, seed, version);
    }

    void throwIfCannotProduceFinalizedResult() const override
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Aggregate function {} can only be used as an aggregate state. "
            "Use {}State or {}MergeState with bloomFilterContains",
            AggregateFunctionGroupBloomFilterData::name,
            AggregateFunctionGroupBloomFilterData::name,
            AggregateFunctionGroupBloomFilterData::name);
    }

    void insertResultInto(AggregateDataPtr __restrict /* place */, IColumn & /* to */, Arena *) const override
    {
        throwIfCannotProduceFinalizedResult();
    }
};

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
        const Array & parameters_,
        const Array & canonical_parameters_)
        : AggregateFunctionGroupBloomFilterBase<AggregateFunctionGroupBloomFilter<T>>(
            type, filter_size_bytes_, num_hashes_, seed_, parameters_, canonical_parameters_)
    {
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const T value = canonicalizeGroupBloomFilterValue(assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]);
        IAggregateFunctionDataHelper<AggregateFunctionGroupBloomFilterData, AggregateFunctionGroupBloomFilter<T>>::data(place).add(reinterpret_cast<const char *>(&value), sizeof(T));
    }
};


class AggregateFunctionGroupBloomFilterString final
    : public AggregateFunctionGroupBloomFilterBase<AggregateFunctionGroupBloomFilterString>
{
public:
    AggregateFunctionGroupBloomFilterString(
        const DataTypePtr & type,
        size_t filter_size_bytes_,
        size_t num_hashes_,
        size_t seed_,
        const Array & parameters_,
        const Array & canonical_parameters_)
        : AggregateFunctionGroupBloomFilterBase<AggregateFunctionGroupBloomFilterString>(
            type, filter_size_bytes_, num_hashes_, seed_, parameters_, canonical_parameters_)
    {
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const std::string_view value = columns[0]->getDataAt(row_num);
        IAggregateFunctionDataHelper<AggregateFunctionGroupBloomFilterData, AggregateFunctionGroupBloomFilterString>::data(place).add(value.data(), value.size());
    }
};


class AggregateFunctionGroupBloomFilterDateTime64 final
    : public AggregateFunctionGroupBloomFilterBase<AggregateFunctionGroupBloomFilterDateTime64>
{
public:
    AggregateFunctionGroupBloomFilterDateTime64(
        const DataTypePtr & type,
        size_t filter_size_bytes_,
        size_t num_hashes_,
        size_t seed_,
        const Array & parameters_,
        const Array & canonical_parameters_)
        : AggregateFunctionGroupBloomFilterBase<AggregateFunctionGroupBloomFilterDateTime64>(
            type, filter_size_bytes_, num_hashes_, seed_, parameters_, canonical_parameters_)
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
        std::tie(filter_size_bytes, num_hashes) = bloomFilterOptimalParams(
            BLOOM_FILTER_DEFAULT_EXPECTED_ELEMENTS, BLOOM_FILTER_DEFAULT_FALSE_POSITIVE_RATE);
    }
    else if (parameters.size() == 1)
    {
        size_t expected_elements = convertGroupBloomFilterParameterToUInt64(parameters[0], "expected_elements");
        std::tie(filter_size_bytes, num_hashes) = bloomFilterOptimalParams(
            expected_elements, BLOOM_FILTER_DEFAULT_FALSE_POSITIVE_RATE);
    }
    else
    {
        const auto & param2 = parameters[1];
        double param2_as_float = applyVisitor(FieldVisitorConvertToNumber<double>(), param2);
        bool second_is_small_float = param2_as_float > 0.0 && param2_as_float < 1.0
            && (param2.getType() == Field::Types::Float64);

        if (second_is_small_float)
        {
            size_t expected_elements = convertGroupBloomFilterParameterToUInt64(parameters[0], "expected_elements");
            std::tie(filter_size_bytes, num_hashes) = bloomFilterOptimalParams(expected_elements, param2_as_float);
        }
        else
        {
            filter_size_bytes = convertGroupBloomFilterParameterToUInt64(parameters[0], "filter_size_bytes");
            num_hashes = convertGroupBloomFilterParameterToUInt64(param2, "num_hashes");
        }

        if (parameters.size() == 3)
            seed = convertGroupBloomFilterParameterToUInt64(parameters[2], "seed");
    }

    if (filter_size_bytes == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bloom filter size cannot be zero");
    if (filter_size_bytes > BLOOM_FILTER_MAX_SIZE_BYTES)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Bloom filter size {} exceeds maximum allowed size {}",
            filter_size_bytes, BLOOM_FILTER_MAX_SIZE_BYTES);
    if (num_hashes == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Number of hash functions cannot be zero");
    if (num_hashes > BLOOM_FILTER_MAX_HASHES)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Number of hash functions {} exceeds maximum allowed {}",
            num_hashes, BLOOM_FILTER_MAX_HASHES);

    const DataTypePtr & arg_type = argument_types[0];
    WhichDataType which(arg_type);
    const Array canonical_parameters = getCanonicalGroupBloomFilterParameters(filter_size_bytes, num_hashes, seed);

    if (which.isUInt8())
        return std::make_shared<AggregateFunctionGroupBloomFilter<UInt8>>(arg_type, filter_size_bytes, num_hashes, seed, parameters, canonical_parameters);
    if (which.isUInt16())
        return std::make_shared<AggregateFunctionGroupBloomFilter<UInt16>>(arg_type, filter_size_bytes, num_hashes, seed, parameters, canonical_parameters);
    if (which.isUInt32())
        return std::make_shared<AggregateFunctionGroupBloomFilter<UInt32>>(arg_type, filter_size_bytes, num_hashes, seed, parameters, canonical_parameters);
    if (which.isUInt64())
        return std::make_shared<AggregateFunctionGroupBloomFilter<UInt64>>(arg_type, filter_size_bytes, num_hashes, seed, parameters, canonical_parameters);
    if (which.isUInt128())
        return std::make_shared<AggregateFunctionGroupBloomFilter<UInt128>>(arg_type, filter_size_bytes, num_hashes, seed, parameters, canonical_parameters);
    if (which.isUInt256())
        return std::make_shared<AggregateFunctionGroupBloomFilter<UInt256>>(arg_type, filter_size_bytes, num_hashes, seed, parameters, canonical_parameters);
    if (which.isInt8())
        return std::make_shared<AggregateFunctionGroupBloomFilter<Int8>>(arg_type, filter_size_bytes, num_hashes, seed, parameters, canonical_parameters);
    if (which.isInt16())
        return std::make_shared<AggregateFunctionGroupBloomFilter<Int16>>(arg_type, filter_size_bytes, num_hashes, seed, parameters, canonical_parameters);
    if (which.isInt32())
        return std::make_shared<AggregateFunctionGroupBloomFilter<Int32>>(arg_type, filter_size_bytes, num_hashes, seed, parameters, canonical_parameters);
    if (which.isInt64())
        return std::make_shared<AggregateFunctionGroupBloomFilter<Int64>>(arg_type, filter_size_bytes, num_hashes, seed, parameters, canonical_parameters);
    if (which.isInt128())
        return std::make_shared<AggregateFunctionGroupBloomFilter<Int128>>(arg_type, filter_size_bytes, num_hashes, seed, parameters, canonical_parameters);
    if (which.isInt256())
        return std::make_shared<AggregateFunctionGroupBloomFilter<Int256>>(arg_type, filter_size_bytes, num_hashes, seed, parameters, canonical_parameters);
    if (which.isFloat32())
        return std::make_shared<AggregateFunctionGroupBloomFilter<Float32>>(arg_type, filter_size_bytes, num_hashes, seed, parameters, canonical_parameters);
    if (which.isFloat64())
        return std::make_shared<AggregateFunctionGroupBloomFilter<Float64>>(arg_type, filter_size_bytes, num_hashes, seed, parameters, canonical_parameters);
    if (which.isDate())
        return std::make_shared<AggregateFunctionGroupBloomFilter<UInt16>>(arg_type, filter_size_bytes, num_hashes, seed, parameters, canonical_parameters);
    if (which.isDate32())
        return std::make_shared<AggregateFunctionGroupBloomFilter<Int32>>(arg_type, filter_size_bytes, num_hashes, seed, parameters, canonical_parameters);
    if (which.isDateTime())
        return std::make_shared<AggregateFunctionGroupBloomFilter<UInt32>>(arg_type, filter_size_bytes, num_hashes, seed, parameters, canonical_parameters);
    if (which.isDateTime64())
        return std::make_shared<AggregateFunctionGroupBloomFilterDateTime64>(arg_type, filter_size_bytes, num_hashes, seed, parameters, canonical_parameters);
    if (which.isEnum8())
        return std::make_shared<AggregateFunctionGroupBloomFilter<Int8>>(arg_type, filter_size_bytes, num_hashes, seed, parameters, canonical_parameters);
    if (which.isEnum16())
        return std::make_shared<AggregateFunctionGroupBloomFilter<Int16>>(arg_type, filter_size_bytes, num_hashes, seed, parameters, canonical_parameters);
    if (which.isUUID())
        return std::make_shared<AggregateFunctionGroupBloomFilter<UUID>>(arg_type, filter_size_bytes, num_hashes, seed, parameters, canonical_parameters);
    if (which.isIPv4())
        return std::make_shared<AggregateFunctionGroupBloomFilter<IPv4>>(arg_type, filter_size_bytes, num_hashes, seed, parameters, canonical_parameters);
    if (which.isIPv6())
        return std::make_shared<AggregateFunctionGroupBloomFilter<IPv6>>(arg_type, filter_size_bytes, num_hashes, seed, parameters, canonical_parameters);
    if (which.isString() || which.isFixedString())
        return std::make_shared<AggregateFunctionGroupBloomFilterString>(arg_type, filter_size_bytes, num_hashes, seed, parameters, canonical_parameters);

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        "Aggregate function {} does not support type {}",
        name, arg_type->getName());
}

}


void registerAggregateFunctionGroupBloomFilter(AggregateFunctionFactory & factory);
void registerAggregateFunctionGroupBloomFilter(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Builds a probabilistic Bloom filter from column values and returns it as an aggregate state.
The Bloom filter can be used with [`bloomFilterContains`](/reference/functions/regular-functions/other-functions#bloomFilterContains)
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

The parameter form is selected by the second parameter: if it is a `Float64` value in `(0, 1)`,
it is interpreted as `false_positive_rate`; otherwise, it is interpreted as `num_hashes`.

`groupBloomFilter` supports the `-State` and `-MergeState` combinators. The `-State` combinator can be combined
with `-If`, `-Array`, or `-ArrayIf`; the resulting states can be used with `bloomFilterContains`.
Arguments of `groupBloomFilterIfState` and `groupBloomFilterArrayIfState`, including array elements and the condition,
must not be `Nullable`.
An untyped `NULL` argument is rejected because its element type cannot be inferred. Cast it to an explicit
`Nullable(T)` type to create an empty typed state.
The `-Distinct` combinator is not supported because duplicate values do not change a Bloom filter and its
additional state cannot be consumed by `bloomFilterContains`.
Other combinators that change or replicate the aggregate state, including `-ArgMin`, `-ArgMax`, `-ForEach`,
`-Map`, `-Resample`, `-SimpleState`, and `-Tuple`, are rejected during aggregate-function construction.
    )";
    FunctionDocumentation::Syntax syntax = R"(
groupBloomFilter(column)
groupBloomFilterState(column)
groupBloomFilter(expected_elements[, false_positive_rate[, seed]])(column)
groupBloomFilterState(expected_elements[, false_positive_rate[, seed]])(column)
groupBloomFilter(filter_size_bytes, num_hashes[, seed])(column)
groupBloomFilterState(filter_size_bytes, num_hashes[, seed])(column)
    )";
    FunctionDocumentation::Arguments arguments = {
        {
            "column",
            "Column values to add to the Bloom filter. Supported types: UInt8, UInt16, UInt32, UInt64, UInt128, UInt256, "
            "Int8, Int16, Int32, Int64, Int128, Int256, Float32, Float64, String, FixedString, Date, Date32, "
            "DateTime, DateTime64, UUID, IPv4, IPv6, Enum8, Enum16.",
            {}
        }
    };
    FunctionDocumentation::Parameters parameters = {
        {"expected_elements", "Expected number of distinct elements."},
        {"false_positive_rate", "Desired false positive rate in (0, 1). Default: 0.025."},
        {"filter_size_bytes", "Size of the Bloom filter in bytes for the direct parameter form."},
        {"num_hashes", "Number of hash functions for the direct parameter form."},
        {"seed", "Seed for hash functions. Default: 0."}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the Bloom filter state as `AggregateFunction(groupBloomFilter, T)` (default form) or "
        "`AggregateFunction(groupBloomFilter(params...), T)` (parameterized form, e.g. `AggregateFunction(groupBloomFilter(1000), String)`) "
        "when using the `-State` combinator. "
        "The `-If`, `-Array`, and `-ArrayIf` variants use the corresponding aggregate function name and argument types. "
        "Parameterized forms must resolve to the same effective `filter_size_bytes`, `num_hashes`, and `seed` when defining `AggregatingMergeTree` columns explicitly. "
        "The finalized form throws an exception because Bloom filters do not have a meaningful scalar result.",
        {"AggregateFunction"}
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
    FunctionDocumentation::IntroducedIn introduced_in = {26, 9};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction(
        AggregateFunctionGroupBloomFilterData::name,
        {createAggregateFunctionGroupBloomFilter, documentation, {
            .rejects_nullable_arguments_with_if = true,
            .rejects_only_null_arguments = true,
        }});
}

}
