#include <config.h>

#if USE_DATASKETCHES

#include <Storages/Statistics/StatisticsHistogram.h>

#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Common/Exception.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <functional>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_STATISTICS;
extern const int LOGICAL_ERROR;
}

namespace
{

constexpr UInt8 HISTOGRAM_PAYLOAD_VERSION = 3;
constexpr UInt64 MAX_SERIALIZED_SKETCH_SIZE = 1ULL << 20;

Float64 clampCount(Float64 value, Float64 upper_bound)
{
    if (!std::isfinite(value))
        return value > 0 ? upper_bound : 0.0;
    return std::clamp(value, 0.0, upper_bound);
}

bool isNegativeZero(Float64 value)
{
    return value == 0 && std::signbit(value);
}

/// Collects the weight that purges remove from the Misra-Gries counters.
struct SpillObserver
{
    HistogramFrequentValues::Spill & spill;
    UInt64 & retained_weight;

    void operator()(const Float64 & value, UInt64 removed)
    {
        /// `reserveSpill` guarantees that this does not allocate in the middle of a purge.
        spill.emplace_back(value, removed);
        retained_weight -= removed;
    }
};

}

size_t HistogramFrequentValues::CanonicalFloat64Hash::operator()(Float64 value) const
{
    return std::bit_cast<UInt64>(value);
}

UInt8 HistogramFrequentValues::getLgMapSize(UInt64 values)
{
    UInt8 lg_map_size = Sketch::LG_MIN_MAP_SIZE;
    while (getMapCapacity(lg_map_size) < values)
        ++lg_map_size;
    return lg_map_size;
}

UInt64 HistogramFrequentValues::getMapCapacity(UInt8 lg_map_size)
{
    /// Same as the capacity of DataSketches' reverse_purge_hash_map, whose load factor is 0.75.
    return (1ULL << lg_map_size) * 3 / 4;
}

HistogramFrequentValues::HistogramFrequentValues(UInt64 requested_values_)
    : requested_values(requested_values_)
    , sketch(getLgMapSize(requested_values_))
{
    if (requested_values < MIN_VALUES || requested_values > MAX_VALUES)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid number of histogram frequent values: {}", requested_values);
}

void HistogramFrequentValues::reserveSpill(Spill & spill) const
{
    /// A purge reports every active counter, including the one just inserted.
    const size_t required = spill.size() + sketch.get_num_active_items() + 1;
    if (spill.capacity() < required)
        spill.reserve(std::max(required, 2 * spill.capacity()));
}

void HistogramFrequentValues::update(Float64 finite_value, Spill & spill)
{
    chassert(std::isfinite(finite_value) && !isNegativeZero(finite_value));
    reserveSpill(spill);
    SpillObserver observer{spill, retained_weight};
    retained_weight += 1;
    sketch.update(finite_value, 1, observer);
}

void HistogramFrequentValues::merge(const HistogramFrequentValues & other, Spill & spill)
{
    /// A purge deletes at least half of the capacity + 1 counters, so at least capacity / 2 insertions happen between
    /// purges, and each purge reports at most capacity + 1 counters. Reserve for all of them, because a purge cannot be
    /// interrupted.
    const size_t max_purges = other.size() / std::max<UInt64>(1, getCapacity() / 2) + 1;
    const size_t required = spill.size() + max_purges * (getCapacity() + 1);
    if (spill.capacity() < required)
        spill.reserve(required);

    SpillObserver observer{spill, retained_weight};
    retained_weight += other.retained_weight;
    /// Merge in value order, so the result does not depend on the hash map layout of `other`.
    sketch.merge(other.sketch, observer, std::less<>());
}

HistogramFrequentValues::Items HistogramFrequentValues::getSortedItems() const
{
    Items items;
    items.reserve(sketch.get_num_active_items());
    /// Every tracked value has an upper bound above zero.
    for (const auto & row : sketch.get_frequent_items(datasketches::NO_FALSE_NEGATIVES, 0))
        items.push_back({row.get_item(), row.get_lower_bound()});
    std::sort(items.begin(), items.end(), [](const Item & lhs, const Item & rhs) { return lhs.value < rhs.value; });
    return items;
}

void HistogramFrequentValues::serialize(WriteBuffer & buf) const
{
    const auto items = getSortedItems();
    writeVarUInt(getMaximumError(), buf);
    writeVarUInt(items.size(), buf);
    for (const auto & item : items)
    {
        writeBinary(item.value, buf);
        writeVarUInt(item.count, buf);
    }
}

void HistogramFrequentValues::deserialize(ReadBuffer & buf, UInt64 finite_rows, Float64 finite_min, Float64 finite_max)
{
    UInt64 maximum_error = 0;
    readVarUInt(maximum_error, buf);

    UInt64 item_count = 0;
    readVarUInt(item_count, buf);
    if (item_count > getCapacity() || item_count > finite_rows)
        throw Exception(
            ErrorCodes::ILLEGAL_STATISTICS,
            "Histogram has {} frequent values, but at most {} are possible for {} finite rows",
            item_count,
            getCapacity(),
            finite_rows);

    std::vector<Float64> values(item_count);
    std::vector<UInt64> counts(item_count);
    UInt64 sum = 0;
    for (UInt64 i = 0; i < item_count; ++i)
    {
        readBinary(values[i], buf);
        readVarUInt(counts[i], buf);

        const Float64 value = values[i];
        if (!std::isfinite(value) || isNegativeZero(value))
            throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Histogram frequent value {} is not finite or not canonical", value);
        if (i > 0 && !(values[i - 1] < value))
            throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Histogram frequent values are not strictly increasing");
        if (value < finite_min || value > finite_max)
            throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Histogram frequent value {} is outside of the finite bounds", value);
        if (counts[i] == 0 || counts[i] > finite_rows - sum)
            throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Histogram frequent value {} has an invalid count {}", value, counts[i]);
        sum += counts[i];
    }

    /// Equivalent to `delta <= kll.n` and `delta = 0 <=> kll.n = 0`, because kll.n = F - sum(c) is validated by the caller.
    if (maximum_error > finite_rows - sum || (maximum_error == 0) != (sum == finite_rows))
        throw Exception(
            ErrorCodes::ILLEGAL_STATISTICS,
            "Histogram frequent values have an inconsistent maximum error {} for {} retained of {} finite rows",
            maximum_error,
            sum,
            finite_rows);

    try
    {
        /// The logical state does not depend on the current map size, so use the smallest map that holds the items.
        sketch = Sketch::restore(
            sketch.get_lg_max_map_size(),
            getLgMapSize(item_count),
            finite_rows,
            maximum_error,
            values.data(),
            counts.data(),
            static_cast<UInt32>(item_count));
    }
    catch (const std::exception & e)
    {
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Cannot restore histogram frequent values: {}", e.what());
    }
    retained_weight = sum;
}

namespace
{

std::optional<StatisticsHistogram::Parameters>
parseHistogramParameters(const SingleStatisticsDescription & description, bool require_parameters, String & error)
{
    if (!description.ast || description.ast->as<ASTIdentifier>())
    {
        if (require_parameters)
        {
            error = "Statistics histogram requires syntax histogram(N, M)";
            return std::nullopt;
        }
        return StatisticsHistogram::Parameters{
            StatisticsHistogram::DEFAULT_BUCKETS_FOR_DESERIALIZATION, StatisticsHistogram::DEFAULT_MCV_VALUES_FOR_DESERIALIZATION};
    }

    const auto * function = description.ast->as<ASTFunction>();
    if (!function)
    {
        error = "Statistics histogram requires syntax histogram(N, M)";
        return std::nullopt;
    }

    if (!function->arguments || function->arguments->children.size() != 2)
    {
        error = "Statistics histogram requires exactly two parameters: histogram(N, M), where N is the number of buckets "
                "and M is the number of most common values";
        return std::nullopt;
    }

    auto get_parameter = [&](size_t index, std::string_view name, UInt64 min, UInt64 max) -> std::optional<UInt64>
    {
        const auto * literal = function->arguments->children[index]->as<ASTLiteral>();
        if (!literal || literal->value.getType() != Field::Types::UInt64)
        {
            error = fmt::format("Statistics histogram(N, M): {} must be an unsigned integer literal", name);
            return std::nullopt;
        }
        const UInt64 value = literal->value.safeGet<UInt64>();
        if (value < min || value > max)
        {
            error = fmt::format("Statistics histogram(N, M): {} must be between {} and {}, got {}", name, min, max, value);
            return std::nullopt;
        }
        return value;
    };

    const auto bucket_count
        = get_parameter(0, "the number of buckets N", StatisticsHistogram::MIN_BUCKETS, StatisticsHistogram::MAX_BUCKETS);
    if (!bucket_count)
        return std::nullopt;
    const auto mcv_values
        = get_parameter(1, "the number of most common values M", StatisticsHistogram::MIN_MCV_VALUES, StatisticsHistogram::MAX_MCV_VALUES);
    if (!mcv_values)
        return std::nullopt;
    return StatisticsHistogram::Parameters{*bucket_count, *mcv_values};
}

}

StatisticsHistogram::Parameters
StatisticsHistogram::getParametersFromDescription(const SingleStatisticsDescription & description, bool require_parameters)
{
    String error;
    if (auto parameters = parseHistogramParameters(description, require_parameters, error))
        return *parameters;
    throw Exception::createDeprecated(error, ErrorCodes::BAD_ARGUMENTS);
}

std::optional<StatisticsHistogram::Parameters>
StatisticsHistogram::tryGetParametersFromDescription(const SingleStatisticsDescription & description)
{
    String error;
    return parseHistogramParameters(description, false, error);
}

UInt16 StatisticsHistogram::getSketchK(UInt64 buckets)
{
    return static_cast<UInt16>(std::max<UInt64>(buckets, datasketches::kll_constants::DEFAULT_K));
}

bool StatisticsHistogram::RandomBitGenerator::operator()()
{
    state += 0x9e3779b97f4a7c15ULL;
    UInt64 value = state;
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
    value ^= value >> 31;
    return value >> 63;
}

StatisticsHistogram::StatisticsHistogram(
    const SingleStatisticsDescription & description, const DataTypePtr & data_type_, UInt64 random_seed)
    : IStatistics(description)
    , data_type(removeLowCardinalityAndNullable(data_type_))
    , data_type_name(data_type_->getName())
    , bucket_count(getParametersFromDescription(description, false).bucket_count)
    , random_bit_generator(random_seed ^ (bucket_count * 0x9e3779b97f4a7c15ULL))
    , frequent_values(getParametersFromDescription(description, false).mcv_values)
    , sketch(getSketchK(bucket_count))
{
}

void StatisticsHistogram::invalidateCache()
{
    std::lock_guard lock(cache_mutex);
    cache_valid = false;
    model = {};
}

Float64 StatisticsHistogram::projectToFloat64(const IColumn & column, size_t row) const
{
    /// All supported types are projected to Float64. IPv4 columns do not implement `getFloat64`.
    if (isIPv4(data_type))
    {
        auto converted = StatisticsUtils::tryConvertToFloat64(column[row], data_type);
        if (!converted)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot convert IPv4 value while building histogram statistics");
        return *converted;
    }
    return column.getFloat64(row);
}

void StatisticsHistogram::flushSpill()
{
    if (spill_buffer.empty())
        return;

    /// Canonical KLL input: sorted by value, with the weights of a value spilled by several purges combined.
    std::sort(spill_buffer.begin(), spill_buffer.end(), [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });
    size_t last = 0;
    for (size_t i = 1; i < spill_buffer.size(); ++i)
    {
        if (spill_buffer[i].first == spill_buffer[last].first)
            spill_buffer[last].second += spill_buffer[i].second;
        else
            spill_buffer[++last] = spill_buffer[i];
    }
    spill_buffer.resize(last + 1);

    sketch.update_weighted(spill_buffer.begin(), spill_buffer.end(), random_bit_generator);
    spill_buffer.clear();
}

void StatisticsHistogram::assertInvariants() const
{
#ifndef NDEBUG
    const UInt64 finite_rows = getFiniteCount();
    const UInt64 maximum_error = frequent_values.getMaximumError();
    chassert(frequent_values.getTotalWeight() == finite_rows);
    chassert(frequent_values.getRetainedWeight() + sketch.get_n() == finite_rows);
    chassert(sketch.get_n() >= maximum_error);
    chassert((maximum_error == 0) == (sketch.get_n() == 0));
    chassert(spill_buffer.empty());
#endif
}

void StatisticsHistogram::build(const ColumnPtr & column)
{
    for (size_t row = 0; row < column->size(); ++row)
    {
        if (column->isNullAt(row))
            continue;

        ++non_null_count;
        Float64 value = projectToFloat64(*column, row);

        if (std::isnan(value))
            ++nan_count;
        else if (value == -std::numeric_limits<Float64>::infinity())
            ++negative_inf_count;
        else if (value == std::numeric_limits<Float64>::infinity())
            ++positive_inf_count;
        else
        {
            /// -0.0 and +0.0 are the same value.
            if (value == 0)
                value = 0.0;
            finite_min = finite_min ? std::min(*finite_min, value) : value;
            finite_max = finite_max ? std::max(*finite_max, value) : value;

            /// Flush at every purge, not at the end of the block, so the result does not depend on block boundaries.
            frequent_values.update(value, spill_buffer);
            if (!spill_buffer.empty())
                flushSpill();
        }
    }
    assertInvariants();
    invalidateCache();
}

void StatisticsHistogram::merge(const StatisticsPtr & other_stats)
{
    const auto * other = typeid_cast<const StatisticsHistogram *>(other_stats.get());
    if (!other || data_type_name != other->data_type_name)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot merge incompatible histogram statistics");

    bucket_count = std::min(bucket_count, other->bucket_count);
    non_null_count += other->non_null_count;
    nan_count += other->nan_count;
    negative_inf_count += other->negative_inf_count;
    positive_inf_count += other->positive_inf_count;
    if (other->finite_min)
        finite_min = finite_min ? std::min(*finite_min, *other->finite_min) : other->finite_min;
    if (other->finite_max)
        finite_max = finite_max ? std::max(*finite_max, *other->finite_max) : other->finite_max;

    /// Merge the residuals first; purges while merging the frequent values spill into the merged residual.
    /// The frequent values keep the size of this (accumulating) statistic.
    sketch.merge(other->sketch, random_bit_generator);
    frequent_values.merge(other->frequent_values, spill_buffer);
    flushSpill();

    assertInvariants();
    invalidateCache();
}

void StatisticsHistogram::serialize(WriteBuffer & buf)
{
    std::lock_guard lock(cache_mutex);

    writeBinary(HISTOGRAM_PAYLOAD_VERSION, buf);
    writeVarUInt(bucket_count, buf);
    writeVarUInt(frequent_values.getRequestedValues(), buf);
    writeVarUInt(non_null_count, buf);
    writeVarUInt(nan_count, buf);
    writeVarUInt(negative_inf_count, buf);
    writeVarUInt(positive_inf_count, buf);

    const UInt8 has_finite_values = finite_min && finite_max ? 1 : 0;
    writeBinary(has_finite_values, buf);
    if (has_finite_values)
    {
        writeBinary(*finite_min, buf);
        writeBinary(*finite_max, buf);
    }

    writeBinary(random_bit_generator.getState(), buf);

    frequent_values.serialize(buf);

    /// Native KLL serialization preserves retained levels and weights exactly. Sort level zero
    /// first so serialization is canonical and cannot depend on whether a query built KLL's lazy view.
    [[maybe_unused]] const auto sorted_view = sketch.get_sorted_view();
    const auto bytes = sketch.serialize();
    writeVarUInt(bytes.size(), buf);
    buf.write(reinterpret_cast<const char *>(bytes.data()), bytes.size());
}

void StatisticsHistogram::deserialize(ReadBuffer & buf, StatisticsFileVersion /*version*/)
{
    UInt8 payload_version = 0;
    readBinary(payload_version, buf);
    if (payload_version != HISTOGRAM_PAYLOAD_VERSION)
    {
        /// Older payloads were written only by development builds and newer ones cannot be interpreted.
        /// Skip the payload instead of failing, `ColumnStatistics::deserialize` drops the statistic.
        loaded = false;
        return;
    }

    UInt64 stored_bucket_count = 0;
    readVarUInt(stored_bucket_count, buf);
    if (stored_bucket_count < MIN_BUCKETS || stored_bucket_count > MAX_BUCKETS)
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Invalid histogram bucket count {}", stored_bucket_count);

    UInt64 stored_mcv_values = 0;
    readVarUInt(stored_mcv_values, buf);
    if (stored_mcv_values < MIN_MCV_VALUES || stored_mcv_values > MAX_MCV_VALUES)
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Invalid histogram number of most common values {}", stored_mcv_values);

    readVarUInt(non_null_count, buf);
    readVarUInt(nan_count, buf);
    readVarUInt(negative_inf_count, buf);
    readVarUInt(positive_inf_count, buf);

    UInt8 has_finite_values = 0;
    readBinary(has_finite_values, buf);
    if (has_finite_values > 1)
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Invalid finite-value flag in histogram payload");
    if (has_finite_values)
    {
        Float64 stored_min = 0;
        Float64 stored_max = 0;
        readBinary(stored_min, buf);
        readBinary(stored_max, buf);
        if (!std::isfinite(stored_min) || !std::isfinite(stored_max) || stored_min > stored_max || isNegativeZero(stored_min)
            || isNegativeZero(stored_max))
            throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Invalid finite bounds in histogram payload");
        finite_min = stored_min;
        finite_max = stored_max;
    }
    else
    {
        finite_min.reset();
        finite_max.reset();
    }

    if (nan_count > non_null_count || negative_inf_count > non_null_count - nan_count
        || positive_inf_count > non_null_count - nan_count - negative_inf_count)
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Histogram row-count invariant is violated");
    const UInt64 finite_rows = getFiniteCount();
    if (finite_rows > datasketches::kll_constants::MAX_N)
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Histogram finite row count is too large: {}", finite_rows);
    if ((finite_rows != 0) != static_cast<bool>(has_finite_values))
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Histogram finite bounds do not match its row count");

    UInt64 stored_random_state = 0;
    readBinary(stored_random_state, buf);

    HistogramFrequentValues restored_frequent_values(stored_mcv_values);
    restored_frequent_values.deserialize(buf, finite_rows, finite_min.value_or(0.0), finite_max.value_or(0.0));

    UInt64 serialized_size = 0;
    readVarUInt(serialized_size, buf);
    if (serialized_size > MAX_SERIALIZED_SKETCH_SIZE)
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Serialized histogram KLL is too large: {}", serialized_size);

    Sketch::vector_bytes bytes;
    bytes.resize(serialized_size);
    buf.readStrict(reinterpret_cast<char *>(bytes.data()), serialized_size);

    Sketch restored;
    try
    {
        restored = Sketch::deserialize(bytes.data(), bytes.size());
    }
    catch (const std::exception & e)
    {
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Cannot deserialize histogram KLL: {}", e.what());
    }

    if (restored.get_k() < getSketchK(stored_bucket_count) || restored.get_k() > getSketchK(MAX_BUCKETS))
        throw Exception(
            ErrorCodes::ILLEGAL_STATISTICS,
            "Histogram KLL parameter {} is inconsistent with {} buckets",
            restored.get_k(),
            stored_bucket_count);
    const UInt64 residual_rows = finite_rows - restored_frequent_values.getRetainedWeight();
    if (restored.get_n() != residual_rows)
        throw Exception(
            ErrorCodes::ILLEGAL_STATISTICS,
            "Histogram row-count invariant is violated: expected {} residual rows, native sketch {}",
            residual_rows,
            restored.get_n());
    if (!restored.is_empty()
        && (!std::isfinite(restored.get_min_item()) || !std::isfinite(restored.get_max_item()) || restored.get_min_item() < *finite_min
            || restored.get_max_item() > *finite_max))
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Histogram KLL finite bounds are inconsistent");

    bucket_count = stored_bucket_count;
    frequent_values = std::move(restored_frequent_values);
    sketch = std::move(restored);
    random_bit_generator = RandomBitGenerator(stored_random_state);
    loaded = true;
    assertInvariants();
    invalidateCache();
}

UInt64 StatisticsHistogram::Model::residualWeightAt(Float64 value) const
{
    auto it = std::lower_bound(residual_values.begin(), residual_values.end(), value);
    if (it == residual_values.end() || *it != value)
        return 0;
    const size_t index = it - residual_values.begin();
    return residual_cumulative[index] - (index == 0 ? 0 : residual_cumulative[index - 1]);
}

UInt64 StatisticsHistogram::Model::residualCountBelow(Float64 value, bool inclusive) const
{
    auto it = inclusive ? std::upper_bound(residual_values.begin(), residual_values.end(), value)
                        : std::lower_bound(residual_values.begin(), residual_values.end(), value);
    if (it == residual_values.begin())
        return 0;
    return residual_cumulative[it - residual_values.begin() - 1];
}

UInt64 StatisticsHistogram::Model::frequentCountBelow(Float64 value, bool inclusive) const
{
    auto it = inclusive ? std::upper_bound(mcv_values.begin(), mcv_values.end(), value)
                        : std::lower_bound(mcv_values.begin(), mcv_values.end(), value);
    return mcv_prefix[it - mcv_values.begin()];
}

Float64 StatisticsHistogram::Model::interpolateResidualBelow(Float64 value, bool inclusive) const
{
    const Float64 residual_rows = static_cast<Float64>(residual_total);
    if (residual_total == 0 || bucket_bounds.empty())
        return 0.0;

    auto upper = std::lower_bound(bucket_bounds.begin(), bucket_bounds.end(), value);
    if (upper == bucket_bounds.begin())
    {
        if (upper != bucket_bounds.end() && *upper == value)
            return inclusive ? counts_less_or_equal.front() : counts_less.front();
        return 0.0;
    }
    if (upper == bucket_bounds.end())
        return residual_rows;

    const size_t upper_index = static_cast<size_t>(upper - bucket_bounds.begin());
    if (*upper == value)
        return inclusive ? counts_less_or_equal[upper_index] : counts_less[upper_index];

    const size_t lower_index = upper_index - 1;
    const Float64 lower_bound = bucket_bounds[lower_index];
    const Float64 upper_bound = bucket_bounds[upper_index];
    const Float64 lower_count = counts_less_or_equal[lower_index];
    const Float64 upper_count = std::max(lower_count, counts_less[upper_index]);
    const long double numerator = static_cast<long double>(value) - static_cast<long double>(lower_bound);
    const long double denominator = static_cast<long double>(upper_bound) - static_cast<long double>(lower_bound);
    const Float64 fraction = static_cast<Float64>(std::clamp(numerator / denominator, 0.0L, 1.0L));
    return clampCount(lower_count + fraction * (upper_count - lower_count), residual_rows);
}

const StatisticsHistogram::Model & StatisticsHistogram::getModel() const
{
    std::lock_guard lock(cache_mutex);
    if (cache_valid)
        return model;

    Model result;
    for (const auto & item : frequent_values.getSortedItems())
    {
        result.mcv_values.push_back(item.value);
        result.mcv_counts.push_back(item.count);
    }
    result.mcv_kll_weights.assign(result.mcv_values.size(), 0);

    /// Split the KLL sorted view into the weight at MCVs and the residual values.
    /// Building the view sorts KLL level zero, which is why this happens under the mutex.
    if (!sketch.is_empty())
    {
        const auto view = sketch.get_sorted_view();
        for (auto it = view.begin(); it != view.end(); ++it)
        {
            const Float64 value = (*it).first;
            const UInt64 weight = it.get_weight();
            auto mcv = std::lower_bound(result.mcv_values.begin(), result.mcv_values.end(), value);
            if (mcv != result.mcv_values.end() && *mcv == value)
            {
                result.mcv_kll_weights[mcv - result.mcv_values.begin()] += weight;
                continue;
            }

            result.residual_total += weight;
            if (!result.residual_values.empty() && result.residual_values.back() == value)
                result.residual_cumulative.back() = result.residual_total;
            else
            {
                result.residual_values.push_back(value);
                result.residual_cumulative.push_back(result.residual_total);
            }
        }
    }

    result.mcv_prefix.assign(result.mcv_values.size() + 1, 0);
    for (size_t i = 0; i < result.mcv_values.size(); ++i)
        result.mcv_prefix[i + 1] = result.mcv_prefix[i] + result.mcv_counts[i] + result.mcv_kll_weights[i];

    if (result.residual_total != 0)
    {
        /// The exact KLL bounds are the residual bounds unless they are MCVs, in which case the residual
        /// bound is not known exactly and the closest retained residual value is used.
        auto is_mcv = [&](Float64 value) { return std::binary_search(result.mcv_values.begin(), result.mcv_values.end(), value); };
        const Float64 residual_min = is_mcv(sketch.get_min_item()) ? result.residual_values.front() : sketch.get_min_item();
        const Float64 residual_max = is_mcv(sketch.get_max_item()) ? result.residual_values.back() : sketch.get_max_item();

        result.bucket_bounds.reserve(bucket_count + 1);
        result.counts_less.reserve(bucket_count + 1);
        result.counts_less_or_equal.reserve(bucket_count + 1);
        for (UInt64 i = 0; i <= bucket_count; ++i)
        {
            Float64 boundary = 0;
            if (i == 0)
                boundary = residual_min;
            else if (i == bucket_count)
                boundary = residual_max;
            else
            {
                /// Same semantics as inclusive `get_quantile`: the first value whose cumulative weight reaches the rank.
                const Float64 rank = static_cast<Float64>(i) / static_cast<Float64>(bucket_count);
                const auto target = static_cast<UInt64>(std::ceil(rank * static_cast<Float64>(result.residual_total)));
                auto it = std::lower_bound(result.residual_cumulative.begin(), result.residual_cumulative.end(), target);
                if (it == result.residual_cumulative.end())
                    --it;
                boundary = result.residual_values[it - result.residual_cumulative.begin()];
            }
            if (!result.bucket_bounds.empty() && boundary == result.bucket_bounds.back())
                continue;

            result.bucket_bounds.push_back(boundary);
            result.counts_less.push_back(static_cast<Float64>(result.residualCountBelow(boundary, false)));
            result.counts_less_or_equal.push_back(static_cast<Float64>(result.residualCountBelow(boundary, true)));
        }
    }

    model = std::move(result);
    cache_valid = true;
    return model;
}

const std::vector<Float64> & StatisticsHistogram::getBucketBounds() const
{
    return getModel().bucket_bounds;
}

Float64 StatisticsHistogram::estimateFiniteBelow(Float64 value, bool inclusive) const
{
    const UInt64 finite_rows = getFiniteCount();
    if (finite_rows == 0)
        return 0.0;

    const auto & current_model = getModel();
    const Float64 frequent = static_cast<Float64>(current_model.frequentCountBelow(value, inclusive));
    return clampCount(frequent + current_model.interpolateResidualBelow(value, inclusive), static_cast<Float64>(finite_rows));
}

std::optional<Float64> StatisticsHistogram::estimateEqual(const Field & val) const
{
    auto converted = StatisticsUtils::tryConvertToFloat64(val, data_type);
    if (!converted)
        return std::nullopt;
    if (std::isnan(*converted))
        return 0.0;
    if (*converted == -std::numeric_limits<Float64>::infinity())
        return static_cast<Float64>(negative_inf_count);
    if (*converted == std::numeric_limits<Float64>::infinity())
        return static_cast<Float64>(positive_inf_count);

    const UInt64 finite_rows = getFiniteCount();
    if (finite_rows == 0)
        return 0.0;

    const Float64 value = *converted == 0 ? 0.0 : *converted;
    const auto & current_model = getModel();
    const UInt64 maximum_error = frequent_values.getMaximumError();

    /// A tracked value: its counter is a lower bound, and the evicted share is at most delta.
    auto mcv = std::lower_bound(current_model.mcv_values.begin(), current_model.mcv_values.end(), value);
    if (mcv != current_model.mcv_values.end() && *mcv == value)
    {
        const size_t index = mcv - current_model.mcv_values.begin();
        return static_cast<Float64>(current_model.mcv_counts[index] + std::min(current_model.mcv_kll_weights[index], maximum_error));
    }

    /// An untracked value occurs at most delta times.
    if (maximum_error == 0 || value < *finite_min || value > *finite_max)
        return 0.0;

    /// The residual KLL sketch retains all its values exactly unless it is in estimation mode. Otherwise abstain unless the
    /// observed mass is larger than the sketch's double-sided rank error; retained-item weight alone is not evidence that
    /// an arbitrary value is frequent. Weighted spills put the sketch into estimation mode even without compaction,
    /// which makes this test conservative.
    const UInt64 mass = current_model.residualWeightAt(value);
    const Float64 rank_error = sketch.get_normalized_rank_error(true) * static_cast<Float64>(sketch.get_n());
    if (!sketch.is_estimation_mode() || static_cast<Float64>(mass) > rank_error)
        return static_cast<Float64>(std::min(mass, maximum_error));
    return std::nullopt;
}

StatisticsHistogram::NonFrequentSummary StatisticsHistogram::getNonFrequentSummary() const
{
    NonFrequentSummary summary;
    summary.finite_rows = static_cast<Float64>(getFiniteCount());
    summary.maximum_error = frequent_values.getMaximumError();
    const auto & current_model = getModel();
    for (size_t i = 0; i < current_model.mcv_values.size(); ++i)
    {
        if (current_model.mcv_counts[i] <= summary.maximum_error)
            continue;
        summary.frequent_rows += static_cast<Float64>(current_model.mcv_counts[i] + current_model.mcv_kll_weights[i]);
        ++summary.frequent_values;
    }
    return summary;
}

std::optional<Float64> StatisticsHistogram::estimateLess(const Field & val) const
{
    auto converted = StatisticsUtils::tryConvertToFloat64(val, data_type);
    if (!converted)
        return std::nullopt;
    if (std::isnan(*converted))
        return 0.0;
    if (*converted == -std::numeric_limits<Float64>::infinity())
        return 0.0;
    if (*converted == std::numeric_limits<Float64>::infinity())
        return static_cast<Float64>(negative_inf_count + getFiniteCount());
    return static_cast<Float64>(negative_inf_count) + estimateFiniteBelow(*converted, false);
}

std::optional<Float64> StatisticsHistogram::estimateLessOrEqual(const Field & val) const
{
    auto converted = StatisticsUtils::tryConvertToFloat64(val, data_type);
    if (!converted)
        return std::nullopt;
    if (std::isnan(*converted))
        return 0.0;
    if (*converted == -std::numeric_limits<Float64>::infinity())
        return static_cast<Float64>(negative_inf_count);
    if (*converted == std::numeric_limits<Float64>::infinity())
        return static_cast<Float64>(negative_inf_count + getFiniteCount() + positive_inf_count);
    return static_cast<Float64>(negative_inf_count) + estimateFiniteBelow(*converted, true);
}

Float64 StatisticsHistogram::comparableCount() const
{
    return static_cast<Float64>(non_null_count - nan_count);
}

std::optional<Float64> StatisticsHistogram::estimateGreater(const Field & val) const
{
    auto converted = StatisticsUtils::tryConvertToFloat64(val, data_type);
    if (!converted)
        return std::nullopt;
    if (std::isnan(*converted) || *converted == std::numeric_limits<Float64>::infinity())
        return 0.0;
    const Float64 finite_rows = static_cast<Float64>(getFiniteCount());
    if (*converted == -std::numeric_limits<Float64>::infinity())
        return finite_rows + static_cast<Float64>(positive_inf_count);
    return static_cast<Float64>(positive_inf_count) + finite_rows - estimateFiniteBelow(*converted, true);
}

std::optional<Float64> StatisticsHistogram::estimateGreaterOrEqual(const Field & val) const
{
    auto converted = StatisticsUtils::tryConvertToFloat64(val, data_type);
    if (!converted)
        return std::nullopt;
    if (std::isnan(*converted))
        return 0.0;
    if (*converted == -std::numeric_limits<Float64>::infinity())
        return comparableCount();
    if (*converted == std::numeric_limits<Float64>::infinity())
        return static_cast<Float64>(positive_inf_count);
    return static_cast<Float64>(positive_inf_count) + static_cast<Float64>(getFiniteCount()) - estimateFiniteBelow(*converted, false);
}

String StatisticsHistogram::getNameForLogs() const
{
    return fmt::format("Histogram({}, {})", bucket_count, frequent_values.getRequestedValues());
}

bool StatisticsHistogram::isCompatibleWith(const IStatistics & other) const
{
    /// Different parameters are fine: the merged statistic keeps the smaller bucket count and its own summary size.
    const auto * histogram = typeid_cast<const StatisticsHistogram *>(&other);
    return histogram && data_type_name == histogram->data_type_name;
}

bool histogramStatisticsValidator(const SingleStatisticsDescription & description, const DataTypePtr & data_type)
{
    const bool is_internal_placeholder = !description.ast || description.ast->as<ASTIdentifier>();
    StatisticsHistogram::getParametersFromDescription(description, !is_internal_placeholder);
    const DataTypePtr inner_type = removeLowCardinalityAndNullable(data_type);
    return inner_type->isValueRepresentedByNumber();
}

StatisticsPtr histogramStatisticsCreator(
    const SingleStatisticsDescription & description, const DataTypePtr & data_type, UInt64 random_seed)
{
    return std::make_shared<StatisticsHistogram>(description, data_type, random_seed);
}

}

#endif
