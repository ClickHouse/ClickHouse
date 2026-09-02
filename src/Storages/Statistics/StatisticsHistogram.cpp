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
#include <map>

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

constexpr UInt8 HISTOGRAM_PAYLOAD_VERSION = 1;
constexpr UInt64 MAX_RETAINED_ITEMS = 65536;

Float64 clampCount(Float64 value, Float64 upper_bound)
{
    if (!std::isfinite(value))
        return value > 0 ? upper_bound : 0.0;
    return std::clamp(value, 0.0, upper_bound);
}

}

UInt64 StatisticsHistogram::getBucketCountFromDescription(const SingleStatisticsDescription & description, bool require_parameter)
{
    if (!description.ast || description.ast->as<ASTIdentifier>())
    {
        if (require_parameter)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Statistics histogram requires exactly one bucket-count parameter");
        return DEFAULT_BUCKETS_FOR_DESERIALIZATION;
    }

    const auto * function = description.ast->as<ASTFunction>();
    if (!function)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Statistics histogram requires syntax histogram(N)");

    if (!function->arguments || function->arguments->children.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Statistics histogram requires exactly one bucket-count parameter");

    const auto * literal = function->arguments->children.front()->as<ASTLiteral>();
    if (!literal || literal->value.getType() != Field::Types::UInt64)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Statistics histogram bucket count must be an unsigned integer literal");

    const UInt64 buckets = literal->value.safeGet<UInt64>();
    if (buckets < MIN_BUCKETS || buckets > MAX_BUCKETS)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Statistics histogram bucket count must be between {} and {}", MIN_BUCKETS, MAX_BUCKETS);
    return buckets;
}

UInt16 StatisticsHistogram::getSketchK(UInt64 buckets)
{
    return static_cast<UInt16>(std::max<UInt64>(buckets, datasketches::kll_constants::DEFAULT_K));
}

StatisticsHistogram::StatisticsHistogram(const SingleStatisticsDescription & description, const DataTypePtr & data_type_)
    : IStatistics(description)
    , data_type(removeLowCardinalityAndNullable(data_type_))
    , data_type_name(data_type_->getName())
    , bucket_count(getBucketCountFromDescription(description, false))
    , sketch(getSketchK(bucket_count))
{
}

void StatisticsHistogram::invalidateCache()
{
    cache_valid = false;
    bucket_bounds.clear();
    counts_less.clear();
    counts_less_or_equal.clear();
}

void StatisticsHistogram::build(const ColumnPtr & column)
{
    for (size_t row = 0; row < column->size(); ++row)
    {
        if (column->isNullAt(row))
            continue;

        ++non_null_count;
        Float64 value = 0;
        if (isIPv4(data_type))
        {
            auto converted = StatisticsUtils::tryConvertToFloat64((*column)[row], data_type);
            if (!converted)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot convert IPv4 value while building histogram statistics");
            value = *converted;
        }
        else
        {
            value = column->getFloat64(row);
        }

        if (std::isnan(value))
            ++nan_count;
        else if (value == -std::numeric_limits<Float64>::infinity())
            ++negative_inf_count;
        else if (value == std::numeric_limits<Float64>::infinity())
            ++positive_inf_count;
        else
        {
            sketch.update(value);
            finite_min = finite_min ? std::min(*finite_min, value) : value;
            finite_max = finite_max ? std::max(*finite_max, value) : value;
        }
    }
    invalidateCache();
}

void StatisticsHistogram::merge(const StatisticsPtr & other_stats)
{
    const auto * other = typeid_cast<const StatisticsHistogram *>(other_stats.get());
    if (!other || data_type_name != other->data_type_name)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot merge incompatible histogram statistics");

    sketch.merge(other->sketch);
    bucket_count = std::min(bucket_count, other->bucket_count);
    non_null_count += other->non_null_count;
    nan_count += other->nan_count;
    negative_inf_count += other->negative_inf_count;
    positive_inf_count += other->positive_inf_count;
    if (other->finite_min)
        finite_min = finite_min ? std::min(*finite_min, *other->finite_min) : other->finite_min;
    if (other->finite_max)
        finite_max = finite_max ? std::max(*finite_max, *other->finite_max) : other->finite_max;
    invalidateCache();
}

void StatisticsHistogram::serialize(WriteBuffer & buf)
{
    writeBinary(HISTOGRAM_PAYLOAD_VERSION, buf);
    writeVarUInt(bucket_count, buf);
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

    /// A heterogeneous merge may keep the accumulator's larger configured k
    /// while KLL records the smaller source k only as internal min_k. Persist
    /// the conservative k implied by the surviving bucket resolution.
    writeVarUInt(getSketchK(bucket_count), buf);
    const auto sorted_view = sketch.get_sorted_view();
    writeVarUInt(sorted_view.size(), buf);
    for (auto it = sorted_view.begin(); it != sorted_view.end(); ++it)
    {
        const auto item = *it;
        writeBinary(item.first, buf);
        writeVarUInt(it.get_weight(), buf);
    }
}

void StatisticsHistogram::deserialize(ReadBuffer & buf, StatisticsFileVersion /*version*/)
{
    UInt8 payload_version = 0;
    readBinary(payload_version, buf);
    if (payload_version != HISTOGRAM_PAYLOAD_VERSION)
        throw Exception(
            ErrorCodes::ILLEGAL_STATISTICS, "Unsupported histogram statistics payload version {}", static_cast<UInt64>(payload_version));

    UInt64 stored_bucket_count = 0;
    readVarUInt(stored_bucket_count, buf);
    if (stored_bucket_count < MIN_BUCKETS || stored_bucket_count > MAX_BUCKETS)
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Invalid histogram bucket count {}", stored_bucket_count);

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
        if (!std::isfinite(stored_min) || !std::isfinite(stored_max) || stored_min > stored_max)
            throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Invalid finite bounds in histogram payload");
        finite_min = stored_min;
        finite_max = stored_max;
    }
    else
    {
        finite_min.reset();
        finite_max.reset();
    }

    UInt64 stored_k = 0;
    readVarUInt(stored_k, buf);
    if (stored_k < datasketches::kll_constants::MIN_K || stored_k > datasketches::kll_constants::MAX_K)
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Invalid histogram KLL parameter {}", stored_k);

    UInt64 retained_items = 0;
    readVarUInt(retained_items, buf);
    if (retained_items > MAX_RETAINED_ITEMS)
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Histogram KLL has too many retained items: {}", retained_items);

    bucket_count = stored_bucket_count;
    if (stored_k != getSketchK(bucket_count))
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Histogram KLL parameter {} does not match {} buckets", stored_k, bucket_count);

    if (nan_count > non_null_count || negative_inf_count > non_null_count - nan_count
        || positive_inf_count > non_null_count - nan_count - negative_inf_count)
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Histogram row-count invariant is violated");
    const UInt64 special_count = nan_count + negative_inf_count + positive_inf_count;
    const UInt64 expected_finite_count = non_null_count - special_count;
    if ((expected_finite_count != 0) != static_cast<bool>(has_finite_values))
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Histogram finite bounds do not match its row count");

    const UInt64 max_retained_for_state = expected_finite_count == 0
        ? 0
        : static_cast<UInt64>(Sketch::get_max_serialized_size_bytes(getSketchK(MAX_BUCKETS), expected_finite_count) / sizeof(Float64));
    if (retained_items > max_retained_for_state)
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Histogram KLL retained-item count is inconsistent with k and row count");

    std::map<UInt64, std::vector<Float64>> items_by_weight;
    UInt64 restored_finite_count = 0;
    for (UInt64 item = 0; item < retained_items; ++item)
    {
        Float64 value = 0;
        UInt64 weight = 0;
        readBinary(value, buf);
        readVarUInt(weight, buf);
        if (!std::isfinite(value) || !std::has_single_bit(weight) || weight > expected_finite_count - restored_finite_count
            || value < *finite_min || value > *finite_max)
            throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Invalid weighted item in histogram KLL payload");

        items_by_weight[weight].push_back(value);
        restored_finite_count += weight;
    }

    sketch = Sketch(static_cast<UInt16>(stored_k));
    for (const auto & [weight, values] : items_by_weight)
    {
        Sketch weighted(static_cast<UInt16>(stored_k));
        for (const Float64 value : values)
            weighted.update(value);

        UInt64 represented_weight = 1;
        while (represented_weight < weight)
        {
            Sketch copy(weighted);
            weighted.merge(copy);
            represented_weight *= 2;
        }
        sketch.merge(weighted);
    }

    if (restored_finite_count != expected_finite_count || sketch.get_n() != expected_finite_count)
        throw Exception(
            ErrorCodes::ILLEGAL_STATISTICS,
            "Histogram row-count invariant is violated: expected {}, retained weights {}, reconstructed sketch {}",
            expected_finite_count,
            restored_finite_count,
            sketch.get_n());
    invalidateCache();
}

void StatisticsHistogram::buildCache() const
{
    if (cache_valid)
        return;

    bucket_bounds.clear();
    counts_less.clear();
    counts_less_or_equal.clear();

    const UInt64 finite_rows = sketch.get_n();
    if (finite_rows == 0)
    {
        cache_valid = true;
        return;
    }

    bucket_bounds.reserve(bucket_count + 1);
    counts_less.reserve(bucket_count + 1);
    counts_less_or_equal.reserve(bucket_count + 1);

    for (UInt64 i = 0; i <= bucket_count; ++i)
    {
        const Float64 rank = static_cast<Float64>(i) / static_cast<Float64>(bucket_count);
        const Float64 boundary = i == 0 ? *finite_min : (i == bucket_count ? *finite_max : sketch.get_quantile(rank));
        if (!bucket_bounds.empty() && boundary == bucket_bounds.back())
            continue;

        bucket_bounds.push_back(boundary);
        const Float64 finite_rows_as_float = static_cast<Float64>(finite_rows);
        counts_less.push_back(clampCount(sketch.get_rank(boundary, false) * finite_rows_as_float, finite_rows_as_float));
        counts_less_or_equal.push_back(clampCount(sketch.get_rank(boundary, true) * finite_rows_as_float, finite_rows_as_float));
    }

    cache_valid = true;
}

const std::vector<Float64> & StatisticsHistogram::getBucketBounds() const
{
    buildCache();
    return bucket_bounds;
}

Float64 StatisticsHistogram::estimateFiniteLess(Float64 value, bool inclusive) const
{
    const Float64 finite_rows = static_cast<Float64>(sketch.get_n());
    if (finite_rows == 0)
        return 0.0;

    buildCache();
    auto upper = std::lower_bound(bucket_bounds.begin(), bucket_bounds.end(), value);
    if (upper == bucket_bounds.begin())
    {
        if (upper != bucket_bounds.end() && *upper == value)
            return inclusive ? counts_less_or_equal.front() : counts_less.front();
        return 0.0;
    }
    if (upper == bucket_bounds.end())
        return finite_rows;

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
    return clampCount(lower_count + fraction * (upper_count - lower_count), finite_rows);
}

std::optional<Float64> StatisticsHistogram::estimateEqual(const Field & val) const
{
    /// KLL directly exposes inclusive and exclusive rank at retained values.
    auto converted = StatisticsUtils::tryConvertToFloat64(val, data_type);
    if (!converted)
        return std::nullopt;
    if (std::isnan(*converted))
        return 0.0;
    if (*converted == -std::numeric_limits<Float64>::infinity())
        return static_cast<Float64>(negative_inf_count);
    if (*converted == std::numeric_limits<Float64>::infinity())
        return static_cast<Float64>(positive_inf_count);

    const UInt64 finite_rows = sketch.get_n();
    if (finite_rows == 0)
        return 0.0;

    const Float64 finite_rows_as_float = static_cast<Float64>(finite_rows);
    const Float64 mass
        = clampCount((sketch.get_rank(*converted, true) - sketch.get_rank(*converted, false)) * finite_rows_as_float, finite_rows_as_float);

    /// Below KLL's capacity all input values are retained and equality mass is
    /// exact. In estimation mode, abstain unless the observed rank jump is
    /// larger than the sketch's double-sided rank error; retained-item weight
    /// alone is not evidence that an arbitrary value is frequent.
    if (!sketch.is_estimation_mode() || mass > sketch.get_normalized_rank_error(true) * finite_rows_as_float)
        return mass;
    return std::nullopt;
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
        return static_cast<Float64>(negative_inf_count + sketch.get_n());
    return static_cast<Float64>(negative_inf_count) + estimateFiniteLess(*converted, false);
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
        return static_cast<Float64>(negative_inf_count + sketch.get_n() + positive_inf_count);
    return static_cast<Float64>(negative_inf_count) + estimateFiniteLess(*converted, true);
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
    if (*converted == -std::numeric_limits<Float64>::infinity())
        return static_cast<Float64>(sketch.get_n() + positive_inf_count);
    return static_cast<Float64>(positive_inf_count) + static_cast<Float64>(sketch.get_n()) - estimateFiniteLess(*converted, true);
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
    return static_cast<Float64>(positive_inf_count) + static_cast<Float64>(sketch.get_n()) - estimateFiniteLess(*converted, false);
}

String StatisticsHistogram::getNameForLogs() const
{
    return "Histogram(" + std::to_string(bucket_count) + ")";
}

bool StatisticsHistogram::isCompatibleWith(const IStatistics & other) const
{
    const auto * histogram = typeid_cast<const StatisticsHistogram *>(&other);
    return histogram && data_type_name == histogram->data_type_name;
}

bool histogramStatisticsValidator(const SingleStatisticsDescription & description, const DataTypePtr & data_type)
{
    const bool is_internal_placeholder = description.ast && description.ast->as<ASTIdentifier>();
    StatisticsHistogram::getBucketCountFromDescription(description, !is_internal_placeholder);
    const DataTypePtr inner_type = removeLowCardinalityAndNullable(data_type);
    return inner_type->isValueRepresentedByNumber();
}

StatisticsPtr histogramStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type)
{
    return std::make_shared<StatisticsHistogram>(description, data_type);
}

}

#endif
