#include <list>

#include <Columns/ColumnSet.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/MergeRuntimeFiltersStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/typeid_cast.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <optional>

namespace DB
{
void registerBuildRuntimeFilterStep(QueryPlanStepRegistry & registry);
}

using namespace DB;

namespace DB::ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int SUPPORT_IS_DISABLED;
}

namespace
{

constexpr UInt64 BLOOM_BYTES = 4096;
constexpr UInt64 EXACT_VALUES_LIMIT = 64;
constexpr UInt64 HASH_FUNCTIONS = 3;

RuntimeFilterGeometry makeGeometry(UInt64 bloom_bytes = BLOOM_BYTES)
{
    return RuntimeFilterGeometry{
        .exact_values_limit = EXACT_VALUES_LIMIT,
        .exact_bytes_limit = bloom_bytes,
        .bloom_filter_bytes = bloom_bytes,
        .bloom_filter_hash_functions = HASH_FUNCTIONS,
        .pass_ratio_threshold_for_disabling = 1.0,
        .blocks_to_skip_before_reenabling = 0,
        .max_ratio_of_set_bits_in_bloom_filter = 1.0,
    };
}

RuntimeFilterConfig makeConfig(const RuntimeFilterGeometry & geometry)
{
    return RuntimeFilterConfig{geometry.pass_ratio_threshold_for_disabling, geometry.blocks_to_skip_before_reenabling};
}

std::unique_ptr<AdaptiveSetRuntimeFilter> makeFilter()
{
    return std::make_unique<AdaptiveSetRuntimeFilter>(
        std::make_shared<DataTypeUInt64>(),
        makeGeometry(),
        /*distinct_keys_hint_=*/std::nullopt,
        /*distinct_keys_hint_matches_filter_key_=*/false);
}

/// A partial in flight is not wrapped in a `RuntimeFilter`, so it has no evaluation state of its
/// own; lend it a private one for the call, the way the transport transforms do.
void finishInsert(AdaptiveSetRuntimeFilter & filter, const RuntimeFilterGeometry & geometry = makeGeometry())
{
    RuntimeFilterEvaluationState evaluation_state(makeConfig(geometry));
    filter.finishInsert(evaluation_state);
}

/// Publishes a completed union the way `MergeRuntimeFiltersTransform` does in `RegisterUnion` mode.
UniqueRuntimeFilterPtr publish(AdaptiveSetRuntimeFilter && filter)
{
    return std::make_unique<RuntimeFilter>(/*filters_to_merge_=*/0, makeConfig(makeGeometry()), std::move(filter));
}

ColumnPtr makeColumn(UInt64 from, UInt64 to)
{
    auto column = ColumnUInt64::create();
    for (UInt64 i = from; i < to; ++i)
        column->insertValue(i);
    return column;
}

String serializeToString(AdaptiveSetRuntimeFilter & filter)
{
    WriteBufferFromOwnString out;
    filter.serialize(out);
    return out.str();
}

std::unique_ptr<AdaptiveSetRuntimeFilter> deserializeFromString(const String & data, UInt64 bloom_bytes = BLOOM_BYTES)
{
    ReadBufferFromString in(data);
    return std::make_unique<AdaptiveSetRuntimeFilter>(
        AdaptiveSetRuntimeFilter::deserialize(in, std::make_shared<DataTypeUInt64>(), makeGeometry(bloom_bytes)));
}

std::vector<bool> maskToVector(const ColumnPtr & mask, size_t rows)
{
    auto full = mask->convertToFullColumnIfConst();
    std::vector<bool> found(rows);
    for (size_t i = 0; i < found.size(); ++i)
        found[i] = full->getUInt(i) != 0;
    return found;
}

std::vector<bool> probe(const AdaptiveSetRuntimeFilter & filter, UInt64 from, UInt64 to)
{
    std::optional<size_t> rows_passed;
    return maskToVector(filter.find({makeColumn(from, to), std::make_shared<DataTypeUInt64>(), "probe"}, rows_passed), to - from);
}

std::vector<bool> probe(const RuntimeFilter & filter, UInt64 from, UInt64 to)
{
    return maskToVector(filter.find({makeColumn(from, to), std::make_shared<DataTypeUInt64>(), "probe"}), to - from);
}

/// The second byte of the state is the phase tag: 0 = exact values, 1 = bloom filter.
bool isBloomState(const String & state)
{
    return state.size() > 1 && state[1] == 1;
}

}

TEST(RuntimeFilterSerialization, RoundTripExactValues)
{
    auto filter = makeFilter();
    filter->insert(makeColumn(0, 10));

    const String state = serializeToString(*filter);
    EXPECT_FALSE(isBloomState(state));
    auto restored = deserializeFromString(state);

    finishInsert(*filter);
    finishInsert(*restored);

    EXPECT_EQ(probe(*filter, 0, 20), probe(*restored, 0, 20));
    EXPECT_EQ(probe(*restored, 0, 10), std::vector<bool>(10, true));
    EXPECT_EQ(probe(*restored, 10, 20), std::vector<bool>(10, false));
}

TEST(RuntimeFilterSerialization, RoundTripBloom)
{
    auto filter = makeFilter();
    filter->insert(makeColumn(0, 1000));

    const String state = serializeToString(*filter);
    EXPECT_TRUE(isBloomState(state));
    auto restored = deserializeFromString(state);

    /// Restored state must be byte-identical, not merely probe-equivalent.
    EXPECT_EQ(serializeToString(*restored), state);

    finishInsert(*filter);
    finishInsert(*restored);
    EXPECT_EQ(probe(*filter, 0, 10000), probe(*restored, 0, 10000));
}

TEST(RuntimeFilterSerialization, UnionMatchesSingleBuild)
{
    auto direct = makeFilter();
    direct->insert(makeColumn(0, 3000));

    /// Three partials with different phases: two below the exact-values limit, one switched to bloom.
    auto part1 = makeFilter();
    part1->insert(makeColumn(0, 30));
    auto part2 = makeFilter();
    part2->insert(makeColumn(30, 90));
    auto part3 = makeFilter();
    part3->insert(makeColumn(90, 3000));

    auto merged = deserializeFromString(serializeToString(*part1));
    auto source2 = deserializeFromString(serializeToString(*part2));
    auto source3 = deserializeFromString(serializeToString(*part3));
    merged->mergeFrom(*source2);
    merged->mergeFrom(*source3);

    /// Byte identity holds because both unions end in the bloom phase, whose bits are a pure
    /// function of the value set; exact-phase unions may store values in a different order.
    EXPECT_EQ(serializeToString(*merged), serializeToString(*direct));

    finishInsert(*direct);
    finishInsert(*merged);
    EXPECT_EQ(probe(*direct, 0, 6000), probe(*merged, 0, 6000));
}

TEST(RuntimeFilterSerialization, EmptyPartial)
{
    auto part = makeFilter();
    part->insert(makeColumn(0, 10));
    auto empty = makeFilter();

    auto merged = deserializeFromString(serializeToString(*part));
    auto empty_source = deserializeFromString(serializeToString(*empty));
    merged->mergeFrom(*empty_source);
    finishInsert(*merged);

    auto alone = deserializeFromString(serializeToString(*part));
    finishInsert(*alone);
    EXPECT_EQ(probe(*merged, 0, 20), probe(*alone, 0, 20));

    auto all_empty = deserializeFromString(serializeToString(*empty));
    auto another_empty = deserializeFromString(serializeToString(*empty));
    all_empty->mergeFrom(*another_empty);
    finishInsert(*all_empty);
    EXPECT_EQ(probe(*all_empty, 0, 10), std::vector<bool>(10, false));
}

TEST(RuntimeFilterSerialization, RoundTripLowCardinalityExactValues)
{
    /// The Set strips LowCardinality from its elements, so the serialized block is typed without it.
    const auto lc_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    const UInt64 bytes_limit = 1 << 20;
    auto make_lc_filter = [&]
    { return std::make_unique<AdaptiveSetRuntimeFilter>(
          lc_type, makeGeometry(bytes_limit), /*distinct_keys_hint_=*/std::nullopt,
          /*distinct_keys_hint_matches_filter_key_=*/false); };
    auto make_lc_column = [&](UInt64 from, UInt64 to)
    {
        auto column = lc_type->createColumn();
        for (UInt64 i = from; i < to; ++i)
            column->insert("value_" + std::to_string(i));
        return ColumnPtr(std::move(column));
    };

    auto filter = make_lc_filter();
    filter->insert(make_lc_column(0, 10));

    const String state = serializeToString(*filter);
    EXPECT_FALSE(isBloomState(state));

    ReadBufferFromString in(state);
    auto restored = AdaptiveSetRuntimeFilter::deserialize(in, lc_type, makeGeometry(bytes_limit));

    finishInsert(*filter, makeGeometry(bytes_limit));
    finishInsert(restored, makeGeometry(bytes_limit));
    auto probe_lc = [&](const AdaptiveSetRuntimeFilter & f, UInt64 from, UInt64 to)
    {
        std::optional<size_t> rows_passed;
        return maskToVector(f.find({make_lc_column(from, to), lc_type, "probe"}, rows_passed), to - from);
    };
    EXPECT_EQ(probe_lc(*filter, 0, 20), probe_lc(restored, 0, 20));
    EXPECT_EQ(probe_lc(restored, 0, 10), std::vector<bool>(10, true));
    EXPECT_EQ(probe_lc(restored, 10, 20), std::vector<bool>(10, false));
}

TEST(RuntimeFilterSerialization, GarbageFailsLoudly)
{
    WriteBufferFromOwnString bad_version;
    writeVarUInt(99, bad_version);
    EXPECT_THROW(deserializeFromString(bad_version.str()), Exception);

    WriteBufferFromOwnString bad_phase;
    writeVarUInt(1, bad_phase);
    writeBinary(UInt8(7), bad_phase);
    EXPECT_THROW(deserializeFromString(bad_phase.str()), Exception);

    auto bloom = makeFilter();
    bloom->insert(makeColumn(0, 1000));
    const String bloom_state = serializeToString(*bloom);
    EXPECT_THROW(deserializeFromString(bloom_state.substr(0, bloom_state.size() / 2)), Exception);
    EXPECT_THROW(deserializeFromString(bloom_state, /*bloom_bytes=*/2 * BLOOM_BYTES), Exception);

    auto values = makeFilter();
    values->insert(makeColumn(0, 10));
    const String values_state = serializeToString(*values);
    EXPECT_THROW(deserializeFromString(values_state.substr(0, values_state.size() / 2)), Exception);

    /// Trailing bytes mean a framing bug: accepting them could silently drop part of the state.
    EXPECT_THROW(deserializeFromString(values_state + "x"), Exception);
    EXPECT_THROW(deserializeFromString(bloom_state + "x"), Exception);
    EXPECT_THROW(deserializeFromString(values_state + values_state), Exception);

    /// A row count above the exact-values limit is rejected before any column is allocated.
    WriteBufferFromOwnString oversized;
    writeVarUInt(1, oversized);
    writeBinary(UInt8(0), oversized);
    writeVarUInt(1000 * 1000 * 1000, oversized);
    EXPECT_THROW(deserializeFromString(oversized.str()), Exception);
}

TEST(RuntimeFilterSerialization, TransportedGeometryBounds)
{
    EXPECT_NO_THROW(makeGeometry().validateTransported());

    /// All-unset geometry could not have been produced by the building side.
    EXPECT_THROW(RuntimeFilterGeometry{}.validateTransported(), Exception);

    auto oversized = makeGeometry();
    oversized.exact_bytes_limit = MAX_RUNTIME_BLOOM_FILTER_BYTES + 1;
    EXPECT_THROW(oversized.validateTransported(), Exception);

    /// The transport sizing never lowers the exact budget below the bloom size.
    auto shrunk = makeGeometry();
    shrunk.exact_bytes_limit = BLOOM_BYTES - 1;
    EXPECT_THROW(shrunk.validateTransported(), Exception);

    auto too_many_hashes = makeGeometry();
    too_many_hashes.bloom_filter_hash_functions = MAX_RUNTIME_BLOOM_FILTER_HASH_FUNCTIONS + 1;
    EXPECT_THROW(too_many_hashes.validateTransported(), Exception);
}

TEST(RuntimeFilterSerialization, MergeOrderIndependence)
{
    /// The merge tree delivers partial states in a nondeterministic order; the union must not
    /// depend on it. Two exact-phase partials and one bloom-phase partial cover every merge
    /// combination (exact into exact, exact into bloom, bloom into exact).
    auto part1 = makeFilter();
    part1->insert(makeColumn(0, 30));
    auto part2 = makeFilter();
    part2->insert(makeColumn(30, 90));
    auto part3 = makeFilter();
    part3->insert(makeColumn(90, 3000));

    const std::vector<String> states{serializeToString(*part1), serializeToString(*part2), serializeToString(*part3)};

    std::optional<String> reference_state;
    std::vector<size_t> order{0, 1, 2};
    do
    {
        auto merged = deserializeFromString(states[order[0]]);
        merged->mergeFrom(*deserializeFromString(states[order[1]]));
        merged->mergeFrom(*deserializeFromString(states[order[2]]));

        /// Every permutation ends in the bloom phase, whose bits are a pure function of the value
        /// set, so even the serialized bytes must match.
        const String merged_state = serializeToString(*merged);
        EXPECT_TRUE(isBloomState(merged_state));
        if (!reference_state)
            reference_state = merged_state;
        else
            EXPECT_EQ(merged_state, *reference_state);
    } while (std::next_permutation(order.begin(), order.end()));
}

namespace
{

DataTypePtr stringType()
{
    return std::make_shared<DataTypeString>();
}

ColumnPtr makeStringColumn(size_t count, size_t value_bytes)
{
    auto column = ColumnString::create();
    for (size_t i = 0; i < count; ++i)
    {
        String value = "value_" + std::to_string(i);
        value.resize(value_bytes, 'x');
        column->insertData(value.data(), value.size());
    }
    return column;
}

ColumnPtr makeShortStringColumn(size_t from, size_t to)
{
    auto column = ColumnString::create();
    for (size_t i = from; i < to; ++i)
    {
        const String value = std::to_string(i);
        column->insertData(value.data(), value.size());
    }
    return column;
}

}

TEST(RuntimeFilterSerialization, SenderBoundsExactStateByKeyBytes)
{
    /// The hash table buffer alone undercounts string keys (their bytes live outside it), so the
    /// exact-phase byte budget must also count the actual key bytes: 10 strings of 2 KiB blow the
    /// 4 KiB budget and the state degrades to a bloom filter even though the row count is tiny.
    AdaptiveSetRuntimeFilter filter(
        stringType(), makeGeometry(),
        /*distinct_keys_hint_=*/std::nullopt,
        /*distinct_keys_hint_matches_filter_key_=*/false);
    filter.insert(makeStringColumn(10, 2048));

    EXPECT_TRUE(isBloomState(serializeToString(filter)));
}

TEST(RuntimeFilterSerialization, LongTypeNameFitsTheExactStateBound)
{
    /// The `Native` block carries the element type name verbatim, and an `Enum` name can exceed
    /// any fixed framing slack on its own; the receive bound must budget for it, or a compliant
    /// tiny exact state would be rejected. The byte budget must fit the 16-bit set's fixed hash
    /// table, so the state stays in the exact phase.
    DataTypeEnum16::Values values;
    values.reserve(30000);
    for (Int16 i = 0; i < 30000; ++i)
        values.emplace_back("value_padded_to_a_long_name_xxxxxxxxxxxxxxxxxxxx_" + std::to_string(i), i);
    const auto enum_type = std::make_shared<DataTypeEnum16>(std::move(values));

    const UInt64 bytes_limit = 1 << 20;
    ASSERT_GT(enum_type->getName().size(), bytes_limit + 64 * 1024);

    AdaptiveSetRuntimeFilter filter(
        enum_type, makeGeometry(bytes_limit),
        /*distinct_keys_hint_=*/std::nullopt,
        /*distinct_keys_hint_matches_filter_key_=*/false);
    auto column = enum_type->createColumn();
    for (Int16 i = 0; i < 10; ++i)
        column->insert(i);
    filter.insert(std::move(column));

    const String state = serializeToString(filter);
    EXPECT_FALSE(isBloomState(state));

    ReadBufferFromString in(state);
    EXPECT_NO_THROW(AdaptiveSetRuntimeFilter::deserialize(in, enum_type, makeGeometry(bytes_limit)));
}

TEST(RuntimeFilterSerialization, OversizedExactStateRejected)
{
    /// The declared row count bounds nothing for variable-width keys. The receiver must reject a
    /// state whose serialized bytes exceed the exact budget (plus framing slack) before it decodes
    /// the column.
    auto relaxed_geometry = makeGeometry();
    relaxed_geometry.exact_bytes_limit = 1 << 20;

    AdaptiveSetRuntimeFilter big(
        stringType(), relaxed_geometry,
        /*distinct_keys_hint_=*/std::nullopt,
        /*distinct_keys_hint_matches_filter_key_=*/false);
    big.insert(makeStringColumn(10, 20 * 1024));
    const String state = serializeToString(big);
    EXPECT_FALSE(isBloomState(state));
    EXPECT_GT(state.size(), makeGeometry().exact_bytes_limit + 64 * 1024);

    /// A receiver whose plan carries the relaxed budget accepts the state.
    {
        ReadBufferFromString in(state);
        EXPECT_NO_THROW(AdaptiveSetRuntimeFilter::deserialize(in, stringType(), relaxed_geometry));
    }

    /// A receiver with the standard budget rejects it, rows notwithstanding.
    {
        ReadBufferFromString in(state);
        try
        {
            AdaptiveSetRuntimeFilter::deserialize(in, stringType(), makeGeometry());
            FAIL() << "expected an exception";
        }
        catch (const Exception & e)
        {
            EXPECT_TRUE(e.message().contains("exceeds the limit")) << e.message();
        }
    }
}

TEST(RuntimeFilterSerialization, ShortStringKeysStayExactUpToTheRaisedRowBound)
{
    /// 20000 distinct short `String` keys, row bound raised to the estimate, byte budget at the
    /// settings floor. Actual key bytes fit, so the state stays exact through serialize,
    /// deserialize, and merge. Only the row and key-byte caps bound the exact phase; the hash
    /// table buffer is not a cap (for short keys it is many times the key-byte size).
    auto geometry = makeGeometry(/*bloom_bytes=*/512 * 1024);
    geometry.exact_values_limit = 20000;

    const auto probe_strings = [&](const AdaptiveSetRuntimeFilter & filter, size_t from, size_t to)
    {
        std::optional<size_t> rows_passed;
        return maskToVector(filter.find({makeShortStringColumn(from, to), stringType(), "probe"}, rows_passed), to - from);
    };

    AdaptiveSetRuntimeFilter part1(
        stringType(), geometry,
        /*distinct_keys_hint_=*/std::nullopt,
        /*distinct_keys_hint_matches_filter_key_=*/false);
    part1.insert(makeShortStringColumn(0, 10000));
    AdaptiveSetRuntimeFilter part2(
        stringType(), geometry,
        /*distinct_keys_hint_=*/std::nullopt,
        /*distinct_keys_hint_matches_filter_key_=*/false);
    part2.insert(makeShortStringColumn(10000, 20000));

    const String state1 = serializeToString(part1);
    const String state2 = serializeToString(part2);
    EXPECT_FALSE(isBloomState(state1));
    EXPECT_FALSE(isBloomState(state2));

    ReadBufferFromString in1(state1);
    auto merged = AdaptiveSetRuntimeFilter::deserialize(in1, stringType(), geometry);
    ReadBufferFromString in2(state2);
    merged.mergeFrom(AdaptiveSetRuntimeFilter::deserialize(in2, stringType(), geometry));

    /// The complete 20000-key union still fits both caps, so it forwards exact as well.
    EXPECT_FALSE(isBloomState(serializeToString(merged)));

    finishInsert(merged, geometry);
    EXPECT_EQ(probe_strings(merged, 0, 20000), std::vector<bool>(20000, true));
    EXPECT_EQ(probe_strings(merged, 20000, 20100), std::vector<bool>(100, false));
}

TEST(RuntimeFilterSerialization, LongStringKeysStillDegradeAtTheByteCap)
{
    /// The raised row bound must not weaken the byte cap: keys whose actual bytes blow
    /// `exact_bytes_limit` degrade to the settings-sized bloom filter at build time, so a
    /// degraded partial never costs more on the wire than the settings geometry.
    auto geometry = makeGeometry(/*bloom_bytes=*/512 * 1024);
    geometry.exact_values_limit = 20000;

    AdaptiveSetRuntimeFilter filter(
        stringType(), geometry,
        /*distinct_keys_hint_=*/std::nullopt,
        /*distinct_keys_hint_matches_filter_key_=*/false);
    filter.insert(makeStringColumn(20000, 200));

    const String state = serializeToString(filter);
    EXPECT_TRUE(isBloomState(state));
    /// Bloom parameters, seed, and words; framing is a few varints.
    EXPECT_LE(state.size(), geometry.bloom_filter_bytes + 64);
}

TEST(RuntimeFilterSerialization, DegradedBloomIsSizedByGeometryNotTheExactByteBound)
{
    /// A transported plan may raise the exact-phase byte budget above the settings geometry, because
    /// the estimate-raised row bound needs room for the keys. The bloom filter that the exact phase
    /// degrades to must not follow it: it stays at `bloom_filter_bytes`, the width every node that
    /// merges a partial of this exchange allocates. Blooms of different widths cannot be merged.
    const auto type = std::make_shared<DataTypeUInt64>();
    auto geometry = makeGeometry();
    geometry.exact_bytes_limit = 64 * BLOOM_BYTES;
    geometry.exact_values_limit = 4;

    AdaptiveSetRuntimeFilter filter(
        type, geometry,
        /*distinct_keys_hint_=*/std::nullopt,
        /*distinct_keys_hint_matches_filter_key_=*/false);
    /// Overflows the row bound while the raised byte budget is nowhere near exhausted.
    filter.insert(makeColumn(0, 100));

    const String state = serializeToString(filter);
    ASSERT_TRUE(isBloomState(state));

    /// `deserialize` accepts a bloom state only if the width it declares is exactly
    /// `geometry.bloom_filter_bytes`, so a successful round trip pins the allocation.
    ReadBufferFromString in(state);
    EXPECT_NO_THROW(AdaptiveSetRuntimeFilter::deserialize(in, type, geometry));

    /// Had the bloom been sized from the exact byte bound, this is the geometry that would take it.
    auto sized_from_exact_bound = geometry;
    sized_from_exact_bound.bloom_filter_bytes = geometry.exact_bytes_limit;
    ReadBufferFromString sized_from_exact_bound_in(state);
    EXPECT_THROW(AdaptiveSetRuntimeFilter::deserialize(sized_from_exact_bound_in, type, sized_from_exact_bound), Exception);
}

TEST(RuntimeFilterSerialization, RegisteredUnionIsFindable)
{
    auto part1 = makeFilter();
    part1->insert(makeColumn(0, 10));
    auto part2 = makeFilter();
    part2->insert(makeColumn(10, 20));

    auto merged = deserializeFromString(serializeToString(*part1));
    auto source = deserializeFromString(serializeToString(*part2));
    merged->mergeFrom(*source);

    auto lookup = createRuntimeFilterLookup();
    lookup->add("key", "name", publish(std::move(*merged)));

    auto found = lookup->find("key");
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(probe(*found, 0, 20), std::vector<bool>(20, true));
    EXPECT_EQ(probe(*found, 20, 30), std::vector<bool>(10, false));
}

namespace
{

SharedHeader serializationHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block{ColumnWithTypeAndName(ColumnUInt64::create(), type, "x")});
}

BuildRuntimeFilterStep makeBuildStep(bool with_topology, const String & filter_key = "rendezvous-key")
{
    auto header = serializationHeader();
    BuildRuntimeFilterStep step(
        header,
        "x",
        std::make_shared<DataTypeUInt64>(),
        "f",
        filter_key,
        RuntimeFilterBuildOptions{
            .geometry = makeGeometry(),
            .polarity = RuntimeFilterPolarity::Contains,
            .track_key_range = false,
            .distinct_keys_hint = std::nullopt,
            .distinct_keys_hint_matches_filter_key = false});
    if (with_topology)
        step.addExchange("exchange_7", {"0", "1", "2", "3"});
    return step;
}

void expectGeometryMatches(const RuntimeFilterGeometry & actual, const RuntimeFilterGeometry & expected)
{
    EXPECT_EQ(actual.exact_values_limit, expected.exact_values_limit);
    EXPECT_EQ(actual.exact_bytes_limit, expected.exact_bytes_limit);
    EXPECT_EQ(actual.bloom_filter_bytes, expected.bloom_filter_bytes);
    EXPECT_EQ(actual.bloom_filter_hash_functions, expected.bloom_filter_hash_functions);
    EXPECT_DOUBLE_EQ(actual.pass_ratio_threshold_for_disabling, expected.pass_ratio_threshold_for_disabling);
    EXPECT_EQ(actual.blocks_to_skip_before_reenabling, expected.blocks_to_skip_before_reenabling);
    EXPECT_DOUBLE_EQ(actual.max_ratio_of_set_bits_in_bloom_filter, expected.max_ratio_of_set_bits_in_bloom_filter);
}

/// The step version a stream at `plan_version` carries for `BuildRuntimeFilter`, chosen by the
/// registry the way `QueryPlan::serialize` chooses it.
UInt64 buildStepVersionToWrite(UInt64 plan_version)
{
    QueryPlanStepRegistry registry;
    registerBuildRuntimeFilterStep(registry);
    return registry.versionToWrite("BuildRuntimeFilter", plan_version);
}

/// Writes the step's settings into `settings` and returns the serialized step body.
String serializeBuildStep(
    const BuildRuntimeFilterStep & step, UInt64 version, QueryPlanSerializationSettings & settings, bool for_cache_key = false)
{
    step.serializeSettings(settings, version);

    WriteBufferFromOwnString out;
    SerializedSetsRegistry serialize_registry;
    serialize_registry.for_cache_key = for_cache_key;
    IQueryPlanStep::Serialization serialization{
        .out = out,
        .registry = serialize_registry,
        .for_cache_key = for_cache_key,
        .version = version,
        .step_version = buildStepVersionToWrite(version),
    };
    step.serialize(serialization);
    return out.str();
}

QueryPlanStepPtr roundTripBuildStep(const BuildRuntimeFilterStep & step, UInt64 version)
{
    QueryPlanSerializationSettings settings;
    const String payload = serializeBuildStep(step, version, settings);

    ReadBufferFromString in(payload);
    DeserializedSetsRegistry deserialize_registry;
    SharedHeaders input_headers;
    input_headers.push_back(step.getInputHeaders().front());
    ContextPtr context = getContext().context;
    IQueryPlanStep::Deserialization deserialization{
        .in = in,
        .registry = deserialize_registry,
        .storage_holders = {},
        .context = context,
        .input_headers = input_headers,
        .output_header = step.getOutputHeader(),
        .settings = settings,
        .version = version,
        .step_version = buildStepVersionToWrite(version),
    };
    return BuildRuntimeFilterStep::deserialize(deserialization);
}

}

TEST(RuntimeFilterSerialization, BuildStepTopologyRoundTrips)
{
    auto step = makeBuildStep(/*with_topology=*/true);
    ASSERT_TRUE(step.hasFilterExchanges());

    QueryPlanSerializationSettings settings;
    const String payload = serializeBuildStep(step, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, settings);

    auto restored_ptr = roundTripBuildStep(step, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    auto * restored = typeid_cast<BuildRuntimeFilterStep *>(restored_ptr.get());
    ASSERT_NE(restored, nullptr);

    EXPECT_TRUE(restored->hasFilterExchanges());
    EXPECT_EQ(restored->getFilterKey(), "rendezvous-key");
    EXPECT_EQ(restored->getFilterName(), "f");
    EXPECT_EQ(restored->getFilterColumnName(), "x");
    EXPECT_TRUE(restored->getFilterColumnType()->equals(*std::make_shared<DataTypeUInt64>()));
    EXPECT_TRUE(restored->allowsNotExactFilter());
    expectGeometryMatches(restored->getGeometry(), makeGeometry());
    restored->getGeometry().validateTransported();

    QueryPlanSerializationSettings restored_settings;
    EXPECT_EQ(serializeBuildStep(*restored, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, restored_settings), payload);
}

TEST(RuntimeFilterSerialization, BuildStepTopologyRequiresRuntimeFilterExchangesVersion)
{
    constexpr UInt64 pre_exchanges_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_RUNTIME_FILTER_EXCHANGES - 1;
    auto with_topology = makeBuildStep(/*with_topology=*/true);
    QueryPlanSerializationSettings settings;
    try
    {
        serializeBuildStep(with_topology, pre_exchanges_version, settings);
        FAIL() << "serializing filter exchanges below the runtime-filter-exchanges version should throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::SUPPORT_IS_DISABLED);
    }

    auto without_topology = makeBuildStep(/*with_topology=*/false);
    ASSERT_FALSE(without_topology.hasFilterExchanges());
    auto restored_ptr = roundTripBuildStep(without_topology, pre_exchanges_version);
    auto * restored = typeid_cast<BuildRuntimeFilterStep *>(restored_ptr.get());
    ASSERT_NE(restored, nullptr);
    EXPECT_FALSE(restored->hasFilterExchanges());
    EXPECT_TRUE(restored->getFilterKey().empty());
    EXPECT_EQ(restored->getFilterName(), "f");
    EXPECT_EQ(restored->getFilterColumnName(), "x");
    /// A stream below the filter-exchanges version omits `join_runtime_filter_exact_bytes_limit`, so
    /// the reader bounds the exact phase by the bloom filter size.
    expectGeometryMatches(restored->getGeometry(), makeGeometry());
}

TEST(RuntimeFilterSerialization, MergeStepRequiresRuntimeFilterExchangesVersion)
{
    constexpr UInt64 pre_exchanges_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_RUNTIME_FILTER_EXCHANGES - 1;
    const MergeRuntimeFiltersStep step(
        "f", std::make_shared<DataTypeUInt64>(), makeGeometry(), "exchange_1", {"0", "1"}, 2, {{"exchange_2", {"0"}}});

    auto serialize_step = [](const IQueryPlanStep & merge_step, UInt64 version, QueryPlanSerializationSettings & settings)
    {
        merge_step.serializeSettings(settings, version);
        WriteBufferFromOwnString out;
        SerializedSetsRegistry registry;
        IQueryPlanStep::Serialization serialization{.out = out, .registry = registry, .version = version};
        merge_step.serialize(serialization);
        return out.str();
    };
    auto deserialize_step = [](const String & payload, const QueryPlanSerializationSettings & settings, UInt64 version)
    {
        ReadBufferFromString in(payload);
        DeserializedSetsRegistry registry;
        const SharedHeaders input_headers;
        const SharedHeader output_header = std::make_shared<const Block>();
        ContextPtr context = getContext().context;
        IQueryPlanStep::Deserialization deserialization{
            .in = in,
            .registry = registry,
            .storage_holders = {},
            .context = context,
            .input_headers = input_headers,
            .output_header = output_header,
            .settings = settings,
            .version = version,
        };
        return MergeRuntimeFiltersStep::deserialize(deserialization);
    };

    QueryPlanSerializationSettings pre_exchanges_settings;
    try
    {
        serialize_step(step, pre_exchanges_version, pre_exchanges_settings);
        FAIL() << "serializing a merge step below the runtime-filter-exchanges version should throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::SUPPORT_IS_DISABLED);
    }

    QueryPlanSerializationSettings settings;
    const String payload = serialize_step(step, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, settings);
    try
    {
        deserialize_step(payload, settings, pre_exchanges_version);
        FAIL() << "deserializing a merge step below the runtime-filter-exchanges version should throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::INCORRECT_DATA);
    }

    auto restored = deserialize_step(payload, settings, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    ASSERT_NE(typeid_cast<MergeRuntimeFiltersStep *>(restored.get()), nullptr);
    QueryPlanSerializationSettings restored_settings;
    EXPECT_EQ(serialize_step(*restored, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, restored_settings), payload);
}

TEST(RuntimeFilterSerialization, BuildStepTreeExchangeRoundTrips)
{
    auto step = makeBuildStep(/*with_topology=*/false);
    Strings source_buckets;
    source_buckets.reserve(20);
    for (size_t i = 0; i < 20; ++i)
        source_buckets.push_back(std::to_string(i));
    step.setTreeExchange("exchange_tree", source_buckets, 16);
    ASSERT_TRUE(step.hasFilterExchanges());

    QueryPlanSerializationSettings settings;
    const String payload = serializeBuildStep(step, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, settings);

    auto restored_ptr = roundTripBuildStep(step, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    auto * restored = typeid_cast<BuildRuntimeFilterStep *>(restored_ptr.get());
    ASSERT_NE(restored, nullptr);

    EXPECT_TRUE(restored->hasFilterExchanges());
    EXPECT_EQ(restored->getFilterKey(), "rendezvous-key");
    EXPECT_EQ(restored->getFilterName(), "f");
    EXPECT_EQ(restored->getFilterColumnName(), "x");
    EXPECT_TRUE(restored->getFilterColumnType()->equals(*std::make_shared<DataTypeUInt64>()));
    EXPECT_TRUE(restored->allowsNotExactFilter());
    expectGeometryMatches(restored->getGeometry(), makeGeometry());
    restored->getGeometry().validateTransported();

    QueryPlanSerializationSettings restored_settings;
    EXPECT_EQ(serializeBuildStep(*restored, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, restored_settings), payload);
}

TEST(RuntimeFilterSerialization, BuildStepWithoutTopologyRoundTrips)
{
    auto step = makeBuildStep(/*with_topology=*/false);
    ASSERT_FALSE(step.hasFilterExchanges());

    QueryPlanSerializationSettings settings;
    const String payload = serializeBuildStep(step, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, settings);

    auto restored_ptr = roundTripBuildStep(step, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    auto * restored = typeid_cast<BuildRuntimeFilterStep *>(restored_ptr.get());
    ASSERT_NE(restored, nullptr);
    EXPECT_FALSE(restored->hasFilterExchanges());
    EXPECT_TRUE(restored->getFilterKey().empty());
    EXPECT_EQ(restored->getFilterName(), "f");
    EXPECT_EQ(restored->getFilterColumnName(), "x");
    expectGeometryMatches(restored->getGeometry(), makeGeometry());

    QueryPlanSerializationSettings restored_settings;
    EXPECT_EQ(serializeBuildStep(*restored, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, restored_settings), payload);
}

TEST(RuntimeFilterSerialization, BuildStepCacheKeyOmitsRendezvousKey)
{
    auto first = makeBuildStep(/*with_topology=*/true, "key-1");
    auto second = makeBuildStep(/*with_topology=*/true, "key-2");

    QueryPlanSerializationSettings settings;
    EXPECT_NE(
        serializeBuildStep(first, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, settings),
        serializeBuildStep(second, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, settings));
    EXPECT_EQ(
        serializeBuildStep(first, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, settings, /*for_cache_key=*/true),
        serializeBuildStep(second, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, settings, /*for_cache_key=*/true));
}
