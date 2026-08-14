#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <optional>

using namespace DB;

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

std::unique_ptr<ApproximateRuntimeFilter> makeFilter(size_t filters_to_merge = 0)
{
    return std::make_unique<ApproximateRuntimeFilter>(
        filters_to_merge, std::make_shared<DataTypeUInt64>(), makeGeometry(), /*distinct_keys_hint_=*/std::nullopt);
}

ColumnPtr makeColumn(UInt64 from, UInt64 to)
{
    auto column = ColumnUInt64::create();
    for (UInt64 i = from; i < to; ++i)
        column->insertValue(i);
    return column;
}

String serializeToString(ApproximateRuntimeFilter & filter)
{
    WriteBufferFromOwnString out;
    filter.serialize(out);
    return out.str();
}

std::unique_ptr<ApproximateRuntimeFilter>
deserializeFromString(const String & data, size_t filters_to_merge = 0, UInt64 bloom_bytes = BLOOM_BYTES)
{
    ReadBufferFromString in(data);
    return ApproximateRuntimeFilter::deserialize(in, filters_to_merge, std::make_shared<DataTypeUInt64>(), makeGeometry(bloom_bytes));
}

std::vector<bool> probe(const IRuntimeFilter & filter, UInt64 from, UInt64 to)
{
    auto result = filter.find({makeColumn(from, to), std::make_shared<DataTypeUInt64>(), "probe"});
    auto full = result->convertToFullColumnIfConst();
    std::vector<bool> found(to - from);
    for (size_t i = 0; i < found.size(); ++i)
        found[i] = full->getUInt(i) != 0;
    return found;
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

    filter->finishInsert();
    restored->finishInsert();

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

    /// The restored filter must hold bit-identical state, not just equivalent behavior.
    EXPECT_EQ(serializeToString(*restored), state);

    filter->finishInsert();
    restored->finishInsert();
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

    auto merged = deserializeFromString(serializeToString(*part1), /*filters_to_merge=*/2);
    auto source2 = deserializeFromString(serializeToString(*part2));
    auto source3 = deserializeFromString(serializeToString(*part3));
    merged->merge(source2.get());
    merged->merge(source3.get());

    /// Byte identity holds because both unions end in the bloom phase, whose bits are a pure
    /// function of the value set; exact-phase unions may store values in a different order.
    EXPECT_EQ(serializeToString(*merged), serializeToString(*direct));

    direct->finishInsert();
    merged->finishInsert();
    EXPECT_EQ(probe(*direct, 0, 6000), probe(*merged, 0, 6000));
}

TEST(RuntimeFilterSerialization, EmptyPartial)
{
    auto part = makeFilter();
    part->insert(makeColumn(0, 10));
    auto empty = makeFilter();

    auto merged = deserializeFromString(serializeToString(*part), /*filters_to_merge=*/1);
    auto empty_source = deserializeFromString(serializeToString(*empty));
    merged->merge(empty_source.get());
    merged->finishInsert();

    auto alone = deserializeFromString(serializeToString(*part));
    alone->finishInsert();
    EXPECT_EQ(probe(*merged, 0, 20), probe(*alone, 0, 20));

    auto all_empty = deserializeFromString(serializeToString(*empty), /*filters_to_merge=*/1);
    auto another_empty = deserializeFromString(serializeToString(*empty));
    all_empty->merge(another_empty.get());
    all_empty->finishInsert();
    EXPECT_EQ(probe(*all_empty, 0, 10), std::vector<bool>(10, false));
}

TEST(RuntimeFilterSerialization, RoundTripLowCardinalityExactValues)
{
    /// The Set strips LowCardinality from its elements, so the serialized block is typed without it.
    const auto lc_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    const UInt64 bytes_limit = 1 << 20;
    auto make_lc_filter = [&]
    { return std::make_unique<ApproximateRuntimeFilter>(0, lc_type, makeGeometry(bytes_limit), /*distinct_keys_hint_=*/std::nullopt); };
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
    auto restored = ApproximateRuntimeFilter::deserialize(in, 0, lc_type, makeGeometry(bytes_limit));

    filter->finishInsert();
    restored->finishInsert();
    auto probe_lc = [&](const IRuntimeFilter & f, UInt64 from, UInt64 to)
    {
        auto result = f.find({make_lc_column(from, to), lc_type, "probe"});
        auto full = result->convertToFullColumnIfConst();
        std::vector<bool> found(to - from);
        for (size_t i = 0; i < found.size(); ++i)
            found[i] = full->getUInt(i) != 0;
        return found;
    };
    EXPECT_EQ(probe_lc(*filter, 0, 20), probe_lc(*restored, 0, 20));
    EXPECT_EQ(probe_lc(*restored, 0, 10), std::vector<bool>(10, true));
    EXPECT_EQ(probe_lc(*restored, 10, 20), std::vector<bool>(10, false));
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
    EXPECT_THROW(deserializeFromString(bloom_state, 0, /*bloom_bytes=*/2 * BLOOM_BYTES), Exception);

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
        auto merged = deserializeFromString(states[order[0]], /*filters_to_merge=*/2);
        merged->merge(deserializeFromString(states[order[1]]).get());
        merged->merge(deserializeFromString(states[order[2]]).get());

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
    ApproximateRuntimeFilter filter(0, stringType(), makeGeometry(), /*distinct_keys_hint_=*/std::nullopt);
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

    ApproximateRuntimeFilter filter(0, enum_type, makeGeometry(bytes_limit), /*distinct_keys_hint_=*/std::nullopt);
    auto column = enum_type->createColumn();
    for (Int16 i = 0; i < 10; ++i)
        column->insert(i);
    filter.insert(std::move(column));

    const String state = serializeToString(filter);
    EXPECT_FALSE(isBloomState(state));

    ReadBufferFromString in(state);
    EXPECT_NO_THROW(ApproximateRuntimeFilter::deserialize(in, 0, enum_type, makeGeometry(bytes_limit)));
}

TEST(RuntimeFilterSerialization, OversizedExactStateRejected)
{
    /// Regression for the receive-side gap: the declared row count bounds nothing for
    /// variable-width keys, so a state whose serialized bytes blow the exact budget (plus framing
    /// slack) must be rejected before the decoded column is materialized.
    auto relaxed_geometry = makeGeometry();
    relaxed_geometry.exact_bytes_limit = 1 << 20;

    ApproximateRuntimeFilter big(0, stringType(), relaxed_geometry, /*distinct_keys_hint_=*/std::nullopt);
    big.insert(makeStringColumn(10, 20 * 1024));
    const String state = serializeToString(big);
    EXPECT_FALSE(isBloomState(state));
    EXPECT_GT(state.size(), makeGeometry().exact_bytes_limit + 64 * 1024);

    /// A receiver whose plan carries the relaxed budget accepts the state.
    {
        ReadBufferFromString in(state);
        EXPECT_NO_THROW(ApproximateRuntimeFilter::deserialize(in, 0, stringType(), relaxed_geometry));
    }

    /// A receiver with the standard budget rejects it, rows notwithstanding.
    {
        ReadBufferFromString in(state);
        try
        {
            ApproximateRuntimeFilter::deserialize(in, 0, stringType(), makeGeometry());
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
    /// The reviewer's trace of the transported geometry: 20000 distinct short `String` keys with
    /// the exact-values limit raised to the cardinality estimate while the byte budget stays at
    /// the settings floor. The keys' actual bytes fit the budget, so the state must stay exact
    /// through serialize -> deserialize -> merge; only the row and key-byte caps bound the exact
    /// phase, not the hash table buffer, which for short keys is many times their byte size.
    auto geometry = makeGeometry(/*bloom_bytes=*/512 * 1024);
    geometry.exact_values_limit = 20000;

    const auto probe_strings = [&](const IRuntimeFilter & filter, size_t from, size_t to)
    {
        auto result = filter.find({makeShortStringColumn(from, to), stringType(), "probe"});
        auto full = result->convertToFullColumnIfConst();
        std::vector<bool> found(to - from);
        for (size_t i = 0; i < found.size(); ++i)
            found[i] = full->getUInt(i) != 0;
        return found;
    };

    ApproximateRuntimeFilter part1(0, stringType(), geometry, /*distinct_keys_hint_=*/std::nullopt);
    part1.insert(makeShortStringColumn(0, 10000));
    ApproximateRuntimeFilter part2(0, stringType(), geometry, /*distinct_keys_hint_=*/std::nullopt);
    part2.insert(makeShortStringColumn(10000, 20000));

    const String state1 = serializeToString(part1);
    const String state2 = serializeToString(part2);
    EXPECT_FALSE(isBloomState(state1));
    EXPECT_FALSE(isBloomState(state2));

    ReadBufferFromString in1(state1);
    auto merged = ApproximateRuntimeFilter::deserialize(in1, /*filters_to_merge_=*/1, stringType(), geometry);
    ReadBufferFromString in2(state2);
    auto source = ApproximateRuntimeFilter::deserialize(in2, 0, stringType(), geometry);
    merged->merge(source.get());

    /// The complete 20000-key union still fits both caps, so it forwards exact as well.
    EXPECT_FALSE(isBloomState(serializeToString(*merged)));

    merged->finishInsert();
    EXPECT_EQ(probe_strings(*merged, 0, 20000), std::vector<bool>(20000, true));
    EXPECT_EQ(probe_strings(*merged, 20000, 20100), std::vector<bool>(100, false));
}

TEST(RuntimeFilterSerialization, LongStringKeysStillDegradeAtTheByteCap)
{
    /// The raised row bound must not weaken the byte cap: keys whose actual bytes blow
    /// `exact_bytes_limit` degrade to the settings-sized bloom filter at build time, so a
    /// degraded partial never costs more on the wire than the settings geometry.
    auto geometry = makeGeometry(/*bloom_bytes=*/512 * 1024);
    geometry.exact_values_limit = 20000;

    ApproximateRuntimeFilter filter(0, stringType(), geometry, /*distinct_keys_hint_=*/std::nullopt);
    filter.insert(makeStringColumn(20000, 200));

    const String state = serializeToString(filter);
    EXPECT_TRUE(isBloomState(state));
    /// Bloom parameters, seed, and words; framing is a few varints.
    EXPECT_LE(state.size(), geometry.bloom_filter_bytes + 64);
}

TEST(RuntimeFilterSerialization, RegisteredUnionIsFindable)
{
    auto part1 = makeFilter();
    part1->insert(makeColumn(0, 10));
    auto part2 = makeFilter();
    part2->insert(makeColumn(10, 20));

    auto merged = deserializeFromString(serializeToString(*part1), /*filters_to_merge=*/1);
    auto source = deserializeFromString(serializeToString(*part2));
    merged->merge(source.get());

    auto lookup = createRuntimeFilterLookup();
    lookup->add("key", "name", std::move(merged));

    auto found = lookup->find("key");
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(probe(*found, 0, 20), std::vector<bool>(20, true));
    EXPECT_EQ(probe(*found, 20, 30), std::vector<bool>(10, false));
}
