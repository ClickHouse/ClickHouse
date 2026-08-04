#include "cas_test_helpers.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcMaintenanceStateFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcMaintenanceState.h>
#include <fmt/format.h>

using namespace DB::Cas;

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LIMIT_EXCEEDED;
    extern const int UNKNOWN_FORMAT_VERSION;
}

namespace
{
class FailingMaintenanceReadBackend : public InMemoryBackend
{
public:
    std::optional<GetResult> get(const String &, Range) override
    {
        throw std::runtime_error("injected maintenance read failure");
    }
};
}

TEST(CASGCMaintenanceStateFormat, RegistryLayoutAndCanonicalCodec)
{
    EXPECT_EQ(static_cast<uint16_t>(FormatId::GcMaintenanceState), 25);
    const auto points = changePoints(FormatId::GcMaintenanceState);
    ASSERT_EQ(points.size(), 1u);
    EXPECT_EQ(points[0].generation, 7);
    EXPECT_EQ(points[0].min_reader, 7);
    const FormatTraits & traits = traitsFor(FormatId::GcMaintenanceState);
    EXPECT_EQ(traits.type, "cas_gc_maintenance_state");
    EXPECT_EQ(traits.family, TextFamily::Control);
    EXPECT_EQ(traits.strictness, KeyStrictness::Strict);
    EXPECT_EQ(traits.compression, CompressionPolicy::Never);
    EXPECT_EQ(traits.object_cap, 512 * 1024);
    EXPECT_EQ(traits.line_cap, 512 * 1024);
    EXPECT_EQ(storedSuffix(FormatId::GcMaintenanceState), "");
    EXPECT_EQ(traitsForType("cas_gc_maintenance_state"), &traits);

    const Layout layout("p");
    EXPECT_EQ(layout.gcMaintenanceStateKey(), "p/gc/maintenance_state");
    EXPECT_NE(layout.gcMaintenanceStateKey(), layout.gcStateKey());
    EXPECT_NE(layout.gcMaintenanceStateKey(), layout.gcHbKey());

    const GcMaintenanceState empty;
    EXPECT_EQ(encodeGcMaintenanceState(empty), fmt::format(
        "{{\"type\":\"cas_gc_maintenance_state\",\"v\":{}}}\n{{\"cur\":\"\"}}\n", currentCompatibilityVersion()));
    const GcMaintenanceState state{.janitor_cursor = R"(cas/ns/a/"quoted"\\next)"};
    EXPECT_EQ(decodeGcMaintenanceState(encodeGcMaintenanceState(state)), state);
}

TEST(CASGCMaintenanceStateFormat, RejectsMalformedAndBoundsCursor)
{
    const auto bad = [](std::string_view body)
    {
        return "{\"type\":\"cas_gc_maintenance_state\",\"v\":7}\n" + String(body);
    };
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { (void)decodeGcMaintenanceState(bad("{}\n")); });
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { (void)decodeGcMaintenanceState(bad("{\"cur\":\"a\",\"cur\":\"b\"}\n")); });
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { (void)decodeGcMaintenanceState(bad("{\"cur\":\"a\",\"extra\":1}\n")); });
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { (void)decodeGcMaintenanceState(bad("{\"cur\":\"a\"}\nx")); });

    const GcMaintenanceState at_limit{.janitor_cursor = String(kMaxGcMaintenanceCursorBytes, 'x')};
    EXPECT_EQ(decodeGcMaintenanceState(encodeGcMaintenanceState(at_limit)), at_limit);
    const GcMaintenanceState over_limit{.janitor_cursor = String(kMaxGcMaintenanceCursorBytes + 1, 'x')};
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LIMIT_EXCEEDED,
        [&] { (void)encodeGcMaintenanceState(over_limit); });
    const String raw = "{\"type\":\"cas_gc_maintenance_state\",\"v\":7}\n{\"cur\":\"" + over_limit.janitor_cursor + "\"}\n";
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { (void)decodeGcMaintenanceState(raw); });
    String oversized = R"({"type":"cas_gc_maintenance_state","v":7,"pad":")";
    oversized.append(448 * 1024, 'x');
    oversized += "\"}\n{\"cur\":\"";
    oversized.append(kMaxGcMaintenanceCursorBytes, 'y');
    oversized += "\"}\n";
    ASSERT_GT(oversized.size(), traitsFor(FormatId::GcMaintenanceState).object_cap);
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { (void)decodeGcMaintenanceState(oversized); });
}

TEST(CASGCMaintenanceState, ReadsAndCasWithoutAdoptingConflicts)
{
    InMemoryBackend backend;
    const Layout layout("p");
    const String key = layout.gcMaintenanceStateKey();
    const GcMaintenanceReadResult absent = readGcMaintenanceState(backend, layout);
    EXPECT_EQ(absent.status, GcMaintenanceReadStatus::Absent);
    EXPECT_FALSE(absent.state);
    EXPECT_FALSE(absent.token);

    const GcMaintenanceState first{.janitor_cursor = "cas/ns/first"};
    const GcMaintenanceCasResult created = casGcMaintenanceState(backend, layout, std::nullopt, first);
    EXPECT_EQ(created.outcome, GcMaintenanceCasOutcome::Committed);
    const GcMaintenanceReadResult valid = readGcMaintenanceState(backend, layout);
    ASSERT_EQ(valid.status, GcMaintenanceReadStatus::Valid);
    ASSERT_TRUE(valid.token);
    ASSERT_TRUE(valid.state);
    EXPECT_EQ(*valid.state, first);

    const GcMaintenanceCasResult advanced = casGcMaintenanceState(backend, layout, valid.token,
        GcMaintenanceState{.janitor_cursor = "cas/ns/advanced"});
    ASSERT_EQ(advanced.outcome, GcMaintenanceCasOutcome::Committed);
    const auto advanced_body = backend.get(key);
    ASSERT_TRUE(advanced_body);

    ASSERT_EQ(backend.casPut(key, encodeGcMaintenanceState({.janitor_cursor = "winner"}), advanced_body->token).outcome,
        CasOutcome::Committed);
    const GcMaintenanceCasResult conflict = casGcMaintenanceState(backend, layout, valid.token,
        GcMaintenanceState{.janitor_cursor = "loser"});
    EXPECT_EQ(conflict.outcome, GcMaintenanceCasOutcome::Conflict);
    EXPECT_EQ(decodeGcMaintenanceState(backend.get(key)->bytes).janitor_cursor, "winner");
}

TEST(CASGCMaintenanceState, ClassifiesCorruptionAndResetsOnlyExactToken)
{
    InMemoryBackend backend;
    const Layout layout("p");
    const String key = layout.gcMaintenanceStateKey();
    ASSERT_EQ(backend.putIfAbsent(key, "malformed").outcome, PutOutcome::Done);
    const GcMaintenanceReadResult corrupt = readGcMaintenanceState(backend, layout);
    ASSERT_EQ(corrupt.status, GcMaintenanceReadStatus::Corrupt);
    ASSERT_TRUE(corrupt.token);
    EXPECT_FALSE(corrupt.state);
    EXPECT_FALSE(corrupt.diagnostic.empty());
    ASSERT_EQ(casGcMaintenanceState(backend, layout, corrupt.token, {}).outcome, GcMaintenanceCasOutcome::Committed);
    EXPECT_EQ(decodeGcMaintenanceState(backend.get(key)->bytes), GcMaintenanceState{});
}

TEST(CASGCMaintenanceState, UsesExactlyOneReadOrCasAttempt)
{
    DB::Cas::tests::CountingBackend backend;
    const Layout layout("p");
    const String key = layout.gcMaintenanceStateKey();
    EXPECT_EQ(readGcMaintenanceState(backend, layout).status, GcMaintenanceReadStatus::Absent);
    EXPECT_EQ(backend.getCount(key), 1u);

    backend.resetCounts();
    ASSERT_EQ(casGcMaintenanceState(backend, layout, std::nullopt, {}).outcome,
        GcMaintenanceCasOutcome::Committed);
    EXPECT_EQ(backend.casPutCount(key), 1u);
    EXPECT_EQ(backend.getCount(key), 0u);

    backend.resetCounts();
    EXPECT_EQ(casGcMaintenanceState(backend, layout, std::nullopt,
        GcMaintenanceState{.janitor_cursor = "loser"}).outcome, GcMaintenanceCasOutcome::Conflict);
    EXPECT_EQ(backend.casPutCount(key), 1u);
    EXPECT_EQ(backend.getCount(key), 0u);

    const auto current = backend.get(key);
    ASSERT_TRUE(current);
    ASSERT_EQ(backend.casPut(key, encodeGcMaintenanceState({.janitor_cursor = "winner"}), current->token).outcome,
        CasOutcome::Committed);
    backend.resetCounts();
    EXPECT_EQ(casGcMaintenanceState(backend, layout, current->token,
        GcMaintenanceState{.janitor_cursor = "stale"}).outcome, GcMaintenanceCasOutcome::Conflict);
    EXPECT_EQ(backend.casPutCount(key), 1u);
    EXPECT_EQ(backend.getCount(key), 0u);
    EXPECT_EQ(decodeGcMaintenanceState(backend.InMemoryBackend::get(key)->bytes).janitor_cursor, "winner");
}

TEST(CASGCMaintenanceState, FutureVersionPropagatesInsteadOfResetting)
{
    DB::Cas::tests::CountingBackend backend;
    const Layout layout("p");
    const String key = layout.gcMaintenanceStateKey();
    ASSERT_EQ(backend.putIfAbsent(key, fmt::format(
        "{{\"type\":\"cas_gc_maintenance_state\",\"v\":{}}}\n{{\"cur\":\"\"}}\n", currentCompatibilityVersion() + 1)).outcome,
        PutOutcome::Done);
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::UNKNOWN_FORMAT_VERSION,
        [&] { (void)readGcMaintenanceState(backend, layout); });
    EXPECT_EQ(backend.casPutCount(key), 0u);

    FailingMaintenanceReadBackend failing;
    EXPECT_THROW((void)readGcMaintenanceState(failing, layout), std::runtime_error);
}

TEST(CASGCMaintenanceState, LosingCorruptResetPreservesConcurrentWinner)
{
    DB::Cas::tests::CountingBackend backend;
    const Layout layout("p");
    const String key = layout.gcMaintenanceStateKey();
    ASSERT_EQ(backend.putIfAbsent(key, "corrupt").outcome, PutOutcome::Done);
    const auto corrupt = readGcMaintenanceState(backend, layout);
    ASSERT_EQ(corrupt.status, GcMaintenanceReadStatus::Corrupt);
    ASSERT_TRUE(corrupt.token);
    ASSERT_EQ(backend.casPut(key, encodeGcMaintenanceState({.janitor_cursor = "winner"}), corrupt.token).outcome,
        CasOutcome::Committed);
    backend.resetCounts();
    EXPECT_EQ(casGcMaintenanceState(backend, layout, corrupt.token, {}).outcome,
        GcMaintenanceCasOutcome::Conflict);
    EXPECT_EQ(backend.casPutCount(key), 1u);
    EXPECT_EQ(backend.getCount(key), 0u);
    EXPECT_EQ(decodeGcMaintenanceState(backend.InMemoryBackend::get(key)->bytes).janitor_cursor, "winner");
}
