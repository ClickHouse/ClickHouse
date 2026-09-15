#include "cas_test_helpers.h"
#include "cas_format_test_battery.h"
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
    std::optional<DB::Cas::Backend::Raw> read(const String &, DB::Cas::TransportAccess &) override
    {
        throw std::runtime_error("injected maintenance read failure");
    }
};
}

CAS_BATTERY_COVERS(GcMaintenanceState);

TEST(CASFormatBattery, GcMaintenanceState)
{
    GcMaintenanceState state{.janitor_cursor = "cas/ns/a"};
    runFormatBattery({FormatId::GcMaintenanceState,
        [&] { return sealObject(FormatId::GcMaintenanceState, encodeGcMaintenanceState(state)); },
        [](std::string_view s) { decodeGcMaintenanceState(std::string(openObject(FormatId::GcMaintenanceState, s))); },
        currentFormatHeader("cas_gc_maintenance_state") + "{\"janitor_cursor\":\"cas/ns/a\"}\n"});
}

TEST(CASGCMaintenanceStateFormat, RegistryLayoutAndCanonicalCodec)
{
    EXPECT_EQ(static_cast<uint16_t>(FormatId::GcMaintenanceState), 25);
    const auto points = changePoints(FormatId::GcMaintenanceState);
    ASSERT_EQ(points.size(), 1u);
    EXPECT_EQ(points[0].generation, 1);
    EXPECT_EQ(points[0].min_reader, 1);
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
        "{{\"type\":\"cas_gc_maintenance_state\",\"v\":{}}}\n{{\"janitor_cursor\":\"\"}}\n", currentCompatibilityVersion()));
    const GcMaintenanceState state{.janitor_cursor = R"(cas/ns/a/"quoted"\\next)"};
    EXPECT_EQ(decodeGcMaintenanceState(encodeGcMaintenanceState(state)), state);
}

TEST(CASGCMaintenanceStateFormat, RejectsMalformedAndBoundsCursor)
{
    const auto bad = [](std::string_view body)
    {
        return "{\"type\":\"cas_gc_maintenance_state\",\"v\":1}\n" + String(body);
    };
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { (void)decodeGcMaintenanceState(bad("{}\n")); });
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { (void)decodeGcMaintenanceState(bad("{\"janitor_cursor\":\"a\",\"janitor_cursor\":\"b\"}\n")); });
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { (void)decodeGcMaintenanceState(bad("{\"janitor_cursor\":\"a\",\"extra\":1}\n")); });
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { (void)decodeGcMaintenanceState(bad("{\"janitor_cursor\":\"a\"}\nx")); });

    const GcMaintenanceState at_limit{.janitor_cursor = String(kMaxGcMaintenanceCursorBytes, 'x')};
    EXPECT_EQ(decodeGcMaintenanceState(encodeGcMaintenanceState(at_limit)), at_limit);
    const GcMaintenanceState over_limit{.janitor_cursor = String(kMaxGcMaintenanceCursorBytes + 1, 'x')};
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LIMIT_EXCEEDED,
        [&] { (void)encodeGcMaintenanceState(over_limit); });
    const String raw = "{\"type\":\"cas_gc_maintenance_state\",\"v\":1}\n{\"janitor_cursor\":\"" + over_limit.janitor_cursor + "\"}\n";
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { (void)decodeGcMaintenanceState(raw); });
    String oversized = R"({"type":"cas_gc_maintenance_state","v":1,"pad":")";
    oversized.append(448 * 1024, 'x');
    oversized += "\"}\n{\"janitor_cursor\":\"";
    oversized.append(kMaxGcMaintenanceCursorBytes, 'y');
    oversized += "\"}\n";
    ASSERT_GT(oversized.size(), traitsFor(FormatId::GcMaintenanceState).object_cap);
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { (void)decodeGcMaintenanceState(oversized); });
}

TEST(CASGCMaintenanceState, ReadsAndCasWithoutAdoptingConflicts)
{
    auto backend = std::make_shared<InMemoryBackend>();
    CasRequests requests(backend, Fence::open());
    const Layout layout("p");
    const String key = layout.gcMaintenanceStateKey();
    auto op = requests.admit();

    const GcMaintenanceReadResult absent = readGcMaintenanceState(op, layout);
    EXPECT_EQ(absent.status, GcMaintenanceReadStatus::Absent);
    EXPECT_FALSE(absent.state);
    EXPECT_FALSE(absent.etag);

    const GcMaintenanceState first{.janitor_cursor = "cas/ns/first"};
    ASSERT_TRUE(std::holds_alternative<Committed>(
        casGcMaintenanceState(op, layout, std::nullopt, first, Retry::standard())));
    const GcMaintenanceReadResult valid = readGcMaintenanceState(op, layout);
    ASSERT_EQ(valid.status, GcMaintenanceReadStatus::Valid);
    ASSERT_TRUE(valid.etag);
    ASSERT_TRUE(valid.state);
    EXPECT_EQ(*valid.state, first);

    const WriteResult advanced = casGcMaintenanceState(op, layout, valid.etag,
        GcMaintenanceState{.janitor_cursor = "cas/ns/advanced"}, Retry::standard());
    ASSERT_TRUE(std::holds_alternative<Committed>(advanced));
    const Etag advanced_etag = std::get<Committed>(advanced).etag;

    ASSERT_TRUE(std::holds_alternative<Committed>(
        op.replace(key, encodeGcMaintenanceState({.janitor_cursor = "winner"}), advanced_etag, Retry::standard())));
    const WriteResult conflict = casGcMaintenanceState(op, layout, valid.etag,
        GcMaintenanceState{.janitor_cursor = "loser"}, Retry::standard());
    EXPECT_TRUE(std::holds_alternative<Conflict>(conflict));
    EXPECT_EQ(decodeGcMaintenanceState(op.read(key, Retry::standard())->bytes).janitor_cursor, "winner");
}

TEST(CASGCMaintenanceState, ClassifiesCorruptionAndResetsOnlyExactToken)
{
    auto backend = std::make_shared<InMemoryBackend>();
    CasRequests requests(backend, Fence::open());
    const Layout layout("p");
    const String key = layout.gcMaintenanceStateKey();
    auto op = requests.admit();

    ASSERT_TRUE(std::holds_alternative<Committed>(op.create(key, "malformed", Retry::once())));
    const GcMaintenanceReadResult corrupt = readGcMaintenanceState(op, layout);
    ASSERT_EQ(corrupt.status, GcMaintenanceReadStatus::Corrupt);
    ASSERT_TRUE(corrupt.etag);
    EXPECT_FALSE(corrupt.state);
    EXPECT_FALSE(corrupt.diagnostic.empty());
    ASSERT_TRUE(std::holds_alternative<Committed>(
        casGcMaintenanceState(op, layout, corrupt.etag, {}, Retry::standard())));
    EXPECT_EQ(decodeGcMaintenanceState(op.read(key, Retry::standard())->bytes), GcMaintenanceState{});
}

TEST(CASGCMaintenanceState, UsesExactlyOneReadOrCasAttempt)
{
    auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
    CasRequests requests(backend, Fence::open());
    const Layout layout("p");
    const String key = layout.gcMaintenanceStateKey();
    auto op = requests.admit();

    EXPECT_EQ(readGcMaintenanceState(op, layout).status, GcMaintenanceReadStatus::Absent);
    EXPECT_EQ(backend->getCount(key), 1u);

    backend->resetCounts();
    ASSERT_TRUE(std::holds_alternative<Committed>(
        casGcMaintenanceState(op, layout, std::nullopt, {}, Retry::standard())));
    EXPECT_EQ(backend->writeTotal(), 1u);
    EXPECT_EQ(backend->getCount(key), 0u);

    backend->resetCounts();
    const WriteResult loser_attempt = casGcMaintenanceState(op, layout, std::nullopt,
        GcMaintenanceState{.janitor_cursor = "loser"}, Retry::standard());
    ASSERT_TRUE(std::holds_alternative<Conflict>(loser_attempt));
    EXPECT_EQ(backend->writeTotal(), 1u);
    /// Unlike the legacy CAS, a refused precondition is settled by ONE exact read before the write
    /// reports the conflict -- `Conflict`'s observation needs to know what is actually there.
    EXPECT_EQ(backend->getCount(key), 1u);

    const std::optional<Object> current = op.read(key, Retry::standard());
    ASSERT_TRUE(current);
    ASSERT_TRUE(std::holds_alternative<Committed>(
        op.replace(key, encodeGcMaintenanceState({.janitor_cursor = "winner"}), current->etag, Retry::standard())));

    backend->resetCounts();
    const WriteResult stale_attempt = casGcMaintenanceState(op, layout, current->etag,
        GcMaintenanceState{.janitor_cursor = "stale"}, Retry::standard());
    ASSERT_TRUE(std::holds_alternative<Conflict>(stale_attempt));
    EXPECT_EQ(backend->writeTotal(), 1u);
    EXPECT_EQ(backend->getCount(key), 1u);
    EXPECT_EQ(decodeGcMaintenanceState(op.read(key, Retry::standard())->bytes).janitor_cursor, "winner");
}

TEST(CASGCMaintenanceState, FutureVersionPropagatesInsteadOfResetting)
{
    auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
    CasRequests requests(backend, Fence::open());
    const Layout layout("p");
    const String key = layout.gcMaintenanceStateKey();
    auto op = requests.admit();

    ASSERT_TRUE(std::holds_alternative<Committed>(op.create(key, fmt::format(
        "{{\"type\":\"cas_gc_maintenance_state\",\"v\":{}}}\n{{\"janitor_cursor\":\"\"}}\n", currentCompatibilityVersion() + 1),
        Retry::once())));

    /// The seed write above lands through the same `write` primitive `CountingBackend` counts, so
    /// reset before measuring what the read itself does.
    backend->resetCounts();
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::UNKNOWN_FORMAT_VERSION,
        [&] { (void)readGcMaintenanceState(op, layout); });
    EXPECT_EQ(backend->writeTotal(), 0u);

    auto failing = std::make_shared<FailingMaintenanceReadBackend>();
    CasRequests failing_requests(failing, Fence::open());
    auto failing_op = failing_requests.admit();
    EXPECT_THROW((void)readGcMaintenanceState(failing_op, layout), std::runtime_error);
}

TEST(CASGCMaintenanceState, LosingCorruptResetPreservesConcurrentWinner)
{
    auto backend = std::make_shared<DB::Cas::tests::CountingBackend>();
    CasRequests requests(backend, Fence::open());
    const Layout layout("p");
    const String key = layout.gcMaintenanceStateKey();
    auto op = requests.admit();

    ASSERT_TRUE(std::holds_alternative<Committed>(op.create(key, "corrupt", Retry::once())));
    const GcMaintenanceReadResult corrupt = readGcMaintenanceState(op, layout);
    ASSERT_EQ(corrupt.status, GcMaintenanceReadStatus::Corrupt);
    ASSERT_TRUE(corrupt.etag);
    ASSERT_TRUE(std::holds_alternative<Committed>(
        op.replace(key, encodeGcMaintenanceState({.janitor_cursor = "winner"}), *corrupt.etag, Retry::standard())));

    backend->resetCounts();
    const WriteResult reset_attempt = casGcMaintenanceState(op, layout, corrupt.etag, {}, Retry::standard());
    ASSERT_TRUE(std::holds_alternative<Conflict>(reset_attempt));
    EXPECT_EQ(backend->writeTotal(), 1u);
    EXPECT_EQ(backend->getCount(key), 1u);
    EXPECT_EQ(decodeGcMaintenanceState(op.read(key, Retry::standard())->bytes).janitor_cursor, "winner");
}
