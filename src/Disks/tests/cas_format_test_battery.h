#pragma once
#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Common/Exception.h>
#include <fmt/format.h>
#include <functional>

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int UNKNOWN_FORMAT_VERSION;
}

/// The shape-level failure-mode battery every v3 format registers with (spec §testing): one call
/// exercises decode-of-encode, golden text, truncation at line boundaries and inside line 1,
/// the v+1 gate, wrong-type, and leading garbage. Key-level rules (tolerant/strict/critical/
/// duplicate) are unit-tested once on JsonObjectReader — the battery stays format-agnostic.

struct FormatBatteryCase
{
    DB::Cas::FormatId id;
    std::function<String()> encode;
    std::function<void(std::string_view)> decode;
    String golden;
    /// Optional format-specific construction for the unsupported-version sample. Fixed-size formats
    /// use this to preserve their physical envelope while growing a version field across a digit
    /// boundary; ordinary line-oriented formats use the default textual replacement below.
    std::function<String(std::string_view)> make_future_version = {};
};

/// Canonical object headers track the current compatibility generation. The type remains an
/// explicit test literal at every call site, so a registry/type mismatch cannot be hidden by a
/// self-derived expectation.
inline String currentFormatHeader(std::string_view type)
{
    return fmt::format("{{\"type\":\"{}\",\"v\":{}}}\n", type, DB::Cas::currentCompatibilityVersion());
}

namespace cas_battery_detail
{
template <typename F>
void expectCode(int code, F && f, const String & context)
{
    try
    {
        f();
        FAIL() << context << ": expected exception " << code;
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), code) << context << ": " << e.message();
    }
}
}

inline void runFormatBattery(const FormatBatteryCase & c)
{
    using namespace DB::Cas;
    namespace ec = DB::ErrorCodes;
    const FormatTraits & t = traitsFor(c.id);

    const String stored = c.encode();
    c.decode(stored); /// round-trip: must not throw

    /// Work on the canonical text (identical to `stored` for raw formats).
    const String text = openObject(c.id, stored);
    ASSERT_TRUE(text.starts_with("{\"type\":\"")) << t.type;

    if (!c.golden.empty())
        EXPECT_EQ(text, c.golden) << "golden text drifted for " << t.type;
    if (looksZstd(stored) && !c.golden.empty())
        EXPECT_EQ(stored, sealObject(c.id, c.golden)) << "pinned compressed arm drifted for " << t.type;

    /// Truncation at every line boundary (drop the terminator too) fails closed.
    for (size_t i = 0; i < text.size(); ++i)
        if (text[i] == '\n')
            cas_battery_detail::expectCode(ec::CORRUPTED_DATA,
                [&] { c.decode(text.substr(0, i)); }, fmt::format("{}: cut at line boundary {}", t.type, i));

    /// Truncation inside line 1.
    const size_t line1 = text.find('\n');
    ASSERT_NE(line1, String::npos);
    for (size_t i = 1; i < line1; i += 3)
        cas_battery_detail::expectCode(ec::CORRUPTED_DATA,
            [&] { c.decode(text.substr(0, i)); }, fmt::format("{}: cut inside header at {}", t.type, i));

    /// v+1 gate.
    const String v_now = fmt::format("\"v\":{}", currentCompatibilityVersion());
    const String v_next = fmt::format("\"v\":{}", currentCompatibilityVersion() + 1);
    String future;
    if (c.make_future_version)
        future = c.make_future_version(text);
    else
    {
        future = text;
        future.replace(future.find(v_now), v_now.size(), v_next);
    }
    cas_battery_detail::expectCode(ec::UNKNOWN_FORMAT_VERSION, [&] { c.decode(future); },
        fmt::format("{}: v+1", t.type));

    /// Wrong type: another VALID registered type in the header.
    const std::string_view other = (t.id == FormatId::PoolMeta) ? "cas_owner" : "cas_pool_meta";
    String mistyped = text;
    mistyped.replace(mistyped.find(t.type), t.type.size(), String(other));
    cas_battery_detail::expectCode(ec::CORRUPTED_DATA, [&] { c.decode(mistyped); },
        fmt::format("{}: wrong type", t.type));

    /// Leading garbage.
    cas_battery_detail::expectCode(ec::CORRUPTED_DATA, [&] { c.decode("X" + text); },
        fmt::format("{}: garbage byte", t.type));
}
