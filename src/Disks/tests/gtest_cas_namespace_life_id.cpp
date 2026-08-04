#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasNamespaceLifeId.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Common/Exception.h>
/// Explicit rather than relying on a transitive path: `DEBUG_OR_SANITIZER_BUILD` (used below to gate
/// the `*DeathTest` split) must resolve in THIS translation unit.
#include <base/defines.h>

#include <concepts>
#include <type_traits>

using namespace DB::Cas;

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

namespace
{

/// The two incarnations of ONE namespace used throughout: distinct, nonzero, and rendering to two
/// hex segments that differ in the first character, so a key that carried the wrong one is visible
/// in the failure message rather than hidden in the tail of 32 digits.
UInt128 incarnationA()
{
    return (static_cast<UInt128>(0x1122'3344'5566'7788ULL) << 64) | static_cast<UInt128>(0x99aa'bbcc'ddee'ff01ULL);
}

UInt128 incarnationB()
{
    return (static_cast<UInt128>(0xfedc'ba98'7654'3210ULL) << 64) | static_cast<UInt128>(0x0123'4567'89ab'cdefULL);
}

const String kNs = "srv1/tbl@cas@";
const String kHexA = "112233445566778899aabbccddeeff01";
const String kHexB = "fedcba98765432100123456789abcdef";
const String kTxn = "0000000000000007-000000000000008e";

/// Asserts that `body` refuses with CORRUPTED_DATA and that the message names `key` -- the refusal is
/// only useful to an operator if it says which object was rejected (the CI-observability rule).
template <class F>
void expectRefusalNaming(F && body, const String & key)
{
    try
    {
        std::forward<F>(body)();
        FAIL() << "expected a refusal for key '" << key << "', got none";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA) << "for key '" << key << "'";
        EXPECT_NE(e.message().find(key), String::npos)
            << "refusal does not name the offending key '" << key << "'; message: " << e.message();
    }
}

/// The compile-time half of spec §9 r9-5 #3, one pair per migrated helper. The NEGATIVE proves the
/// namespace-only overload is gone; the POSITIVE proves the concept is actually looking at a real
/// member, so a typo in the requires-clause cannot make the negative pass vacuously. Both halves are
/// genuinely templated on `L`, so a missing member is a substitution failure rather than a hard error.
template <class L>
concept HasNamespaceOnlyRefsNamespacePrefix =
    requires(const L & l, const RootNamespace & ns) { l.namespaceStreamPrefix(ns); };
template <class L>
concept HasIncarnationRefsNamespacePrefix =
    requires(const L & l, const NamespaceLifeId & id) { l.namespaceStreamPrefix(id); };

template <class L>
concept HasNamespaceOnlyRefLogKey =
    requires(const L & l, const RootNamespace & ns, const RefTxnId & id) { l.refLogKey(ns, id); };
template <class L>
concept HasIncarnationRefLogKey =
    requires(const L & l, const NamespaceLifeId & ns_id, const RefTxnId & id) { l.refLogKey(ns_id, id); };

template <class L>
concept HasNamespaceOnlyRefSnapshotKey =
    requires(const L & l, const RootNamespace & ns, const RefTxnId & id) { l.refSnapshotKey(ns, id); };
template <class L>
concept HasIncarnationRefSnapshotKey =
    requires(const L & l, const NamespaceLifeId & ns_id, const RefTxnId & id) { l.refSnapshotKey(ns_id, id); };

template <class L>
concept HasNamespaceOnlyRefCkptKey =
    requires(const L & l, const RootNamespace & ns) { l.refCkptKey(ns); };
template <class L>
concept HasIncarnationRefCkptKey =
    requires(const L & l, const NamespaceLifeId & id) { l.refCkptKey(id); };

/// The namespace-FILE half of the same pattern (directive §1: "Delete all namespace-only ref and
/// namespace-file key overloads"), paired the same way.
template <class L>
concept HasNamespaceOnlyNamespaceFileKey =
    requires(const L & l, const RootNamespace & ns, const String & n) { l.namespaceFileKey(ns, n); };
template <class L>
concept HasIncarnationNamespaceFileKey =
    requires(const L & l, const NamespaceLifeId & life, const String & n) { l.namespaceFileKey(life, n); };

template <class L>
concept HasNamespaceOnlyNamespaceFilesPrefix =
    requires(const L & l, const RootNamespace & ns) { l.namespaceFilesPrefix(ns); };
template <class L>
concept HasIncarnationNamespaceFilesPrefix =
    requires(const L & l, const NamespaceLifeId & life) { l.namespaceFilesPrefix(life); };

/// The two OUT-OF-SCOPE families (Constraint 12, directive §2 "Keep these unchanged"): loose
/// mountpoint objects and part manifests keep the identity they have today. Each is paired in the
/// opposite direction from the migrated helpers -- the POSITIVE is the un-life-scoped overload that
/// must survive, the NEGATIVE is the life-scoped overload that must never appear.
template <class L>
concept HasUnscopedMountpointObjectKey =
    requires(const L & l, const String & key) { l.mountpointObjectKey(key); };
template <class L>
concept HasLifeScopedMountpointObjectKey =
    requires(const L & l, const NamespaceLifeId & life, const String & key) { l.mountpointObjectKey(life, key); };

template <class L>
concept HasNamespaceOnlyManifestNamespacePrefix =
    requires(const L & l, const RootNamespace & ns) { l.manifestNamespacePrefix(ns); };
template <class L>
concept HasLifeScopedManifestNamespacePrefix =
    requires(const L & l, const NamespaceLifeId & life) { l.manifestNamespacePrefix(life); };

}

/// Every ref-layer key names one life by an opaque fixed-width physical id. The logical namespace is
/// intentionally absent and must be supplied by a catalog join.
TEST(CASNamespaceLifeId, KeysCarryTheIncarnationSegment)
{
    Layout l("p");
    const NamespaceLifeId id = NamespaceLifeId::fromCatalogEntry(RootNamespace{kNs}, incarnationA());
    const RefTxnId txn{7, 0x8e};
    const String life = "p/cas/ns/stream/" + kHexA + "/";
    const String state = "p/cas/ns/state/" + kHexA + "/";

    EXPECT_EQ(l.namespaceStreamPrefix(id), life);
    EXPECT_EQ(l.refLogKey(id, txn), life + "_log/" + kTxn + ".zst");
    EXPECT_EQ(l.refSnapshotKey(id, txn), life + "_snap/" + kTxn + ".zst");
    EXPECT_EQ(l.refCkptKey(id), state + "_ckpt");

    const auto parsed_log = l.parseRefObjectKey(l.refLogKey(id, txn));
    ASSERT_TRUE(parsed_log.has_value());
    EXPECT_EQ(parsed_log->life_id, id.incarnation);
    EXPECT_EQ(parsed_log->kind, RefObjectKind::Log);
    EXPECT_EQ(parsed_log->txn_id, txn);

    const auto parsed_snap = l.parseRefObjectKey(l.refSnapshotKey(id, txn));
    ASSERT_TRUE(parsed_snap.has_value());
    EXPECT_EQ(parsed_snap->life_id, id.incarnation);
    EXPECT_EQ(parsed_snap->kind, RefObjectKind::Snap);

    EXPECT_EQ(l.parseRefCkptKey(l.refCkptKey(id)), id.incarnation);
}

/// The property the type exists for: two lives of the SAME namespace name share no key at all, so a
/// reborn namespace can neither read nor delete the previous life's objects by name.
TEST(CASNamespaceLifeId, TwoLivesOfOneNamespaceShareNoKeys)
{
    Layout l("p");
    const RefTxnId txn{7, 0x8e};
    const NamespaceLifeId first = NamespaceLifeId::fromCatalogEntry(RootNamespace{kNs}, incarnationA());
    const NamespaceLifeId second = NamespaceLifeId::fromCatalogEntry(RootNamespace{kNs}, incarnationB());

    EXPECT_EQ(first.ns, second.ns);
    EXPECT_NE(first, second);
    EXPECT_NE(l.namespaceStreamPrefix(first), l.namespaceStreamPrefix(second));
    EXPECT_NE(l.refLogKey(first, txn), l.refLogKey(second, txn));
    EXPECT_NE(l.refCkptKey(first), l.refCkptKey(second));

    /// Neither life's prefix covers the other: a LIST of one enumerates only its own objects.
    EXPECT_FALSE(l.refLogKey(second, txn).starts_with(l.namespaceStreamPrefix(first)));
    EXPECT_FALSE(l.refLogKey(first, txn).starts_with(l.namespaceStreamPrefix(second)));

    /// A key spelling the other life parses back to the OTHER id -- the parser reports what the key
    /// says; it is the catalog, not the parser, that decides which lives are current.
    const auto parsed = l.parseRefObjectKey(l.refLogKey(second, txn));
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->life_id, second.incarnation);
    EXPECT_NE(parsed->life_id, first.incarnation);
}

/// Zero is not a wildcard and not "the namespace itself": it can never be constructed, so it can
/// never reach a key builder.
///
/// Both throws below raise `LOGICAL_ERROR`, which aborts the process in debug/sanitizer builds
/// instead of behaving like a catchable exception (`Common/Exception.cpp`'s `handle_error_code`) --
/// `CASNamespaceLifeIdDeathTest.ZeroIncarnationIsUnconstructibleAborts` below proves the abort
/// positively in those builds instead.
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(CASNamespaceLifeId, ZeroIncarnationIsUnconstructible)
{
    EXPECT_THROW(NamespaceLifeId::fromCatalogEntry(RootNamespace{kNs}, UInt128{0}), DB::Exception);
    EXPECT_THROW(renderIncarnation(UInt128{0}), DB::Exception);
    EXPECT_NO_THROW(NamespaceLifeId::fromCatalogEntry(RootNamespace{kNs}, incarnationA()));
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(CASNamespaceLifeIdDeathTest, ZeroIncarnationIsUnconstructibleAborts)
{
    EXPECT_DEATH({ (void)NamespaceLifeId::fromCatalogEntry(RootNamespace{kNs}, UInt128{0}); }, "incarnation must be nonzero");
    EXPECT_DEATH({ (void)renderIncarnation(UInt128{0}); }, "incarnation must be nonzero");
    EXPECT_NO_THROW(NamespaceLifeId::fromCatalogEntry(RootNamespace{kNs}, incarnationA()));
}
#endif

/// Generation-5 namespace-bearing keys are outside the generation-6 parser roots altogether. Pool
/// admission rejects their generation before any listed-key parser is involved.
TEST(CASNamespaceLifeId, GenerationFiveNamespaceBearingKeysAreOutsideTheFinalGrammar)
{
    Layout l("p");
    const String legacy_log = "p/cas/refs/" + kNs + "/_log/" + kTxn + ".zst";
    const String legacy_snap = "p/cas/refs/" + kNs + "/_snap/" + kTxn + ".zst";
    const String legacy_cleanup = "p/cas/refs/" + kNs + "/_cleanup/" + kTxn;
    const String legacy_ckpt = "p/cas/refs/" + kNs + "/_ckpt";
    /// A single-segment namespace leaves nothing at all where the incarnation belongs.
    const String legacy_single_segment = "p/cas/refs/srv1/_log/" + kTxn + ".zst";

    EXPECT_FALSE(l.parseRefObjectKey(legacy_log));
    EXPECT_FALSE(l.parseRefObjectKey(legacy_snap));
    EXPECT_FALSE(l.parseRefObjectKey(legacy_cleanup));
    EXPECT_FALSE(l.parseRefObjectKey(legacy_single_segment));
    EXPECT_FALSE(l.parseRefCkptKey(legacy_ckpt));
}

/// An all-zero incarnation segment is well-formed hex naming no life, so it is corruption on the read
/// side exactly as it is unconstructible on the write side.
TEST(CASNamespaceLifeId, ParsersRefuseAZeroIncarnation)
{
    Layout l("p");
    const String zeros(32, '0');
    const String zero_log = "p/cas/ns/stream/" + zeros + "/_log/" + kTxn + ".zst";
    const String zero_ckpt = "p/cas/ns/state/" + zeros + "/_ckpt";

    expectRefusalNaming([&] { l.parseRefObjectKey(zero_log); }, zero_log);
    expectRefusalNaming([&] { l.parseRefCkptKey(zero_ckpt); }, zero_ckpt);
}

/// The incarnation segment has ONE canonical spelling. A key that is nearly right -- wrong width,
/// upper case, a non-hex digit -- is refused rather than repaired, so two spellings of one life can
/// never both exist.
TEST(CASNamespaceLifeId, ParsersRefuseAMalformedIncarnationSegment)
{
    Layout l("p");
    const String upper = "112233445566778899AABBCCDDEEFF01";
    const String too_short = kHexA.substr(0, 31);
    const String too_long = kHexA + "0";
    const String non_hex = kHexA.substr(0, 31) + "z";

    for (const String & bad : {upper, too_short, too_long, non_hex})
    {
        const String log_key = "p/cas/ns/stream/" + bad + "/_log/" + kTxn + ".zst";
        const String ckpt_key = "p/cas/ns/state/" + bad + "/_ckpt";
        expectRefusalNaming([&] { l.parseRefObjectKey(log_key); }, log_key);
        expectRefusalNaming([&] { l.parseRefCkptKey(ckpt_key); }, ckpt_key);
    }
}

/// The boundary between "corrupt" and "not ours". Refusal is reserved for keys the parser has already
/// recognized as OUR ref objects; anything else keeps returning `std::nullopt`, because classifying an
/// untrusted listed key remains an ordinary "is this ours" question and a sweep must be able to walk
/// past foreign debris without an exception.
TEST(CASNamespaceLifeId, ForeignAndUnrecognizedKeysStayInert)
{
    Layout l("p");
    const NamespaceLifeId id = NamespaceLifeId::fromCatalogEntry(RootNamespace{kNs}, incarnationA());
    const RefTxnId txn{7, 0x8e};

    /// Foreign top-level prefix (another pool, another subtree).
    EXPECT_FALSE(l.parseRefObjectKey("q/cas/refs/" + kNs + "/" + kHexA + "/_log/" + kTxn + ".zst").has_value());
    EXPECT_FALSE(l.parseRefCkptKey("q/cas/refs/" + kNs + "/" + kHexA + "/_ckpt").has_value());
    EXPECT_FALSE(l.parseRefObjectKey("p/cas/manifests/" + kNs + "/" + kHexA + "/_log/" + kTxn).has_value());
    /// An unrecognized kind directory is not one of our ref objects, so its incarnation segment is
    /// never even reached.
    EXPECT_FALSE(l.parseRefObjectKey("p/cas/refs/" + kNs + "/_bogus/" + kTxn).has_value());
    /// A non-canonical transaction id likewise loses the key before the incarnation is judged.
    EXPECT_FALSE(l.parseRefObjectKey(l.namespaceStreamPrefix(id) + "_log/7-8e").has_value());
    /// The two parsers stay disjoint: neither claims the other's objects.
    EXPECT_FALSE(l.parseRefObjectKey(l.refCkptKey(id)).has_value());
    EXPECT_FALSE(l.parseRefCkptKey(l.refLogKey(id, txn)).has_value());
    /// No namespace and no incarnation at all.
    EXPECT_FALSE(l.parseRefObjectKey("p/cas/refs/_log/" + kTxn + ".zst").has_value());
    EXPECT_FALSE(l.parseRefCkptKey("p/cas/refs/_ckpt").has_value());
}

/// Namespace files are life-keyed too: `cas/ns/state/<life_id>/_files/<relative-name>`. The
/// round trip covers a flat name and a NESTED one, because the dedup log's segments live in a
/// table-level subdirectory and the nested shape is the one on the insert path.
TEST(CASNamespaceLifeId, NamespaceFileKeysCarryTheIncarnationSegment)
{
    Layout l("p");
    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(RootNamespace{kNs}, incarnationA());
    const String files = "p/cas/ns/state/" + kHexA + "/_files/";

    EXPECT_EQ(l.namespaceFilesPrefix(life), files);
    EXPECT_EQ(l.namespaceFileKey(life, "format_version.txt"), files + "format_version.txt");
    EXPECT_EQ(l.namespaceFileKey(life, "deduplication_logs/deduplication_log_1.txt"),
              files + "deduplication_logs/deduplication_log_1.txt");

    for (const String & name : {String("format_version.txt"), String("deduplication_logs/deduplication_log_1.txt")})
    {
        const auto parsed = l.parseNamespaceFileKey(l.namespaceFileKey(life, name));
        ASSERT_TRUE(parsed.has_value()) << "for name '" << name << "'";
        EXPECT_EQ(parsed->life_id, life.incarnation);
        EXPECT_EQ(parsed->relative_name, name);
    }

    /// Two lives of one namespace share no file key either, and neither files prefix covers the other.
    const NamespaceLifeId second = NamespaceLifeId::fromCatalogEntry(RootNamespace{kNs}, incarnationB());
    EXPECT_NE(l.namespaceFileKey(life, "format_version.txt"), l.namespaceFileKey(second, "format_version.txt"));
    EXPECT_FALSE(l.namespaceFileKey(second, "format_version.txt").starts_with(l.namespaceFilesPrefix(life)));
}

/// Generation-5 namespace-bearing file keys are outside the final parser root. Malformed ids under the
/// final state root are corruption and name the offending key.
TEST(CASNamespaceLifeId, NamespaceFileParserRefusesLegacyAndMalformedIncarnations)
{
    Layout l("p");
    const String zeros(32, '0');
    const String legacy = "p/roots/" + kNs + "/_files/format_version.txt";
    /// A single-segment namespace leaves nothing at all where the incarnation belongs.
    const String legacy_single_segment = "p/roots/srv1/_files/format_version.txt";
    const String zero_inc = "p/cas/ns/state/" + zeros + "/_files/format_version.txt";

    EXPECT_FALSE(l.parseNamespaceFileKey(legacy));
    EXPECT_FALSE(l.parseNamespaceFileKey(legacy_single_segment));
    expectRefusalNaming([&] { l.parseNamespaceFileKey(zero_inc); }, zero_inc);

    const String upper = "112233445566778899AABBCCDDEEFF01";
    const String too_short = kHexA.substr(0, 31);
    const String too_long = kHexA + "0";
    const String non_hex = kHexA.substr(0, 31) + "z";
    for (const String & bad : {upper, too_short, too_long, non_hex})
    {
        const String key = "p/cas/ns/state/" + bad + "/_files/format_version.txt";
        expectRefusalNaming([&] { l.parseNamespaceFileKey(key); }, key);
    }
}

/// The corrupt/not-ours boundary for file keys, mirroring the ref parsers': refusal is reserved for
/// keys already identified as OUR namespace files by their reserved `_files` segment. A loose
/// mountpoint object has no such segment and is a legitimate inhabitant of `roots/`, so it must parse
/// as `std::nullopt` and never as damage.
TEST(CASNamespaceLifeId, ForeignAndMountpointKeysStayInertForTheFileParser)
{
    Layout l("p");
    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(RootNamespace{kNs}, incarnationA());

    EXPECT_FALSE(l.parseNamespaceFileKey("q/roots/" + kNs + "/" + kHexA + "/_files/x").has_value());
    EXPECT_FALSE(l.parseNamespaceFileKey(l.mountpointObjectKey("srv1/clickhouse_access_check_abc")).has_value());
    EXPECT_FALSE(l.parseNamespaceFileKey("p/cas/refs/" + kNs + "/" + kHexA + "/_files/x").has_value());
    /// The files prefix itself names no file: there is no relative name after the reserved segment.
    EXPECT_FALSE(l.parseNamespaceFileKey(l.namespaceFilesPrefix(life)).has_value());
    /// And the ref parsers do not claim a file key.
    EXPECT_FALSE(l.parseRefObjectKey(l.namespaceFileKey(life, "x")).has_value());
    EXPECT_FALSE(l.parseRefCkptKey(l.namespaceFileKey(life, "x")).has_value());
}

/// Physical file keys use only `life_id`, so changing the logical spelling cannot redirect a key. A
/// relative name may still contain `_files` and round-trips after the fixed delimiter.
TEST(CASNamespaceLifeId, PhysicalFileKeysIgnoreLogicalNamespaceSpelling)
{
    Layout l("p");
    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(RootNamespace{kNs}, incarnationA());
    const NamespaceLifeId differently_named = NamespaceLifeId::fromCatalogEntry(
        RootNamespace{"different/_files/spelling"}, incarnationA());
    EXPECT_EQ(l.namespaceFilesPrefix(life), l.namespaceFilesPrefix(differently_named));
    const String nested_name = "deduplication_logs/_files/log_1.txt";
    const auto parsed = l.parseNamespaceFileKey(l.namespaceFileKey(life, nested_name));
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed->life_id, life.incarnation);
    EXPECT_EQ(parsed->relative_name, nested_name);
}

/// The "cannot compile" half of spec §9 r9-5 #3: after this task there is no way to reach a ref-layer
/// key from a namespace alone, so dropping the incarnation is a compile error rather than an aliasing
/// bug. Each helper is asserted twice -- the namespace-only form absent, the incarnation form present.
TEST(CASNamespaceLifeId, NamespaceOnlyKeyHelpersDoNotExist)
{
    static_assert(!HasNamespaceOnlyRefsNamespacePrefix<Layout>);
    static_assert(HasIncarnationRefsNamespacePrefix<Layout>);

    static_assert(!HasNamespaceOnlyRefLogKey<Layout>);
    static_assert(HasIncarnationRefLogKey<Layout>);

    static_assert(!HasNamespaceOnlyRefSnapshotKey<Layout>);
    static_assert(HasIncarnationRefSnapshotKey<Layout>);

    static_assert(!HasNamespaceOnlyRefCkptKey<Layout>);
    static_assert(HasIncarnationRefCkptKey<Layout>);

    static_assert(!HasNamespaceOnlyNamespaceFileKey<Layout>);
    static_assert(HasIncarnationNamespaceFileKey<Layout>);

    static_assert(!HasNamespaceOnlyNamespaceFilesPrefix<Layout>);
    static_assert(HasIncarnationNamespaceFilesPrefix<Layout>);

    SUCCEED();
}

/// Directive §1's remaining requirements on the type, fenced rather than fixed: the type declares no
/// conversion operator and no `RootNamespace` constructor takes a `NamespaceLifeId`, so nothing
/// interconverts in either direction today and only an explicit `.ns` crosses. Without these
/// assertions a later convenience conversion would land unnoticed, and dropping the incarnation would
/// become representable again -- which is the property the whole re-keying rests on.
TEST(CASNamespaceLifeId, NamespaceLifeIdAndRootNamespaceDoNotInterconvert)
{
    static_assert(!std::convertible_to<NamespaceLifeId, RootNamespace>);
    static_assert(!std::constructible_from<RootNamespace, NamespaceLifeId>);
    static_assert(!std::is_default_constructible_v<NamespaceLifeId>);

    SUCCEED();
}

/// The out-of-scope fences, and they are POSITIVE on purpose: Constraint 12 keeps loose mountpoint
/// objects and part manifests on the identity they have today, so this task must NOT have qualified
/// them. If a negative here fails, someone added a life-scoped overload to a family the amendment
/// explicitly excluded; if a positive fails, someone removed the un-scoped one those callers use.
TEST(CASNamespaceLifeId, MountpointObjectsAndManifestsStayUnqualified)
{
    static_assert(HasUnscopedMountpointObjectKey<Layout>);
    static_assert(!HasLifeScopedMountpointObjectKey<Layout>);

    static_assert(HasNamespaceOnlyManifestNamespacePrefix<Layout>);
    static_assert(!HasLifeScopedManifestNamespacePrefix<Layout>);

    SUCCEED();
}
