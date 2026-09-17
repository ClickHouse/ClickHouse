#include <DataTypes/UDT/AuthorityInventory.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <limits>
#include <string_view>
#include <vector>

namespace DB::UDT
{
namespace
{

UUID uuid(UInt64 high, UInt64 low)
{
    UUID result{};
    UUIDHelpers::getHighBytes(result) = high;
    UUIDHelpers::getLowBytes(result) = low;
    return result;
}

Digest digest(UInt8 first)
{
    Digest result{};
    for (size_t index = 0; index < result.size(); ++index)
        result[index] = static_cast<UInt8>(first + index);
    return result;
}

String toHex(std::string_view bytes)
{
    constexpr char digits[] = "0123456789abcdef";
    String result;
    result.reserve(bytes.size() * 2);
    for (const unsigned char byte : bytes)
    {
        result.push_back(digits[byte >> 4]);
        result.push_back(digits[byte & 0x0f]);
    }
    return result;
}

String toHex(const Digest & bytes)
{
    return toHex({reinterpret_cast<const char *>(bytes.data()), bytes.size()});
}

AuthorityInventoryLeaf leaf(AuthorityInventoryRecordKind kind, UUID object_uuid, UInt64 revision, UInt8 digest_first)
{
    return {
        .key = {.format_version = authority_inventory_format_version, .record_kind = kind, .object_uuid = object_uuid},
        .object_revision = revision,
        .canonical_record_hash = digest(digest_first),
    };
}

template <typename Callback>
void expectInventoryError(AuthorityInventoryError::Code expected, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "Expected AuthorityInventoryError";
    }
    catch (const AuthorityInventoryError & error)
    {
        EXPECT_EQ(error.code, expected);
    }
}

TEST(UDTAuthorityInventory, LeafCodecHasStableBytes)
{
    const auto definition_leaf = leaf(
        AuthorityInventoryRecordKind::TypeDefinition, uuid(0x0011223344556677ULL, 0x8899aabbccddeeffULL), 0x0102030405060708ULL, 0x80);
    const String leaf_bytes = encodeAuthorityInventoryLeaf(definition_leaf);
    EXPECT_EQ(
        toHex(leaf_bytes),
        "01000100112233445566778899aabbccddeeff0807060504030201"
        "808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9f");
    EXPECT_EQ(decodeAuthorityInventoryLeaf(leaf_bytes), definition_leaf);
}

TEST(UDTAuthorityInventory, MerkleRadixRootIsCanonicalAndBounded)
{
    std::vector leaves{
        leaf(AuthorityInventoryRecordKind::TypeDefinition, uuid(0, 1), 1, 0x10),
        leaf(AuthorityInventoryRecordKind::TypeDefinition, uuid(0, 2), 2, 0x30),
        leaf(AuthorityInventoryRecordKind::SidecarExpectation, uuid(0, 1), 7, 0x50),
    };
    std::ranges::sort(leaves, authorityInventoryLeafLess);

    UInt64 hashed_nodes = 0;
    const Digest root = computeAuthorityInventoryRoot(leaves, {}, &hashed_nodes);
    EXPECT_EQ(hashed_nodes, 5);
    EXPECT_EQ(toHex(root), "a0af926155207162368caab429020b87e79eb150ee70cdd59342b3b359dcd8a5");

    const auto summary = buildAuthorityInventorySummary(leaves);
    EXPECT_EQ(summary.leaf_count, 3);
    EXPECT_EQ(summary.merkle_radix_root, root);

    const auto inventory = AuthorityInventory::create(summary, leaves);
    EXPECT_EQ(inventory->getSummary(), summary);
    ASSERT_NE(inventory->find(leaves[1].key), nullptr);
    EXPECT_EQ(*inventory->find(leaves[1].key), leaves[1]);
    EXPECT_EQ(
        inventory->find({
            .format_version = authority_inventory_format_version,
            .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
            .object_uuid = uuid(0, 99),
        }),
        nullptr);
}

TEST(UDTAuthorityInventory, EmptyRootIsStable)
{
    UInt64 hashed_nodes = 0;
    const Digest root = computeAuthorityInventoryRoot({}, {}, &hashed_nodes);
    EXPECT_EQ(hashed_nodes, 1);
    EXPECT_EQ(toHex(root), "52f3846ec2767274b439747f62f21b458a282940803be363934787b180d84480");

    const auto summary = buildAuthorityInventorySummary({});
    EXPECT_EQ(summary.leaf_count, 0);
    EXPECT_EQ(summary.merkle_radix_root, root);
    EXPECT_TRUE(AuthorityInventory::create(summary, {})->getLeaves().empty());
}

TEST(UDTAuthorityInventory, RejectsMalformedAndNoncanonicalRecords)
{
    const auto valid_leaf = leaf(AuthorityInventoryRecordKind::TypeDefinition, uuid(1, 2), 3, 0x20);
    const String valid_leaf_bytes = encodeAuthorityInventoryLeaf(valid_leaf);

    expectInventoryError(
        AuthorityInventoryError::Code::Truncated,
        [&]
        { static_cast<void>(decodeAuthorityInventoryLeaf(std::string_view(valid_leaf_bytes).substr(0, valid_leaf_bytes.size() - 1))); });

    String trailing_leaf = valid_leaf_bytes;
    trailing_leaf.push_back('\0');
    expectInventoryError(
        AuthorityInventoryError::Code::TrailingData, [&] { static_cast<void>(decodeAuthorityInventoryLeaf(trailing_leaf)); });

    String unknown_version = valid_leaf_bytes;
    unknown_version[0] = 2;
    expectInventoryError(
        AuthorityInventoryError::Code::UnsupportedVersion, [&] { static_cast<void>(decodeAuthorityInventoryLeaf(unknown_version)); });

    String unknown_kind = valid_leaf_bytes;
    unknown_kind[2] = static_cast<char>(0x7f);
    expectInventoryError(
        AuthorityInventoryError::Code::InvalidValue, [&] { static_cast<void>(decodeAuthorityInventoryLeaf(unknown_kind)); });

    auto zero_revision = valid_leaf;
    zero_revision.object_revision = 0;
    expectInventoryError(
        AuthorityInventoryError::Code::InvalidValue, [&] { static_cast<void>(encodeAuthorityInventoryLeaf(zero_revision)); });

    auto zero_hash = valid_leaf;
    zero_hash.canonical_record_hash = {};
    EXPECT_EQ(decodeAuthorityInventoryLeaf(encodeAuthorityInventoryLeaf(zero_hash)), zero_hash);

    AuthorityInventoryLimits small_leaf;
    small_leaf.maximum_leaf_bytes = valid_leaf_bytes.size() - 1;
    expectInventoryError(
        AuthorityInventoryError::Code::LimitExceeded,
        [&] { static_cast<void>(decodeAuthorityInventoryLeaf(valid_leaf_bytes, small_leaf)); });
}

TEST(UDTAuthorityInventory, RejectsSummaryMismatch)
{
    const auto valid_leaf = leaf(AuthorityInventoryRecordKind::TypeDefinition, uuid(1, 2), 3, 0x20);
    std::vector leaves{valid_leaf};
    const auto summary = buildAuthorityInventorySummary(leaves);

    auto wrong_count = summary;
    wrong_count.leaf_count = 2;
    expectInventoryError(
        AuthorityInventoryError::Code::InvalidValue, [&] { static_cast<void>(AuthorityInventory::create(wrong_count, leaves)); });

    auto wrong_root = summary;
    wrong_root.merkle_radix_root[0] ^= 0x80;
    expectInventoryError(
        AuthorityInventoryError::Code::InvalidValue, [&] { static_cast<void>(AuthorityInventory::create(wrong_root, leaves)); });
}

TEST(UDTAuthorityInventory, RejectsUnsortedDuplicateAndOversizedLeafSets)
{
    auto first = leaf(AuthorityInventoryRecordKind::TypeDefinition, uuid(0, 1), 1, 0x10);
    auto second = leaf(AuthorityInventoryRecordKind::TypeDefinition, uuid(0, 2), 2, 0x30);

    std::vector unsorted{second, first};
    expectInventoryError(
        AuthorityInventoryError::Code::NonCanonical, [&] { static_cast<void>(computeAuthorityInventoryRoot(unsorted)); });

    std::vector duplicate{first, first};
    expectInventoryError(
        AuthorityInventoryError::Code::NonCanonical, [&] { static_cast<void>(computeAuthorityInventoryRoot(duplicate)); });

    AuthorityInventoryLimits one_leaf;
    one_leaf.maximum_leaves = 1;
    std::vector sorted{first, second};
    expectInventoryError(
        AuthorityInventoryError::Code::LimitExceeded, [&] { static_cast<void>(computeAuthorityInventoryRoot(sorted, one_leaf)); });

    AuthorityInventoryLimits short_leaf;
    short_leaf.maximum_leaf_bytes = 1;
    expectInventoryError(
        AuthorityInventoryError::Code::LimitExceeded,
        [&] { static_cast<void>(buildAuthorityInventorySummary(std::span<const AuthorityInventoryLeaf>(&first, 1), short_leaf)); });
}

TEST(UDTAuthorityInventory, PersistentDeltaMatchesFullBuildWithoutScanningUntouchedLeaves)
{
    std::vector<AuthorityInventoryLeaf> leaves;
    leaves.reserve(4'096);
    for (UInt64 index = 1; index <= 4'096; ++index)
        leaves.push_back(leaf(AuthorityInventoryRecordKind::TypeDefinition, uuid(0xA000, index), index, static_cast<UInt8>(index)));
    std::ranges::sort(leaves, authorityInventoryLeafLess);
    const auto base = AuthorityInventory::create(buildAuthorityInventorySummary(leaves), leaves);

    const auto first_removed = leaves[1'023];
    const auto second_removed = leaves[3'071];
    std::vector<AuthorityInventoryLeafDelta> deltas{
        {.key = first_removed.key, .before = first_removed, .after = std::nullopt},
        {.key = second_removed.key, .before = second_removed, .after = std::nullopt},
    };
    std::ranges::sort(deltas, [](const auto & lhs, const auto & rhs) { return authorityInventoryKeyLess(lhs.key, rhs.key); });

    AuthorityInventoryMutationStatistics statistics;
    const auto next = AuthorityInventory::applyMutation(base, deltas, {}, &statistics);
    EXPECT_EQ(statistics.deltas_applied, 2u);
    EXPECT_EQ(statistics.leaves_materialized, 0u);
    EXPECT_LE(statistics.nodes_visited, 2u * 38u);
    EXPECT_LE(statistics.nodes_hashed, 2u * 39u);

    std::erase_if(
        leaves, [&](const auto & candidate) { return candidate.key == first_removed.key || candidate.key == second_removed.key; });
    const auto rebuilt = AuthorityInventory::create(buildAuthorityInventorySummary(leaves), leaves);
    EXPECT_EQ(next->getSummary(), rebuilt->getSummary());
    EXPECT_TRUE(std::ranges::equal(next->getLeaves(), rebuilt->getLeaves()));

    const auto first_only = AuthorityInventory::applyMutation(base, std::span<const AuthorityInventoryLeafDelta>(deltas).first(1));
    const auto sequential = AuthorityInventory::applyMutation(first_only, std::span<const AuthorityInventoryLeafDelta>(deltas).subspan(1));
    EXPECT_EQ(sequential->getSummary(), rebuilt->getSummary());
}

TEST(UDTAuthorityInventory, BatchLimitUsesFinalCountAndValidationDoesNoMutationWork)
{
    std::vector<AuthorityInventoryLeaf> leaves{
        leaf(AuthorityInventoryRecordKind::SidecarExpectation, uuid(0, 10), 1, 0x10),
        leaf(AuthorityInventoryRecordKind::SidecarExpectation, uuid(0, 20), 2, 0x20),
    };
    std::ranges::sort(leaves, authorityInventoryLeafLess);
    AuthorityInventoryLimits limits;
    limits.maximum_leaves = leaves.size();
    const auto base = AuthorityInventory::create(buildAuthorityInventorySummary(leaves, limits), leaves, limits);

    const auto addition = leaf(AuthorityInventoryRecordKind::TypeDefinition, uuid(0, 30), 3, 0x30);
    std::vector<AuthorityInventoryLeafDelta> balanced{
        {.key = addition.key, .before = std::nullopt, .after = addition},
        {.key = leaves.front().key, .before = leaves.front(), .after = std::nullopt},
    };
    ASSERT_TRUE(authorityInventoryKeyLess(balanced.front().key, balanced.back().key));
    AuthorityInventoryMutationStatistics statistics;
    const auto next = AuthorityInventory::applyMutation(base, balanced, limits, &statistics);
    EXPECT_EQ(next->getSummary().leaf_count, limits.maximum_leaves);
    EXPECT_EQ(statistics.deltas_applied, 2u);
    EXPECT_NE(next->find(addition.key), nullptr);
    EXPECT_EQ(next->find(leaves.front().key), nullptr);

    const AuthorityInventoryLeafDelta excess{.key = addition.key, .before = std::nullopt, .after = addition};
    statistics = {.deltas_applied = 99, .nodes_visited = 99, .nodes_created = 99, .nodes_hashed = 99, .leaves_materialized = 99};
    expectInventoryError(
        AuthorityInventoryError::Code::LimitExceeded,
        [&] { static_cast<void>(AuthorityInventory::applyMutation(base, std::span(&excess, 1), limits, &statistics)); });
    EXPECT_EQ(statistics, AuthorityInventoryMutationStatistics{});
    EXPECT_EQ(base->find(addition.key), nullptr);

    auto wrong_before = leaves.back();
    ++wrong_before.object_revision;
    const AuthorityInventoryLeafDelta stale{
        .key = leaves.back().key,
        .before = wrong_before,
        .after = std::nullopt,
    };
    statistics = {.deltas_applied = 99, .nodes_visited = 99, .nodes_created = 99, .nodes_hashed = 99, .leaves_materialized = 99};
    expectInventoryError(
        AuthorityInventoryError::Code::NonCanonical,
        [&] { static_cast<void>(AuthorityInventory::applyMutation(base, std::span(&stale, 1), limits, &statistics)); });
    EXPECT_EQ(statistics, AuthorityInventoryMutationStatistics{});
    EXPECT_NE(base->find(leaves.back().key), nullptr);
}

TEST(UDTAuthorityInventory, EmptyBaseAdditionEnforcesLeafByteLimitBeforeMutationWork)
{
    const auto base = AuthorityInventory::create(buildAuthorityInventorySummary({}), {});
    const auto addition = leaf(AuthorityInventoryRecordKind::TypeDefinition, uuid(0, 30), 3, 0x30);
    const AuthorityInventoryLeafDelta delta{.key = addition.key, .before = std::nullopt, .after = addition};

    AuthorityInventoryLimits limits;
    limits.maximum_leaf_bytes = encodeAuthorityInventoryLeaf(addition).size() - 1;
    AuthorityInventoryMutationStatistics statistics{
        .deltas_applied = 99,
        .nodes_visited = 99,
        .nodes_created = 99,
        .nodes_hashed = 99,
        .leaves_materialized = 99,
    };
    expectInventoryError(
        AuthorityInventoryError::Code::LimitExceeded,
        [&] { static_cast<void>(AuthorityInventory::applyMutation(base, std::span(&delta, 1), limits, &statistics)); });
    EXPECT_EQ(statistics, AuthorityInventoryMutationStatistics{});
    EXPECT_EQ(base->getSummary().leaf_count, 0u);
    EXPECT_EQ(base->find(addition.key), nullptr);
}

}
}
