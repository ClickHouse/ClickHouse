#include <DataTypes/UDT/AuthorityInventorySnapshot.h>

#include <gtest/gtest.h>

#include <array>

namespace DB::UDT
{
namespace
{

String toHex(std::span<const CanonicalByte> bytes)
{
    constexpr std::array<char, 16> alphabet{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    String result;
    result.reserve(bytes.size() * 2);
    for (const auto byte : bytes)
    {
        result.push_back(alphabet[byte >> 4]);
        result.push_back(alphabet[byte & 0x0f]);
    }
    return result;
}

template <typename Function>
void expectError(AuthorityInventorySnapshotError::Code code, Function && function)
{
    try
    {
        function();
        FAIL() << "expected AuthorityInventorySnapshotError";
    }
    catch (const AuthorityInventorySnapshotError & error)
    {
        EXPECT_EQ(error.code, code);
    }
}

UUID databaseUUID()
{
    CanonicalUUID bytes{};
    for (size_t index = 0; index < bytes.size(); ++index)
        bytes[index] = static_cast<CanonicalByte>(index);
    return uuidFromCanonicalBytes(bytes);
}

}

TEST(AuthorityInventorySnapshot, EmptySnapshotWireAndHashAreStable)
{
    const auto snapshot = makeAuthorityInventorySnapshot(databaseUUID(), {});
    const String encoded = encodeAuthorityInventorySnapshot(snapshot);
    EXPECT_EQ(
        toHex(std::span<const CanonicalByte>(reinterpret_cast<const CanonicalByte *>(encoded.data()), encoded.size())),
        "0100000102030405060708090a0b0c0d0e0f0001000000");
    EXPECT_EQ(toHex(computeAuthorityInventorySnapshotHash(snapshot)), "37b24ed44c0d4269ed542aae8ef397d81d13086b998a19daca79e20b839f982d");
    EXPECT_EQ(decodeAuthorityInventorySnapshot(encoded), snapshot);
    EXPECT_EQ(buildAuthorityInventoryFromSnapshot(snapshot)->getSummary().leaf_count, 0);
}

TEST(AuthorityInventorySnapshot, RejectsUnsortedDuplicateAndNonminimalLeaves)
{
    AuthorityInventoryLeaf first{
        .key = {
            .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
            .object_uuid = uuidFromCanonicalBytes(CanonicalUUID{1}),
        },
        .object_revision = 1,
    };
    AuthorityInventoryLeaf second = first;
    second.key.object_uuid = uuidFromCanonicalBytes(CanonicalUUID{2});
    const auto sorted = makeAuthorityInventorySnapshot(databaseUUID(), {first, second});
    EXPECT_EQ(decodeAuthorityInventorySnapshot(encodeAuthorityInventorySnapshot(sorted)), sorted);

    expectError(
        AuthorityInventorySnapshotError::Code::NonCanonical,
        [&] { static_cast<void>(makeAuthorityInventorySnapshot(databaseUUID(), {second, first})); });
    expectError(
        AuthorityInventorySnapshotError::Code::NonCanonical,
        [&] { static_cast<void>(makeAuthorityInventorySnapshot(databaseUUID(), {first, first})); });

    auto nonminimal = encodeAuthorityInventorySnapshot(sorted);
    nonminimal[18] = static_cast<char>(0x82);
    nonminimal.insert(nonminimal.begin() + 19, '\0');
    expectError(
        AuthorityInventorySnapshotError::Code::NonCanonical, [&] { static_cast<void>(decodeAuthorityInventorySnapshot(nonminimal)); });
}

TEST(AuthorityInventorySnapshot, RejectsTruncationTrailingDataAndConfiguredBounds)
{
    const auto snapshot = makeAuthorityInventorySnapshot(databaseUUID(), {});
    const auto encoded = encodeAuthorityInventorySnapshot(snapshot);
    for (size_t size = 0; size < encoded.size(); ++size)
    {
        expectError(
            AuthorityInventorySnapshotError::Code::Truncated,
            [&] { static_cast<void>(decodeAuthorityInventorySnapshot(std::string_view(encoded).substr(0, size))); });
    }

    auto trailing = encoded;
    trailing.push_back('\0');
    expectError(
        AuthorityInventorySnapshotError::Code::TrailingData, [&] { static_cast<void>(decodeAuthorityInventorySnapshot(trailing)); });

    AuthorityInventorySnapshotLimits limits;
    limits.maximum_snapshot_bytes = encoded.size() - 1;
    expectError(
        AuthorityInventorySnapshotError::Code::LimitExceeded,
        [&] { static_cast<void>(decodeAuthorityInventorySnapshot(encoded, limits)); });
}

}
