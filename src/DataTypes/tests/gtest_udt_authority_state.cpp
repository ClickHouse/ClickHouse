#include <DataTypes/UDT/AuthorityState.h>

#include <gtest/gtest.h>

#include <limits>
#include <string_view>

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

template <typename Callback>
void expectAuthorityStateError(AuthorityStateError::Code expected, Callback && callback)
{
    try
    {
        callback();
        FAIL() << "Expected AuthorityStateError";
    }
    catch (const AuthorityStateError & error)
    {
        EXPECT_EQ(error.code, expected);
    }
}

TEST(UDTAuthorityState, RoundTripHasStableBytesAndAnchor)
{
    const auto state = makeAuthorityState(
        uuid(0x0011223344556677ULL, 0x8899aabbccddeeffULL),
        0x0102030405060708ULL,
        dependent_object_authority_capability_mask,
        300,
        digest(0x20),
        digest(0x60));

    const String encoded = encodeAuthorityState(state);
    EXPECT_EQ(
        toHex(encoded),
        "010000112233445566778899aabbccddeeff08070605040302011f00000000000000ac02"
        "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"
        "606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f"
        "5feff2d796fb2a75ebf9c90a44b934e35b836b25771f002eb8186b53454aff76");
    EXPECT_EQ(decodeAuthorityState(encoded), state);
}

TEST(UDTAuthorityState, DefinitionOnlyAndDependentObjectCapabilitySetsAreDistinct)
{
    const auto definition_only = makeAuthorityState(uuid(1, 2), 1, definition_authority_capability_mask, 1, digest(1), digest(33));
    const auto dependent_object = activateDependentObjectAuthority(definition_only);
    EXPECT_NE(definition_only.anchor_hash, dependent_object.anchor_hash);
    EXPECT_EQ(dependent_object.database_catalog_epoch, definition_only.database_catalog_epoch + 1);
    EXPECT_EQ(dependent_object.leaf_count, definition_only.leaf_count);
    EXPECT_EQ(dependent_object.inventory_root, definition_only.inventory_root);
    EXPECT_EQ(dependent_object.schema_graph_root, definition_only.schema_graph_root);
    EXPECT_EQ(decodeAuthorityState(encodeAuthorityState(definition_only)), definition_only);
    EXPECT_EQ(decodeAuthorityState(encodeAuthorityState(dependent_object)), dependent_object);
}

TEST(UDTAuthorityState, DigestBitPatternsHaveNoReservedSentinel)
{
    const auto state = makeAuthorityState(uuid(1, 2), 1, definition_authority_capability_mask, 0, {}, {});
    EXPECT_EQ(decodeAuthorityState(encodeAuthorityState(state)), state);
}

TEST(UDTAuthorityState, RejectsMalformedAndInvalidStates)
{
    const auto valid = makeAuthorityState(uuid(1, 2), 3, definition_authority_capability_mask, 1, digest(1), digest(33));
    const String encoded = encodeAuthorityState(valid);

    expectAuthorityStateError(
        AuthorityStateError::Code::Truncated,
        [&] { static_cast<void>(decodeAuthorityState(std::string_view(encoded).substr(0, encoded.size() - 1))); });

    String trailing = encoded;
    trailing.push_back('\0');
    expectAuthorityStateError(AuthorityStateError::Code::TrailingData, [&] { static_cast<void>(decodeAuthorityState(trailing)); });

    String nonminimal;
    constexpr size_t count_offset = sizeof(UInt16) + sizeof(CanonicalUUID) + 2 * sizeof(UInt64);
    nonminimal.append(encoded.data(), count_offset);
    nonminimal.push_back(static_cast<char>(0x81));
    nonminimal.push_back('\0');
    nonminimal.append(encoded.data() + count_offset + 1, encoded.size() - count_offset - 1);
    expectAuthorityStateError(AuthorityStateError::Code::NonCanonical, [&] { static_cast<void>(decodeAuthorityState(nonminimal)); });

    auto invalid_anchor = valid;
    invalid_anchor.anchor_hash[0] ^= 0x80;
    expectAuthorityStateError(AuthorityStateError::Code::InvalidValue, [&] { static_cast<void>(encodeAuthorityState(invalid_anchor)); });

    expectAuthorityStateError(
        AuthorityStateError::Code::InvalidValue,
        [&] { static_cast<void>(makeAuthorityState(uuid(1, 2), 3, 3, 1, digest(1), digest(33))); });

    const auto already_dependent_object = makeAuthorityState(uuid(1, 2), 3, dependent_object_authority_capability_mask, 1, digest(1), digest(33));
    expectAuthorityStateError(
        AuthorityStateError::Code::InvalidValue, [&] { static_cast<void>(activateDependentObjectAuthority(already_dependent_object)); });

    const auto exhausted_epoch = makeAuthorityState(
        uuid(1, 2), std::numeric_limits<UInt64>::max(), definition_authority_capability_mask, 1, digest(1), digest(33));
    expectAuthorityStateError(
        AuthorityStateError::Code::LimitExceeded, [&] { static_cast<void>(activateDependentObjectAuthority(exhausted_epoch)); });

    AuthorityStateLimits no_leaves;
    no_leaves.maximum_leaves = 0;
    expectAuthorityStateError(
        AuthorityStateError::Code::InvalidValue,
        [&]
        {
            static_cast<void>(
                makeAuthorityState(uuid(1, 2), 3, definition_authority_capability_mask, 1, digest(1), digest(33), no_leaves));
        });

    AuthorityStateLimits short_record;
    short_record.maximum_encoded_bytes = 100;
    expectAuthorityStateError(
        AuthorityStateError::Code::LimitExceeded,
        [&]
        {
            static_cast<void>(
                makeAuthorityState(uuid(1, 2), 3, definition_authority_capability_mask, 1, digest(1), digest(33), short_record));
        });
}

}
}
