#include <Access/AccessControl.h>
#include <Access/AccessEntityIO.h>
#include <Access/AccessRights.h>
#include <Access/Common/UDTAccessTarget.h>
#include <Access/ContextAccess.h>
#include <Access/ContextAccessParams.h>
#include <Access/User.h>
#include <Access/UDTUsageAccess.h>
#include <Access/UsersConfigAccessStorage.h>
#include <Core/Settings.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Access/InterpreterGrantQuery.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Parsers/Access/ASTCheckGrantQuery.h>
#include <Parsers/Access/ASTGrantQuery.h>
#include <Parsers/Access/ParserCheckGrantQuery.h>
#include <Parsers/Access/ParserGrantQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/typeid_cast.h>

#include <gtest/gtest.h>
#include <Poco/Util/XMLConfiguration.h>

#include <array>
#include <memory>
#include <optional>
#include <sstream>
#include <string_view>
#include <vector>

namespace
{
using namespace DB;
using namespace DB::UDT;

constexpr std::string_view database_uuid_text = "11111111-1111-1111-1111-111111111111";
constexpr std::string_view first_type_uuid_text = "22222222-2222-2222-2222-222222222222";
constexpr std::string_view recreated_type_uuid_text = "33333333-3333-3333-3333-333333333333";

AccessTarget makeTarget(std::string_view type_uuid = first_type_uuid_text)
{
    return {
        .database_uuid = parseFromString<UUID>(database_uuid_text),
        .type_uuid = parseFromString<UUID>(type_uuid),
    };
}

ASTPtr parseGrant(const String & text)
{
    ParserGrantQuery parser;
    return parseQuery(parser, text, "user-defined type usage access test", 0, 150, 0);
}

ASTPtr parseCheckGrant(const String & text)
{
    ParserCheckGrantQuery parser;
    return parseQuery(parser, text, "user-defined type usage access check test", 0, 150, 0);
}

void expectDecodeError(const String & encoded, AccessTargetError::Code expected_code)
{
    try
    {
        static_cast<void>(decodeAccessTarget(encoded));
        FAIL() << "Expected a user-defined type access target decoding error";
    }
    catch (const AccessTargetError & error)
    {
        EXPECT_EQ(error.code(), expected_code);
    }
}

Poco::AutoPtr<Poco::Util::XMLConfiguration> createConfigFromXML(const String & xml)
{
    std::istringstream input{xml};
    return new Poco::Util::XMLConfiguration(input);
}

}

TEST(UDTUsageAccess, StableWireIsStrictAndCanonical)
{
    const auto target = makeTarget();
    const String expected = "clickhouse:udt-access-target:v1:11111111-1111-1111-1111-111111111111:22222222-2222-2222-2222-222222222222";
    const String encoded = encodeAccessTarget(target);
    EXPECT_EQ(encoded, expected);
    EXPECT_EQ(decodeAccessTarget(encoded), target);

    String unknown_version = encoded;
    unknown_version.replace(unknown_version.find(":v1:"), 4, ":v2:");
    expectDecodeError(unknown_version, AccessTargetError::Code::UnsupportedVersion);
    expectDecodeError(encoded.substr(0, encoded.size() - 1), AccessTargetError::Code::Truncated);
    expectDecodeError(encoded + "x", AccessTargetError::Code::NonCanonical);
    expectDecodeError("not-a-type-target", AccessTargetError::Code::Truncated);

    String non_canonical = encoded;
    non_canonical[non_canonical.find("22222222")] = 'A';
    expectDecodeError(non_canonical, AccessTargetError::Code::NonCanonical);

    auto nil_target = target;
    nil_target.type_uuid = UUIDHelpers::Nil;
    EXPECT_THROW(encodeAccessTarget(nil_target), AccessTargetError);
}

TEST(UDTUsageAccess, ParserFormatterAndAccessEntitySerializationRoundTrip)
{
    const String grant_sql = "GRANT USAGE TYPE ON TYPE UUID '11111111-1111-1111-1111-111111111111' "
                             "'22222222-2222-2222-2222-222222222222' TO udt_role WITH GRANT OPTION";
    const auto parsed = parseGrant(grant_sql);
    const auto * grant = parsed->as<ASTGrantQuery>();
    ASSERT_NE(grant, nullptr);
    ASSERT_EQ(grant->access_rights_elements.size(), 1u);

    const auto & element = grant->access_rights_elements.front();
    EXPECT_EQ(element.access_flags, AccessFlags{AccessType::USAGE_TYPE});
    EXPECT_EQ(element.access_flags.getParameterType(), AccessFlags::TYPE_OBJECT);
    EXPECT_TRUE(element.database.empty());
    EXPECT_TRUE(element.table.empty());
    EXPECT_TRUE(element.grant_option);
    EXPECT_EQ(decodeAccessTarget(element.parameter), makeTarget());

    const String formatted = parsed->formatWithSecretsOneLine();
    EXPECT_EQ(formatted, grant_sql);
    const auto reparsed = parseGrant(formatted);
    EXPECT_EQ(reparsed->as<ASTGrantQuery>()->access_rights_elements, grant->access_rights_elements);

    const String revoke_sql = "REVOKE GRANT OPTION FOR USAGE TYPE ON TYPE UUID '11111111-1111-1111-1111-111111111111' "
                              "'22222222-2222-2222-2222-222222222222' FROM udt_role";
    const auto revoke = parseGrant(revoke_sql);
    EXPECT_EQ(revoke->formatWithSecretsOneLine(), revoke_sql);
    EXPECT_EQ(decodeAccessTarget(revoke->as<ASTGrantQuery>()->access_rights_elements.front().parameter), makeTarget());

    const String check_sql = "CHECK GRANT USAGE TYPE ON TYPE UUID '11111111-1111-1111-1111-111111111111' "
                             "'22222222-2222-2222-2222-222222222222'";
    const auto check = parseCheckGrant(check_sql);
    EXPECT_EQ(check->formatWithSecretsOneLine(), check_sql);
    EXPECT_EQ(decodeAccessTarget(check->as<ASTCheckGrantQuery>()->access_rights_elements.front().parameter), makeTarget());

    User user;
    user.setName("udt_user");
    user.access.grantWithGrantOption(element);
    const String serialized = serializeAccessEntity(user);
    EXPECT_NE(serialized.find("USAGE TYPE ON TYPE UUID"), String::npos);
    const auto restored = typeid_cast<std::shared_ptr<const User>>(deserializeAccessEntity(serialized));
    ASSERT_NE(restored, nullptr);
    EXPECT_TRUE(restored->access.isGranted(element));
    EXPECT_TRUE(restored->access.hasGrantOption(element));
}

TEST(UDTUsageAccess, ParserRejectsUnstableOrMalformedTargets)
{
    EXPECT_THROW(parseGrant("GRANT USAGE TYPE ON app.events TO udt_role"), Exception);
    EXPECT_THROW(parseGrant("GRANT USAGE TYPE ON *.* TO udt_role"), Exception);
    EXPECT_THROW(
        parseGrant(
            "GRANT USAGE TYPE ON TYPE UUID VERSION '11111111-1111-1111-1111-111111111111' "
            "'22222222-2222-2222-2222-222222222222' TO udt_role"),
        Exception);
    EXPECT_THROW(parseGrant("GRANT USAGE TYPE ON TYPE UUID '11111111-1111-1111-1111-111111111111' TO udt_role"), Exception);
    EXPECT_THROW(
        parseGrant(
            "GRANT USAGE TYPE(secret) ON TYPE UUID '11111111-1111-1111-1111-111111111111' "
            "'22222222-2222-2222-2222-222222222222' TO udt_role"),
        Exception);
    EXPECT_THROW(
        parseGrant(
            "GRANT USAGE TYPE ON TYPE UUID '00000000-0000-0000-0000-000000000000' "
            "'22222222-2222-2222-2222-222222222222' TO udt_role"),
        Exception);

    const auto wildcard = parseGrant("GRANT USAGE TYPE ON TYPE * TO udt_role");
    const auto & element = wildcard->as<ASTGrantQuery>()->access_rights_elements.front();
    EXPECT_TRUE(element.anyParameter());
    EXPECT_EQ(wildcard->formatWithSecretsOneLine(), "GRANT USAGE TYPE ON TYPE * TO udt_role");
}

TEST(UDTUsageAccess, GrantRevokeRenameRecreateAndGrantOptionUseStableIdentity)
{
    const auto original = makeUsageAccessElement(makeTarget());
    const auto same_identity_after_rename = makeUsageAccessElement(makeTarget());
    const auto same_name_after_recreate = makeUsageAccessElement(makeTarget(recreated_type_uuid_text));

    AccessRights rights;
    rights.grantWithGrantOption(original);
    EXPECT_TRUE(rights.isGranted(same_identity_after_rename));
    EXPECT_TRUE(rights.hasGrantOption(same_identity_after_rename));
    EXPECT_FALSE(rights.isGranted(same_name_after_recreate));

    rights.revokeGrantOption(original);
    EXPECT_TRUE(rights.isGranted(original));
    EXPECT_FALSE(rights.hasGrantOption(original));

    rights.revoke(original);
    EXPECT_FALSE(rights.isGranted(original));
}

TEST(UDTUsageAccess, ParsedGrantAndRevokeMutateAccessRightsWithoutChangingTheTarget)
{
    User user;
    user.setName("udt_user");
    const auto target = makeUsageAccessElement(makeTarget());

    const auto grant = parseGrant(
        "GRANT USAGE TYPE ON TYPE UUID '11111111-1111-1111-1111-111111111111' "
        "'22222222-2222-2222-2222-222222222222' TO udt_user WITH GRANT OPTION");
    InterpreterGrantQuery::updateUserFromQuery(user, grant->as<ASTGrantQuery &>());
    EXPECT_TRUE(user.access.isGranted(target));
    EXPECT_TRUE(user.access.hasGrantOption(target));

    const auto revoke_option = parseGrant(
        "REVOKE GRANT OPTION FOR USAGE TYPE ON TYPE UUID '11111111-1111-1111-1111-111111111111' "
        "'22222222-2222-2222-2222-222222222222' FROM udt_user");
    InterpreterGrantQuery::updateUserFromQuery(user, revoke_option->as<ASTGrantQuery &>());
    EXPECT_TRUE(user.access.isGranted(target));
    EXPECT_FALSE(user.access.hasGrantOption(target));

    const auto revoke = parseGrant(
        "REVOKE USAGE TYPE ON TYPE UUID '11111111-1111-1111-1111-111111111111' "
        "'22222222-2222-2222-2222-222222222222' FROM udt_user");
    InterpreterGrantQuery::updateUserFromQuery(user, revoke->as<ASTGrantQuery &>());
    EXPECT_FALSE(user.access.isGranted(target));
}

TEST(UDTUsageAccess, TypeObjectRightsNeverImplyTableRights)
{
    AccessControl access_control;
    AccessRights type_rights;
    const auto element = makeUsageAccessElement(makeTarget());
    type_rights.grant(element);

    const AccessRights with_implicit = ContextAccess::addImplicitAccessRights(type_rights, access_control);
    EXPECT_TRUE(with_implicit.isGranted(element));
    EXPECT_FALSE(with_implicit.isGranted(AccessType::SELECT, "app", "events"));
    EXPECT_FALSE(with_implicit.isGranted(AccessType::SHOW_TABLES, "app", "events"));
    EXPECT_FALSE(with_implicit.isGranted(AccessType::CREATE_TABLE, "app", "events"));

    AccessRights table_rights;
    table_rights.grant(AccessType::SELECT, "app", "events");
    const AccessRights table_with_implicit = ContextAccess::addImplicitAccessRights(table_rights, access_control);
    EXPECT_FALSE(table_with_implicit.isGranted(element));

    AccessRights database_all;
    database_all.grant(AccessType::ALL, "app");
    EXPECT_FALSE(database_all.isGranted(element));

    AccessRights table_all;
    table_all.grant(AccessType::ALL, "app", "events");
    EXPECT_FALSE(table_all.isGranted(element));

    EXPECT_FALSE(AccessFlags::allTableFlags().contains(AccessType::USAGE_TYPE));
    EXPECT_FALSE(AccessFlags::allDatabaseFlags().contains(AccessType::USAGE_TYPE));
}

TEST(UDTUsageAccess, TypeObjectRadixPathCannotCollideWithADatabaseName)
{
    const auto target = makeTarget();
    const String encoded_target = encodeAccessTarget(target);
    const auto usage = makeUsageAccessElement(target);

    AccessRights database_rights;
    database_rights.grant(AccessType::ALL, encoded_target);
    EXPECT_TRUE(database_rights.isGranted(AccessType::SELECT, encoded_target));
    EXPECT_FALSE(database_rights.isGranted(usage));

    AccessRightsElement parsed_database_all{AccessType::ALL, encoded_target};
    parsed_database_all.eraseNotGrantable();
    EXPECT_FALSE(parsed_database_all.access_flags.contains(AccessType::USAGE_TYPE));

    database_rights.grant(AccessType::USAGE_TYPE, encoded_target);
    EXPECT_TRUE(database_rights.isGranted(usage));

    AccessRights default_database_rights;
    default_database_rights.grant(AccessType::SELECT, "", "events");
    default_database_rights.grant(usage);
    EXPECT_TRUE(default_database_rights.isGranted(AccessType::SELECT, "", "events"));
    EXPECT_TRUE(default_database_rights.isGranted(usage));
    EXPECT_NO_THROW(static_cast<void>(default_database_rights.getElements()));

    User user;
    user.setName("udt_default_database_coexistence");
    user.access = default_database_rights;
    const auto restored = typeid_cast<std::shared_ptr<const User>>(deserializeAccessEntity(serializeAccessEntity(user)));
    ASSERT_NE(restored, nullptr);
    EXPECT_TRUE(restored->access.isGranted(AccessType::SELECT, "", "events"));
    EXPECT_TRUE(restored->access.isGranted(usage));
}

TEST(UDTUsageAccess, ExistingMixedParameterFlagsKeepTheirRawPathSemantics)
{
    const AccessFlags existing_mixed = AccessType::CREATE_USER | AccessType::TABLE_ENGINE;
    AccessRights rights;

    EXPECT_NO_THROW(rights.grant(existing_mixed, "existing_parameter"));
    EXPECT_TRUE(rights.isGranted(existing_mixed, "existing_parameter"));
    EXPECT_NO_THROW(static_cast<void>(rights.getElements()));
}

TEST(UDTUsageAccess, UsersConfigDatabaseAllCannotBecomeTypeUsage)
{
    const auto target = makeTarget();
    const String encoded_target = encodeAccessTarget(target);
    const String xml = "<clickhouse><users><udt_config_user><no_password/><grants><query>GRANT ALL ON `" + encoded_target
        + "`.*</query></grants></udt_config_user></users></clickhouse>";

    AccessControl access_control;
    UsersConfigAccessStorage storage{"udt_usage_users_config", access_control, false};
    const auto config = createConfigFromXML(xml);
    storage.setConfig(*config);

    const auto user = storage.tryRead<User>("udt_config_user");
    ASSERT_NE(user, nullptr);
    EXPECT_TRUE(user->access.isGranted(AccessType::SELECT, encoded_target));
    EXPECT_FALSE(user->access.isGranted(makeUsageAccessElement(target)));
}

TEST(UDTUsageAccess, GlobalAllIncludesTypeUsageWithoutMergingItsExactPath)
{
    const auto usage = makeUsageAccessElement(makeTarget());

    User user;
    user.setName("udt_global_all_user");
    user.access.grant(AccessType::ALL);
    EXPECT_TRUE(user.access.isGranted(usage));

    const auto restored = typeid_cast<std::shared_ptr<const User>>(deserializeAccessEntity(serializeAccessEntity(user)));
    ASSERT_NE(restored, nullptr);
    EXPECT_TRUE(restored->access.isGranted(usage));
}

TEST(UDTUsageAccess, ExactPartialRevokeUnderTypeWildcardRoundTrips)
{
    const auto first = makeUsageAccessElement(makeTarget());
    const auto second = makeUsageAccessElement(makeTarget(recreated_type_uuid_text));

    User user;
    user.setName("udt_partial_revoke_user");
    user.access.grant(AccessType::USAGE_TYPE);
    user.access.revoke(first);
    EXPECT_FALSE(user.access.isGranted(first));
    EXPECT_TRUE(user.access.isGranted(second));

    const String serialized = serializeAccessEntity(user);
    EXPECT_NE(serialized.find("GRANT USAGE TYPE ON TYPE *"), String::npos);
    EXPECT_NE(serialized.find("REVOKE USAGE TYPE ON TYPE UUID"), String::npos);

    const auto restored = typeid_cast<std::shared_ptr<const User>>(deserializeAccessEntity(serialized));
    ASSERT_NE(restored, nullptr);
    EXPECT_FALSE(restored->access.isGranted(first));
    EXPECT_TRUE(restored->access.isGranted(second));
}

TEST(UDTUsageAccess, UnsupportedTypeObjectElementShapesFailClosed)
{
    const auto target = makeTarget();
    const String encoded_target = encodeAccessTarget(target);
    const auto valid = makeUsageAccessElement(target);
    AccessRights rights;

    AccessRightsElement direct{AccessType::USAGE_TYPE, encoded_target};
    EXPECT_TRUE(direct.database.empty());
    EXPECT_EQ(direct.parameter, encoded_target);
    EXPECT_NO_THROW(validateUsageAccessElement(direct));

    auto filtered = valid;
    filtered.filter = ".*";
    EXPECT_THROW(rights.grant(filtered), AccessTargetError);
    EXPECT_THROW(filtered.toString(), AccessTargetError);

    auto wildcard = valid;
    wildcard.wildcard = true;
    EXPECT_THROW(rights.grant(wildcard), AccessTargetError);
    EXPECT_THROW(wildcard.toString(), AccessTargetError);

    auto database_shaped = valid;
    database_shaped.database = "app";
    EXPECT_THROW(rights.grant(database_shaped), AccessTargetError);
    EXPECT_THROW(database_shaped.toString(), AccessTargetError);

    auto table_shaped = valid;
    table_shaped.table = "events";
    EXPECT_THROW(rights.grant(table_shaped), AccessTargetError);

    auto column_shaped = valid;
    column_shaped.columns.emplace_back("value");
    EXPECT_THROW(rights.grant(column_shaped), AccessTargetError);

    auto default_database_shaped = valid;
    default_database_shaped.default_database = true;
    EXPECT_THROW(rights.grant(default_database_shaped), AccessTargetError);

    EXPECT_THROW(rights.grant(AccessType::USAGE_TYPE, encoded_target, "filter"), AccessTargetError);
    EXPECT_THROW(rights.grantWildcard(AccessType::USAGE_TYPE, encoded_target), AccessTargetError);
    EXPECT_THROW(rights.grant(AccessType::USAGE_TYPE, "not-a-canonical-target"), AccessTargetError);
    EXPECT_TRUE(rights.isEmpty());

    AccessRights empty;
    EXPECT_THROW(empty.revoke(AccessType::USAGE_TYPE, encoded_target, "filter"), AccessTargetError);
    EXPECT_THROW(empty.isGranted(AccessType::USAGE_TYPE, encoded_target, "filter"), AccessTargetError);
    EXPECT_THROW(empty.revokeWildcard(AccessType::USAGE_TYPE, encoded_target), AccessTargetError);
    EXPECT_THROW(empty.isGrantedWildcard(AccessType::USAGE_TYPE, encoded_target), AccessTargetError);
    EXPECT_TRUE(empty.isEmpty());
}

TEST(UDTUsageAccess, OperationBoundaryCanonicalizesAndDeduplicatesTargets)
{
    const auto first = makeTarget();
    const auto second = makeTarget(recreated_type_uuid_text);
    const std::array targets{second, first, first, second};
    const auto elements = makeUsageAccessElements(targets);

    ASSERT_EQ(elements.size(), 2u);
    EXPECT_LT(elements[0].parameter, elements[1].parameter);
    EXPECT_EQ(decodeAccessTarget(elements[0].parameter), first);
    EXPECT_EQ(decodeAccessTarget(elements[1].parameter), second);

    EXPECT_TRUE(makeUsageAccessElements(std::span<const AccessTarget>{}).empty());
}

TEST(UDTUsageAccess, OperationBoundaryHelperAcceptsCanonicalBatch)
{
    const std::array targets{makeTarget(recreated_type_uuid_text), makeTarget(), makeTarget()};
    const auto context = Context::createCopy(getContext().context);
    EXPECT_NO_THROW(checkUsageAccess(context, targets));
}

TEST(UDTUsageAccess, ExistingContextAccessGenerationObservesGrantAndRevoke)
{
    AccessControl access_control;
    access_control.addMemoryStorage("udt_usage_test", false);

    auto user = std::make_shared<User>();
    user->setName("udt_usage_user");
    const UUID user_id = access_control.insert(user);

    Settings settings;
    ClientInfo client_info;
    const ContextAccessParams params(user_id, false, true, {}, {}, {}, settings, "default", client_info, std::nullopt);
    const auto cached_access = access_control.getContextAccess(params);
    const auto element = makeUsageAccessElement(makeTarget());
    EXPECT_FALSE(cached_access->getAccessRightsWithImplicit()->isGranted(element));

    access_control.update(
        user_id,
        [&](const AccessEntityPtr & entity, const UUID &)
        {
            auto changed = typeid_cast<std::shared_ptr<User>>(entity->clone());
            changed->access.grant(element);
            return changed;
        });
    EXPECT_TRUE(cached_access->getAccessRightsWithImplicit()->isGranted(element));

    access_control.update(
        user_id,
        [&](const AccessEntityPtr & entity, const UUID &)
        {
            auto changed = typeid_cast<std::shared_ptr<User>>(entity->clone());
            changed->access.revoke(element);
            return changed;
        });
    EXPECT_FALSE(cached_access->getAccessRightsWithImplicit()->isGranted(element));
}
