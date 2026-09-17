#include <Access/Common/AccessFlags.h>
#include <DataTypes/UDT/Record.h>
#include <Databases/UDT/ILifecycleAdapter.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/UDTLifecycleIntrospection.h>
#include <Interpreters/UDTLifecycleRequest.h>
#include <Parsers/ASTAlterTypeCommentQuery.h>
#include <Parsers/ASTCreateTypeQuery.h>
#include <Parsers/ASTDropTypeQuery.h>
#include <Parsers/ASTRenameTypeQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/Exception.h>

#include <gtest/gtest.h>

#include <array>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
}

namespace
{
using namespace DB;
using namespace DB::UDT;

constexpr std::string_view type_uuid_text = "11111111-1111-1111-1111-111111111111";
constexpr std::string_view database_uuid_text = "22222222-2222-2222-2222-222222222222";
constexpr std::string_view zero_hash = "0000000000000000000000000000000000000000000000000000000000000000";

ASTPtr parseStatement(const String & text)
{
    ParserQuery parser(text.data() + text.size());
    return parseQuery(parser, text, "user-defined type lifecycle wiring test", 0, 150, 0);
}

Record makeRecord(String local_name, UInt64 revision = 7, String comment = {})
{
    Record record;
    record.identity = {
        .database_uuid = parseFromString<UUID>(database_uuid_text),
        .type_uuid = parseFromString<UUID>(type_uuid_text),
        .revision = revision,
    };
    record.normalized_name = "app." + local_name;
    record.normalized_local_name = local_name;
    record.comment = comment;
    record.canonical_physical_template_sql = "UInt64";
    record.owner_display_name = "alice";
    record.creation_time_us_utc = 123'456;
    record.canonical_definition_sql = "ATTACH TYPE app." + local_name + " UUID '" + String{type_uuid_text} + "' REVISION "
        + std::to_string(revision) + " AS UInt64 DEFINITION HASH '" + String{zero_hash} + "'";
    if (!comment.empty())
        record.canonical_definition_sql += " COMMENT '" + comment + "'";
    return record;
}

template <typename Callback>
void expectDBExceptionCode(Callback && callback, int expected_code)
{
    try
    {
        callback();
        FAIL() << "Expected DB::Exception";
    }
    catch (const Exception & exception)
    {
        EXPECT_EQ(exception.code(), expected_code) << exception.message();
    }
}

void expectIntrospectionError(const Record & record, LifecycleIntrospectionError::Code expected_code)
{
    try
    {
        static_cast<void>(makeShowCreateTypeQuery(record));
        FAIL() << "Expected lifecycle introspection error";
    }
    catch (const LifecycleIntrospectionError & exception)
    {
        EXPECT_EQ(exception.code, expected_code) << exception.what();
    }
}

}

TEST(UDTLifecycleRequest, ClassificationCarriesExactDatabaseRights)
{
    struct Case
    {
        String query;
        LifecycleQueryKind kind;
        AccessType access;
        bool mutation;
        bool internal;
        String database;
        String local_name;
        String cluster;
    };

    const std::array cases{
        Case{
            "CREATE TYPE app.Alpha AS UInt64",
            LifecycleQueryKind::Create,
            AccessType::CREATE_TYPE,
            true,
            false,
            "app",
            "Alpha",
            ""},
        Case{
            "ATTACH TYPE app.Alpha UUID '11111111-1111-1111-1111-111111111111' REVISION 7 AS UInt64 "
            "DEFINITION HASH '0000000000000000000000000000000000000000000000000000000000000000'",
            LifecycleQueryKind::Attach,
            AccessType::CREATE_TYPE,
            true,
            true,
            "app",
            "Alpha",
            ""},
        Case{
            "ALTER TYPE IF EXISTS app.Alpha RENAME TO Beta",
            LifecycleQueryKind::Rename,
            AccessType::ALTER_TYPE,
            true,
            false,
            "app",
            "Alpha",
            ""},
        Case{
            "ALTER TYPE IF EXISTS app.Alpha COMMENT 'new comment'",
            LifecycleQueryKind::Comment,
            AccessType::ALTER_TYPE,
            true,
            false,
            "app",
            "Alpha",
            ""},
        Case{
            "DROP TYPE IF EXISTS app.Alpha RESTRICT",
            LifecycleQueryKind::DropRestrict,
            AccessType::DROP_TYPE,
            true,
            false,
            "app",
            "Alpha",
            ""},
        Case{
            "SHOW TYPES FROM app LIKE 'A%'",
            LifecycleQueryKind::ShowTypes,
            AccessType::SHOW_TYPES,
            false,
            false,
            "app",
            "",
            ""},
        Case{
            "SHOW CREATE TYPE app.Alpha",
            LifecycleQueryKind::ShowCreate,
            AccessType::SHOW_TYPES,
            false,
            false,
            "app",
            "Alpha",
            ""},
        Case{
            "DESCRIBE TYPE app.Alpha",
            LifecycleQueryKind::Describe,
            AccessType::SHOW_TYPES,
            false,
            false,
            "app",
            "Alpha",
            ""},
        Case{
            "PHYSICALIZE TYPE REFERENCES OBJECT TABLE app.events ON CLUSTER c DRY RUN",
            LifecycleQueryKind::DeferredPhysicalization,
            AccessType::NONE,
            false,
            false,
            "app",
            "",
            "c"},
    };

    for (const auto & test_case : cases)
    {
        SCOPED_TRACE(test_case.query);
        const ASTPtr query = parseStatement(test_case.query);
        const auto descriptor = classifyLifecycleRequest(*query);
        EXPECT_EQ(descriptor.kind, test_case.kind);
        EXPECT_EQ(descriptor.required_access, test_case.access);
        EXPECT_EQ(descriptor.mutation, test_case.mutation);
        EXPECT_EQ(descriptor.requires_internal_query, test_case.internal);
        EXPECT_EQ(getLifecycleRequestDatabase(*query), test_case.database);
        EXPECT_EQ(getLifecycleRequestLocalName(*query), test_case.local_name);
        EXPECT_EQ(getLifecycleRequestCluster(*query), test_case.cluster);
    }

    const ASTPtr unrelated = parseStatement("SELECT 1");
    expectDBExceptionCode([&] { static_cast<void>(classifyLifecycleRequest(*unrelated)); }, ErrorCodes::BAD_ARGUMENTS);
}

TEST(UDTLifecycleIntrospection, ShowTypesLikeIsCaseSensitiveAndDeterministic)
{
    std::vector<Record> records;
    records.push_back(makeRecord("Gamma"));
    records.push_back(makeRecord("Alpha"));
    records.push_back(makeRecord("Beta"));

    const auto all = selectRecordsForShow(records);
    ASSERT_EQ(all.size(), 3u);
    EXPECT_EQ(all[0]->normalized_local_name, "Alpha");
    EXPECT_EQ(all[1]->normalized_local_name, "Beta");
    EXPECT_EQ(all[2]->normalized_local_name, "Gamma");

    const auto upper = selectRecordsForShow(records, "A%");
    ASSERT_EQ(upper.size(), 1u);
    EXPECT_EQ(upper.front()->normalized_local_name, "Alpha");
    EXPECT_TRUE(selectRecordsForShow(records, "a%").empty());
    EXPECT_EQ(selectRecordsForShow(records, "%a").size(), 3u);
    EXPECT_EQ(selectRecordsForShow(records, "%").size(), 3u);

    std::vector<Record> escaped_records;
    escaped_records.push_back(makeRecord("A_Exact"));
    escaped_records.push_back(makeRecord("AXExact"));
    const auto escaped = selectRecordsForShow(escaped_records, R"(A\_%)");
    ASSERT_EQ(escaped.size(), 1u);
    EXPECT_EQ(escaped.front()->normalized_local_name, "A_Exact");
    EXPECT_THROW(selectRecordsForShow(records, "\\"), Exception);
}

TEST(UDTLifecycleIntrospection, ShowCreateStripsInternalReplayFields)
{
    const Record record = makeRecord("Alpha", 7, "external id");
    const ASTPtr shown = makeShowCreateTypeQuery(record);
    const auto * create = shown->as<ASTCreateTypeQuery>();
    ASSERT_NE(create, nullptr);
    EXPECT_FALSE(create->attach);
    EXPECT_FALSE(create->if_not_exists);
    EXPECT_FALSE(create->uuid.has_value());
    EXPECT_FALSE(create->revision.has_value());
    EXPECT_FALSE(create->definition_hash.has_value());
    EXPECT_TRUE(create->cluster.empty());
    EXPECT_EQ(shown->formatWithSecretsOneLine(), "CREATE TYPE app.Alpha AS UInt64 COMMENT 'external id'");
    EXPECT_EQ(record.canonical_definition_sql.find("ATTACH TYPE"), 0u);

    Record explicit_empty_comment = makeRecord("EmptyComment");
    explicit_empty_comment.canonical_definition_sql += " COMMENT ''";
    EXPECT_EQ(
        makeShowCreateTypeQuery(explicit_empty_comment)->formatWithSecretsOneLine(), "CREATE TYPE app.EmptyComment AS UInt64 COMMENT ''");
}

TEST(UDTLifecycleIntrospection, ShowCreateRejectsRecordDisagreement)
{
    Record record = makeRecord("Alpha", 7, "record comment");

    Record user_create = record;
    user_create.canonical_definition_sql = "CREATE TYPE app.Alpha AS UInt64 COMMENT 'record comment'";
    expectIntrospectionError(user_create, LifecycleIntrospectionError::Code::InvalidCanonicalSQL);

    Record bad_name = record;
    bad_name.normalized_local_name = "Other";
    bad_name.normalized_name = "app.Other";
    expectIntrospectionError(bad_name, LifecycleIntrospectionError::Code::RecordNameMismatch);

    Record bad_identity = record;
    bad_identity.identity.revision = 8;
    expectIntrospectionError(bad_identity, LifecycleIntrospectionError::Code::RecordIdentityMismatch);

    Record bad_comment = record;
    bad_comment.comment = "different";
    expectIntrospectionError(bad_comment, LifecycleIntrospectionError::Code::RecordCommentMismatch);

    Record bad_sql = record;
    bad_sql.canonical_definition_sql = "not a type query";
    expectIntrospectionError(bad_sql, LifecycleIntrospectionError::Code::InvalidCanonicalSQL);
}

TEST(UDTLifecycleIntrospection, DescribeRowsHaveTheFrozenPublicShape)
{
    Record record = makeRecord("Nested", 7, "public id");
    record.canonical_physical_template_sql = "Tuple(T, UInt16)";
    record.parameters = {
        {.normalized_name = "T", .kind = ParameterKind::Type},
        {.normalized_name = "N", .kind = ParameterKind::UInt16},
    };
    record.decreasing_parameter = 1;
    record.checker_abi = 2;
    record.dependencies = {{
        .type_uuid = parseFromString<UUID>("33333333-3333-3333-3333-333333333333"),
        .revision = 9,
        .target_definition_hash = {},
    }};

    const DescribeRows expected{
        {"database", "renamed_app"},
        {"name", "Nested"},
        {"uuid", "11111111-1111-1111-1111-111111111111"},
        {"revision", "7"},
        {"underlying_type", "Tuple(T, UInt16)"},
        {"definition_hash", String{zero_hash}},
        {"semantic_abi_identity", "checker_abi=2;checker_charge_abi=1;policy_abi=1;function_registry_abi=1"},
        {"checker_certificate_hash", String{zero_hash}},
        {"parameters", "[('T', 'Type'), ('N', 'UInt16')]"},
        {"decreases_parameter", "N"},
        {"dependencies",
         "[('33333333-3333-3333-3333-333333333333', 9, "
         "'0000000000000000000000000000000000000000000000000000000000000000')]"},
        {"owner", "alice"},
        {"comment", "public id"},
        {"creation_time", "1970-01-01T00:00:00.123456Z"},
        {"storage_backend", "AtomicDisk"},
        {"status", "ACTIVE"},
    };
    EXPECT_EQ(makeDescribeTypeRows("renamed_app", record), expected);

    record.decreasing_parameter = 2;
    try
    {
        static_cast<void>(makeDescribeTypeRows("renamed_app", record));
        FAIL() << "Expected invalid record rejection";
    }
    catch (const LifecycleIntrospectionError & exception)
    {
        EXPECT_EQ(exception.code, LifecycleIntrospectionError::Code::InvalidRecord);
    }
}

TEST(UDTLifecycleAdapter, UnsupportedSingletonFailsThroughOneBoundary)
{
    auto & adapter = getUnsupportedLifecycleAdapter();
    EXPECT_EQ(&adapter, &getUnsupportedLifecycleAdapter());
    EXPECT_EQ(adapter.getCapabilities().mask, 0u);
    EXPECT_EQ(adapter.getDatabaseUUID(), UUIDHelpers::Nil);

    const auto durable_alias = typeAuthorityCapabilityBit(TypeAuthorityCapability::DurableAlias);
    expectDBExceptionCode([&] { adapter.requireCapabilities(durable_alias, "CREATE TYPE"); }, ErrorCodes::NOT_IMPLEMENTED);
    expectDBExceptionCode([&] { static_cast<void>(adapter.acquireSnapshot()); }, ErrorCodes::NOT_IMPLEMENTED);

    LifecycleActor actor;
    const ASTPtr create = parseStatement("CREATE TYPE app.Alpha AS UInt64");
    const ASTPtr rename = parseStatement("ALTER TYPE app.Alpha RENAME TO Beta");
    const ASTPtr comment = parseStatement("ALTER TYPE app.Alpha COMMENT 'new comment'");
    const ASTPtr drop = parseStatement("DROP TYPE app.Alpha RESTRICT");
    expectDBExceptionCode([&] { adapter.createOrAttach(create->as<ASTCreateTypeQuery &>(), actor); }, ErrorCodes::NOT_IMPLEMENTED);
    expectDBExceptionCode([&] { adapter.rename(rename->as<ASTRenameTypeQuery &>(), actor); }, ErrorCodes::NOT_IMPLEMENTED);
    expectDBExceptionCode([&] { adapter.comment(comment->as<ASTAlterTypeCommentQuery &>(), actor); }, ErrorCodes::NOT_IMPLEMENTED);
    expectDBExceptionCode([&] { adapter.dropRestrict(drop->as<ASTDropTypeQuery &>(), actor); }, ErrorCodes::NOT_IMPLEMENTED);
}
