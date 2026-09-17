#include <Access/AccessControl.h>
#include <Access/AccessRights.h>
#include <Access/Common/AccessRightsElement.h>
#include <Access/ContextAccess.h>
#include <Parsers/Access/ASTGrantQuery.h>
#include <Parsers/Access/ParserGrantQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/Exception.h>

#include <gtest/gtest.h>

#include <array>
#include <string_view>

namespace
{
using namespace DB;

ASTPtr parseGrant(const String & text)
{
    ParserGrantQuery parser;
    return parseQuery(parser, text, "user-defined type access test", 0, 150, 0);
}

}

TEST(UDTAccess, LifecycleKeywordsAreDistinctDatabaseRights)
{
    static constexpr std::array rights{
        std::pair{AccessType::CREATE_TYPE, std::string_view{"CREATE TYPE"}},
        std::pair{AccessType::ALTER_TYPE, std::string_view{"ALTER TYPE"}},
        std::pair{AccessType::DROP_TYPE, std::string_view{"DROP TYPE"}},
        std::pair{AccessType::SHOW_TYPES, std::string_view{"SHOW TYPES"}},
    };

    for (const auto & [type, keyword] : rights)
    {
        SCOPED_TRACE(keyword);
        const AccessFlags flags{type};
        EXPECT_EQ(AccessFlags{keyword}, flags);
        EXPECT_EQ(flags.toString(), keyword);
        EXPECT_TRUE(AccessFlags::allDatabaseFlags().contains(flags));
        EXPECT_FALSE(AccessFlags::allTableFlags().contains(flags));

        const AccessRightsElement database_element{flags, "app"};
        EXPECT_NO_THROW(database_element.throwIfNotGrantable());
        EXPECT_EQ(database_element.toString(), "GRANT " + String{keyword} + " ON app.*");

        const AccessRightsElement table_element{flags, "app", "events"};
        EXPECT_THROW(table_element.throwIfNotGrantable(), Exception);
    }
}

TEST(UDTAccess, GrantParserRoundTripsOnlyAtDatabaseScope)
{
    const String text = "GRANT CREATE TYPE, ALTER TYPE, DROP TYPE, SHOW TYPES ON app.* TO udt_role";
    const ASTPtr parsed = parseGrant(text);
    const auto * grant = parsed->as<ASTGrantQuery>();
    ASSERT_NE(grant, nullptr);
    ASSERT_EQ(grant->access_rights_elements.size(), 4u);

    const AccessFlags lifecycle = AccessType::CREATE_TYPE | AccessType::ALTER_TYPE | AccessType::DROP_TYPE | AccessType::SHOW_TYPES;
    AccessFlags parsed_lifecycle;
    for (const auto & element : grant->access_rights_elements)
    {
        parsed_lifecycle |= element.access_flags;
        EXPECT_EQ(element.database, "app");
        EXPECT_TRUE(element.anyTable());
        EXPECT_FALSE(element.default_database);
    }
    EXPECT_EQ(parsed_lifecycle, lifecycle);

    const String formatted = parsed->formatWithSecretsOneLine();
    const ASTPtr reparsed = parseGrant(formatted);
    const auto * reparsed_grant = reparsed->as<ASTGrantQuery>();
    ASSERT_NE(reparsed_grant, nullptr);
    EXPECT_EQ(reparsed_grant->access_rights_elements, grant->access_rights_elements);

    EXPECT_THROW(parseGrant("GRANT CREATE TYPE ON app.events TO udt_role"), Exception);
}

TEST(UDTAccess, ImplicitVisibilityStaysInTheTypeNamespace)
{
    AccessControl access_control;
    AccessRights explicit_rights;
    explicit_rights.grant(AccessType::CREATE_TYPE | AccessType::ALTER_TYPE | AccessType::DROP_TYPE, "app");

    const AccessRights implicit = ContextAccess::addImplicitAccessRights(explicit_rights, access_control);
    EXPECT_TRUE(implicit.isGranted(AccessType::SHOW_TYPES, "app"));
    EXPECT_FALSE(implicit.isGranted(AccessType::SHOW_TYPES, "other"));
    EXPECT_FALSE(implicit.isGranted(AccessType::SHOW_TABLES, "app"));
    EXPECT_FALSE(implicit.isGranted(AccessType::CREATE_TABLE, "app"));
    EXPECT_FALSE(implicit.isGranted(AccessType::ALTER_TABLE, "app"));
    EXPECT_FALSE(implicit.isGranted(AccessType::DROP_TABLE, "app"));

    AccessRights table_rights;
    table_rights.grant(AccessType::SELECT, "app", "events");
    const AccessRights table_implicit = ContextAccess::addImplicitAccessRights(table_rights, access_control);
    EXPECT_TRUE(table_implicit.isGranted(AccessType::SHOW_TABLES, "app", "events"));
    EXPECT_FALSE(table_implicit.isGranted(AccessType::SHOW_TABLES, "app", "other"));
    EXPECT_FALSE(table_implicit.isGranted(AccessType::SHOW_TABLES, "app"));
    EXPECT_FALSE(table_implicit.isGranted(AccessType::SHOW_TYPES, "app"));
}
