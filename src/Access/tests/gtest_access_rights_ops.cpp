#include <gtest/gtest.h>
#include <Access/AccessRights.h>
#include <Access/AccessRights.cpp>  // NOLINT(bugprone-suspicious-include)
#include <IO/WriteBufferFromString.h>

using namespace DB;

TEST(AccessRights, Radix)
{
    AccessRights root;
    root.grant(AccessType::SELECT, "team");
    root.grant(AccessType::SELECT, "toast");
    root.grant(AccessType::SELECT, "toaster");
    root.grant(AccessType::INSERT, "toaster", "bread");
    root.grant(AccessType::ALTER_ADD_COLUMN, "toaster", "bread", "jam");

    root.grantWildcard(AccessType::CREATE_TABLE, "t");

    WriteBufferFromOwnString out;
    root.dumpTree(out);

    ASSERT_EQ(out.str(),
              "Tree():  level=GLOBAL_LEVEL, name=NULL, flags=USAGE, min_flags=USAGE, max_flags=SELECT, INSERT, ALTER ADD COLUMN, CREATE TABLE, wildcard_grant=false, num_children=1\n"
              "Tree(): - level=DATABASE_LEVEL, name=t, flags=CREATE TABLE, min_flags=CREATE TABLE, max_flags=SELECT, INSERT, ALTER ADD COLUMN, CREATE TABLE, wildcard_grant=true, num_children=2\n"
              "Tree(): -- level=DATABASE_LEVEL, name=eam, flags=CREATE TABLE, min_flags=CREATE TABLE, max_flags=SELECT, CREATE TABLE, wildcard_grant=false, num_children=1\n"
              "Tree(): --- level=DATABASE_LEVEL, name=NULL, flags=SELECT, CREATE TABLE, min_flags=SELECT, CREATE TABLE, max_flags=SELECT, CREATE TABLE, wildcard_grant=false, num_children=0\n"
              "Tree(): -- level=DATABASE_LEVEL, name=oast, flags=CREATE TABLE, min_flags=CREATE TABLE, max_flags=SELECT, INSERT, ALTER ADD COLUMN, CREATE TABLE, wildcard_grant=false, num_children=2\n"
              "Tree(): --- level=DATABASE_LEVEL, name=NULL, flags=SELECT, CREATE TABLE, min_flags=SELECT, CREATE TABLE, max_flags=SELECT, CREATE TABLE, wildcard_grant=false, num_children=0\n"
              "Tree(): --- level=DATABASE_LEVEL, name=er, flags=CREATE TABLE, min_flags=CREATE TABLE, max_flags=SELECT, INSERT, ALTER ADD COLUMN, CREATE TABLE, wildcard_grant=false, num_children=1\n"
              "Tree(): ---- level=DATABASE_LEVEL, name=NULL, flags=SELECT, CREATE TABLE, min_flags=SELECT, CREATE TABLE, max_flags=SELECT, INSERT, ALTER ADD COLUMN, CREATE TABLE, wildcard_grant=false, num_children=1\n"
              "Tree(): ----- level=TABLE_LEVEL, name=bread, flags=SELECT, CREATE TABLE, min_flags=SELECT, CREATE TABLE, max_flags=SELECT, INSERT, ALTER ADD COLUMN, CREATE TABLE, wildcard_grant=false, num_children=1\n"
              "Tree(): ------ level=TABLE_LEVEL, name=NULL, flags=SELECT, INSERT, CREATE TABLE, min_flags=SELECT, INSERT, CREATE TABLE, max_flags=SELECT, INSERT, ALTER ADD COLUMN, CREATE TABLE, wildcard_grant=false, num_children=1\n"
              "Tree(): ------- level=COLUMN_LEVEL, name=jam, flags=SELECT, INSERT, min_flags=SELECT, INSERT, max_flags=SELECT, INSERT, ALTER ADD COLUMN, wildcard_grant=false, num_children=1\n"
              "Tree(): -------- level=COLUMN_LEVEL, name=NULL, flags=SELECT, INSERT, ALTER ADD COLUMN, min_flags=SELECT, INSERT, ALTER ADD COLUMN, max_flags=SELECT, INSERT, ALTER ADD COLUMN, wildcard_grant=false, num_children=0\n");
}

TEST(AccessRights, GrantWildcard)
{
    AccessRights root;
    root.grant(AccessType::SELECT, "team");
    root.grant(AccessType::SELECT, "toast");
    root.grant(AccessType::SELECT, "toaster");
    root.grant(AccessType::INSERT, "toaster", "bread");
    root.grant(AccessType::ALTER_ADD_COLUMN, "toaster", "bread", "jam");

    root.grantWildcard(AccessType::CREATE_TABLE, "t");

    ASSERT_EQ(root.isGranted(AccessType::CREATE_TABLE, "tick"), true);
    ASSERT_EQ(root.isGranted(AccessType::CREATE_TABLE, "team"), true);
    ASSERT_EQ(root.isGranted(AccessType::CREATE_TABLE, "to"), true);
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "t"), false);
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "to"), false);
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "team"), true);
    ASSERT_EQ(root.isGranted(AccessType::INSERT, "toaster"), false);
    ASSERT_EQ(root.isGranted(AccessType::INSERT, "toaster", "bread"), true);
    ASSERT_EQ(root.isGranted(AccessType::ALTER_ADD_COLUMN, "toaster", "bread", "jam"), true);

    ASSERT_EQ(root.toString(), "GRANT CREATE TABLE ON t*.*, GRANT SELECT ON team.*, GRANT SELECT ON toast.*, GRANT SELECT ON toaster.*, GRANT INSERT, ALTER ADD COLUMN(jam) ON toaster.bread");

    root.revokeWildcard(AccessType::CREATE_TABLE, "t");
    ASSERT_EQ(root.toString(), "GRANT SELECT ON team.*, GRANT SELECT ON toast.*, GRANT SELECT ON toaster.*, GRANT INSERT, ALTER ADD COLUMN(jam) ON toaster.bread");

    root.revokeWildcard(AccessType::SELECT, "t");
    ASSERT_EQ(root.toString(), "GRANT INSERT, ALTER ADD COLUMN(jam) ON toaster.bread");

    root.revokeWildcard(AccessType::ALL, "t");

    ASSERT_EQ(root.toString(), "GRANT USAGE ON *.*");

    root.grant(AccessType::SELECT);
    root.revokeWildcard(AccessType::SELECT, "test");
    root.grant(AccessType::SELECT, "tester", "foo");

    ASSERT_EQ(root.toString(), "GRANT SELECT ON *.*, REVOKE SELECT ON test*.*, GRANT SELECT ON tester.foo");

    root.grant(AccessType::SELECT);
    ASSERT_EQ(root.toString(), "GRANT SELECT ON *.*");

    root = {};
    root.grant(AccessType::SELECT, "test");
    root.grantWildcard(AccessType::CREATE_TABLE, "test");
    ASSERT_EQ(root.toString(), "GRANT CREATE TABLE ON test*.*, GRANT SELECT ON test.*");

    root = {};
    root.grant(AccessType::SELECT, "test");
    root.grantWildcard(AccessType::SELECT, "test");
    ASSERT_EQ(root.toString(), "GRANT SELECT ON test*.*");

    root = {};
    root.grantWildcard(AccessType::SELECT, "default", "test");
    root.grantWildcard(AccessType::SELECT, "default", "t");
    ASSERT_EQ(root.toString(), "GRANT SELECT ON default.t*");

    root = {};
    root.grant(AccessType::SELECT, "default", "t");
    root.grantWildcard(AccessType::INSERT, "default", "t");
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "default", "t"), true);
    ASSERT_EQ(root.isGranted(AccessType::INSERT, "default", "t"), true);
    ASSERT_EQ(root.toString(), "GRANT SELECT ON default.t, GRANT INSERT ON default.t*");

    root.revoke(AccessType::INSERT, "default", "t");
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "default", "t"), true);
    ASSERT_EQ(root.isGranted(AccessType::INSERT, "default", "t"), false);
    ASSERT_EQ(root.isGranted(AccessType::INSERT, "default", "test"), true);
    ASSERT_EQ(root.toString(), "GRANT SELECT ON default.t, GRANT INSERT ON default.t*, REVOKE INSERT ON default.t");

    root = {};
    root.grant(AccessType::SELECT, "default", "t");
    root.revokeWildcard(AccessType::SELECT, "default", "t", "col");
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "default", "t"), false);
    ASSERT_EQ(root.isGrantedWildcard(AccessType::SELECT, "default", "t"), false);
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "default", "t", "col"), false);
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "default", "t", "col1"), false);
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "default", "t", "co"), true);
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "default", "t", "test"), true);
    ASSERT_EQ(root.toString(), "GRANT SELECT ON default.t, REVOKE SELECT(col*) ON default.t");

    root = {};
    root.grantWildcard(AccessType::ALTER_UPDATE, "prod");
    root.grant(AccessType::ALTER_UPDATE, "prod", "users");
    ASSERT_EQ(root.isGranted(AccessType::ALTER_UPDATE, "prod"), true);
    ASSERT_EQ(root.isGranted(AccessType::ALTER_UPDATE, "prod", "users"), true);
    ASSERT_EQ(root.isGranted(AccessType::ALTER_UPDATE, "prod", "orders"), true);
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "prod"), false);
    ASSERT_EQ(root.toString(), "GRANT ALTER UPDATE ON prod*.*");

    root.revoke(AccessType::ALTER_UPDATE, "prod", "users");
    ASSERT_EQ(root.isGranted(AccessType::ALTER_UPDATE, "prod", "users"), false);
    ASSERT_EQ(root.isGranted(AccessType::ALTER_UPDATE, "prod", "orders"), true);
    ASSERT_EQ(root.toString(), "GRANT ALTER UPDATE ON prod*.*, REVOKE ALTER UPDATE ON prod.users");

    root.grantWildcard(AccessType::ALTER_DELETE, "prod");
    root.grant(AccessType::ALTER_DELETE, "prod", "archive");
    ASSERT_EQ(root.isGranted(AccessType::ALTER_DELETE, "prod", "archive"), true);
    ASSERT_EQ(root.isGranted(AccessType::ALTER_DELETE, "prod", "current"), true);

    root.revokeWildcard(AccessType::ALTER_DELETE, "prod");
    ASSERT_EQ(root.isGranted(AccessType::ALTER_DELETE, "prod", "archive"), false);
    ASSERT_EQ(root.isGranted(AccessType::ALTER_DELETE, "prod", "current"), false);

    ASSERT_EQ(root.toString(), "GRANT ALTER UPDATE ON prod*.*, REVOKE ALTER UPDATE ON prod.users");

    root = {};
    root.grantWildcard(AccessType::SELECT, "test");
    root.grantWildcard(AccessType::INSERT, "test");
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "test"), true);
    ASSERT_EQ(root.isGranted(AccessType::INSERT, "test"), true);
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "testdata"), true);
    ASSERT_EQ(root.isGranted(AccessType::INSERT, "testdata"), true);
    ASSERT_EQ(root.toString(), "GRANT SELECT, INSERT ON test*.*");

    root.revokeWildcard(AccessType::SELECT, "test");
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "test"), false);
    ASSERT_EQ(root.isGranted(AccessType::INSERT, "test"), true);
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "testdata"), false);
    ASSERT_EQ(root.isGranted(AccessType::INSERT, "testdata"), true);
    ASSERT_EQ(root.isGrantedWildcard(AccessType::INSERT, "testdata"), true);
    ASSERT_EQ(root.isGrantedWildcard(AccessType::INSERT, "test"), true);
    ASSERT_EQ(root.toString(), "GRANT INSERT ON test*.*");

    root = {};
    root.grant(AccessType::SELECT, "foo");
    root.grantWildcard(AccessType::SELECT, "foo", "bar");
    ASSERT_EQ(root.isGrantedWildcard(AccessType::SELECT, "foo"), false);
    ASSERT_EQ(root.toString(), "GRANT SELECT ON foo.*");

    root = {};
    root.grantWildcard(AccessType::ALTER_UPDATE, "");
    ASSERT_EQ(root.isGranted(AccessType::ALTER_UPDATE, ""), true);
    ASSERT_EQ(root.isGranted(AccessType::ALTER_UPDATE, "anything"), true);

    root.revokeWildcard(AccessType::ALTER_UPDATE, "");
    ASSERT_EQ(root.isGranted(AccessType::ALTER_UPDATE, ""), false);
    ASSERT_EQ(root.isGranted(AccessType::ALTER_UPDATE, "anything"), false);

    root.grantWildcard(AccessType::CREATE_VIEW, "proj");
    ASSERT_EQ(root.isGranted(AccessType::CREATE_VIEW, "project"), true);
    ASSERT_EQ(root.isGranted(AccessType::CREATE_VIEW, "pro"), false);
    ASSERT_EQ(root.isGranted(AccessType::CREATE_VIEW, "projX"), true);

    root.revokeWildcard(AccessType::CREATE_VIEW, "proj");
    ASSERT_EQ(root.isGranted(AccessType::CREATE_VIEW, "project"), false);
    ASSERT_EQ(root.isGranted(AccessType::CREATE_VIEW, "projX"), false);

    root = {};
    root.grantWildcard(AccessType::SELECT, "db");
    root.grantWildcard(AccessType::INSERT, "db");
    root.grantWildcard(AccessType::ALTER_UPDATE, "db");
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "db"), true);
    ASSERT_EQ(root.isGranted(AccessType::INSERT, "db"), true);
    ASSERT_EQ(root.isGranted(AccessType::ALTER_UPDATE, "db"), true);

    root.revokeWildcard(AccessType::SELECT, "db");
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "db"), false);
    ASSERT_EQ(root.isGranted(AccessType::INSERT, "db"), true);
    ASSERT_EQ(root.isGranted(AccessType::ALTER_UPDATE, "db"), true);

    root.revokeWildcard(AccessType::INSERT, "db");
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "db"), false);
    ASSERT_EQ(root.isGranted(AccessType::INSERT, "db"), false);
    ASSERT_EQ(root.isGranted(AccessType::ALTER_UPDATE, "db"), true);

    root.revokeWildcard(AccessType::ALTER_UPDATE, "db");
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "db"), false);
    ASSERT_EQ(root.isGranted(AccessType::INSERT, "db"), false);
    ASSERT_EQ(root.isGranted(AccessType::ALTER_UPDATE, "db"), false);

    root = {};
    root.grant(AccessType::SELECT, "db", "table");
    root.revoke(AccessType::SELECT, "db", "table", "a");
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "db", "table", "a"), false);

    root = {};
    root.grant(AccessType::SHOW_NAMED_COLLECTIONS);
    root.revoke(AccessType::SHOW_NAMED_COLLECTIONS, "collection1");
    ASSERT_EQ(root.isGranted(AccessType::SHOW_NAMED_COLLECTIONS, "collection1"), false);
    ASSERT_EQ(root.isGranted(AccessType::SHOW_NAMED_COLLECTIONS, "collection2"), true);

    root = {};
    root.grant(AccessType::SHOW_NAMED_COLLECTIONS);
    root.revokeWildcard(AccessType::SHOW_NAMED_COLLECTIONS, "collection");
    ASSERT_EQ(root.isGranted(AccessType::SHOW_NAMED_COLLECTIONS, "collection1"), false);
    ASSERT_EQ(root.isGranted(AccessType::SHOW_NAMED_COLLECTIONS, "collection2"), false);
    ASSERT_EQ(root.isGranted(AccessType::SHOW_NAMED_COLLECTIONS, "foo"), true);

    root = {};
    root.grantWildcardWithGrantOption(AccessType::SELECT, "db");
    root.grant(AccessType::INSERT, "db", "table");
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "db", "table"), true);
    ASSERT_EQ(root.hasGrantOption(AccessType::SELECT, "db_1", "table"), true);
    ASSERT_EQ(root.hasGrantOptionWildcard(AccessType::SELECT, "db", "table"), true);
    ASSERT_EQ(root.hasGrantOptionWildcard(AccessType::SELECT, "db"), true);
    ASSERT_EQ(root.isGranted(AccessType::INSERT, "db", "table"), true);
    ASSERT_EQ(root.isGranted(AccessType::INSERT, "db", "other_table"), false);
    ASSERT_EQ(root.toString(), "GRANT SELECT ON db*.* WITH GRANT OPTION, GRANT INSERT ON db.`table`");
}

TEST(AccessRights, Union)
{
    AccessRights lhs;
    AccessRights rhs;
    lhs.grant(AccessType::CREATE_TABLE, "db1", "tb1");
    rhs.grant(AccessType::SELECT, "db2");
    lhs.makeUnion(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT CREATE TABLE ON db1.tb1, GRANT SELECT ON db2.*");

    lhs.clear();
    rhs.clear();
    rhs.grant(AccessType::SELECT, "db2");
    lhs.grant(AccessType::CREATE_TABLE, "db1", "tb1");
    lhs.makeUnion(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT CREATE TABLE ON db1.tb1, GRANT SELECT ON db2.*");

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT);
    rhs.grant(AccessType::SELECT, "db1", "tb1");
    lhs.makeUnion(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT SELECT ON *.*");

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT, "db1", "tb1", Strings{"col1", "col2"});
    rhs.grant(AccessType::SELECT, "db1", "tb1", Strings{"col2", "col3"});
    lhs.makeUnion(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT SELECT(col1, col2, col3) ON db1.tb1");

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT, "db1", "tb1", Strings{"col1", "col2"});
    rhs.grantWithGrantOption(AccessType::SELECT, "db1", "tb1", Strings{"col2", "col3"});
    lhs.makeUnion(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT SELECT(col1) ON db1.tb1, GRANT SELECT(col2, col3) ON db1.tb1 WITH GRANT OPTION");

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT, "db1", "tb1", Strings{"col1", "col2", "test"});
    rhs.grant(AccessType::SELECT, "db1", "tb1");
    lhs.makeUnion(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT SELECT ON db1.tb1");

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT, "db1", "tb1", Strings{"col1", "col2", "test"});
    rhs.grantWildcardWithGrantOption(AccessType::SELECT, "db1", "tb1");
    rhs.revokeWildcardGrantOption(AccessType::SELECT, "db1", "tb1", "col");
    lhs.makeUnion(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT SELECT ON db1.tb1* WITH GRANT OPTION, REVOKE GRANT OPTION SELECT(col*) ON db1.tb1");

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT, "db1", "tb1", Strings{"col1", "col2", "test"});
    rhs.grantWildcardWithGrantOption(AccessType::SELECT, "db1", "tb1", "col");
    lhs.makeUnion(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT SELECT(col*) ON db1.tb1 WITH GRANT OPTION, GRANT SELECT(test) ON db1.tb1");

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT);
    lhs.revoke(AccessType::SELECT, "test");
    rhs.grant(AccessType::SELECT, "test", "table");
    lhs.makeUnion(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT SELECT ON *.*, REVOKE SELECT ON test.*, GRANT SELECT ON test.`table`");
}

TEST(AccessRights, Intersection)
{
    AccessRights lhs;
    AccessRights rhs;
    lhs.grant(AccessType::CREATE_TABLE, "db1", "tb1");
    rhs.grant(AccessType::SELECT, "db2");
    lhs.makeIntersection(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT USAGE ON *.*");

    lhs.clear();
    rhs.clear();
    lhs.grant(AccessType::SELECT, "db2");
    rhs.grant(AccessType::CREATE_TABLE, "db1", "tb1");
    lhs.makeIntersection(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT USAGE ON *.*");

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT);
    rhs.grant(AccessType::SELECT, "db1", "tb1");
    lhs.makeIntersection(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT SELECT ON db1.tb1");

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT, "db1", "tb1", Strings{"col1", "col2"});
    rhs.grant(AccessType::SELECT, "db1", "tb1", Strings{"col2", "col3"});
    lhs.makeIntersection(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT SELECT(col2) ON db1.tb1");

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT, "db1", "tb1", Strings{"col1", "col2"});
    rhs.grantWithGrantOption(AccessType::SELECT, "db1", "tb1", Strings{"col2", "col3"});
    lhs.makeIntersection(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT SELECT(col2) ON db1.tb1");

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::INSERT);
    rhs.grant(AccessType::ALL, "db1");
    lhs.makeIntersection(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT INSERT ON db1.*");

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT, "db1", "tb1", Strings{"col1", "col2", "test"});
    rhs.grant(AccessType::SELECT, "db1", "tb1");
    lhs.makeIntersection(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT SELECT(col1, col2, test) ON db1.tb1");

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT, "db1", "tb1", Strings{"col1", "col2", "test"});
    rhs.grantWildcardWithGrantOption(AccessType::SELECT, "db1", "tb1");
    rhs.revokeWildcardGrantOption(AccessType::SELECT, "db1", "tb1", "col");
    lhs.makeIntersection(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT SELECT(col1, col2, test) ON db1.tb1");

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT, "db1", "tb1", Strings{"col1", "col2", "test"});
    rhs.grantWildcard(AccessType::SELECT, "db1", "tb1", "col");
    lhs.makeIntersection(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT SELECT(col1, col2) ON db1.tb1");

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT, "db1", "tb1", Strings{"col1", "col2", "test"});
    lhs.grantWithGrantOption(AccessType::SELECT, "db1", "tb1", "col1");
    rhs.grantWildcard(AccessType::SELECT, "db1", "tb1", "col");
    rhs.grantWithGrantOption(AccessType::SELECT, "db1", "tb1", "col1");
    lhs.makeIntersection(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT SELECT(col1) ON db1.tb1 WITH GRANT OPTION, GRANT SELECT(col2) ON db1.tb1");

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT, "team");
    lhs.grant(AccessType::SELECT, "test");
    lhs.grant(AccessType::SELECT, "toast");
    lhs.grant(AccessType::SELECT, "toaster");
    rhs.grant(AccessType::SELECT, "test");
    rhs.grant(AccessType::SELECT, "tear");
    rhs.grant(AccessType::SELECT, "team");
    rhs.grant(AccessType::SELECT, "tea");
    lhs.makeIntersection(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT SELECT ON team.*, GRANT SELECT ON test.*");

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT, "team");
    lhs.grant(AccessType::SELECT, "test");
    lhs.grantWildcard(AccessType::SELECT, "toast");
    rhs.grant(AccessType::SELECT, "test");
    rhs.grant(AccessType::SELECT, "tear");
    rhs.grant(AccessType::SELECT, "team");
    rhs.grant(AccessType::SELECT, "tea");
    rhs.grant(AccessType::SELECT, "toaster");
    lhs.makeIntersection(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT SELECT ON team.*, GRANT SELECT ON test.*, GRANT SELECT ON toaster.*");

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT, "team");
    lhs.grantWildcard(AccessType::SELECT, "toast");
    rhs.grantWildcard(AccessType::SELECT, "tea");
    rhs.grant(AccessType::SELECT, "toaster", "foo");
    lhs.makeIntersection(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT SELECT ON team.*, GRANT SELECT ON toaster.foo");

    lhs = {};
    rhs = {};
    rhs.grantWildcard(AccessType::SELECT, "toaster");
    lhs.grantWildcard(AccessType::SELECT, "toast");
    lhs.makeIntersection(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT SELECT ON toaster*.*");

    lhs = {};
    rhs = {};
    lhs.grantWildcard(AccessType::SELECT, "toast");
    rhs.grant(AccessType::SELECT, "toaster");
    lhs.makeIntersection(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT SELECT ON toaster.*");
}

TEST(AccessRights, Difference)
{
    AccessRights lhs;
    AccessRights rhs;
    lhs.grant(AccessType::SELECT);
    rhs.grant(AccessType::SELECT);
    rhs.revoke(AccessType::SELECT, "system");
    lhs.makeDifference(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT SELECT ON system.*");

    lhs = {};
    rhs = {};
    lhs.grantWildcard(AccessType::SELECT, "toast");
    rhs.grant(AccessType::SELECT);
    rhs.revoke(AccessType::SELECT, "toaster");
    lhs.makeDifference(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT SELECT ON toaster.*");

    lhs = {};
    rhs = {};
    lhs.grantWildcard(AccessType::SELECT, "toast");
    lhs.grant(AccessType::CREATE_TABLE, "jam");
    auto lhs_old = lhs;
    lhs.makeDifference(rhs);
    ASSERT_EQ(lhs, lhs_old);

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT, "toast");
    rhs.grant(AccessType::CREATE_TABLE, "jam");
    lhs_old = lhs;
    lhs.makeDifference(rhs);
    ASSERT_EQ(lhs, lhs_old);

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::ALL);
    rhs.grant(AccessType::ALL);
    rhs.revoke(AccessType::SELECT, "system");
    lhs.makeDifference(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT SELECT ON system.*");
}

TEST(AccessRights, Contains)
{
    AccessRights lhs;
    AccessRights rhs;
    lhs.grant(AccessType::SELECT, "db1");
    rhs.grant(AccessType::SELECT, "db1", "tb1");
    ASSERT_EQ(lhs.contains(rhs), true);

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT, "db1");
    lhs.grant(AccessType::SELECT, "db2");
    rhs.grant(AccessType::SELECT, "db23", "tb1");
    rhs.grant(AccessType::SELECT, "db24", "tb1");
    ASSERT_EQ(lhs.contains(rhs), false);

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT, "db1");
    lhs.grant(AccessType::SELECT, "db2");
    rhs.grant(AccessType::SELECT, "db2", "tb1");
    ASSERT_EQ(lhs.contains(rhs), true);

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::ALL, "db1");
    rhs.grant(AccessType::SELECT, "db1", "tb1");
    rhs.grant(AccessType::SELECT, "db1", "tb2", "col1");
    ASSERT_EQ(lhs.contains(rhs), true);

    lhs = {};
    rhs = {};
    lhs.grantWildcard(AccessType::SELECT, "db");
    rhs.grant(AccessType::SELECT, "db1");
    ASSERT_EQ(lhs.contains(rhs), true);
}

TEST(AccessRights, Iterator)
{
    AccessRights root;
    root.grant(AccessType::SELECT, "team");
    root.grant(AccessType::SELECT, "toast");
    root.grant(AccessType::SELECT, "toaster");
    root.grant(AccessType::INSERT, "toaster", "bread");
    root.grant(AccessType::ALTER_ADD_COLUMN, "toaster", "bread", "jam");
    root.grantWildcard(AccessType::CREATE_TABLE, "t");

    auto res = root.dumpNodes();
    ASSERT_EQ(res.size(), 4);
    ASSERT_EQ(res[0], "t");
    ASSERT_EQ(res[1], "team");
    ASSERT_EQ(res[2], "toast");
    ASSERT_EQ(res[3], "toaster");
}

TEST(AccessRights, Filter)
{
    AccessRights root;
    root.grant(AccessType::READ, "S3", "s3://url1.*");
    root.grant(AccessType::READ, "S3", "s3://url2.*");
    root.grant(AccessType::READ, "URL", "https://url3.*");

    auto res = root.getFilters("S3");
    ASSERT_EQ(res.size(), 2);
    ASSERT_EQ(res[0].path, "s3://url1.*");
    ASSERT_EQ(res[1].path, "s3://url2.*");

    res = root.getFilters("URL");
    ASSERT_EQ(res.size(), 1);
    ASSERT_EQ(res[0].path, "https://url3.*");

    root.revoke(AccessType::READ, "URL");
    res = root.getFilters("URL");
    ASSERT_EQ(res.size(), 0);
}

TEST(AccessRights, RevokeWithParameters)
{
    AccessRights root;
    root.grantWithGrantOption(AccessType::SELECT);
    root.grantWithGrantOption(AccessType::CREATE_USER);
    root.revokeWildcard(AccessType::SELECT, "default", "zoo");
    ASSERT_EQ(root.toString(), "GRANT SELECT ON *.* WITH GRANT OPTION, GRANT CREATE USER ON * WITH GRANT OPTION, REVOKE SELECT ON default.zoo*");

    root = {};
    root.grantWithGrantOption(AccessType::SELECT);
    root.grantWithGrantOption(AccessType::CREATE_USER);
    root.revokeWildcard(AccessType::SELECT, "default", "foo", "bar");
    ASSERT_EQ(root.toString(), "GRANT SELECT ON *.* WITH GRANT OPTION, GRANT CREATE USER ON * WITH GRANT OPTION, REVOKE SELECT(bar*) ON default.foo");
}

TEST(AccessRights, RevokeWithParametersWithGrantOption)
{
    AccessRights root;
    root.grantWithGrantOption(AccessType::ALL);
    root.revokeWildcard(AccessType::INTROSPECTION, "system");  // global grant, do nothing for database revoke
    ASSERT_EQ(root.toString(), "GRANT ALL ON *.* WITH GRANT OPTION");

    root = {};
    root.grant(AccessType::SELECT);
    root.grant(AccessType::INTROSPECTION);
    root.grant(AccessType::CREATE_USER);
    root.revokeWildcard(AccessType::CREATE_USER, "system");
    ASSERT_EQ(root.toString(), "GRANT SELECT, INTROSPECTION ON *.*, GRANT CREATE USER ON *, REVOKE CREATE USER ON system*");

    root.grantWithGrantOption(AccessType::SELECT);
    root.grantWithGrantOption(AccessType::INTROSPECTION);
    root.grantWithGrantOption(AccessType::CREATE_USER);
    root.revokeWildcard(AccessType::CREATE_USER, "system");
    ASSERT_EQ(root.toString(), "GRANT SELECT, INTROSPECTION ON *.* WITH GRANT OPTION, GRANT CREATE USER ON * WITH GRANT OPTION, REVOKE CREATE USER ON system*");
}

TEST(AccessRights, ParialRevokeWithGrantOption)
{
    AccessRights root;
    root.grant(AccessType::SELECT);
    root.revoke(AccessType::SELECT, "default", "zookeeper");
    ASSERT_FALSE(root.isGrantedWildcard(AccessType::SELECT, "default", "zoo"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "default", "zoo"));

    root = {};
    root.grantWithGrantOption(AccessType::SELECT);
    root.revoke(AccessType::SELECT, "default", "zookeeper");
    ASSERT_FALSE(root.isGrantedWildcard(AccessType::SELECT, "default", "zoo"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "default", "zoo"));
}

TEST(AccessRights, WildcardGrantEdgeCases)
{
    AccessRights root;
    root.grantWildcard(AccessType::SELECT, "a");
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "abc"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "a"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "bcd"));
    ASSERT_EQ(root.toString(), "GRANT SELECT ON a*.*");

    root = {};
    root.grantWildcard(AccessType::SELECT, "test");
    root.grant(AccessType::INSERT, "test");
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "test"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "testing"));
    ASSERT_TRUE(root.isGranted(AccessType::INSERT, "test"));
    ASSERT_FALSE(root.isGranted(AccessType::INSERT, "testing"));
    ASSERT_EQ(root.toString(), "GRANT SELECT ON test*.*, GRANT INSERT ON test.*");

    root = {};
    root.grantWildcard(AccessType::SELECT, "prod");
    root.grantWildcard(AccessType::INSERT, "production");
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "prod"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "production"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "product"));
    ASSERT_FALSE(root.isGranted(AccessType::INSERT, "prod"));
    ASSERT_FALSE(root.isGranted(AccessType::INSERT, "product"));
    ASSERT_TRUE(root.isGranted(AccessType::INSERT, "production"));
    ASSERT_TRUE(root.isGranted(AccessType::INSERT, "production_v2"));

    root = {};
    root.grantWildcard(AccessType::SELECT, "db", "tbl");
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "db", "tbl"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "db", "tbl_backup"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "db", "tbl123"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "db", "other"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "db2", "tbl"));

    root = {};
    root.grantWildcard(AccessType::SELECT, "db", "tbl", "col");
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "db", "tbl", "col"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "db", "tbl", "col_id"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "db", "tbl", "column"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "db", "tbl", "other"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "db", "tbl"));

    root = {};
    root.grantWildcard(AccessType::SELECT, "sys");
    root.grantWildcard(AccessType::INSERT, "sys", "log");
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "sys", "any_table", "any_col"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "system", "tables"));
    ASSERT_TRUE(root.isGranted(AccessType::INSERT, "sys", "log_events"));
    ASSERT_FALSE(root.isGranted(AccessType::INSERT, "sys", "data"));
}

TEST(AccessRights, PartialRevokeWithWildcard)
{
    AccessRights root;
    root.grant(AccessType::SELECT);
    root.revokeWildcard(AccessType::SELECT, "system");
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "default"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "production"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "system"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "system_logs"));
    ASSERT_EQ(root.toString(), "GRANT SELECT ON *.*, REVOKE SELECT ON system*.*");

    root = {};
    root.grantWildcard(AccessType::SELECT, "prod");
    root.revoke(AccessType::SELECT, "production");
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "prod"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "product"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "production"));
    ASSERT_EQ(root.toString(), "GRANT SELECT ON prod*.*, REVOKE SELECT ON production.*");

    root = {};
    root.grantWildcard(AccessType::SELECT, "test");
    root.revokeWildcard(AccessType::SELECT, "testing");
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "test"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "test_env"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "testing"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "testing_v2"));
    ASSERT_EQ(root.toString(), "GRANT SELECT ON test*.*, REVOKE SELECT ON testing*.*");

    root = {};
    root.grantWildcard(AccessType::SELECT, "db");
    root.revoke(AccessType::SELECT, "db", "secret");
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "db", "public"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "db123", "any"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "db", "secret"));
    ASSERT_EQ(root.toString(), "GRANT SELECT ON db*.*, REVOKE SELECT ON db.secret");

    root = {};
    root.grant(AccessType::SELECT);
    root.revokeWildcard(AccessType::SELECT, "secret");
    root.revokeWildcard(AccessType::SELECT, "private");
    root.revoke(AccessType::SELECT, "system");
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "public"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "secret"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "secret_data"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "private"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "private_logs"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "system"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "system_public"));

    root = {};
    root.grant(AccessType::SELECT);
    root.revokeWildcard(AccessType::SELECT, "internal");
    root.grant(AccessType::SELECT, "internal_public");
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "internal"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "internal_data"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "internal_public"));
    ASSERT_EQ(root.toString(), "GRANT SELECT ON *.*, REVOKE SELECT ON internal*.*, GRANT SELECT ON internal_public.*");
}

TEST(AccessRights, WildcardGrantOptionInteractions)
{
    AccessRights root;
    root.grantWildcardWithGrantOption(AccessType::SELECT, "db");
    root.revokeGrantOption(AccessType::SELECT, "db", "sensitive");
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "db", "sensitive"));
    ASSERT_TRUE(root.hasGrantOption(AccessType::SELECT, "db", "public"));
    ASSERT_FALSE(root.hasGrantOption(AccessType::SELECT, "db", "sensitive"));
    ASSERT_TRUE(root.hasGrantOption(AccessType::SELECT, "db123", "any"));

    root = {};
    root.grantWildcard(AccessType::SELECT, "prod");
    root.grantWithGrantOption(AccessType::SELECT, "production");
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "prod"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "production"));
    ASSERT_FALSE(root.hasGrantOption(AccessType::SELECT, "prod"));
    ASSERT_TRUE(root.hasGrantOption(AccessType::SELECT, "production"));
    ASSERT_FALSE(root.hasGrantOption(AccessType::SELECT, "product"));

    root = {};
    root.grantWildcardWithGrantOption(AccessType::SELECT, "test");
    root.revokeWildcardGrantOption(AccessType::SELECT, "testing");
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "testing"));
    ASSERT_TRUE(root.hasGrantOption(AccessType::SELECT, "test"));
    ASSERT_FALSE(root.hasGrantOption(AccessType::SELECT, "testing"));
    ASSERT_FALSE(root.hasGrantOption(AccessType::SELECT, "testing_v2"));
    ASSERT_TRUE(root.hasGrantOption(AccessType::SELECT, "test_env"));

    root = {};
    root.grantWildcardWithGrantOption(AccessType::INSERT, "analytics");
    ASSERT_TRUE(root.hasGrantOptionWildcard(AccessType::INSERT, "analytics"));
    ASSERT_TRUE(root.hasGrantOptionWildcard(AccessType::INSERT, "analytics_v2"));
    ASSERT_FALSE(root.hasGrantOptionWildcard(AccessType::INSERT, "other"));

    root = {};
    root.grantWildcardWithGrantOption(AccessType::SELECT, "db");
    root.revokeWildcardGrantOption(AccessType::SELECT, "db", "tbl");
    ASSERT_TRUE(root.hasGrantOption(AccessType::SELECT, "db", "other"));
    ASSERT_TRUE(root.hasGrantOption(AccessType::SELECT, "db123", "any"));
    ASSERT_FALSE(root.hasGrantOption(AccessType::SELECT, "db", "tbl"));
    ASSERT_FALSE(root.hasGrantOption(AccessType::SELECT, "db", "tbl_backup"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "db", "tbl"));
}

TEST(AccessRights, UnionWithPartialRevokes)
{
    AccessRights lhs;
    AccessRights rhs;

    lhs.grant(AccessType::SELECT);
    lhs.revoke(AccessType::SELECT, "secret");
    rhs.grant(AccessType::SELECT, "secret", "public_table");
    lhs.makeUnion(rhs);
    ASSERT_TRUE(lhs.isGranted(AccessType::SELECT, "default"));
    ASSERT_TRUE(lhs.isGranted(AccessType::SELECT, "secret", "public_table"));
    ASSERT_FALSE(lhs.isGranted(AccessType::SELECT, "secret"));

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT);
    lhs.revokeWildcard(AccessType::SELECT, "sys");
    rhs.grantWildcard(AccessType::SELECT, "system");
    lhs.makeUnion(rhs);
    ASSERT_TRUE(lhs.isGranted(AccessType::SELECT, "default"));
    ASSERT_TRUE(lhs.isGranted(AccessType::SELECT, "system"));
    ASSERT_TRUE(lhs.isGranted(AccessType::SELECT, "system_logs"));
    ASSERT_FALSE(lhs.isGranted(AccessType::SELECT, "sys"));

    lhs = {};
    rhs = {};
    lhs.grantWildcard(AccessType::SELECT, "prod");
    rhs.grantWildcard(AccessType::INSERT, "prod");
    lhs.makeUnion(rhs);
    ASSERT_TRUE(lhs.isGranted(AccessType::SELECT, "production"));
    ASSERT_TRUE(lhs.isGranted(AccessType::INSERT, "production"));
    ASSERT_EQ(lhs.toString(), "GRANT SELECT, INSERT ON prod*.*");

    lhs = {};
    rhs = {};
    lhs.grantWildcard(AccessType::SELECT, "test");
    rhs.grantWildcard(AccessType::SELECT, "testing");
    lhs.makeUnion(rhs);
    ASSERT_TRUE(lhs.isGranted(AccessType::SELECT, "test"));
    ASSERT_TRUE(lhs.isGranted(AccessType::SELECT, "testing"));
    ASSERT_EQ(lhs.toString(), "GRANT SELECT ON test*.*");
}

TEST(AccessRights, IntersectionWithPartialRevokes)
{
    AccessRights lhs;
    AccessRights rhs;

    lhs.grant(AccessType::SELECT);
    lhs.revoke(AccessType::SELECT, "secret");
    rhs.grant(AccessType::SELECT);
    rhs.revoke(AccessType::SELECT, "private");
    lhs.makeIntersection(rhs);
    ASSERT_TRUE(lhs.isGranted(AccessType::SELECT, "default"));
    ASSERT_FALSE(lhs.isGranted(AccessType::SELECT, "secret"));
    ASSERT_FALSE(lhs.isGranted(AccessType::SELECT, "private"));

    lhs = {};
    rhs = {};
    lhs.grantWildcard(AccessType::SELECT, "test");
    rhs.grant(AccessType::SELECT, "test");
    rhs.grant(AccessType::SELECT, "testing");
    lhs.makeIntersection(rhs);
    ASSERT_TRUE(lhs.isGranted(AccessType::SELECT, "test"));
    ASSERT_TRUE(lhs.isGranted(AccessType::SELECT, "testing"));
    ASSERT_FALSE(lhs.isGranted(AccessType::SELECT, "test_env"));

    lhs = {};
    rhs = {};
    lhs.grantWildcard(AccessType::SELECT, "prod");
    rhs.grantWildcard(AccessType::SELECT, "production");
    lhs.makeIntersection(rhs);
    ASSERT_FALSE(lhs.isGranted(AccessType::SELECT, "prod"));
    ASSERT_FALSE(lhs.isGranted(AccessType::SELECT, "product"));
    ASSERT_TRUE(lhs.isGranted(AccessType::SELECT, "production"));
    ASSERT_TRUE(lhs.isGranted(AccessType::SELECT, "production_v2"));

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT);
    rhs.grant(AccessType::SELECT);
    rhs.revokeWildcard(AccessType::SELECT, "sys");
    lhs.makeIntersection(rhs);
    ASSERT_TRUE(lhs.isGranted(AccessType::SELECT, "default"));
    ASSERT_FALSE(lhs.isGranted(AccessType::SELECT, "sys"));
    ASSERT_FALSE(lhs.isGranted(AccessType::SELECT, "system"));
}

TEST(AccessRights, DifferenceWithPartialRevokes)
{
    AccessRights lhs;
    AccessRights rhs;

    lhs.grant(AccessType::SELECT);
    rhs.grant(AccessType::SELECT);
    rhs.revoke(AccessType::SELECT, "secret");
    lhs.makeDifference(rhs);
    ASSERT_FALSE(lhs.isGranted(AccessType::SELECT, "default"));
    ASSERT_TRUE(lhs.isGranted(AccessType::SELECT, "secret"));

    lhs = {};
    rhs = {};
    lhs.grantWildcard(AccessType::SELECT, "test");
    rhs.grant(AccessType::SELECT, "test");
    lhs.makeDifference(rhs);
    ASSERT_FALSE(lhs.isGranted(AccessType::SELECT, "test"));
    ASSERT_TRUE(lhs.isGranted(AccessType::SELECT, "testing"));
    ASSERT_TRUE(lhs.isGranted(AccessType::SELECT, "test_env"));

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT);
    rhs.grant(AccessType::SELECT);
    rhs.revokeWildcard(AccessType::SELECT, "internal");
    lhs.makeDifference(rhs);
    ASSERT_FALSE(lhs.isGranted(AccessType::SELECT, "default"));
    ASSERT_TRUE(lhs.isGranted(AccessType::SELECT, "internal"));
    ASSERT_TRUE(lhs.isGranted(AccessType::SELECT, "internal_data"));
}

TEST(AccessRights, ContainsWithWildcardsAndPartialRevokes)
{
    AccessRights lhs;
    AccessRights rhs;

    lhs.grantWildcard(AccessType::SELECT, "test");
    rhs.grant(AccessType::SELECT, "testing");
    ASSERT_TRUE(lhs.contains(rhs));

    lhs = {};
    rhs = {};
    lhs.grantWildcard(AccessType::SELECT, "test");
    rhs.grant(AccessType::SELECT, "prod");
    ASSERT_FALSE(lhs.contains(rhs));

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT);
    lhs.revoke(AccessType::SELECT, "secret");
    rhs.grant(AccessType::SELECT, "default");
    ASSERT_TRUE(lhs.contains(rhs));

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT);
    lhs.revoke(AccessType::SELECT, "secret");
    rhs.grant(AccessType::SELECT, "secret");
    ASSERT_FALSE(lhs.contains(rhs));

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT);
    lhs.revokeWildcard(AccessType::SELECT, "sys");
    rhs.grant(AccessType::SELECT, "default");
    rhs.grant(AccessType::SELECT, "prod");
    ASSERT_TRUE(lhs.contains(rhs));

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT);
    lhs.revokeWildcard(AccessType::SELECT, "sys");
    rhs.grant(AccessType::SELECT, "system");
    ASSERT_FALSE(lhs.contains(rhs));

    lhs = {};
    rhs = {};
    lhs.grantWildcard(AccessType::SELECT, "testing");
    rhs.grantWildcard(AccessType::SELECT, "test");
    ASSERT_FALSE(lhs.contains(rhs));

    lhs = {};
    rhs = {};
    lhs.grantWithGrantOption(AccessType::SET_DEFINER);
    lhs.revoke(AccessType::SET_DEFINER, "internal-user-1");
    rhs.grantWithGrantOption(AccessType::SET_DEFINER);
    rhs.revoke(AccessType::SET_DEFINER, "internal-user-1");
    rhs.revoke(AccessType::SET_DEFINER, "internal-user-2");
    rhs.revoke(AccessType::SET_DEFINER, "internal-user-3");
    ASSERT_TRUE(lhs.contains(rhs));

    lhs = {};
    rhs = {};
    rhs.grantWithGrantOption(AccessType::SET_DEFINER);
    rhs.revoke(AccessType::SET_DEFINER, "internal-user-1");
    lhs.grantWithGrantOption(AccessType::SET_DEFINER);
    lhs.revoke(AccessType::SET_DEFINER, "internal-user-1");
    lhs.revoke(AccessType::SET_DEFINER, "internal-user-2");
    lhs.revoke(AccessType::SET_DEFINER, "internal-user-3");
    ASSERT_FALSE(lhs.contains(rhs));

    lhs = {};
    rhs = {};
    lhs.grantWithGrantOption(AccessType::SET_DEFINER);
    lhs.revoke(AccessType::SET_DEFINER, "internal-user-1");
    lhs.revoke(AccessType::SET_DEFINER, "internal-user-2");
    rhs.grantWithGrantOption(AccessType::SET_DEFINER);
    rhs.revoke(AccessType::SET_DEFINER, "internal-user-1");
    rhs.revoke(AccessType::SET_DEFINER, "internal-user-2");
    ASSERT_TRUE(lhs.contains(rhs));

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::CREATE_ROLE);
    lhs.grant(AccessType::ROLE_ADMIN);
    lhs.grantWithGrantOption(AccessType::SET_DEFINER);
    lhs.revoke(AccessType::SET_DEFINER, "internal-user-1");
    rhs.grantWithGrantOption(AccessType::SET_DEFINER);
    rhs.revoke(AccessType::SET_DEFINER, "internal-user-1");
    rhs.revoke(AccessType::SET_DEFINER, "internal-user-2");
    rhs.revoke(AccessType::SET_DEFINER, "internal-user-3");
    ASSERT_TRUE(lhs.contains(rhs));

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT);
    lhs.revoke(AccessType::SELECT, "secret_db1");
    rhs.grant(AccessType::SELECT);
    rhs.revoke(AccessType::SELECT, "secret_db1");
    rhs.revoke(AccessType::SELECT, "secret_db2");
    rhs.revoke(AccessType::SELECT, "secret_db3");
    ASSERT_TRUE(lhs.contains(rhs));
    ASSERT_FALSE(rhs.contains(lhs));
}

TEST(AccessRights, ColumnLevelWildcardOperations)
{
    AccessRights root;
    root.grant(AccessType::SELECT, "db", "users");
    root.revokeWildcard(AccessType::SELECT, "db", "users", "secret");
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "db", "users", "name"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "db", "users", "email"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "db", "users", "secret"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "db", "users", "secret_key"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "db", "users"));

    root = {};
    root.grant(AccessType::SELECT, "db", "table");
    root.revokeWildcard(AccessType::SELECT, "db", "table", "private");
    root.revokeWildcard(AccessType::SELECT, "db", "table", "secret");
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "db", "table", "public_col"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "db", "table", "private_data"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "db", "table", "secret_key"));

    root = {};
    root.grantWildcard(AccessType::SELECT, "db", "table", "col");
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "db", "table", "col"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "db", "table", "col_id"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "db", "table", "column"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "db", "table", "other"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "db", "table"));

    root = {};
    root.grantWildcardWithGrantOption(AccessType::SELECT, "db", "table", "visible");
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "db", "table", "visible"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "db", "table", "visible_data"));
    ASSERT_TRUE(root.hasGrantOption(AccessType::SELECT, "db", "table", "visible"));
    ASSERT_TRUE(root.hasGrantOption(AccessType::SELECT, "db", "table", "visible_data"));
    ASSERT_FALSE(root.hasGrantOption(AccessType::SELECT, "db", "table", "hidden"));
}

TEST(AccessRights, ComplexWildcardScenarios)
{
    AccessRights root;
    root.grant(AccessType::ALL);
    root.revokeWildcard(AccessType::SELECT, "internal");
    root.grantWildcard(AccessType::SELECT, "internal_public");
    root.revoke(AccessType::SELECT, "internal_public_secret");
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "default"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "internal"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "internal_data"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "internal_public"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "internal_public_api"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "internal_public_secret"));

    root = {};
    root.grantWildcard(AccessType::SELECT, "prod");
    root.revokeWildcard(AccessType::SELECT, "prod", "secret");
    root.grant(AccessType::SELECT, "prod", "secret_allowed");
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "production", "users"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "prod", "secret_data"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "prod", "secret_allowed"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "prod", "public"));

    root = {};
    root.grantWildcard(AccessType::SELECT, "analytics");
    root.grantWildcard(AccessType::INSERT, "analytics_write");
    root.revoke(AccessType::SELECT, "analytics_read_only", "sensitive");
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "analytics", "any"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "analytics_read_only", "normal"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "analytics_read_only", "sensitive"));
    ASSERT_TRUE(root.isGranted(AccessType::INSERT, "analytics_write", "data"));
    ASSERT_FALSE(root.isGranted(AccessType::INSERT, "analytics", "data"));

    root = {};
    root.grantWildcard(AccessType::SELECT, "");
    root.revokeWildcard(AccessType::SELECT, "a");
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "b"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "z"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "a"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "abc"));

    root = {};
    root.grant(AccessType::SELECT);
    root.revoke(AccessType::SELECT);
    root.grantWildcard(AccessType::SELECT, "allowed");
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "default"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "allowed"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "allowed_db"));
}

TEST(AccessRights, PartialRevokePropagation)
{
    AccessRights root;
    root.grant(AccessType::SELECT);
    root.revoke(AccessType::SELECT, "db1", "secret");
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "db1", "secret"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "db1", "public"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "db2", "secret"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "db2"));

    root = {};
    root.grant(AccessType::SELECT);
    root.revokeWildcard(AccessType::SELECT, "temp");
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "permanent"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "temp"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "temp1"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "temporary"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "team"));

    root = {};
    root.grant(AccessType::SELECT, "db");
    root.revoke(AccessType::SELECT, "db", "t1", "col1");
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "db", "t1", "col2"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "db", "t2"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "db", "t1", "col1"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "db", "t1"));

    root = {};
    root.grant(AccessType::SELECT);
    root.grant(AccessType::INSERT);
    root.revokeWildcard(AccessType::SELECT, "readonly");
    root.revokeWildcard(AccessType::INSERT, "writeonly");
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "normal"));
    ASSERT_TRUE(root.isGranted(AccessType::INSERT, "normal"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "readonly"));
    ASSERT_TRUE(root.isGranted(AccessType::INSERT, "readonly"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "writeonly"));
    ASSERT_FALSE(root.isGranted(AccessType::INSERT, "writeonly"));
}

TEST(AccessRights, PartialRevokeIsGrantedWildcard)
{
    AccessRights root;
    root.grant(AccessType::SELECT);
    root.revoke(AccessType::SELECT, "system", "zookeeper");
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "normal"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "system", "query_log"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "system", "zookeeper"));
    ASSERT_FALSE(root.isGrantedWildcard(AccessType::SELECT, "system", "zookeeper"));

    // system.zookeeper is revoked, but system.zookeeper2 is not
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "system", "zookeeper2"));
    ASSERT_TRUE(root.isGrantedWildcard(AccessType::SELECT, "system", "zookeeper2"));
    ASSERT_TRUE(root.isGrantedWildcard(AccessType::SELECT, "system", "query_log"));

    root = {};
    root.grant(AccessType::SELECT);
    root.revokeWildcard(AccessType::SELECT, "system", "zookeeper");
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "system", "zookeeper"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "system", "zookeeper2"));
    ASSERT_TRUE(root.isGrantedWildcard(AccessType::SELECT, "system", "query_log"));
}

TEST(AccessRights, SamePrefixDifferentLevels)
{
    AccessRights root;
    root.grant(AccessType::SELECT);
    root.revoke(AccessType::SELECT, "test");
    root.revoke(AccessType::SELECT, "prod", "test");
    root.revoke(AccessType::SELECT, "dev", "data", "test");

    // "test" is revoked
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "test"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "test", "anytable"));

    // "prod.test" is revoked
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "prod"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "prod", "test"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "prod", "foo"));

    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "prod", "other"));

    // "dev.data.test" is revoked
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "dev", "data", "other"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "dev", "data", "test"));
}

TEST(AccessRights, IsGrantedWildcardDatabaseLevel)
{
    AccessRights root;
    root.grant(AccessType::SELECT, "mydb");

    ASSERT_FALSE(root.isGrantedWildcard(AccessType::SELECT, "mydb"));
    ASSERT_FALSE(root.isGrantedWildcard(AccessType::SELECT, "mydb2"));
    ASSERT_TRUE(root.isGrantedWildcard(AccessType::SELECT, "mydb", "anytable"));

    root = {};
    root.grantWildcard(AccessType::SELECT, "test");

    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "test"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "testing"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "test123"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "tes"));

    ASSERT_TRUE(root.isGrantedWildcard(AccessType::SELECT, "test"));
    ASSERT_TRUE(root.isGrantedWildcard(AccessType::SELECT, "testing"));
    ASSERT_FALSE(root.isGrantedWildcard(AccessType::SELECT, "te"));
}

TEST(AccessRights, MultipleAccessTypesPartialRevoke)
{
    AccessRights root;
    root.grant(AccessType::SELECT);
    root.grant(AccessType::INSERT);
    root.revoke(AccessType::SELECT, "readonly");
    root.revoke(AccessType::INSERT, "readwrite");

    // SELECT
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "normal"));
    ASSERT_FALSE(root.isGranted(AccessType::SELECT, "readonly"));
    ASSERT_TRUE(root.isGranted(AccessType::SELECT, "readwrite"));

    // INSERT
    ASSERT_TRUE(root.isGranted(AccessType::INSERT, "normal"));
    ASSERT_TRUE(root.isGranted(AccessType::INSERT, "readonly"));
    ASSERT_FALSE(root.isGranted(AccessType::INSERT, "readwrite"));

    // Wildcard checks
    ASSERT_FALSE(root.isGrantedWildcard(AccessType::SELECT, "readonl"));
    ASSERT_TRUE(root.isGrantedWildcard(AccessType::INSERT, "readonl"));
}
