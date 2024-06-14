#include <gtest/gtest.h>
#include <Access/AccessRights.h>
#include <Access/AccessRights.cpp>
#include <IO/WriteBufferFromString.h>

#include <list>

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
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "default", "t", "col1"), false);
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "default", "t", "test"), true);
    ASSERT_EQ(root.toString(), "GRANT SELECT ON default.t, REVOKE SELECT(col*) ON default.t");

    root = {};
    root.grantWildcard(AccessType::ALTER_UPDATE, "prod");
    root.grant(AccessType::ALTER_UPDATE, "prod", "users");

    ASSERT_EQ(root.isGranted(AccessType::ALTER_UPDATE, "prod"), true);
    ASSERT_EQ(root.isGranted(AccessType::ALTER_UPDATE, "prod", "users"), true);
    ASSERT_EQ(root.isGranted(AccessType::ALTER_UPDATE, "prod", "orders"), true);
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "prod"), false);

    root.revoke(AccessType::ALTER_UPDATE, "prod", "users");
    ASSERT_EQ(root.isGranted(AccessType::ALTER_UPDATE, "prod", "users"), false);
    ASSERT_EQ(root.isGranted(AccessType::ALTER_UPDATE, "prod", "orders"), true);

    root.grantWildcard(AccessType::ALTER_DELETE, "prod");
    root.grant(AccessType::ALTER_DELETE, "prod", "archive");
    ASSERT_EQ(root.isGranted(AccessType::ALTER_DELETE, "prod", "archive"), true);
    ASSERT_EQ(root.isGranted(AccessType::ALTER_DELETE, "prod", "current"), true);

    root.revokeWildcard(AccessType::ALTER_DELETE, "prod");
    ASSERT_EQ(root.isGranted(AccessType::ALTER_DELETE, "prod", "archive"), false);
    ASSERT_EQ(root.isGranted(AccessType::ALTER_DELETE, "prod", "current"), false);

    ASSERT_EQ(root.toString(), "GRANT ALTER UPDATE ON prod*.*, REVOKE ALTER UPDATE ON prod.users");

    root.grantWildcard(AccessType::SELECT, "test");
    root.grantWildcard(AccessType::INSERT, "test");
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "test"), true);
    ASSERT_EQ(root.isGranted(AccessType::INSERT, "test"), true);
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "testdata"), true);
    ASSERT_EQ(root.isGranted(AccessType::INSERT, "testdata"), true);

    root.revokeWildcard(AccessType::SELECT, "test");
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "test"), false);
    ASSERT_EQ(root.isGranted(AccessType::INSERT, "test"), true);
    ASSERT_EQ(root.isGranted(AccessType::SELECT, "testdata"), false);
    ASSERT_EQ(root.isGranted(AccessType::INSERT, "testdata"), true);

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
}

TEST(AccessRights, Union)
{
    AccessRights lhs, rhs;
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
    lhs.grant(AccessType::INSERT);
    rhs.grant(AccessType::ALL, "db1");
    lhs.makeUnion(rhs);
    ASSERT_EQ(lhs.toString(),
              "GRANT INSERT ON *.*, "
              "GRANT SHOW, SELECT, ALTER, CREATE DATABASE, CREATE TABLE, CREATE VIEW, "
              "CREATE DICTIONARY, DROP DATABASE, DROP TABLE, DROP VIEW, DROP DICTIONARY, UNDROP TABLE, "
              "TRUNCATE, OPTIMIZE, BACKUP, CREATE ROW POLICY, ALTER ROW POLICY, DROP ROW POLICY, "
              "SHOW ROW POLICIES, SYSTEM MERGES, SYSTEM TTL MERGES, SYSTEM FETCHES, "
              "SYSTEM MOVES, SYSTEM PULLING REPLICATION LOG, SYSTEM CLEANUP, SYSTEM VIEWS, SYSTEM SENDS, SYSTEM REPLICATION QUEUES, SYSTEM VIRTUAL PARTS UPDATE, "
              "SYSTEM DROP REPLICA, SYSTEM SYNC REPLICA, SYSTEM RESTART REPLICA, "
              "SYSTEM RESTORE REPLICA, SYSTEM WAIT LOADING PARTS, SYSTEM SYNC DATABASE REPLICA, SYSTEM FLUSH DISTRIBUTED, "
              "SYSTEM UNLOAD PRIMARY KEY, dictGet ON db1.*, GRANT TABLE ENGINE ON db1, "
              "GRANT SET DEFINER ON db1, GRANT NAMED COLLECTION ADMIN ON db1");

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
    ASSERT_EQ(lhs.toString(), "GRANT SELECT ON db1.tb1*, GRANT SELECT ON db1.tb1* WITH GRANT OPTION, REVOKE GRANT OPTION SELECT(col*) ON db1.tb1");

    lhs = {};
    rhs = {};
    lhs.grant(AccessType::SELECT, "db1", "tb1", Strings{"col1", "col2", "test"});
    rhs.grantWildcardWithGrantOption(AccessType::SELECT, "db1", "tb1", "col");
    lhs.makeUnion(rhs);
    ASSERT_EQ(lhs.toString(), "GRANT SELECT(col*) ON db1.tb1 WITH GRANT OPTION, GRANT SELECT(test) ON db1.tb1");
}


TEST(AccessRights, Intersection)
{
    AccessRights lhs, rhs;
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
