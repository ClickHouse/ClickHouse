#include <gtest/gtest.h>
#include <Access/AccessRights.h>

using namespace DB;


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
              "CREATE DICTIONARY, DROP DATABASE, DROP TABLE, DROP VIEW, DROP DICTIONARY, "
              "TRUNCATE, OPTIMIZE, BACKUP, CREATE ROW POLICY, ALTER ROW POLICY, DROP ROW POLICY, "
              "SHOW ROW POLICIES, SYSTEM MERGES, SYSTEM TTL MERGES, SYSTEM FETCHES, "
              "SYSTEM MOVES, SYSTEM SENDS, SYSTEM REPLICATION QUEUES, "
              "SYSTEM DROP REPLICA, SYSTEM SYNC REPLICA, SYSTEM RESTART REPLICA, "
              "SYSTEM RESTORE REPLICA, SYSTEM SYNC DATABASE REPLICA, SYSTEM FLUSH DISTRIBUTED, dictGet ON db1.*");
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
}
