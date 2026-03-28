#include <gtest/gtest.h>
#include <Access/AccessRights.h>
#include <IO/WriteBufferFromString.h>

using namespace DB;

namespace {

/// Partial copy of ContextAccess::addImplicitAccessRights
AccessFlags addImplicitPrivileges(AccessFlags flags,
    const AccessFlags & /*min_flags_with_children*/,
    const AccessFlags & max_flags_with_children,
    const size_t level,
    bool /*grant_option*/,
    bool leaf_or_wildcard)
{
    AccessFlags res = flags;

    static const AccessFlags show_columns = AccessType::SHOW_COLUMNS;
    static const AccessFlags show_tables = AccessType::SHOW_TABLES;

    if (res & AccessFlags::allColumnFlags())
        res |= show_columns;
    if ((res & AccessFlags::allTableFlags())
        || (level <= 2 && (res & show_columns))
        || (leaf_or_wildcard && level == 2 && (max_flags_with_children & show_columns)))
    {
        res |= show_tables;
    }

    return res;
}

std::string dumpAccessRights(AccessRights root, const std::string & prefix)
{
    WriteBufferFromOwnString out;
    root.dumpTree(out);
    return prefix + ":\n" + out.str();
}

}

TEST(AccessRightsExplicitPrivileges, RevokeExplicitPrivilegeModify)
{
    AccessRights root;
    root.grant(AccessType::SELECT);
    root.grant(AccessType::SHOW_TABLES);
    root.grant(AccessType::SHOW_COLUMNS);
    SCOPED_TRACE(dumpAccessRights(root, "granted"));
    root.revoke(AccessType::SHOW_TABLES, "default", "foo");
    SCOPED_TRACE(dumpAccessRights(root, "revoke SHOW_TABLES"));

    ASSERT_FALSE(root.isGranted(AccessType::SHOW_TABLES, "default", "foo"));
    ASSERT_TRUE(root.isGranted(AccessType::SHOW_TABLES, "default", "foo_2"));
}

TEST(AccessRightsImplicitPrivileges, RevokeExplicitPrivilegeModify)
{
    AccessRights root;
    root.grant(AccessType::SELECT);
    SCOPED_TRACE(dumpAccessRights(root, "grant SELECT"));
    root.revoke(AccessType::SELECT, "default", "foo");
    SCOPED_TRACE(dumpAccessRights(root, "revoke SHOW_TABLES"));
    root.modifyFlags(addImplicitPrivileges);
    SCOPED_TRACE(dumpAccessRights(root, "modify"));

    ASSERT_FALSE(root.isGranted(AccessType::SHOW_TABLES, "default", "foo"));
    ASSERT_TRUE(root.isGranted(AccessType::SHOW_TABLES, "default", "foo_2"));
}

TEST(AccessRightsImplicitPrivileges, Table)
{
    AccessRights root;
    root.grant(AccessType::SELECT, "default", "foo");
    SCOPED_TRACE(dumpAccessRights(root, "grant"));
    root.modifyFlags(addImplicitPrivileges);
    SCOPED_TRACE(dumpAccessRights(root, "modify"));

    ASSERT_TRUE(root.isGranted(AccessType::SHOW_TABLES, "default", "foo"));
    ASSERT_FALSE(root.isGranted(AccessType::SHOW_TABLES, "default", "foo_2"));
}

TEST(AccessRightsExplicitPrivileges, Table)
{
    AccessRights root;
    root.grant(AccessType::SELECT, "default", "foo");
    root.grant(AccessType::SHOW_TABLES, "default", "foo");
    root.grant(AccessType::SHOW_COLUMNS, "default", "foo");
    SCOPED_TRACE(dumpAccessRights(root, "grants"));

    ASSERT_TRUE(root.isGranted(AccessType::SHOW_TABLES, "default", "foo"));
    ASSERT_FALSE(root.isGranted(AccessType::SHOW_TABLES, "default", "foo_2"));
}

TEST(AccessRightsImplicitPrivileges, Column)
{
    AccessRights root;
    root.grant(AccessType::SELECT, "default", "foo", "x");
    SCOPED_TRACE(dumpAccessRights(root, "grants"));
    root.modifyFlags(addImplicitPrivileges);
    SCOPED_TRACE(dumpAccessRights(root, "modify"));

    ASSERT_TRUE(root.isGranted(AccessType::SHOW_TABLES, "default", "foo"));
    ASSERT_TRUE(root.isGranted(AccessType::SHOW_COLUMNS, "default", "foo", "x"));
    ASSERT_FALSE(root.isGranted(AccessType::SHOW_COLUMNS, "default", "foo", "x_2"));
}

TEST(AccessRightsImplicitPrivileges, TableWildcard)
{
    AccessRights root;
    root.grantWildcard(AccessType::SELECT, "default", "foo");
    root.modifyFlags(addImplicitPrivileges);
    SCOPED_TRACE(dumpAccessRights(root, "grants"));

    ASSERT_TRUE(root.isGranted(AccessType::SHOW_TABLES, "default", "foo"));
    ASSERT_TRUE(root.isGranted(AccessType::SHOW_TABLES, "default", "foo_2"));

    root.revokeWildcard(AccessType::SHOW_TABLES, "default", "foo_2");
    ASSERT_TRUE(root.isGranted(AccessType::SHOW_TABLES, "default", "foo"));
    ASSERT_FALSE(root.isGranted(AccessType::SHOW_TABLES, "default", "foo_2"));
    ASSERT_TRUE(root.isGranted(AccessType::SHOW_TABLES, "default", "foo_3"));
}
