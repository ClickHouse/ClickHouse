#include <gtest/gtest.h>
#include <Databases/DataLake/RestCatalog.h>


TEST(TestRestCatalogAllowedNamespaces, TestAllAllowed)
{
    DataLake::RestCatalog::AllowedNamespaces namespaces("*");
    EXPECT_TRUE(namespaces.isNamespaceAllowed("foo", /*nested*/ true));
    EXPECT_TRUE(namespaces.isNamespaceAllowed("foo", /*nested*/ false));
    EXPECT_TRUE(namespaces.isNamespaceAllowed("foo.bar", /*nested*/ true));
    EXPECT_TRUE(namespaces.isNamespaceAllowed("foo.bar", /*nested*/ false));
}

TEST(TestRestCatalogAllowedNamespaces, TestAllBlocked)
{
    DataLake::RestCatalog::AllowedNamespaces namespaces("");
    EXPECT_FALSE(namespaces.isNamespaceAllowed("foo", /*nested*/ true));
    EXPECT_FALSE(namespaces.isNamespaceAllowed("foo", /*nested*/ false));
    EXPECT_FALSE(namespaces.isNamespaceAllowed("foo.bar", /*nested*/ true));
    EXPECT_FALSE(namespaces.isNamespaceAllowed("foo.bar", /*nested*/ false));
}

TEST(TestRestCatalogAllowedNamespaces, TestTableInNamespaceAllowed)
{
    DataLake::RestCatalog::AllowedNamespaces namespaces("foo");
    EXPECT_FALSE(namespaces.isNamespaceAllowed("foo", /*nested*/ true));
    EXPECT_TRUE(namespaces.isNamespaceAllowed("foo", /*nested*/ false));
    EXPECT_FALSE(namespaces.isNamespaceAllowed("foo.bar", /*nested*/ true));
    EXPECT_FALSE(namespaces.isNamespaceAllowed("foo.bar", /*nested*/ false));
    EXPECT_FALSE(namespaces.isNamespaceAllowed("biz", /*nested*/ true));
    EXPECT_FALSE(namespaces.isNamespaceAllowed("biz", /*nested*/ false));
}

TEST(TestRestCatalogAllowedNamespaces, TestSpecificNestedNamespaceAllowed)
{
    DataLake::RestCatalog::AllowedNamespaces namespaces("foo.bar");
    EXPECT_TRUE(namespaces.isNamespaceAllowed("foo", /*nested*/ true));
    EXPECT_FALSE(namespaces.isNamespaceAllowed("foo", /*nested*/ false));
    EXPECT_FALSE(namespaces.isNamespaceAllowed("foo.bar", /*nested*/ true));
    EXPECT_TRUE(namespaces.isNamespaceAllowed("foo.bar", /*nested*/ false));
    EXPECT_FALSE(namespaces.isNamespaceAllowed("bar", /*nested*/ true));
    EXPECT_FALSE(namespaces.isNamespaceAllowed("bar", /*nested*/ false));
    EXPECT_FALSE(namespaces.isNamespaceAllowed("biz", /*nested*/ true));
    EXPECT_FALSE(namespaces.isNamespaceAllowed("biz", /*nested*/ false));
    EXPECT_FALSE(namespaces.isNamespaceAllowed("foo.biz", /*nested*/ true));
    EXPECT_FALSE(namespaces.isNamespaceAllowed("foo.biz", /*nested*/ false));
}

TEST(TestRestCatalogAllowedNamespaces, TestNestedNamespacesAllowed)
{
    DataLake::RestCatalog::AllowedNamespaces namespaces("foo.*");
    EXPECT_TRUE(namespaces.isNamespaceAllowed("foo", /*nested*/ true));
    EXPECT_FALSE(namespaces.isNamespaceAllowed("foo", /*nested*/ false));
    EXPECT_TRUE(namespaces.isNamespaceAllowed("foo.bar", /*nested*/ true));
    EXPECT_TRUE(namespaces.isNamespaceAllowed("foo.bar", /*nested*/ false));
    EXPECT_FALSE(namespaces.isNamespaceAllowed("biz", /*nested*/ true));
    EXPECT_FALSE(namespaces.isNamespaceAllowed("biz", /*nested*/ false));
}

TEST(TestRestCatalogAllowedNamespaces, TestTablesAndNestedNamespacesAllowed)
{
    DataLake::RestCatalog::AllowedNamespaces namespaces("foo,foo.*");
    EXPECT_TRUE(namespaces.isNamespaceAllowed("foo", /*nested*/ true));
    EXPECT_TRUE(namespaces.isNamespaceAllowed("foo", /*nested*/ false));
    EXPECT_TRUE(namespaces.isNamespaceAllowed("foo.bar", /*nested*/ true));
    EXPECT_TRUE(namespaces.isNamespaceAllowed("foo.bar", /*nested*/ false));
    EXPECT_FALSE(namespaces.isNamespaceAllowed("biz", /*nested*/ true));
    EXPECT_FALSE(namespaces.isNamespaceAllowed("biz", /*nested*/ false));
}
