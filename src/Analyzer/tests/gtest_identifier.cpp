#include <gtest/gtest.h>

#include <Analyzer/Identifier.h>

using namespace DB;

TEST(Identifier, IdentifierBasics)
{
    {
        Identifier identifier;

        ASSERT_TRUE(identifier.empty());
        ASSERT_TRUE(identifier.isEmpty());
        ASSERT_EQ(identifier.getPartsSize(), 0);
        ASSERT_FALSE(identifier.isShort());
        ASSERT_FALSE(identifier.isCompound());
        ASSERT_FALSE(identifier.startsWith("test"));
        ASSERT_FALSE(identifier.endsWith("test"));
        ASSERT_EQ(identifier.begin(), identifier.end());
        ASSERT_EQ(identifier.getFullName(), "");
    }
    {
        Identifier identifier("value");

        ASSERT_FALSE(identifier.empty());
        ASSERT_FALSE(identifier.isEmpty());
        ASSERT_EQ(identifier.getPartsSize(), 1);
        ASSERT_TRUE(identifier.isShort());
        ASSERT_FALSE(identifier.isCompound());
        ASSERT_EQ(identifier.front(), "value");
        ASSERT_EQ(identifier.back(), "value");
        ASSERT_FALSE(identifier.startsWith("test"));
        ASSERT_FALSE(identifier.endsWith("test"));
        ASSERT_TRUE(identifier.startsWith("value"));
        ASSERT_TRUE(identifier.endsWith("value"));
        ASSERT_EQ(identifier[0], "value");
        ASSERT_NE(identifier.begin(), identifier.end());
        ASSERT_EQ(identifier.getFullName(), "value");
    }
    {
        Identifier identifier("value1.value2");

        ASSERT_FALSE(identifier.empty());
        ASSERT_FALSE(identifier.isEmpty());
        ASSERT_EQ(identifier.getPartsSize(), 2);
        ASSERT_FALSE(identifier.isShort());
        ASSERT_TRUE(identifier.isCompound());
        ASSERT_EQ(identifier.front(), "value1");
        ASSERT_EQ(identifier.back(), "value2");
        ASSERT_FALSE(identifier.startsWith("test"));
        ASSERT_FALSE(identifier.endsWith("test"));
        ASSERT_TRUE(identifier.startsWith("value1"));
        ASSERT_TRUE(identifier.endsWith("value2"));
        ASSERT_EQ(identifier[0], "value1");
        ASSERT_EQ(identifier[1], "value2");
        ASSERT_NE(identifier.begin(), identifier.end());
        ASSERT_EQ(identifier.getFullName(), "value1.value2");
    }
    {
        Identifier identifier1("value1.value2");
        Identifier identifier2("value1.value2");

        ASSERT_EQ(identifier1, identifier2);
    }
    {
        Identifier identifier1("value1.value2");
        Identifier identifier2("value1.value3");

        ASSERT_NE(identifier1, identifier2);
    }
}

TEST(Identifier, IdentifierPushParts)
{
    {
        Identifier identifier;

        identifier.push_back("value1");
        ASSERT_EQ(identifier.getFullName(), "value1");
        identifier.push_back("value2");
        ASSERT_EQ(identifier.getFullName(), "value1.value2");
        identifier.push_back("value3");
        ASSERT_EQ(identifier.getFullName(), "value1.value2.value3");
        ASSERT_FALSE(identifier.isEmpty());
    }
}


TEST(Identifier, IdentifierPopParts)
{
    {
        Identifier identifier("value1.value2.value3");

        ASSERT_EQ(identifier.getFullName(), "value1.value2.value3");
        identifier.popLast();
        ASSERT_EQ(identifier.getFullName(), "value1.value2");
        identifier.popLast();
        ASSERT_EQ(identifier.getFullName(), "value1");
        identifier.popLast();
        ASSERT_EQ(identifier.getFullName(), "");
        ASSERT_TRUE(identifier.isEmpty());
    }
    {
        Identifier identifier("value1.value2.value3");

        ASSERT_EQ(identifier.getFullName(), "value1.value2.value3");
        identifier.popFirst();
        ASSERT_EQ(identifier.getFullName(), "value2.value3");
        identifier.popFirst();
        ASSERT_EQ(identifier.getFullName(), "value3");
        identifier.popFirst();
        ASSERT_EQ(identifier.getFullName(), "");
        ASSERT_TRUE(identifier.isEmpty());
    }
    {
        Identifier identifier("value1.value2.value3");

        ASSERT_EQ(identifier.getFullName(), "value1.value2.value3");
        identifier.popLast();
        ASSERT_EQ(identifier.getFullName(), "value1.value2");
        identifier.popFirst();
        ASSERT_EQ(identifier.getFullName(), "value2");
        identifier.popLast();
        ASSERT_EQ(identifier.getFullName(), "");
        ASSERT_TRUE(identifier.isEmpty());
    }
}

TEST(Identifier, IdentifierViewBasics)
{
    {
        Identifier identifier;
        IdentifierView identifier_view(identifier);

        ASSERT_TRUE(identifier_view.empty());
        ASSERT_TRUE(identifier_view.isEmpty());
        ASSERT_EQ(identifier_view.getPartsSize(), 0);
        ASSERT_FALSE(identifier_view.isShort());
        ASSERT_FALSE(identifier_view.isCompound());
        ASSERT_FALSE(identifier_view.startsWith("test"));
        ASSERT_FALSE(identifier_view.endsWith("test"));
        ASSERT_EQ(identifier_view.begin(), identifier_view.end());
        ASSERT_EQ(identifier_view.getFullName(), "");
    }
    {
        Identifier identifier("value");
        IdentifierView identifier_view(identifier);

        ASSERT_FALSE(identifier_view.empty());
        ASSERT_FALSE(identifier_view.isEmpty());
        ASSERT_EQ(identifier_view.getPartsSize(), 1);
        ASSERT_TRUE(identifier_view.isShort());
        ASSERT_FALSE(identifier_view.isCompound());
        ASSERT_EQ(identifier_view.front(), "value");
        ASSERT_EQ(identifier_view.back(), "value");
        ASSERT_FALSE(identifier_view.startsWith("test"));
        ASSERT_FALSE(identifier_view.endsWith("test"));
        ASSERT_TRUE(identifier_view.startsWith("value"));
        ASSERT_TRUE(identifier_view.endsWith("value"));
        ASSERT_EQ(identifier_view[0], "value");
        ASSERT_NE(identifier_view.begin(), identifier_view.end());
        ASSERT_EQ(identifier_view.getFullName(), "value");
    }
    {
        Identifier identifier("value1.value2");
        IdentifierView identifier_view(identifier);

        ASSERT_FALSE(identifier_view.empty());
        ASSERT_FALSE(identifier_view.isEmpty());
        ASSERT_EQ(identifier_view.getPartsSize(), 2);
        ASSERT_FALSE(identifier_view.isShort());
        ASSERT_TRUE(identifier_view.isCompound());
        ASSERT_FALSE(identifier_view.startsWith("test"));
        ASSERT_FALSE(identifier_view.endsWith("test"));
        ASSERT_TRUE(identifier_view.startsWith("value1"));
        ASSERT_TRUE(identifier_view.endsWith("value2"));
        ASSERT_EQ(identifier_view[0], "value1");
        ASSERT_EQ(identifier_view[1], "value2");
        ASSERT_NE(identifier_view.begin(), identifier_view.end());
        ASSERT_EQ(identifier_view.getFullName(), "value1.value2");
    }
    {
        Identifier identifier1("value1.value2");
        IdentifierView identifier_view1(identifier1);

        Identifier identifier2("value1.value2");
        IdentifierView identifier_view2(identifier2);

        ASSERT_EQ(identifier_view1, identifier_view2);
    }
    {
        Identifier identifier1("value1.value2");
        IdentifierView identifier_view1(identifier1);

        Identifier identifier2("value1.value3");
        IdentifierView identifier_view2(identifier2);

        ASSERT_NE(identifier_view1, identifier_view2);
    }
}

TEST(Identifier, IdentifierViewPopParts)
{
    {
        Identifier identifier("value1.value2.value3");
        IdentifierView identifier_view(identifier);

        ASSERT_EQ(identifier_view.getFullName(), "value1.value2.value3");
        identifier_view.popLast();
        ASSERT_EQ(identifier_view.getFullName(), "value1.value2");
        identifier_view.popLast();
        ASSERT_EQ(identifier_view.getFullName(), "value1");
        identifier_view.popLast();
        ASSERT_EQ(identifier_view.getFullName(), "");
        ASSERT_TRUE(identifier_view.isEmpty());
    }
    {
        Identifier identifier("value1.value2.value3");
        IdentifierView identifier_view(identifier);

        ASSERT_EQ(identifier_view.getFullName(), "value1.value2.value3");
        identifier_view.popFirst();
        ASSERT_EQ(identifier_view.getFullName(), "value2.value3");
        identifier_view.popFirst();
        ASSERT_EQ(identifier_view.getFullName(), "value3");
        identifier_view.popFirst();
        ASSERT_EQ(identifier_view.getFullName(), "");
        ASSERT_TRUE(identifier_view.isEmpty());
    }
    {
        Identifier identifier("value1.value2.value3");
        IdentifierView identifier_view(identifier);

        ASSERT_EQ(identifier_view.getFullName(), "value1.value2.value3");
        identifier_view.popLast();
        ASSERT_EQ(identifier_view.getFullName(), "value1.value2");
        identifier_view.popFirst();
        ASSERT_EQ(identifier_view.getFullName(), "value2");
        identifier_view.popLast();
        ASSERT_EQ(identifier_view.getFullName(), "");
        ASSERT_TRUE(identifier_view.isEmpty());
    }
}
