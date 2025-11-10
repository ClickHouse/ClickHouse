#include <config.h>

#if USE_XRAY

#include <gtest/gtest.h>
#include <string>
#include <Interpreters/InstrumentationManager.h>

using namespace DB;

class InstrumentationManagerTest : public ::testing::Test
{
protected:
    static std::string removeTemplateArgs(std::string_view input)
    {
        return std::string(InstrumentationManager::removeTemplateArgs(input));
    }

    static std::string extractNearestNamespaceAndFunction(std::string_view signature)
    {
        return InstrumentationManager::extractNearestNamespaceAndFunction(signature);
    }
};

// -------- removeTemplateArgs --------

TEST_F(InstrumentationManagerTest, RemoveTemplateArgsNoTemplates)
{
    EXPECT_EQ(removeTemplateArgs("foo"), "foo");
    EXPECT_EQ(removeTemplateArgs("std::move"), "std::move");
}

TEST_F(InstrumentationManagerTest, RemoveTemplateArgsSimpleTemplate)
{
    EXPECT_EQ(removeTemplateArgs("vector<int>"), "vector");
    EXPECT_EQ(removeTemplateArgs("std::map<int,double>"), "std::map");
}

TEST_F(InstrumentationManagerTest, RemoveTemplateArgsNestedTemplate)
{
    EXPECT_EQ(removeTemplateArgs("std::map<int, std::vector<std::string>>"), "std::map");
    EXPECT_EQ(removeTemplateArgs("Outer<Inner<T>>"), "Outer");
}

TEST_F(InstrumentationManagerTest, RemoveTemplateArgsMultipleColons)
{
    EXPECT_EQ(removeTemplateArgs("std::chrono::duration<int>"), "std::chrono::duration");
    EXPECT_EQ(removeTemplateArgs("ns1::ns2::Foo<Bar>"), "ns1::ns2::Foo");
}

TEST_F(InstrumentationManagerTest, RemoveTemplateArgsEdgeCases)
{
    EXPECT_EQ(removeTemplateArgs(""), "");
    EXPECT_EQ(removeTemplateArgs("foo<"), "foo");
    EXPECT_EQ(removeTemplateArgs("foo>"), "foo>");
    EXPECT_EQ(removeTemplateArgs("foo<int>>"), "foo");
}

// -------- extractNearestNamespaceAndFunction --------

TEST_F(InstrumentationManagerTest, ExtractFunctionNoNamespace)
{
    EXPECT_EQ(extractNearestNamespaceAndFunction("int foo(int)"), "foo");
    EXPECT_EQ(extractNearestNamespaceAndFunction("void bar()"), "bar");
}

TEST_F(InstrumentationManagerTest, ExtractWithNamespace)
{
    EXPECT_EQ(extractNearestNamespaceAndFunction("void ns::func(int)"), "ns::func");
    EXPECT_EQ(extractNearestNamespaceAndFunction("int ns1::ns2::compute(double)"), "ns2::compute");
}

TEST_F(InstrumentationManagerTest, ExtractWithClassMethod)
{
    EXPECT_EQ(extractNearestNamespaceAndFunction("void MyClass::method(int)"), "MyClass::method");
    EXPECT_EQ(extractNearestNamespaceAndFunction("int Outer::Inner::doWork(std::string)"), "Inner::doWork");
}

TEST_F(InstrumentationManagerTest, ExtractWithTemplates)
{
    EXPECT_EQ(extractNearestNamespaceAndFunction("std::vector<int>::iterator Foo::bar(std::map<int,int>)"), "Foo::bar");
    EXPECT_EQ(extractNearestNamespaceAndFunction("auto ns::TemplateClass<T>::method(U)"), "TemplateClass::method");
}

TEST_F(InstrumentationManagerTest, ExtractEdgeCases)
{
    EXPECT_EQ(extractNearestNamespaceAndFunction("func"), ""); // no parentheses
    EXPECT_EQ(extractNearestNamespaceAndFunction("void strange name(int)"), "name"); // with space before name
    EXPECT_EQ(extractNearestNamespaceAndFunction("void Foo::operator()(int)"), "Foo::operator"); // operator handling
}

#endif
