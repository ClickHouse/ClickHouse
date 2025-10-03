#include <gtest/gtest.h>
#include <string>
#include <Interpreters/XRayInstrumentationManager.h>


using namespace DB;

class XRayInstrumentationManagerTest : public ::testing::Test
{
protected:
    static std::string removeTemplateArgs(std::string_view input)
    {
        return std::string(XRayInstrumentationManager::removeTemplateArgs(input));
    }

    static std::string extractNearestNamespaceAndFunction(std::string_view signature)
    {
        return XRayInstrumentationManager::extractNearestNamespaceAndFunction(signature);
    }
};

// -------- removeTemplateArgs --------

TEST_F(XRayInstrumentationManagerTest, RemoveTemplateArgsNoTemplates)
{
    EXPECT_EQ(removeTemplateArgs("foo"), "foo");
    EXPECT_EQ(removeTemplateArgs("std::move"), "std::move");
}

TEST_F(XRayInstrumentationManagerTest, RemoveTemplateArgsSimpleTemplate)
{
    EXPECT_EQ(removeTemplateArgs("vector<int>"), "vector");
    EXPECT_EQ(removeTemplateArgs("std::map<int,double>"), "std::map");
}

TEST_F(XRayInstrumentationManagerTest, RemoveTemplateArgsNestedTemplate)
{
    EXPECT_EQ(removeTemplateArgs("std::map<int, std::vector<std::string>>"), "std::map");
    EXPECT_EQ(removeTemplateArgs("Outer<Inner<T>>"), "Outer");
}

TEST_F(XRayInstrumentationManagerTest, RemoveTemplateArgsMultipleColons)
{
    EXPECT_EQ(removeTemplateArgs("std::chrono::duration<int>"), "std::chrono::duration");
    EXPECT_EQ(removeTemplateArgs("ns1::ns2::Foo<Bar>"), "ns1::ns2::Foo");
}

TEST_F(XRayInstrumentationManagerTest, RemoveTemplateArgsEdgeCases)
{
    EXPECT_EQ(removeTemplateArgs(""), "");
    EXPECT_EQ(removeTemplateArgs("foo<"), "foo");
    EXPECT_EQ(removeTemplateArgs("foo>"), "foo>");
    EXPECT_EQ(removeTemplateArgs("foo<int>>"), "foo");
}

// -------- extractNearestNamespaceAndFunction --------

TEST_F(XRayInstrumentationManagerTest, ExtractFunctionNoNamespace)
{
    EXPECT_EQ(extractNearestNamespaceAndFunction("int foo(int)"), "foo");
    EXPECT_EQ(extractNearestNamespaceAndFunction("void bar()"), "bar");
}

TEST_F(XRayInstrumentationManagerTest, ExtractWithNamespace)
{
    EXPECT_EQ(extractNearestNamespaceAndFunction("void ns::func(int)"), "ns::func");
    EXPECT_EQ(extractNearestNamespaceAndFunction("int ns1::ns2::compute(double)"), "ns2::compute");
}

TEST_F(XRayInstrumentationManagerTest, ExtractWithClassMethod)
{
    EXPECT_EQ(extractNearestNamespaceAndFunction("void MyClass::method(int)"), "MyClass::method");
    EXPECT_EQ(extractNearestNamespaceAndFunction("int Outer::Inner::doWork(std::string)"), "Inner::doWork");
}

TEST_F(XRayInstrumentationManagerTest, ExtractWithTemplates)
{
    EXPECT_EQ(extractNearestNamespaceAndFunction("std::vector<int>::iterator Foo::bar(std::map<int,int>)"), "Foo::bar");
    EXPECT_EQ(extractNearestNamespaceAndFunction("auto ns::TemplateClass<T>::method(U)"), "TemplateClass::method");
}

TEST_F(XRayInstrumentationManagerTest, ExtractEdgeCases)
{
    EXPECT_EQ(extractNearestNamespaceAndFunction("func"), ""); // no parentheses
    EXPECT_EQ(extractNearestNamespaceAndFunction("void strange name(int)"), "name"); // with space before name
    EXPECT_EQ(extractNearestNamespaceAndFunction("void Foo::operator()(int)"), "Foo::operator"); // operator handling
}
