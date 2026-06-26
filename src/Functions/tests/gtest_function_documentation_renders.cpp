#include <gtest/gtest.h>

#include <Common/tests/gtest_global_register.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Common/FunctionDocumentation.h>
#include <Functions/FunctionFactory.h>

#include <string>
#include <vector>

using namespace DB;

namespace
{

/// Renders the documentation accessors that go through the type -> documentation-link mapping. These are exactly
/// the calls `system.functions` / `system.documentation` make per row; they must never throw, because in debug and
/// sanitizer builds a LOGICAL_ERROR aborts the whole process at exception construction (before any try/catch),
/// turning one function's bad documentation into a server crash for everyone querying those tables.
void renderDocumentationAccessors(const FunctionDocumentation & doc)
{
    doc.argumentsAsString();
    doc.parametersAsString();
    doc.returnedValueAsString();
}

}

TEST(FunctionDocumentation, RendersUnknownTypeWithoutThrowing)
{
    FunctionDocumentation doc;
    doc.syntax = "fooBar(x)";
    doc.arguments = {{"x", "An argument.", {"ThisTypeDoesNotExist"}}};
    doc.returned_value = {"A result.", {"AnotherUnknownType"}};

    /// The previous implementation threw LOGICAL_ERROR here (and aborted in debug/sanitizer builds).
    EXPECT_NO_THROW(renderDocumentationAccessors(doc));

    /// The unknown type is still shown (as inline code), just without a documentation link.
    const String arguments = doc.argumentsAsString();
    EXPECT_NE(arguments.find("ThisTypeDoesNotExist"), String::npos);
    EXPECT_EQ(arguments.find("]("), String::npos) << "Unknown type must not produce a link: " << arguments;

    const String returned = doc.returnedValueAsString();
    EXPECT_NE(returned.find("AnotherUnknownType"), String::npos);
}

TEST(FunctionDocumentation, RendersKnownTypeWithLink)
{
    FunctionDocumentation doc;
    doc.syntax = "fooBar(x)";
    doc.returned_value = {"A result.", {"String"}};

    /// Known types keep their documentation link (output format unchanged by the graceful-degradation fix).
    const String returned = doc.returnedValueAsString();
    EXPECT_NE(returned.find("[`String`](/sql-reference/data-types/string)"), String::npos) << returned;
}

TEST(FunctionDocumentation, TypeLinkLookupMatchesKnownAndUnknownTypes)
{
    EXPECT_TRUE(documentationTypeHasLink("String"));
    EXPECT_TRUE(documentationTypeHasLink("Array(UInt8)"));
    EXPECT_TRUE(documentationTypeHasLink("const String"));
    EXPECT_TRUE(documentationTypeHasLink("(U)Int*"));
    EXPECT_FALSE(documentationTypeHasLink("ThisTypeDoesNotExist"));
    EXPECT_FALSE(documentationTypeHasLink("string")); /// lower-case is not a known type literal
}

/// Tripwire: every registered function/aggregate function must declare only documentation types that have a link,
/// and its documentation must render without throwing. This is the safe replacement for the old behavior, where a
/// bad type was only "caught" by crashing the server when someone queried `system.functions`.
template <typename Factory>
void checkAllDocsRenderable(const Factory & factory)
{
    for (const auto & name : factory.getAllRegisteredNames())
    {
        if (factory.isAlias(name))
            continue;

        const auto documentation = factory.getDocumentation(name);

        EXPECT_NO_THROW(renderDocumentationAccessors(documentation)) << "Function: " << name;

        auto check_types = [&](const std::vector<FunctionDocumentation::Argument> & args)
        {
            for (const auto & arg : args)
                for (const auto & type : arg.types)
                    EXPECT_TRUE(documentationTypeHasLink(type))
                        << "Function '" << name << "' documents argument '" << arg.name
                        << "' with type '" << type << "' which has no documentation link. "
                        << "Add a mapping in FunctionDocumentation.cpp documentationTypeLink() or fix the type.";
        };
        check_types(documentation.arguments);
        check_types(documentation.parameters);

        for (const auto & type : documentation.returned_value.types)
            EXPECT_TRUE(documentationTypeHasLink(type))
                << "Function '" << name << "' documents a returned value with type '" << type
                << "' which has no documentation link. "
                << "Add a mapping in FunctionDocumentation.cpp documentationTypeLink() or fix the type.";
    }
}

TEST(FunctionDocumentation, AllRegisteredFunctionsUseKnownDocumentationTypes)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    checkAllDocsRenderable(FunctionFactory::instance());
    checkAllDocsRenderable(AggregateFunctionFactory::instance());
}
