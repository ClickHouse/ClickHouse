#include <gtest/gtest.h>

#include <string>

#include <Poco/AutoPtr.h>
#include <Poco/DOM/DOMParser.h>
#include <Poco/DOM/Document.h>
#include <Poco/Exception.h>

/// A C++ exception thrown from a DOM handler mid-parse (here: `Poco::XML::NamePool` overflow on a
/// document with more unique element names than the pool holds) unwinds out of `XML_Parse`,
/// skipping expat's internal handler-call-depth bookkeeping. Without the `ParserEngine` rebalance,
/// `XML_ParserFree` then silently refuses to free the parser — it believes a handler is still
/// running — and the whole parser (DTD, pools, hash tables) leaks; LeakSanitizer flags this test
/// binary. The parser must also stay reusable for a subsequent parse.
TEST(DOMParserHandlerException, ParserIsFreedAndReusableAfterHandlerThrows)
{
    std::string xml = "<root>";
    for (size_t i = 0; i < 2000; ++i)
    {
        const std::string tag = "tag" + std::to_string(i);
        xml += "<" + tag + "></" + tag + ">";
    }
    xml += "</root>";

    Poco::XML::DOMParser parser(/*namePoolSize=*/251);

    EXPECT_THROW(
        {
            Poco::AutoPtr<Poco::XML::Document> doc = parser.parseString(xml);
        },
        Poco::Exception);

    /// The same parser must still work after the aborted parse. Reuse element names the first
    /// parse already interned: the `NamePool` is a persistent member of the parser and is still
    /// full, so a document with new names would overflow it again — a separate Poco property this
    /// test does not pin.
    Poco::AutoPtr<Poco::XML::Document> doc = parser.parseString("<root><tag0>value</tag0></root>");
    ASSERT_TRUE(doc);
    ASSERT_TRUE(doc->documentElement());
    EXPECT_EQ(doc->documentElement()->nodeName(), "root");
}
