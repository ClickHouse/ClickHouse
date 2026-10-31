#pragma once

#include <base/types.h>

#include <string_view>
#include <vector>


namespace DB
{

class ReadBuffer;

/** A minimal non-validating streaming XML parser, sufficient for the Office Open XML parts of `XLSX` files.
  *
  * Element and attribute names are reported without the namespace prefix (`x:row` is reported as `row`),
  * because some producers of `XLSX` files use a prefix for the SpreadsheetML namespace and some do not.
  *
  * Supports elements, attributes, character data, the predefined entities and numeric character references,
  * and CDATA sections. Skips the XML declaration, processing instructions, comments and the document type declaration.
  * A self-closing element `<a/>` is reported as a `StartElement` followed by an `EndElement`.
  */
class XMLPullParser
{
public:
    enum class Event : uint8_t
    {
        StartElement,
        EndElement,
        Text,
        End,
    };

    explicit XMLPullParser(ReadBuffer & in_) : in(in_) {}

    Event next();

    /// The local name of the element, for `StartElement` and `EndElement`.
    const String & name() const { return element_name; }

    /// The value of an attribute with the specified local name, for `StartElement`. Returns `nullptr` if there is no such attribute.
    const String * getAttribute(std::string_view local_name) const;

    /// The character data, for `Text`. Entities are decoded.
    const String & text() const { return text_data; }

    /// The number of currently open elements.
    size_t depth() const { return current_depth; }

    /// Skips the content of the element that has just been started, up to and including its end tag.
    void skipElement();

    /// Appends the character data of the element that has just been started (including the character data of nested elements)
    /// to `out`, and consumes everything up to and including its end tag.
    void readElementText(String & out);

private:
    void readName(String & out);
    void readAttributeValue(char quote, String & out);
    void readText();
    void readCDATA();
    void decodeEntity(String & out);
    void skipUntil(std::string_view terminator);
    void skipDoctype();
    void skipWhitespace();
    char peekChar();
    char getChar();
    void expectChar(char c);

    [[noreturn]] void throwUnexpected(std::string_view what) const;

    ReadBuffer & in;

    String element_name;
    /// Attributes are reused between elements to avoid allocations; only the first `num_attributes` are valid.
    std::vector<std::pair<String, String>> attributes;
    size_t num_attributes = 0;
    String text_data;

    bool pending_end_element = false;
    size_t current_depth = 0;
};

}
