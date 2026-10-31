#include <Processors/Formats/Impl/XMLPullParser.h>

#include <IO/ReadBuffer.h>
#include <base/find_symbols.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{

void appendUTF8(UInt32 code_point, String & out)
{
    if (code_point < 0x80)
    {
        out += static_cast<char>(code_point);
    }
    else if (code_point < 0x800)
    {
        out += static_cast<char>(0xC0 | (code_point >> 6));
        out += static_cast<char>(0x80 | (code_point & 0x3F));
    }
    else if (code_point < 0x10000)
    {
        out += static_cast<char>(0xE0 | (code_point >> 12));
        out += static_cast<char>(0x80 | ((code_point >> 6) & 0x3F));
        out += static_cast<char>(0x80 | (code_point & 0x3F));
    }
    else
    {
        out += static_cast<char>(0xF0 | (code_point >> 18));
        out += static_cast<char>(0x80 | ((code_point >> 12) & 0x3F));
        out += static_cast<char>(0x80 | ((code_point >> 6) & 0x3F));
        out += static_cast<char>(0x80 | (code_point & 0x3F));
    }
}

bool isNameChar(char c)
{
    return !isWhitespaceASCII(c) && c != '>' && c != '/' && c != '=' && c != '<' && c != '"' && c != '\'';
}

std::string_view localName(std::string_view name)
{
    auto pos = name.rfind(':');
    return pos == std::string_view::npos ? name : name.substr(pos + 1);
}

}

void XMLPullParser::throwUnexpected(std::string_view what) const
{
    throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse XML: {}", what);
}

char XMLPullParser::peekChar()
{
    if (in.eof())
        throwUnexpected("unexpected end of data");
    return *in.position();
}

char XMLPullParser::getChar()
{
    char c = peekChar();
    ++in.position();
    return c;
}

void XMLPullParser::expectChar(char c)
{
    if (getChar() != c)
        throwUnexpected(fmt::format("expected '{}'", c));
}

void XMLPullParser::skipWhitespace()
{
    while (!in.eof() && isWhitespaceASCII(*in.position()))
        ++in.position();
}

void XMLPullParser::readName(String & out)
{
    out.clear();
    while (!in.eof())
    {
        const char * begin = in.position();
        const char * end = in.buffer().end();
        const char * pos = begin;
        while (pos < end && isNameChar(*pos))
            ++pos;
        out.append(begin, pos);
        in.position() = const_cast<char *>(pos);
        if (pos < end)
            break;
    }
    if (out.empty())
        throwUnexpected("expected a name");
}

void XMLPullParser::decodeEntity(String & out)
{
    /// The leading '&' is already consumed.
    char buf[16];
    size_t size = 0;
    while (true)
    {
        char c = getChar();
        if (c == ';')
            break;
        if (size == sizeof(buf))
            throwUnexpected("too long entity reference");
        buf[size++] = c;
    }
    std::string_view entity(buf, size);

    if (entity == "amp")
        out += '&';
    else if (entity == "lt")
        out += '<';
    else if (entity == "gt")
        out += '>';
    else if (entity == "quot")
        out += '"';
    else if (entity == "apos")
        out += '\'';
    else if (entity.size() >= 2 && entity[0] == '#')
    {
        UInt32 code_point = 0;
        bool hex = entity[1] == 'x' || entity[1] == 'X';
        size_t start = hex ? 2 : 1;
        if (start == entity.size())
            throwUnexpected("empty numeric character reference");
        for (size_t i = start; i < entity.size(); ++i)
        {
            char c = entity[i];
            UInt32 digit;
            if (isNumericASCII(c))
                digit = c - '0';
            else if (hex && c >= 'a' && c <= 'f')
                digit = c - 'a' + 10;
            else if (hex && c >= 'A' && c <= 'F')
                digit = c - 'A' + 10;
            else
                throwUnexpected(fmt::format("invalid numeric character reference '&{};'", entity));
            code_point = code_point * (hex ? 16 : 10) + digit;
            if (code_point > 0x10FFFF)
                throwUnexpected(fmt::format("invalid numeric character reference '&{};'", entity));
        }
        appendUTF8(code_point, out);
    }
    else
        throwUnexpected(fmt::format("unknown entity '&{};'", entity));
}

void XMLPullParser::readAttributeValue(char quote, String & out)
{
    out.clear();
    while (true)
    {
        if (in.eof())
            throwUnexpected("unexpected end of data in an attribute value");
        const char * begin = in.position();
        const char * end = in.buffer().end();
        const char * pos = quote == '"' ? find_first_symbols<'"', '&'>(begin, end) : find_first_symbols<'\'', '&'>(begin, end);
        out.append(begin, pos);
        in.position() = const_cast<char *>(pos);
        if (pos == end)
            continue;
        ++in.position();
        if (*pos == quote)
            return;
        decodeEntity(out);
    }
}

void XMLPullParser::readText()
{
    text_data.clear();
    while (!in.eof())
    {
        const char * begin = in.position();
        const char * end = in.buffer().end();
        const char * pos = find_first_symbols<'<', '&'>(begin, end);
        text_data.append(begin, pos);
        in.position() = const_cast<char *>(pos);
        if (pos == end)
            continue;
        if (*pos == '<')
            return;
        ++in.position();
        decodeEntity(text_data);
    }
}

void XMLPullParser::skipUntil(std::string_view terminator)
{
    /// Keep a rolling window of the last characters and compare it with the terminator (it is at most 3 characters long).
    char window[3] = {};
    size_t size = terminator.size();
    while (true)
    {
        for (size_t i = 1; i < size; ++i)
            window[i - 1] = window[i];
        window[size - 1] = getChar();
        if (std::string_view(window, size) == terminator)
            return;
    }
}

void XMLPullParser::readCDATA()
{
    text_data.clear();
    /// "<![CDATA[" is already consumed.
    while (true)
    {
        char c = getChar();
        text_data += c;
        if (text_data.ends_with("]]>"))
        {
            text_data.resize(text_data.size() - 3);
            return;
        }
    }
}

void XMLPullParser::skipDoctype()
{
    /// "<!" is already consumed. The internal subset in square brackets may contain '>' characters.
    size_t bracket_depth = 0;
    while (true)
    {
        char c = getChar();
        if (c == '[')
            ++bracket_depth;
        else if (c == ']' && bracket_depth > 0)
            --bracket_depth;
        else if (c == '>' && bracket_depth == 0)
            return;
    }
}

XMLPullParser::Event XMLPullParser::next()
{
    if (pending_end_element)
    {
        pending_end_element = false;
        --current_depth;
        return Event::EndElement;
    }

    while (true)
    {
        if (in.eof())
        {
            if (current_depth != 0)
                throwUnexpected("unexpected end of data, some elements are not closed");
            return Event::End;
        }

        if (*in.position() != '<')
        {
            readText();
            return Event::Text;
        }

        ++in.position();
        char c = peekChar();
        if (c == '/')
        {
            ++in.position();
            readName(element_name);
            element_name = localName(element_name);
            skipWhitespace();
            expectChar('>');
            if (current_depth == 0)
                throwUnexpected(fmt::format("unexpected closing tag '{}'", element_name));
            --current_depth;
            return Event::EndElement;
        }
        if (c == '?')
        {
            skipUntil("?>");
            continue;
        }
        if (c == '!')
        {
            ++in.position();
            if (peekChar() == '-')
            {
                ++in.position();
                expectChar('-');
                skipUntil("-->");
                continue;
            }
            if (peekChar() == '[')
            {
                for (char expected : std::string_view("[CDATA["))
                    expectChar(expected);
                readCDATA();
                return Event::Text;
            }
            skipDoctype();
            continue;
        }

        readName(element_name);
        element_name = localName(element_name);
        num_attributes = 0;
        while (true)
        {
            skipWhitespace();
            c = peekChar();
            if (c == '/')
            {
                ++in.position();
                expectChar('>');
                pending_end_element = true;
                break;
            }
            if (c == '>')
            {
                ++in.position();
                break;
            }

            if (num_attributes == attributes.size())
                attributes.emplace_back();
            auto & attribute = attributes[num_attributes];
            readName(attribute.first);
            skipWhitespace();
            expectChar('=');
            skipWhitespace();
            char quote = getChar();
            if (quote != '"' && quote != '\'')
                throwUnexpected("expected a quoted attribute value");
            readAttributeValue(quote, attribute.second);
            ++num_attributes;
        }
        ++current_depth;
        return Event::StartElement;
    }
}

const String * XMLPullParser::getAttribute(std::string_view local_name) const
{
    for (size_t i = 0; i < num_attributes; ++i)
        if (localName(attributes[i].first) == local_name)
            return &attributes[i].second;
    return nullptr;
}

void XMLPullParser::skipElement()
{
    size_t target_depth = current_depth - 1;
    while (current_depth > target_depth)
    {
        if (next() == Event::End)
            throwUnexpected("unexpected end of data");
    }
}

void XMLPullParser::readElementText(String & out)
{
    size_t target_depth = current_depth - 1;
    while (current_depth > target_depth)
    {
        auto event = next();
        if (event == Event::Text)
            out += text_data;
        else if (event == Event::End)
            throwUnexpected("unexpected end of data");
    }
}

}
