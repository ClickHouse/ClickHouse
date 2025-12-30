#pragma once

#include <cstddef>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Parsers/IAST.h>
#include <base/types.h>

namespace DB
{

class JSONObjectBuilder
{
public:
    JSONObjectBuilder(WriteBuffer & buf_, size_t indent_)
        : buf(buf_)
        , indent(indent_)
    {
    }

    void startObject(const String & type)
    {
        writeIndent();
        writeCString("{\n", buf);
        indent++;
        writeIndent();
        writeCString("\"type\": \"", buf);
        writeString(type, buf);
        writeCString("\"", buf);
        has_fields = false; // Reset for new object
    }

    void endObject()
    {
        writeChar('\n', buf);
        indent--;
        writeIndent();
        writeChar('}', buf);
    }

    void writeField(const String & name, const std::function<void()> & writer, bool condition = true)
    {
        if (!condition)
            return;

        if (has_fields)
            writeChar(',', buf);
        writeChar('\n', buf);
        writeIndent();
        writeChar('"', buf);
        writeString(name, buf);
        writeCString("\": ", buf);

        // Execute the custom writer function
        writer();

        has_fields = true;
    }

    template <typename T>
    requires(!std::invocable<T> || !std::same_as<std::invoke_result_t<T>, void>)
    void writeField(const String & name, const T & value, bool condition = true)
    {
        if (!condition)
            return;

        if (has_fields)
            writeChar(',', buf);
        writeChar('\n', buf);
        writeIndent();
        writeChar('"', buf);
        writeString(name, buf);
        writeCString("\": ", buf);

        if constexpr (std::is_same_v<T, ASTPtr> || std::is_same_v<T, IAST *>)
        {
            if (value)
                value->writeJSON(buf, indent + 1);
            else
                writeCString("null", buf);
        }
        else if constexpr (std::is_same_v<T, ASTs>)
            writeChildrenArray(value);
        else if constexpr (std::is_same_v<T, bool>)
            writeCString(value ? "true" : "false", buf);
        else if constexpr (std::is_arithmetic_v<T>)
            writeIntText(value, buf);
        else
        {
            writeChar('"', buf);
            writeEscapedJSONString(toString(value));
            writeChar('"', buf);
        }

        has_fields = true; // Mark it that we have written at least one field
    }

    template <typename T>
    void writeArray(const String & name, const std::vector<T> & values, bool condition = true)
    {
        if (!condition || values.empty())
            return;

        writeField(name, [&] { writeVector(values); }, condition);
    }

    size_t getIndent() const { return indent; }

private:
    WriteBuffer & buf;
    size_t indent;
    bool has_fields = false;

    void writeIndent()
    {
        for (size_t i = 0; i < indent * 2; ++i)
            writeChar(' ', buf);
    }

    void writeEscapedJSONString(const String & value)
    {
        for (char c : value)
        {
            switch (c)
            {
                case '"':
                    writeCString("\\\"", buf);
                    break;
                case '\\':
                    writeCString("\\\\", buf);
                    break;
                case '\b':
                    writeCString("\\b", buf);
                    break;
                case '\f':
                    writeCString("\\f", buf);
                    break;
                case '\n':
                    writeCString("\\n", buf);
                    break;
                case '\r':
                    writeCString("\\r", buf);
                    break;
                case '\t':
                    writeCString("\\t", buf);
                    break;
                default:
                    if (static_cast<unsigned char>(c) <= 0x1F)
                    {
                        writeCString("\\u00", buf);
                        writeChar(hexDigit((c >> 4) & 0x0F), buf);
                        writeChar(hexDigit(c & 0x0F), buf);
                    }
                    else
                        writeChar(c, buf);

                    break;
            }
        }
    }

    char hexDigit(int value)
    {
        static const char digits[] = "0123456789ABCDEF";
        return digits[value & 0x0F];
    }

    void writeChildrenArray(const ASTs & children)
    {
        if (children.empty())
        {
            writeCString("[]", buf);
            return;
        }
        writeCString("\"children\":", buf);
        writeCString("[\n", buf);
        indent++;

        for (size_t i = 0; i < children.size(); ++i)
        {
            writeIndent();
            if (children[i])
                children[i]->writeJSON(buf, indent);
            else
                writeCString("null", buf);

            if (i < children.size() - 1)
                writeCString(",\n", buf);
            else
                writeChar('\n', buf);
        }

        indent--;
        writeIndent();
        writeChar(']', buf);
    }

    template <typename T>
    void writeVector(const std::vector<T> & values)
    {
        writeCString("[\n", buf);
        indent++;

        for (size_t i = 0; i < values.size(); ++i)
        {
            writeIndent();

            if constexpr (std::is_same_v<T, String>)
            {
                writeChar('"', buf);
                writeEscapedJSONString(values[i]);
                writeChar('"', buf);
            }
            else if constexpr (std::is_same_v<T, bool>)
                writeCString(values[i] ? "true" : "false", buf);
            else if constexpr (std::is_arithmetic_v<T>)
                writeIntText(values[i], buf);
            else
            {
                writeChar('"', buf);
                writeEscapedJSONString(toString(values[i]));
                writeChar('"', buf);
            }

            if (i < values.size() - 1)
                writeCString(",\n", buf);
            else
                writeChar('\n', buf);
        }

        indent--;
        writeIndent();
        writeChar(']', buf);
    }
};

}
