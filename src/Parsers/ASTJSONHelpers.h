#pragma once

#include <IO/Operators.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Formats/FormatSettings.h>
#include <Parsers/IAST.h>

namespace DB
{

class ASTWithAlias;
class Field;

/// Helper for writing AST nodes as JSON objects.
/// Usage:
///   void MyAST::writeJSON(WriteBuffer & out) const
///   {
///       JSONObjectWriter w(out, "MyAST");
///       w.writeString("name", name);
///       w.writeChild("arguments", arguments);
///       w.writeChildren(children);
///   }
///
/// The constructor writes `{"type":"<type_name>"` and the destructor writes `}`.
/// All writeXxx methods prepend a comma automatically.
class JSONObjectWriter
{
public:
    JSONObjectWriter(WriteBuffer & out_, const char * type_name)
        : out(out_)
    {
        out << "{\"type\":";
        writeJSONString(std::string_view(type_name), out, fs);
    }

    ~JSONObjectWriter()
    {
        out << '}';
    }

    JSONObjectWriter(const JSONObjectWriter &) = delete;
    JSONObjectWriter & operator=(const JSONObjectWriter &) = delete;

    /// Write a raw key (followed by colon). Value must be written next.
    void writeKey(const char * k)
    {
        out << ',';
        writeJSONString(std::string_view(k), out, fs);
        out << ':';
    }

    void writeString(const char * k, std::string_view v)
    {
        writeKey(k);
        writeJSONString(v, out, fs);
    }

    /// Note: no separate overload for const String &, since std::string_view handles it.

    void writeBool(const char * k, bool v)
    {
        writeKey(k);
        out << (v ? "true" : "false");
    }

    void writeInt(const char * k, Int64 v)
    {
        writeKey(k);
        out << v;
    }

    void writeUInt(const char * k, UInt64 v)
    {
        writeKey(k);
        out << v;
    }

    void writeFloat(const char * k, double v)
    {
        writeKey(k);
        writeFloatText(v, out);
    }

    void writeNull(const char * k)
    {
        writeKey(k);
        out << "null";
    }

    /// Write a named child node. Skips if child is null.
    void writeChild(const char * k, const ASTPtr & child)
    {
        if (child)
        {
            writeKey(k);
            child->writeJSON(out);
        }
    }

    /// Write a named child from a raw IAST pointer. Skips if null.
    void writeChild(const char * k, const IAST * child)
    {
        if (child)
        {
            writeKey(k);
            child->writeJSON(out);
        }
    }

    /// Write the children array.
    void writeChildren(const ASTs & ch)
    {
        if (ch.empty())
            return;
        writeKey("children");
        writeArray(ch);
    }

    /// Write an array of AST nodes.
    void writeArray(const ASTs & nodes)
    {
        out << '[';
        for (size_t i = 0; i < nodes.size(); ++i)
        {
            if (i > 0) out << ',';
            nodes[i]->writeJSON(out);
        }
        out << ']';
    }

    /// Write alias and prefer_alias_to_column_name for ASTWithAlias nodes.
    void writeAlias(const ASTWithAlias & node);

    /// Write a Field value as a JSON object with field_type and value.
    void writeFieldValue(const char * k, const Field & field);

    /// Access the output buffer directly (for custom writes).
    WriteBuffer & getOut() { return out; }
    const FormatSettings & getFormatSettings() const { return fs; }

private:
    WriteBuffer & out;
    FormatSettings fs;
};

}
