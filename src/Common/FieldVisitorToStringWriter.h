#pragma once

#include <Common/FieldVisitorToString.h>
#include <Common/checkStackSize.h>
#include <IO/WriteBuffer.h>

#include <string_view>

namespace DB
{

struct FieldLiteralFormatting
{
    struct Group
    {
    };

    Group startGroup(WriteBuffer &) const
    {
        return {};
    }
    void writeBreak(WriteBuffer & out, size_t, char flat) const
    {
        if (flat)
            out.write(&flat, 1);
    }
    void writeFillBreak(WriteBuffer & out) const
    {
        out.write(' ');
    }
};

/// Shares SQL container spelling between plain literals and layout-aware formatting.
template <typename Formatting = FieldLiteralFormatting>
class FieldVisitorToStringWriter : public StaticVisitor<void>
{
public:
    FieldVisitorToStringWriter(WriteBuffer & out_, const Formatting & formatting_)
        : out(out_), formatting(formatting_)
    {
    }

    template <typename T>
    void operator() (const T & value) const
    {
        const auto text = FieldVisitorToString()(value);
        out.write(text.data(), text.size());
    }

    void operator() (const Array & value) const
    {
        writeContainer(value, "[", ']');
    }
    void operator() (const Tuple & value) const
    {
        writeContainer(value, value.size() > 1 ? "(" : "tuple(", ')');
    }
    void operator() (const Map & value) const
    {
        writeContainer(value, "[", ']');
    }

private:
    WriteBuffer & out;
    const Formatting & formatting;

    template <typename Container>
    void writeContainer(const Container & value, std::string_view prefix, char suffix) const
    {
        checkStackSize();
        [[maybe_unused]] auto group = formatting.startGroup(out);
        out.write(prefix.data(), prefix.size());
        if (!value.empty())
        {
            formatting.writeBreak(out, 1, '\0');
            for (auto it = value.begin(); it != value.end(); ++it)
            {
                if (it != value.begin())
                {
                    out.write(',');
                    formatting.writeFillBreak(out);
                }
                applyVisitor(*this, *it);
            }
            formatting.writeBreak(out, 0, '\0');
        }
        out.write(suffix);
    }
};

}
