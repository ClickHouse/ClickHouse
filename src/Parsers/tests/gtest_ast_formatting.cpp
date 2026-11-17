#include <IO/Operators.h>
#include <Parsers/ASTFormatting.h>

#include <gtest/gtest.h>

#include <functional>


namespace DB
{
namespace
{

using Formatter = std::function<void(WriteBuffer &, IAST::FormatState &, IAST::FormatStateStacked)>;

class FormattingAST final : public IAST
{
public:
    explicit FormattingAST(Formatter formatter_) : formatter(std::move(formatter_)) {}

    String getID(char) const override { return "FormattingAST"; }
    ASTPtr clone() const override { return make_intrusive<FormattingAST>(*this); }

    void formatImpl(WriteBuffer & out, const FormatSettings &, FormatState & state, FormatStateStacked frame) const override
    {
        formatter(out, state, frame);
    }

private:
    Formatter formatter;
};

String format(const Formatter & formatter, size_t width)
{
    return formatASTWithWrapping(FormattingAST(formatter), IAST::FormatSettings(/*one_line=*/false), width);
}

void writeIndent(WriteBuffer & out, IAST::FormatState & state, size_t indent)
{
    writeASTFormattingIndent(out, IAST::FormatSettings(/*one_line=*/false), state, indent);
}

/// f(<first>, <second>)
void writeCall(WriteBuffer & out, IAST::FormatState & state, const IAST::FormatStateStacked & frame, const char * first, const char * second)
{
    ASTFormattingGroup group(out, state, frame);
    out << "f(";
    writeASTFormattingBreak(out, state, frame, 1, '\0');
    out << first << ",";
    writeASTFormattingBreak(out, state, frame);
    out << second;
    writeASTFormattingBreak(out, state, frame, 0, '\0');
    out << ")";
}

TEST(ASTFormatting, FillBreakMovesWholeNextItem)
{
    const Formatter formatter = [](auto & out, auto & state, auto frame)
    {
        out << "SELECT";
        writeASTFormattingBreak(out, state, frame, 1, ' ', ASTFormattingBreak::Fill);
        writeCall(out, state, frame, "1", "2");
    };

    EXPECT_EQ(format(formatter, 14), "SELECT f(1, 2)");
    EXPECT_EQ(format(formatter, 11), "SELECT\n    f(1, 2)");
}

TEST(ASTFormatting, OptionalBreakMeasuresUpToNextBreak)
{
    const Formatter formatter = [](auto & out, auto & state, auto frame)
    {
        out << "JOIN table";
        writeASTFormattingBreak(out, state, frame, 0, ' ', ASTFormattingBreak::Optional);
        out << "AS b ON ";
        writeCall(out, state, frame, "first_condition", "second_condition");
    };

    EXPECT_EQ(format(formatter, 24), "JOIN table AS b ON f(\n    first_condition,\n    second_condition\n)");
    EXPECT_EQ(format(formatter, 16), "JOIN table\nAS b ON f(\n    first_condition,\n    second_condition\n)");
}

TEST(ASTFormatting, FillBreaksPackItemsInsideBrokenGroup)
{
    const Formatter formatter = [](auto & out, auto & state, auto frame)
    {
        ASTFormattingGroup group(out, state, frame);
        out << "[";
        writeASTFormattingBreak(out, state, frame, 1, '\0');
        for (const auto * value : {"111,", "222,", "333,"})
        {
            out << value;
            writeASTFormattingBreak(out, state, frame, 1, ' ', ASTFormattingBreak::Fill);
        }
        out << "444";
        writeASTFormattingBreak(out, state, frame, 0, '\0');
        out << "]";
    };

    EXPECT_EQ(format(formatter, 14), "[\n    111, 222,\n    333, 444\n]");
    EXPECT_EQ(format(formatter, 19), "[\n    111, 222, 333,\n    444\n]");
}

TEST(ASTFormatting, AttachedCommaParticipatesInFit)
{
    const Formatter formatter = [](auto & out, auto & state, auto frame)
    {
        ASTFormattingGroup outer(out, state, frame);
        out << "f(";
        writeASTFormattingBreak(out, state, frame, 1, '\0');
        writeCall(out, state, frame, "12", "3456");
        out << ",";
        writeASTFormattingBreak(out, state, frame);
        out << "x";
        writeASTFormattingBreak(out, state, frame, 0, '\0');
        out << ")";
    };

    EXPECT_EQ(format(formatter, 16), "f(\n    f(12, 3456),\n    x\n)");
    EXPECT_EQ(format(formatter, 15), "f(\n    f(\n        12,\n        3456\n    ),\n    x\n)");
}

TEST(ASTFormatting, ConsistentGroupBreaksWhenAnyLineOverflows)
{
    const Formatter formatter = [](auto & out, auto & state, auto frame)
    {
        out << "unbreakable_long_identifier\n";
        ASTFormattingGroup group(out, state, frame, ASTFormattingGroup::Mode::ConsistentLines);
        out << "f(";
        writeASTFormattingBreak(out, state, frame, 1, '\0');
        out << "'a\nb',";
        writeASTFormattingBreak(out, state, frame);
        out << "long_argument";
        writeASTFormattingBreak(out, state, frame, 0, '\0');
        out << ")";
    };

    EXPECT_EQ(format(formatter, 18), "unbreakable_long_identifier\nf('a\nb', long_argument)");
    EXPECT_EQ(format(formatter, 17), "unbreakable_long_identifier\nf(\n    'a\nb',\n    long_argument\n)");
}

TEST(ASTFormatting, MovedGroupShiftsOnlyMarkedIndentation)
{
    const Formatter formatter = [](auto & out, auto & state, auto frame)
    {
        out << "SELECT very_long_name";
        writeASTFormattingBreak(out, state, frame, 1, ' ', ASTFormattingBreak::Optional);
        ASTFormattingGroup group(out, state, frame);
        out << "(\n";
        writeIndent(out, state, frame.indent + 1);
        out << "'first\n  second'\n";
        writeIndent(out, state, frame.indent);
        out << ")";
    };

    EXPECT_EQ(format(formatter, 0), "SELECT very_long_name (\n    'first\n  second'\n)");
    EXPECT_EQ(format(formatter, 18), "SELECT very_long_name\n    (\n        'first\n  second'\n    )");
}

TEST(ASTFormatting, IndentationIsLimitedByWidth)
{
    /// [[[...1...]]] with a break after each opening bracket.
    const Formatter formatter = [](auto & out, auto & state, auto frame)
    {
        std::function<void(size_t)> write_level = [&](size_t depth)
        {
            if (depth == 0)
            {
                out << "1";
                return;
            }
            ASTFormattingGroup group(out, state, frame);
            out << "[";
            writeASTFormattingBreak(out, state, frame, 1, '\0');
            write_level(depth - 1);
            writeASTFormattingBreak(out, state, frame, 0, '\0');
            out << "]";
        };
        write_level(1000);
    };

    EXPECT_EQ(format(formatter, 8), "[\n    " + String(999, '[') + "1" + String(999, ']') + "\n]");
}

}
}
