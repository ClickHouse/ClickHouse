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
    explicit FormattingAST(Formatter formatter_)
        : formatter(std::move(formatter_))
    {
    }

    String getID(char) const override
    {
        return "FormattingAST";
    }

    ASTPtr clone() const override
    {
        return make_intrusive<FormattingAST>(*this);
    }

    void formatImpl(WriteBuffer & out, const FormatSettings &, FormatState & state, FormatStateStacked frame) const override
    {
        formatter(out, state, frame);
    }

private:
    Formatter formatter;
};

String format(const Formatter & formatter, size_t width)
{
    return formatASTWithLineLength(FormattingAST(formatter), IAST::FormatSettings(false), width);
}

void writePair(WriteBuffer & out, IAST::FormatState & state, const IAST::FormatStateStacked & frame)
{
    ASTFormattingGroup group(out, state, frame);
    out << "f(";
    writeASTFormattingBreak(out, state, frame, 1, '\0');
    out << "1,";
    writeASTFormattingBreak(out, state, frame);
    out << "2";
    writeASTFormattingBreak(out, state, frame, 0, '\0');
    out << ")";
}

TEST(ASTFormatting, GroupFitsAfterMovingToNextLine)
{
    const Formatter formatter = [](auto & out, auto & state, auto frame)
    {
        out << "SELECT";
        writeASTFormattingOptionalBreak(out, state, frame);
        writePair(out, state, frame);
    };

    EXPECT_EQ(format(formatter, 0), "SELECT f(1, 2)");
    EXPECT_EQ(format(formatter, 14), "SELECT f(1, 2)");
    EXPECT_EQ(format(formatter, 11), "SELECT\n    f(1, 2)");
}

TEST(ASTFormatting, LeadingZeroBreakKeepsParentBreakIndentation)
{
    const Formatter formatter = [](auto & out, auto & state, auto frame)
    {
        out << "SELECT";
        writeASTFormattingOptionalBreak(out, state, frame);
        ASTFormattingGroup group(out, state, frame);
        writeASTFormattingBreak(out, state, frame, 1, '\0');
        out << "long_condition";
        writeASTFormattingBreak(out, state, frame);
        out << "OR next_condition";
    };

    EXPECT_EQ(format(formatter, 12), "SELECT\n    long_condition\n    OR next_condition");
}

TEST(ASTFormatting, LeadingZeroBreakKeepsStructuralIndentation)
{
    const Formatter formatter = [](auto & out, auto & state, auto frame)
    {
        out << "SELECT\n";
        ++frame.indent;
        writeASTFormattingIndent(out, state, frame);
        ASTFormattingGroup group(out, state, frame);
        writeASTFormattingBreak(out, state, frame, 1, '\0');
        out << "long_condition";
        writeASTFormattingBreak(out, state, frame);
        out << "OR next_condition";
    };

    EXPECT_EQ(format(formatter, 12), "SELECT\n    long_condition\n    OR next_condition");
}

TEST(ASTFormatting, ConsistentMultilineGroupBreaksWhenFirstLineOverflows)
{
    const Formatter formatter = [](auto & out, auto & state, auto frame)
    {
        ASTFormattingGroup group(out, state, frame, ASTFormattingGroup::Mode::ConsistentLines);
        out << "long_function(";
        writeASTFormattingBreak(out, state, frame, 1, '\0');
        out << "'a\nb',";
        writeASTFormattingBreak(out, state, frame);
        out << "x";
        writeASTFormattingBreak(out, state, frame, 0, '\0');
        out << ")";
    };

    EXPECT_EQ(format(formatter, 15), "long_function(\n    'a\nb',\n    x\n)");
}

TEST(ASTFormatting, ConsistentMultilineGroupBreaksWhenLastLineOverflows)
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

TEST(ASTFormatting, ConsistentMultilineGroupBreaksWhenMiddleLineOverflows)
{
    const Formatter formatter = [](auto & out, auto & state, auto frame)
    {
        ASTFormattingGroup group(out, state, frame, ASTFormattingGroup::Mode::ConsistentLines);
        out << "f(";
        writeASTFormattingBreak(out, state, frame, 1, '\0');
        out << "'a\nb',";
        writeASTFormattingBreak(out, state, frame);
        out << "long_argument,";
        writeASTFormattingBreak(out, state, frame);
        out << "'c\nd'";
        writeASTFormattingBreak(out, state, frame, 0, '\0');
        out << ")";
    };

    EXPECT_EQ(format(formatter, 20), "f(\n    'a\nb',\n    long_argument,\n    'c\nd'\n)");
}

TEST(ASTFormatting, AttachedCommaParticipatesInFit)
{
    const Formatter formatter = [](auto & out, auto & state, auto frame)
    {
        ASTFormattingGroup outer(out, state, frame);
        out << "f(";
        writeASTFormattingBreak(out, state, frame, 1, '\0');
        {
            ASTFormattingGroup inner(out, state, frame);
            out << "inner(";
            writeASTFormattingBreak(out, state, frame, 1, '\0');
            out << "123456";
            writeASTFormattingBreak(out, state, frame, 0, '\0');
            out << ")";
        }
        out << ",";
        writeASTFormattingBreak(out, state, frame);
        out << "x";
        writeASTFormattingBreak(out, state, frame, 0, '\0');
        out << ")";
    };

    EXPECT_EQ(format(formatter, 17), "f(\n    inner(\n        123456\n    ),\n    x\n)");
    EXPECT_EQ(format(formatter, 18), "f(\n    inner(123456),\n    x\n)");
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
            writeASTFormattingFillBreak(out, state, frame);
        }
        out << "444";
        writeASTFormattingBreak(out, state, frame, 0, '\0');
        out << "]";
    };

    EXPECT_EQ(format(formatter, 14), "[\n    111, 222,\n    333, 444\n]");
    EXPECT_EQ(format(formatter, 19), "[\n    111, 222, 333,\n    444\n]");
}

TEST(ASTFormatting, MultilineClosingBreakIncludesOuterSuffix)
{
    const Formatter formatter = [](auto & out, auto & state, auto frame)
    {
        {
            ASTFormattingGroup group(out, state, frame);
            out << "g(\n1234567";
            writeASTFormattingBreak(out, state, frame, 0, '\0');
            out << ")";
        }
        out << ",";
    };

    EXPECT_EQ(format(formatter, 9), "g(\n1234567),");
    EXPECT_EQ(format(formatter, 8), "g(\n1234567\n),");
}

TEST(ASTFormatting, RelocatesOnlyMarkedIndentation)
{
    const Formatter formatter = [](auto & out, auto & state, auto frame)
    {
        out << "SELECT very_long_name";
        writeASTFormattingOptionalBreak(out, state, frame);
        ASTFormattingGroup group(out, state, frame);
        out << "(\n";
        auto nested = frame;
        ++nested.indent;
        writeASTFormattingIndent(out, state, nested);
        out << "'first\n  second'\n";
        writeASTFormattingIndent(out, state, frame);
        out << ")";
    };

    EXPECT_EQ(format(formatter, 0), "SELECT very_long_name (\n    'first\n  second'\n)");
    EXPECT_EQ(format(formatter, 18), "SELECT very_long_name\n    (\n        'first\n  second'\n    )");
}

TEST(ASTFormatting, UnmovedGroupPreservesVirtualFrameIndentation)
{
    const Formatter formatter = [](auto & out, auto & state, auto frame)
    {
        out << "unbreakable_long_identifier\n";
        ++frame.indent;
        ASTFormattingGroup group(out, state, frame);
        out << "(\n";
        auto nested = frame;
        ++nested.indent;
        writeASTFormattingIndent(out, state, nested);
        out << "body\n";
        writeASTFormattingIndent(out, state, frame);
        out << ")";
    };

    EXPECT_EQ(format(formatter, 16), "unbreakable_long_identifier\n(\n        body\n    )");
}

TEST(ASTFormatting, MovedGroupUsesPhysicalSourceIndentation)
{
    const Formatter formatter = [](auto & out, auto & state, auto frame)
    {
        out << "SELECT very_long_name";
        writeASTFormattingOptionalBreak(out, state, frame);
        ++frame.indent;
        ASTFormattingGroup group(out, state, frame);
        out << "(\n";
        auto nested = frame;
        ++nested.indent;
        writeASTFormattingIndent(out, state, nested);
        out << "body\n";
        writeASTFormattingIndent(out, state, frame);
        out << ")";
    };

    EXPECT_EQ(format(formatter, 0), "SELECT very_long_name (\n        body\n    )");
    EXPECT_EQ(format(formatter, 18), "SELECT very_long_name\n    (\n        body\n    )");
}

TEST(ASTFormatting, UnrelatedOverflowPreservesOpaqueNewlines)
{
    const Formatter formatter = [](auto & out, auto & state, auto frame)
    {
        out << "unbreakable_long_identifier\n";
        ASTFormattingGroup group(out, state, frame);
        out << "f(";
        writeASTFormattingBreak(out, state, frame, 1, '\0');
        out << "'a\nb',";
        writeASTFormattingBreak(out, state, frame);
        out << "x";
        writeASTFormattingBreak(out, state, frame, 0, '\0');
        out << ")";
    };

    EXPECT_EQ(format(formatter, 12), "unbreakable_long_identifier\nf('a\nb', x)");
}

TEST(ASTFormatting, ZeroWidthBreakRemovesPrecedingSeparator)
{
    const Formatter formatter = [](auto & out, auto & state, auto frame)
    {
        out << "WHERE ";
        ASTFormattingGroup group(out, state, frame);
        writeASTFormattingBreak(out, state, frame, 1, '\0');
        out << "long_condition";
    };

    EXPECT_EQ(format(formatter, 12), "WHERE\n    long_condition");
}

TEST(ASTFormatting, DisplayWidthUsesRenderedTabPrefix)
{
    const Formatter formatter = [](auto & out, auto & state, auto frame)
    {
        out << "12";
        ASTFormattingGroup group(out, state, frame);
        out << "f(";
        writeASTFormattingBreak(out, state, frame, 1, '\0');
        out << "'\t界'";
        writeASTFormattingBreak(out, state, frame, 0, '\0');
        out << ")";
    };

    EXPECT_EQ(format(formatter, 12), "12f('\t界')");
    EXPECT_EQ(format(formatter, 11), "12f(\n    '\t界'\n)");
}

}
}
