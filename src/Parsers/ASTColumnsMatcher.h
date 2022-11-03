#pragma once

#include <Parsers/IAST.h>

namespace re2
{
class RE2;
}


namespace DB
{

class WriteBuffer;

/** SELECT COLUMNS('regexp') is expanded to multiple columns like * (asterisk).
  * Optional transformers can be attached to further manipulate these expanded columns.
  */
class ASTColumnsRegexpMatcher : public IAST
{
public:
    String getID(char) const override { return "ColumnsRegexpMatcher"; }
    ASTPtr clone() const override;

    void appendColumnName(WriteBuffer & ostr) const override;
    void setPattern(String pattern);
    const std::shared_ptr<re2::RE2> & getMatcher() const;
    bool isColumnMatching(const String & column_name) const;
    void updateTreeHashImpl(SipHash & hash_state) const override;

protected:
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

private:
    std::shared_ptr<re2::RE2> column_matcher;
    String original_pattern;
};

/// Same as the above but use a list of column names to do matching.
class ASTColumnsListMatcher : public IAST
{
public:
    String getID(char) const override { return "ColumnsListMatcher"; }
    ASTPtr clone() const override;
    void appendColumnName(WriteBuffer & ostr) const override;
    void updateTreeHashImpl(SipHash & hash_state) const override;

    ASTPtr column_list;
protected:
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

/// Same as ASTColumnsRegexpMatcher. Qualified identifier is first child.
class ASTQualifiedColumnsRegexpMatcher : public IAST
{
public:
    String getID(char) const override { return "QualifiedColumnsRegexpMatcher"; }
    ASTPtr clone() const override;

    void appendColumnName(WriteBuffer & ostr) const override;
    const std::shared_ptr<re2::RE2> & getMatcher() const;
    void setPattern(String pattern);
    void setMatcher(std::shared_ptr<re2::RE2> matcher);
    void updateTreeHashImpl(SipHash & hash_state) const override;

protected:
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

private:
    std::shared_ptr<re2::RE2> column_matcher;
    String original_pattern;
};

/// Same as ASTColumnsListMatcher. Qualified identifier is first child.
class ASTQualifiedColumnsListMatcher : public IAST
{
public:
    String getID(char) const override { return "QualifiedColumnsListMatcher"; }
    ASTPtr clone() const override;
    void appendColumnName(WriteBuffer & ostr) const override;
    void updateTreeHashImpl(SipHash & hash_state) const override;

    ASTPtr column_list;
protected:
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
