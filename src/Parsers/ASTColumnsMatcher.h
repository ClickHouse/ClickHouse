#pragma once

#include <Parsers/IAST.h>


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
    const String & getPattern() const;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    ASTPtr expression;
    ASTPtr transformers;

    /// When set, format this matcher as the equivalent `[expr.]* LIKE/ILIKE '<asterisk_like_pattern>'`
    /// syntax instead of `COLUMNS('<regexp>')`. Only the query fuzzer sets this, to exercise the
    /// asterisk LIKE/ILIKE parser path; the parser never sets it, so normal query round-trips are
    /// unaffected (a parsed `* LIKE ...` still becomes a plain regexp matcher).
    bool format_as_asterisk_like = false;
    bool asterisk_like_case_insensitive = false;
    String asterisk_like_pattern;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

private:
    String pattern;
};

/// Same as the above but use a list of column names to do matching.
class ASTColumnsListMatcher : public IAST
{
public:
    String getID(char) const override { return "ColumnsListMatcher"; }
    ASTPtr clone() const override;
    void appendColumnName(WriteBuffer & ostr) const override;

    ASTPtr expression;
    ASTPtr column_list;
    ASTPtr transformers;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

/// Same as ASTColumnsRegexpMatcher. Qualified identifier is first child.
class ASTQualifiedColumnsRegexpMatcher : public IAST
{
public:
    String getID(char) const override { return "QualifiedColumnsRegexpMatcher"; }
    ASTPtr clone() const override;

    void appendColumnName(WriteBuffer & ostr) const override;
    void setPattern(String pattern_);
    const String & getPattern() const;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    ASTPtr qualifier;
    ASTPtr transformers;

    /// See ASTColumnsRegexpMatcher: format as `<qualifier>.* LIKE/ILIKE '<asterisk_like_pattern>'`.
    bool format_as_asterisk_like = false;
    bool asterisk_like_case_insensitive = false;
    String asterisk_like_pattern;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

private:
    String pattern;
};

/// Same as ASTColumnsListMatcher. Qualified identifier is first child.
class ASTQualifiedColumnsListMatcher : public IAST
{
public:
    String getID(char) const override { return "QualifiedColumnsListMatcher"; }
    ASTPtr clone() const override;
    void appendColumnName(WriteBuffer & ostr) const override;

    ASTPtr qualifier;
    ASTPtr column_list;
    ASTPtr transformers;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
