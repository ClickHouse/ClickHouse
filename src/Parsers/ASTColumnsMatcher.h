#pragma once

#include <Parsers/IAST.h>

namespace Poco::JSON { class Object; }

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
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

    ASTPtr expression;
    ASTPtr transformers;

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
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

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
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

    ASTPtr qualifier;
    ASTPtr transformers;

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
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;

    ASTPtr qualifier;
    ASTPtr column_list;
    ASTPtr transformers;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
