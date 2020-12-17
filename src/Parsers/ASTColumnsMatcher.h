#pragma once

#include <Parsers/IAST.h>


namespace re2
{
    class RE2;
}


namespace DB
{

class WriteBuffer;

namespace ErrorCodes
{
}

struct AsteriskSemantic;
struct AsteriskSemanticImpl;


/** SELECT COLUMNS('regexp') is expanded to multiple columns like * (asterisk).
  * Optional transformers can be attached to further manipulate these expanded columns.
  */
class ASTColumnsMatcher : public IAST
{
public:
    String getID(char) const override { return "ColumnsMatcher"; }
    ASTPtr clone() const override;

    void appendColumnName(WriteBuffer & ostr) const override;
    void setPattern(String pattern);
    bool isColumnMatching(const String & column_name) const;
    void updateTreeHashImpl(SipHash & hash_state) const override;

    ASTPtr column_list;

protected:
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

private:
    std::shared_ptr<re2::RE2> column_matcher;
    String original_pattern;
    std::shared_ptr<AsteriskSemanticImpl> semantic; /// pimpl

    friend struct AsteriskSemantic;
};


}
