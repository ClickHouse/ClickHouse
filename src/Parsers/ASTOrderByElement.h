#pragma once

#include <Parsers/IAST.h>


namespace DB
{
/** Element of expression with ASC or DESC,
  *  and possibly with COLLATE.
  */
class ASTOrderByElement : public IAST
{
public:
    int direction; /// 1 for ASC, -1 for DESC
    int nulls_direction; /// Same as direction for NULLS LAST, opposite for NULLS FIRST.
    bool nulls_direction_was_explicitly_specified;

    /** Collation for locale-specific string comparison. If empty, then sorting done by bytes. */
    ASTPtr collation;

    bool with_fill;
    ASTPtr fill_from;
    ASTPtr fill_to;
    ASTPtr fill_step;

    String getID(char) const override { return "OrderByElement"; }

    ASTPtr clone() const override
    {
        auto clone = std::make_shared<ASTOrderByElement>(*this);
        clone->cloneChildren();
        return clone;
    }

    void updateTreeHashImpl(SipHash & hash_state) const override;

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};
}
