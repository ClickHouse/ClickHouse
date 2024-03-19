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
    int direction = 0; /// 1 for ASC, -1 for DESC
    int nulls_direction = 0; /// Same as direction for NULLS LAST, opposite for NULLS FIRST.
    bool nulls_direction_was_explicitly_specified = false;

    IAST * expression = nullptr;

    /** Collation for locale-specific string comparison. If empty, then sorting done by bytes. */
    IAST * collation = nullptr;

    bool with_fill = false;
    IAST * fill_from = nullptr;
    IAST * fill_to = nullptr;
    IAST * fill_step = nullptr;

    String getID(char) const override { return "OrderByElement"; }
    ASTPtr clone() const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void forEachPointerToChild(std::function<void(void**)> f) override;
};

}
