#pragma once

#include <Parsers/IAST.h>


namespace DB
{
/** Element of expression with ASC or DESC,
  * and possibly with COLLATE, WITH FILL.
  * Important note: ORDER BY expression should be ALWAYS
  * the first in the list of children - other parts of code rely on it heavily
  * and it simplified the code a bit - there is no need to update the raw pointer
  * stored in the field each time we change the expression.
  *
  * So, expression is always the first and the order of other children doesn't matter
  * taking into account the raw pointer stored in fields always point to them.
  */
class ASTOrderByElement : public IAST
{
public:
    int direction = 0; /// 1 for ASC, -1 for DESC
    int nulls_direction = 0; /// Same as direction for NULLS LAST, opposite for NULLS FIRST.
    bool nulls_direction_was_explicitly_specified = false;

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
