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

    /** Collation for locale-specific string comparison. If empty, then sorting done by bytes. */
    IAST * collation = nullptr;

    bool with_fill = false;
    IAST * fill_from;
    IAST * fill_to;
    IAST * fill_step;

    String getID(char) const override { return "OrderByElement"; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTOrderByElement>(*this);
        res->children.clear();

        if (collation)
            res->set(res->collation, collation->clone());
        if (fill_from)
            res->set(res->fill_from, fill_from->clone());
        if (fill_to)
            res->set(res->fill_to, fill_to->clone());
        if (fill_step)
            res->set(res->fill_step, fill_step->clone());

        return res;
    }

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
