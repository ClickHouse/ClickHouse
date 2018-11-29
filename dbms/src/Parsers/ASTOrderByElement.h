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

    ASTOrderByElement(
        const int direction_, const int nulls_direction_, const bool nulls_direction_was_explicitly_specified_, ASTPtr & collation_)
        : direction(direction_)
        , nulls_direction(nulls_direction_)
        , nulls_direction_was_explicitly_specified(nulls_direction_was_explicitly_specified_)
        , collation(collation_)
    {
    }

    String getID() const override
    {
        return "OrderByElement";
    }

    ASTPtr clone() const override
    {
        auto clone = std::make_shared<ASTOrderByElement>(*this);
        clone->cloneChildren();
        return clone;
    }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};
}
