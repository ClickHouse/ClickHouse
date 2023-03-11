#include <Columns/Collator.h>
#include <Parsers/ASTOrderByElement.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>


namespace DB
{

void ASTOrderByElement::updateTreeHashImpl(SipHash & hash_state) const
{
    hash_state.update(direction);
    hash_state.update(nulls_direction);
    hash_state.update(nulls_direction_was_explicitly_specified);
    hash_state.update(with_fill);
    IAST::updateTreeHashImpl(hash_state);
}

void ASTOrderByElement::formatImpl(const FormattingBuffer & out) const
{
    children.front()->formatImpl(out);
    out.writeKeyword(direction == -1 ? " DESC" : " ASC");

    if (nulls_direction_was_explicitly_specified)
    {
        out.writeKeyword(" NULLS ");
        out.writeKeyword(nulls_direction == direction ? "LAST" : "FIRST");
    }

    if (collation)
    {
        out.writeKeyword(" COLLATE ");
        collation->formatImpl(out);
    }

    if (with_fill)
    {
        out.writeKeyword(" WITH FILL");
        if (fill_from)
        {
            out.writeKeyword(" FROM ");
            fill_from->formatImpl(out);
        }
        if (fill_to)
        {
            out.writeKeyword(" TO ");
            fill_to->formatImpl(out);
        }
        if (fill_step)
        {
            out.writeKeyword(" STEP ");
            fill_step->formatImpl(out);
        }
    }
}

}
