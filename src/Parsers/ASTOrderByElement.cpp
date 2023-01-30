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

void ASTOrderByElement::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    children.front()->formatImpl(settings, state, frame);
    settings.writeKeyword(direction == -1 ? " DESC" : " ASC");

    if (nulls_direction_was_explicitly_specified)
    {
        settings.writeKeyword(" NULLS ");
        settings.writeKeyword(nulls_direction == direction ? "LAST" : "FIRST");
    }

    if (collation)
    {
        settings.writeKeyword(" COLLATE ");
        collation->formatImpl(settings, state, frame);
    }

    if (with_fill)
    {
        settings.writeKeyword(" WITH FILL");
        if (fill_from)
        {
            settings.writeKeyword(" FROM ");
            fill_from->formatImpl(settings, state, frame);
        }
        if (fill_to)
        {
            settings.writeKeyword(" TO ");
            fill_to->formatImpl(settings, state, frame);
        }
        if (fill_step)
        {
            settings.writeKeyword(" STEP ");
            fill_step->formatImpl(settings, state, frame);
        }
    }
}

}
