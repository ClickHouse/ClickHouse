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
    settings.ostr << (settings.hilite ? hilite_keyword : "")
        << (direction == -1 ? " DESC" : " ASC")
        << (settings.hilite ? hilite_none : "");

    if (nulls_direction_was_explicitly_specified)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "")
            << " NULLS "
            << (nulls_direction == direction ? "LAST" : "FIRST")
            << (settings.hilite ? hilite_none : "");
    }

    if (collation)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " COLLATE " << (settings.hilite ? hilite_none : "");
        collation->formatImpl(settings, state, frame);
    }

    if (with_fill)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " WITH FILL" << (settings.hilite ? hilite_none : "");
        if (fill_from)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FROM " << (settings.hilite ? hilite_none : "");
            fill_from->formatImpl(settings, state, frame);
        }
        if (fill_to)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " TO " << (settings.hilite ? hilite_none : "");
            fill_to->formatImpl(settings, state, frame);
        }
        if (fill_step)
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " STEP " << (settings.hilite ? hilite_none : "");
            fill_step->formatImpl(settings, state, frame);
        }
    }
}

}
