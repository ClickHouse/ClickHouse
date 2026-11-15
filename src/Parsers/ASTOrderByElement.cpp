#include <Parsers/ASTOrderByElement.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>


namespace DB
{

void ASTOrderByElement::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(direction);
    hash_state.update(nulls_direction);
    hash_state.update(nulls_direction_was_explicitly_specified);
    hash_state.update(with_fill);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTOrderByElement::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    children.front()->format(ostr, settings, state, frame);
    ostr
        << (direction == -1 ? " DESC" : " ASC")
       ;

    if (nulls_direction_was_explicitly_specified)
    {
        ostr
            << " NULLS "
            << (nulls_direction == direction ? "LAST" : "FIRST")
           ;
    }

    if (auto collation = getCollation())
    {
        ostr << " COLLATE ";
        collation->format(ostr, settings, state, frame);
    }

    if (with_fill)
    {
        ostr << " WITH FILL";
        if (auto fill_from = getFillFrom())
        {
            ostr << " FROM ";
            fill_from->format(ostr, settings, state, frame);
        }
        if (auto fill_to = getFillTo())
        {
            ostr << " TO ";
            fill_to->format(ostr, settings, state, frame);
        }
        if (auto fill_step = getFillStep())
        {
            ostr << " STEP ";
            fill_step->format(ostr, settings, state, frame);
        }
        if (auto fill_staleness = getFillStaleness())
        {
            ostr << " STALENESS ";
            fill_staleness->format(ostr, settings, state, frame);
        }
    }
}

void ASTStorageOrderByElement::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(direction);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTStorageOrderByElement::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    children.front()->format(ostr, settings, state, frame);

    if (direction == -1)
        ostr << " DESC";
}

}
