#include <Parsers/ASTOrderByElement.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>

#include <base/EnumReflection.h>


namespace DB
{

void ASTOrderByElement::updateChildRolesHash(SipHash & hash_state) const
{
    /// Iterate over all enumerators so that a newly added child role is hashed without changing
    /// this code. Without the roles, `WITH FILL FROM 1 TO 2` and `WITH FILL FROM 1 STEP 2` would
    /// hash equally.
    for (auto child : magic_enum::enum_values<Child>())
    {
        auto it = positions.find(child);
        if (it != positions.end())
        {
            hash_state.update(child);
            hash_state.update(it->second);
        }
    }
}

void ASTOrderByElement::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    /// `nulls_direction` already holds the effective direction whether or not `NULLS` was written,
    /// so the explicitness flag only decides whether `formatImpl` prints the modifier. Hashing it
    /// would make `ORDER BY x` and `ORDER BY x ASC NULLS LAST` differ, which is the text-level
    /// strictness this comparison exists to avoid. `SortNode::updateTreeHashImpl` canonicalizes the
    /// same way.
    static_assert(sizeof(*this) == 88, "If members were added to ASTOrderByElement, hash them here unless they are purely cosmetic.");
    updateChildRolesHash(hash_state);
    hash_state.update(direction);
    hash_state.update(nulls_direction);
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
