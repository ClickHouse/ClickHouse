#include <Parsers/ASTGroupByElement.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>


namespace DB
{

void ASTGroupByElement::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(with_cluster);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTGroupByElement::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    children.front()->format(ostr, settings, state, frame);

    if (with_cluster)
    {
        ostr << " WITH CLUSTER ";
        if (auto distance = getClusterDistance())
            distance->format(ostr, settings, state, frame);
    }
}

}
