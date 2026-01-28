#include <Parsers/ASTShuffle.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>


namespace DB
{

void ASTShuffle::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTShuffle::formatImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
    ostr << "SHUFFLE";
}

void ASTStorageShuffle::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}

void ASTStorageShuffle::formatImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
    ostr << "STORAGE SHUFFLE";
}

}
