#include <Parsers/ASTCastTarget.h>


namespace DB
{

ASTPtr ASTCastTarget::clone() const
{
    return make_intrusive<ASTCastTarget>(type->clone());
}

void ASTCastTarget::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    type->format(ostr, settings, state, frame);
}

}
