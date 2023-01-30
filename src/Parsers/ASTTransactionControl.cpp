#include <Parsers/ASTTransactionControl.h>
#include <IO/Operators.h>
#include <Common/SipHash.h>

namespace DB
{

void ASTTransactionControl::formatImpl(const FormatSettings & format /*state*/, FormatState &, FormatStateStacked /*frame*/) const
{
    switch (action)
    {
        case BEGIN:
            format.writeKeyword("BEGIN TRANSACTION");
            break;
        case COMMIT:
            format.writeKeyword("COMMIT");
            break;
        case ROLLBACK:
            format.writeKeyword("ROLLBACK");
            break;
        case SET_SNAPSHOT:
            format.writeKeyword("SET TRANSACTION SNAPSHOT ");
            format.ostr << snapshot;
            break;
    }
}

void ASTTransactionControl::updateTreeHashImpl(SipHash & hash_state) const
{
    hash_state.update(action);
}

}
