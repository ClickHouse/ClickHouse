#include <Parsers/ASTTransactionControl.h>
#include <IO/Operators.h>
#include <Common/SipHash.h>

namespace DB
{

void ASTTransactionControl::formatImpl(const FormattingBuffer & out) const
{
    switch (action)
    {
        case BEGIN:
            out.writeKeyword("BEGIN TRANSACTION");
            break;
        case COMMIT:
            out.writeKeyword("COMMIT");
            break;
        case ROLLBACK:
            out.writeKeyword("ROLLBACK");
            break;
        case SET_SNAPSHOT:
            out.writeKeyword("SET TRANSACTION SNAPSHOT ");
            out.ostr << snapshot;
            break;
    }
}

void ASTTransactionControl::updateTreeHashImpl(SipHash & hash_state) const
{
    hash_state.update(action);
}

}
