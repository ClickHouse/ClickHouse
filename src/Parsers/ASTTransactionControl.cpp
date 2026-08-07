#include <Parsers/ASTTransactionControl.h>
#include <IO/Operators.h>
#include <Common/SipHash.h>

namespace DB
{

void ASTTransactionControl::formatImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked /*frame*/) const
{
    switch (action)
    {
        case BEGIN:
            ostr << "BEGIN TRANSACTION";
            break;
        case COMMIT:
            ostr << "COMMIT";
            break;
        case ROLLBACK:
            ostr << "ROLLBACK";
            break;
        case SET_SNAPSHOT:
            ostr << "SET TRANSACTION SNAPSHOT " << snapshot;
            break;
    }
}

IAST::QueryKind ASTTransactionControl::getQueryKind() const
{
    switch (action)
    {
        case BEGIN:
            return QueryKind::Begin;
        case COMMIT:
            return QueryKind::Commit;
        case ROLLBACK:
            return QueryKind::Rollback;
        case SET_SNAPSHOT:
            return QueryKind::SetTransactionSnapshot;
    }
}

void ASTTransactionControl::updateTreeHashImpl(SipHash & hash_state, bool /*ignore_aliases*/) const
{
    hash_state.update(action);
}

}
