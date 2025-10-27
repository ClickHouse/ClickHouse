#include <Parsers/ASTTransactionControl.h>
#include <IO/Operators.h>
#include <Common/SipHash.h>

namespace DB
{

void ASTTransactionControl::formatImpl(WriteBuffer & ostr, const FormatSettings & format /*state*/, FormatState &, FormatStateStacked /*frame*/) const
{
    switch (action)
    {
        case BEGIN:
            ostr << (format.hilite ? hilite_keyword : "") << "BEGIN TRANSACTION" << (format.hilite ? hilite_none : "");
            break;
        case COMMIT:
            ostr << (format.hilite ? hilite_keyword : "") << "COMMIT" << (format.hilite ? hilite_none : "");
            break;
        case ROLLBACK:
            ostr << (format.hilite ? hilite_keyword : "") << "ROLLBACK" << (format.hilite ? hilite_none : "");
            break;
        case SET_SNAPSHOT:
            ostr << (format.hilite ? hilite_keyword : "") << "SET TRANSACTION SNAPSHOT " << (format.hilite ? hilite_none : "") << snapshot;
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
