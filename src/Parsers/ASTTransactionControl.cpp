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
            format.ostr << (format.hilite ? hilite_keyword : "") << "BEGIN TRANSACTION" << (format.hilite ? hilite_none : "");
            break;
        case COMMIT:
            format.ostr << (format.hilite ? hilite_keyword : "") << "COMMIT" << (format.hilite ? hilite_none : "");
            break;
        case ROLLBACK:
            format.ostr << (format.hilite ? hilite_keyword : "") << "ROLLBACK" << (format.hilite ? hilite_none : "");
            break;
        case SET_SNAPSHOT:
            format.ostr << (format.hilite ? hilite_keyword : "") << "SET TRANSACTION SNAPSHOT " << (format.hilite ? hilite_none : "") << snapshot;
            break;
    }
}

void ASTTransactionControl::updateTreeHashImpl(SipHash & hash_state) const
{
    hash_state.update(action);
}

}
