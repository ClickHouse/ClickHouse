#include <Parsers/ASTTransactionControl.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <IO/Operators.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <base/EnumReflection.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

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

void ASTTransactionControl::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "TransactionControl");
    w.writeInt("action", static_cast<Int64>(action));
    if (action == SET_SNAPSHOT)
        w.writeUInt("snapshot", snapshot);
}

void ASTTransactionControl::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    if (!r.has("action"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'action' field in `TransactionControl` during AST JSON deserialization");
    Int64 action_value = r.getInt("action");
    auto action_opt = magic_enum::enum_cast<QueryType>(static_cast<std::underlying_type_t<QueryType>>(action_value));
    if (!action_opt || static_cast<Int64>(*action_opt) != action_value)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown TransactionControl action: {}", action_value);
    action = *action_opt;
    if (action == SET_SNAPSHOT)
    {
        if (!r.has("snapshot"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing required 'snapshot' field for SET_SNAPSHOT action");
        snapshot = r.getUInt("snapshot");
    }
}

}
