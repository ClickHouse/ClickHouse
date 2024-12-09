#pragma once
#include <Parsers/IAST.h>

namespace DB
{

/// Common AST for TCL queries
class ASTTransactionControl : public IAST
{
public:
    enum QueryType
    {
        BEGIN,
        COMMIT,
        ROLLBACK,
        SET_SNAPSHOT,
    };

    QueryType action;

    UInt64 snapshot;    /// For SET TRANSACTION SNAPSHOT ...

    explicit ASTTransactionControl(QueryType action_) : action(action_) {}

    String getID(char /*delimiter*/) const override { return "ASTTransactionControl"; }
    ASTPtr clone() const override { return std::make_shared<ASTTransactionControl>(*this); }

    void formatImpl(WriteBuffer & ostr, const FormatSettings & format, FormatState & /*state*/, FormatStateStacked /*frame*/) const override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    QueryKind getQueryKind() const override;
};

}
