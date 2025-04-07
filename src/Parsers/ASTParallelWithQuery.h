#pragma once

#include <Parsers/IAST.h>


namespace DB
{

/// Represents a statement like
///     statement1 PARALLEL WITH statement2 PARALLEL WITH statement3 ... PARALLEL WITH statementN
/// That means statements `statement1`, `statement2`, ... `statementN` should be executed in parallel if possible.
class ASTParallelWithQuery : public IAST
{
public:
    String getID(char delim) const override;
    ASTPtr clone() const override;
    QueryKind getQueryKind() const override { return QueryKind::ParallelWithQuery; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
