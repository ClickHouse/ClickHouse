#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Core/Names.h>

namespace DB
{
class ASTDeleteQuery : public ASTQueryWithTableAndOutput
{
public:
    String getID(char delim) const final { return "DeleteQuery" + (delim + database) + delim + table; }

    ASTPtr clone() const final;

    ASTPtr predicate;

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};
}
