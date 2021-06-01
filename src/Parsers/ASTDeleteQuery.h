#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Core/Names.h>

namespace DB
{
class ASTDeleteQuery : public ASTQueryWithTableAndOutput
{
public:
    String getID(char delim) const final;
    ASTPtr clone() const final;

    ASTPtr predicate;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};
}
