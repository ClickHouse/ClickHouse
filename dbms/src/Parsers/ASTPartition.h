#pragma once

#include <Parsers/IAST.h>
#include <common/StringRef.h>


namespace DB
{

/// Either a (possibly compound) expression representing a partition value or a partition ID.
class ASTPartition : public IAST
{
public:
    ASTPtr value;
    StringRef fields_str; /// The extent of comma-separated partition expression fields without parentheses.
    size_t fields_count = 0;

    String id;

    String getID() const override;
    ASTPtr clone() const override;

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
