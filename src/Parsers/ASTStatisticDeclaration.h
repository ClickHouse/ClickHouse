#pragma once

#include <Parsers/IAST.h>

namespace DB
{

class ASTFunction;

/** name BY columns TYPE typename(args) in create query
  */
class ASTStatisticDeclaration : public IAST
{
public:
    IAST * columns;
    /// TODO type should be a list of ASTFunction, for example, 'tdigest(256), hyperloglog(128)', etc.
    String type;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "Stat"; }

    std::vector<String> getColumnNames() const;

    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
