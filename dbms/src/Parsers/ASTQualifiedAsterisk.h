#pragma once

#include <Parsers/IAST.h>


namespace DB
{

/** Something like t.*
  * It will have qualifier as its child ASTIdentifier.
  */
class ASTQualifiedAsterisk : public IAST
{
public:
    String getID() const override { return "QualifiedAsterisk"; }
    ASTPtr clone() const override
    {
        auto clone = std::make_shared<ASTQualifiedAsterisk>(*this);
        clone->cloneChildren();
        return std::move(clone);
    }
    String getColumnName() const override;

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
