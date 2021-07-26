#pragma once

#include <Parsers/IAST.h>


namespace DB
{

struct AsteriskSemantic;
struct AsteriskSemanticImpl;

/** Something like t.*
  * It will have qualifier as its child ASTIdentifier.
  */
class ASTQualifiedAsterisk : public IAST
{
public:
    String getID(char) const override { return "QualifiedAsterisk"; }
    ASTPtr clone() const override
    {
        auto clone = std::make_shared<ASTQualifiedAsterisk>(*this);
        clone->cloneChildren();
        return clone;
    }
    void appendColumnName(WriteBuffer & ostr) const override;

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

private:
    std::shared_ptr<AsteriskSemanticImpl> semantic; /// pimpl

    friend struct AsteriskSemantic;
};

}
