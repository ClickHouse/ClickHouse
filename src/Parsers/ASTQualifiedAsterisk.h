#pragma once

#include <Parsers/IAST.h>


namespace DB
{

/** Something like t.*
  * It will have qualifier as its child ASTIdentifier.
  * Optional transformers can be attached to further manipulate these expanded columns.
  */
class ASTQualifiedAsterisk : public IAST
{
public:
    String getID(char) const override { return "QualifiedAsterisk"; }
    ASTPtr clone() const override
    {
        auto clone = std::make_shared<ASTQualifiedAsterisk>(*this);
        clone->children.clear();

        if (transformers)
        {
            clone->transformers = transformers->clone();
            clone->children.push_back(clone->transformers);
        }

        clone->qualifier = qualifier->clone();
        clone->children.push_back(clone->qualifier);

        return clone;
    }
    void appendColumnName(WriteBuffer & ostr) const override;

    ASTPtr qualifier;
    ASTPtr transformers;
protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
