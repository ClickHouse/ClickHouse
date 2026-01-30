#pragma once

#include <Parsers/IAST.h>


namespace DB
{
/** Element of expression with ASC or DESC,
  *  and possibly with COLLATE.
  */
class ASTShuffle : public IAST
{
public:
    String getID(char) const override { return "Shuffle"; }

    ASTPtr clone() const override
    {
        auto clone = std::make_shared<ASTShuffle>(*this);
        clone->cloneChildren();
        return clone;
    }

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

};

class ASTStorageShuffle : public IAST
{
public:
    int direction = 1; /// 1 for ASC, -1 for DESC

    ASTPtr clone() const override
    {
        auto clone = std::make_shared<ASTStorageShuffle>(*this);
        clone->cloneChildren();
        return clone;
    }

    String getID(char) const override { return "StorageShuffle"; }
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
