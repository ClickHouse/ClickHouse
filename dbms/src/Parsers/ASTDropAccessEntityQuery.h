#pragma once

#include <Parsers/IAST.h>


namespace DB
{

/** DROP QUOTA [IF EXISTS] name [,...]
  */
class ASTDropAccessEntityQuery : public IAST
{
public:
    enum class Kind
    {
        QUOTA,
    };
    const Kind kind;
    const char * const keyword;
    bool if_exists = false;
    Strings names;

    ASTDropAccessEntityQuery(Kind kind_);
    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};
}
