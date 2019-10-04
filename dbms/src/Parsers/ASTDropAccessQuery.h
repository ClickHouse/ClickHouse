#pragma once

#include <Parsers/IAST.h>


namespace DB
{

/** DROP ROLE [IF EXISTS] role [,...]
  *
  * DROP USER [IF EXISTS] user [,...]
  */
class ASTDropAccessQuery : public IAST
{
public:
    enum class Kind
    {
        ROLE,
        USER,
    };
    Kind kind = Kind::ROLE;
    Strings names;
    bool if_exists = false;

    String getID(char) const override { return String("DROP ") + getSQLTypeName() + " query"; }
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

private:
    const char * getSQLTypeName() const;
};
}
