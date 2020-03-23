#pragma once

#include <Parsers/IAST.h>
#include <Access/RowPolicy.h>


namespace DB
{

/** DROP USER [IF EXISTS] name [,...]
  * DROP ROLE [IF EXISTS] name [,...]
  * DROP QUOTA [IF EXISTS] name [,...]
  * DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...]
  * DROP [SETTINGS] PROFILE [IF EXISTS] name [,...]
  */
class ASTDropAccessEntityQuery : public IAST
{
public:
    enum class Kind
    {
        USER,
        ROLE,
        QUOTA,
        ROW_POLICY,
        SETTINGS_PROFILE,
    };

    const Kind kind;
    bool if_exists = false;
    Strings names;
    std::vector<RowPolicy::FullNameParts> row_policies_names;

    ASTDropAccessEntityQuery(Kind kind_);
    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};
}
