#pragma once

#include <Parsers/IAST.h>
#include <Access/RowPolicy.h>


namespace DB
{

/** DROP QUOTA [IF EXISTS] name [,...]
  * DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...]
  */
class ASTDropAccessEntityQuery : public IAST
{
public:
    enum class Kind
    {
        QUOTA,
        ROW_POLICY,
    };
    const Kind kind;
    const char * const keyword;
    bool if_exists = false;
    Strings names;
    std::vector<RowPolicy::FullNameParts> row_policies_names;

    ASTDropAccessEntityQuery(Kind kind_);
    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};
}
