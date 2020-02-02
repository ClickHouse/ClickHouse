#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Access/RowPolicy.h>


namespace DB
{
/** SHOW CREATE QUOTA [name | CURRENT]
  * SHOW CREATE [ROW] POLICY name ON [database.]table
  */
class ASTShowCreateAccessEntityQuery : public ASTQueryWithOutput
{
public:
    enum class Kind
    {
        QUOTA,
        ROW_POLICY,
    };
    const Kind kind;
    const char * const keyword;
    String name;
    bool current_quota = false;
    RowPolicy::FullNameParts row_policy_name;

    ASTShowCreateAccessEntityQuery(Kind kind_);
    String getID(char) const override;
    ASTPtr clone() const override;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
