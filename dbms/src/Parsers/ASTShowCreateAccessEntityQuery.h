#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Access/RowPolicy.h>


namespace DB
{
/** SHOW CREATE QUOTA [name | CURRENT]
  * SHOW CREATE [ROW] POLICY name ON [database.]table
  * SHOW CREATE USER [name | CURRENT_USER]
  */
class ASTShowCreateAccessEntityQuery : public ASTQueryWithOutput
{
public:
    enum class Kind
    {
        USER,
        QUOTA,
        ROW_POLICY,
    };
    const Kind kind;
    const char * const keyword;

    String name;
    bool current_quota = false;
    bool current_user = false;
    RowPolicy::FullNameParts row_policy_name;

    ASTShowCreateAccessEntityQuery(Kind kind_);
    String getID(char) const override;
    ASTPtr clone() const override;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
