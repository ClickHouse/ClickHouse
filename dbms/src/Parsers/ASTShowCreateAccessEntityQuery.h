#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{
/** SHOW CREATE QUOTA [name | CURRENT]
  */
class ASTShowCreateAccessEntityQuery : public ASTQueryWithOutput
{
public:
    enum class Kind
    {
        QUOTA,
    };
    const Kind kind;
    const char * const keyword;
    String name;
    bool current_quota = false;

    ASTShowCreateAccessEntityQuery(Kind kind_);
    String getID(char) const override;
    ASTPtr clone() const override;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
