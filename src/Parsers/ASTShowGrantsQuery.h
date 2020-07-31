#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{
/** SHOW GRANTS [FOR user_name]
  */
class ASTShowGrantsQuery : public ASTQueryWithOutput
{
public:
    String name;
    bool current_user = false;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};
}
