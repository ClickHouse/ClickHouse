#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{
/// SHOW GRANTS FOR user_or_role
class ASTShowGrantsQuery : public ASTQueryWithOutput
{
public:
    String role_name;

    String getID(char) const override;
    ASTPtr clone() const override;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
