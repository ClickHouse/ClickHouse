#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{
/// SHOW CREATE USER user
class ASTShowCreateUserQuery : public ASTQueryWithOutput
{
public:
    String user_name;

    String getID(char) const override;
    ASTPtr clone() const override;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
