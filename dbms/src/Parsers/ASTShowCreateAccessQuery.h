#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{
/// SHOW CREATE USER user
class ASTShowCreateAccessQuery : public ASTQueryWithOutput
{
public:
    enum class Kind
    {
        USER,
        SETTINGS_PROFILE,
    };

    Kind kind = Kind::USER;
    String name;

    String getID(char) const override;
    ASTPtr clone() const override;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
