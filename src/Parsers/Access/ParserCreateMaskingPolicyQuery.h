#pragma once

#include <Parsers/Access/ASTCreateMaskingPolicyQuery.h>
#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses Data Masking Policy CREATE/ALTER */
class ParserCreateMaskingPolicy : public IParserBase
{
public:
    void useAttachMode(bool attach_mode_ = true) { attach_mode = attach_mode_; }

protected:
    const char * getName() const override { return "CREATE MASKING POLICY or ALTER MASKING POLICY query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool attach_mode = false;
};
}
