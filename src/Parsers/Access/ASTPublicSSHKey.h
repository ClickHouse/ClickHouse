#pragma once

#include <Parsers/IAST.h>


namespace DB
{

class ASTPublicSSHKey : public IAST
{
public:
    String key_base64;
    String type;

    ASTPublicSSHKey() = default;
    ASTPublicSSHKey(String key_base64_, String type_)
        : key_base64(key_base64_)
        , type(type_)
    {}
    String getID(char) const override { return "PublicSSHKey"; }
    ASTPtr clone() const override { return std::make_shared<ASTPublicSSHKey>(*this); }
    void formatImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const override;
};

}
