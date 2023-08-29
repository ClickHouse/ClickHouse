#pragma once

#include <Parsers/IAST.h>


namespace DB
{

class ASTPublicSSHKey : public IAST
{
public:
    String key_base64;
    String algorithm;

    // String toString() const;

    ASTPublicSSHKey() = default;
    ASTPublicSSHKey(String key_base64_, String algorithm_)
        : key_base64(key_base64_)
        , algorithm(algorithm_)
    {}
    String getID(char) const override { return "PublicSSHKey"; }
    ASTPtr clone() const override { return std::make_shared<ASTPublicSSHKey>(*this); }
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
