#include <Parsers/ASTFixedStringDataType.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>


namespace DB
{

String ASTFixedStringDataType::getID(char delim) const
{
    return "FixedStringDataType" + (delim + name);
}

ASTPtr ASTFixedStringDataType::clone() const
{
    auto res = std::make_shared<ASTFixedStringDataType>(*this);
    res->children.clear();
    return res;
}

void ASTFixedStringDataType::updateTreeHashImpl(SipHash & hash_state, bool /*ignore_aliases*/) const
{
    hash_state.update(name.size());
    hash_state.update(name);
    hash_state.update(n);
}

void ASTFixedStringDataType::formatImpl(WriteBuffer & ostr, const FormatSettings & /*settings*/, FormatState & /*state*/, FormatStateStacked /*frame*/) const
{
    ostr << name << '(';
    writeText(n, ostr);
    ostr << ')';
}

}
