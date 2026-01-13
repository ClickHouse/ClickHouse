#include <Parsers/ASTDateTime64DataType.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>


namespace DB
{

String ASTDateTime64DataType::getID(char delim) const
{
    return "DateTime64DataType" + (delim + name);
}

ASTPtr ASTDateTime64DataType::clone() const
{
    auto res = std::make_shared<ASTDateTime64DataType>(*this);
    res->children.clear();
    return res;
}

void ASTDateTime64DataType::updateTreeHashImpl(SipHash & hash_state, bool /*ignore_aliases*/) const
{
    hash_state.update(name.size());
    hash_state.update(name);
    hash_state.update(precision);
    hash_state.update(timezone.size());
    hash_state.update(timezone);
}

void ASTDateTime64DataType::formatImpl(WriteBuffer & ostr, const FormatSettings & /*settings*/, FormatState & /*state*/, FormatStateStacked /*frame*/) const
{
    ostr << name << '(';
    writeText(precision, ostr);

    if (!timezone.empty())
    {
        ostr << ", ";
        writeQuotedString(timezone, ostr);
    }

    ostr << ')';
}

}
