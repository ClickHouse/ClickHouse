#include <Parsers/ASTDecimalDataType.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <Poco/String.h>


namespace DB
{

String ASTDecimalDataType::getID(char delim) const
{
    return "DecimalDataType" + (delim + name);
}

ASTPtr ASTDecimalDataType::clone() const
{
    auto res = std::make_shared<ASTDecimalDataType>(*this);
    res->children.clear();
    return res;
}

void ASTDecimalDataType::updateTreeHashImpl(SipHash & hash_state, bool /*ignore_aliases*/) const
{
    hash_state.update(name.size());
    hash_state.update(name);
    hash_state.update(precision);
    hash_state.update(scale);
}

void ASTDecimalDataType::formatImpl(WriteBuffer & ostr, const FormatSettings & /*settings*/, FormatState & /*state*/, FormatStateStacked /*frame*/) const
{
    ostr << name << '(';

    /// Decimal32, Decimal64, Decimal128, Decimal256 only have scale parameter
    /// Decimal has both precision and scale (case-insensitive comparison)
    String name_upper = Poco::toUpper(name);
    if (name_upper == "DECIMAL")
    {
        writeText(precision, ostr);
        ostr << ", ";
    }

    writeText(scale, ostr);
    ostr << ')';
}

}
