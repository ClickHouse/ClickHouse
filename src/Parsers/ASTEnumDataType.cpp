#include <Parsers/ASTEnumDataType.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>


namespace DB
{

String ASTEnumDataType::getID(char delim) const
{
    return "EnumDataType" + (delim + name);
}

ASTPtr ASTEnumDataType::clone() const
{
    auto res = std::make_shared<ASTEnumDataType>(*this);
    res->children.clear();
    /// values vector is copied by the copy constructor
    /// No arguments to clone since we don't use ASTLiteral children
    return res;
}

void ASTEnumDataType::updateTreeHashImpl(SipHash & hash_state, bool /*ignore_aliases*/) const
{
    hash_state.update(name.size());
    hash_state.update(name);

    hash_state.update(values.size());
    for (const auto & [elem_name, elem_value] : values)
    {
        hash_state.update(elem_name.size());
        hash_state.update(elem_name);
        hash_state.update(elem_value);
    }
}

void ASTEnumDataType::formatImpl(WriteBuffer & ostr, const FormatSettings & /*settings*/, FormatState & /*state*/, FormatStateStacked /*frame*/) const
{
    ostr << name;

    if (!values.empty())
    {
        ostr << '(';

        bool first = true;
        for (const auto & [elem_name, elem_value] : values)
        {
            if (!first)
                ostr << ", ";
            first = false;

            writeQuotedString(elem_name, ostr);
            ostr << " = ";
            writeText(elem_value, ostr);
        }

        ostr << ')';
    }
}

}
