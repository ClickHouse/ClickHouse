#include <Parsers/ASTEnumDataType.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

String ASTEnumDataType::getID(char delim) const
{
    return "EnumDataType" + (delim + name);
}

ASTPtr ASTEnumDataType::clone() const
{
    auto res = make_intrusive<ASTEnumDataType>(*this);
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

void ASTEnumDataType::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "EnumDataType");
    w.writeString("name", name);

    /// The enum values live in the `values` vector (not as AST children), so write them explicitly
    /// as an array of `{"name": <string>, "value": <int>}` objects, symmetric to `readJSON`.
    w.writeKey("values");
    auto & o = w.getOut();
    o << '[';
    bool first = true;
    for (const auto & [elem_name, elem_value] : values)
    {
        if (!first)
            o << ',';
        first = false;
        o << "{\"name\":";
        writeJSONString(elem_name, o, w.getFormatSettings());
        o << ",\"value\":";
        writeText(elem_value, o);
        o << '}';
    }
    o << ']';
}

void ASTEnumDataType::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    name = r.getString("name");
    if (name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty 'name' for ASTEnumDataType during AST JSON deserialization");

    if (auto arr = r.getArray("values"))
    {
        values.reserve(arr->size());
        for (unsigned int i = 0; i < arr->size(); ++i)
        {
            auto elem = arr->getObject(i);
            if (!elem)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Null element at index {} in 'values' of ASTEnumDataType during AST JSON deserialization", i);
            if (!elem->has("name") || !elem->has("value"))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Enum value element at index {} requires 'name' and 'value' during AST JSON deserialization", i);
            JSONObjectReader er(*elem);
            values.emplace_back(er.getString("name"), er.getInt("value"));
        }
    }
}

}
