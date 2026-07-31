#include <Parsers/ASTQueryParameter.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void ASTQueryParameter::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "QueryParameter");
    w.writeString("name", name);
    w.writeString("param_type", type);
    w.writeAlias(*this);
}

void ASTQueryParameter::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    name = r.getString("name");
    type = r.getString("param_type");

    /// A query parameter is formatted as `{name:type}` (see `formatImplWithoutAlias`). Both parts are mandatory:
    /// with the defaults an empty object would format as `{:}`, which is not a valid query parameter and could flow
    /// into identifier placeholders. Require both fields to be non-empty.
    if (name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "A query parameter must have a non-empty 'name' during AST JSON deserialization");

    if (type.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "A query parameter must have a non-empty 'param_type' during AST JSON deserialization");

    r.readAlias(*this);
}

void ASTQueryParameter::formatImplWithoutAlias(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
    ostr << '{' << name << ':' << type << '}';
}

ASTPtr ASTQueryParameter::clone() const
{
    auto ret = make_intrusive<ASTQueryParameter>(*this);
    ret->cloneChildren();
    return ret;
}

void ASTQueryParameter::appendColumnNameImpl(WriteBuffer & ostr) const
{
    writeString(name, ostr);
}

void ASTQueryParameter::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    ASTWithAlias::updateTreeHashImpl(hash_state, ignore_aliases);
}

}
