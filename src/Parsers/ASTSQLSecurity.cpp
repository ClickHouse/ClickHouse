
#include <Parsers/ASTSQLSecurity.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <base/EnumReflection.h>
#include <Common/Exception.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void ASTSQLSecurity::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "SQLSecurity");
    if (type.has_value())
        w.writeInt("security_type", static_cast<Int64>(*type));
    if (is_definer_current_user)
        w.writeBool("is_definer_current_user", true);
    if (definer)
    {
        w.writeKey("definer");
        definer->writeJSON(out);
    }
}

void ASTSQLSecurity::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    if (r.has("security_type"))
    {
        Int64 security_type_value = r.getInt("security_type");
        auto security_type_opt = magic_enum::enum_cast<SQLSecurityType>(static_cast<std::underlying_type_t<SQLSecurityType>>(security_type_value));
        if (!security_type_opt || static_cast<Int64>(*security_type_opt) != security_type_value)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown security_type: {}", security_type_value);
        type = security_type_opt;
    }
    is_definer_current_user = r.getBool("is_definer_current_user");
    auto definer_child = r.readChild("definer");
    if (definer_child)
    {
        definer = boost::dynamic_pointer_cast<ASTUserNameWithHost>(definer_child);
        if (!definer)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected ASTUserNameWithHost for 'definer', got {}", definer_child->getID());
    }
}

}
