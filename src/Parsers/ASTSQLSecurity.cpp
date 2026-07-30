
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
        w.writeString("security_type", std::string(magic_enum::enum_name(*type)));
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
        String security_type_str = r.getString("security_type");
        auto security_type_opt = magic_enum::enum_cast<SQLSecurityType>(security_type_str);
        if (!security_type_opt)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown security_type: '{}'", security_type_str);
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

    /// `ParserSQLSecurity` makes `DEFINER = CURRENT_USER` and an explicit `DEFINER = user` mutually
    /// exclusive. With both set, `formatImpl` prints the explicit `definer` while
    /// `processSQLSecurityOption` honours `is_definer_current_user` and substitutes the current user,
    /// so the displayed definer would disagree with the one access checks use. Reject the combination.
    if (is_definer_current_user && definer)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`SQLSecurity` cannot set both 'is_definer_current_user' and an explicit 'definer' during AST JSON deserialization");
}

}
