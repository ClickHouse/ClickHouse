#include <Parsers/ASTReadFromProjectionSettings.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ASTPtr ASTReadFromProjectionSettings::clone() const
{
    auto res = make_intrusive<ASTReadFromProjectionSettings>();
    if (name)
        res->setName(name->clone());
    return res;
}

void ASTReadFromProjectionSettings::setName(ASTPtr name_)
{
    name = std::move(name_);
    children.push_back(name);
}

void ASTReadFromProjectionSettings::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    name->format(ostr, settings, state, frame);
}

void ASTReadFromProjectionSettings::forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f)
{
    f(nullptr, &name);
}

void ASTReadFromProjectionSettings::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "ReadFromProjectionSettings");
    w.writeChild("name", name);
}

void ASTReadFromProjectionSettings::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    auto child = r.readChildOfType<ASTIdentifier>("name");
    if (!child)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`ReadFromProjectionSettings` 'name' must be an identifier during AST JSON deserialization");

    setName(std::move(child));
}

}
