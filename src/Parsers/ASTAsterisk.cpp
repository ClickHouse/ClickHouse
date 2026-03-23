#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/Operators.h>

namespace DB
{

void ASTAsterisk::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "Asterisk");
    w.writeChild("expression", expression);
    w.writeChild("transformers", transformers);
}

void ASTAsterisk::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    auto child = r.readChild("expression");
    if (child)
    {
        this->expression = child;
        this->children.push_back(this->expression);
    }
    child = r.readChild("transformers");
    if (child)
    {
        this->transformers = child;
        this->children.push_back(this->transformers);
    }
}

ASTPtr ASTAsterisk::clone() const
{
    auto clone = make_intrusive<ASTAsterisk>(*this);
    clone->children.clear();

    if (expression) { clone->expression = expression->clone(); clone->children.push_back(clone->expression); }
    if (transformers) { clone->transformers = transformers->clone(); clone->children.push_back(clone->transformers); }

    return clone;
}

void ASTAsterisk::appendColumnName(WriteBuffer & ostr) const
{
    if (expression)
    {
        expression->appendColumnName(ostr);
        writeCString(".", ostr);
    }

    ostr.write('*');
}

void ASTAsterisk::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (expression)
    {
        expression->format(ostr, settings, state, frame);
        ostr << ".";
    }

    ostr << "*";

    if (transformers)
    {
        transformers->format(ostr, settings, state, frame);
    }
}

}
