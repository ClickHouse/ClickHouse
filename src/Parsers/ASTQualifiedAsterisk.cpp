#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void ASTQualifiedAsterisk::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "QualifiedAsterisk");
    w.writeChild("qualifier", qualifier);
    w.writeChild("transformers", transformers);
}

void ASTQualifiedAsterisk::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    /// `qualifier` is parser-produced as an `ASTIdentifier` (`QueryTreeBuilder::buildExpression` reads
    /// `qualifier->as<ASTIdentifier &>`) and `transformers` as an `ASTColumnsTransformerList`; restore
    /// them with typed reads so a wrong node type from malformed `clickhouse_json` fails closed.
    auto child = r.readChildOfType<ASTIdentifier>("qualifier");
    if (!child)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing 'qualifier' field in `QualifiedAsterisk` during AST JSON deserialization");
    this->qualifier = child;
    this->children.push_back(this->qualifier);
    child = r.readChildOfType<ASTColumnsTransformerList>("transformers");
    if (child)
    {
        this->transformers = child;
        this->children.push_back(this->transformers);
    }
}

void ASTQualifiedAsterisk::appendColumnName(WriteBuffer & ostr) const
{
    qualifier->appendColumnName(ostr);
    writeCString(".*", ostr);
}

void ASTQualifiedAsterisk::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    qualifier->format(ostr, settings, state, frame);
    ostr << ".*";

    if (transformers)
    {
        transformers->format(ostr, settings, state, frame);
    }
}

}
