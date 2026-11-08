#include <Parsers/ASTCollation.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

    ASTPtr ASTCollation::clone() const
    {
        auto res = make_intrusive<ASTCollation>(*this);
        res->collation = collation->clone();
        return res;
    }

    void ASTCollation::formatImpl(WriteBuffer & ostr, const FormatSettings &s, FormatState &state, FormatStateStacked frame) const
    {
        if (collation)
            collation->format(ostr, s, state, frame);
    }

    void ASTCollation::writeJSON(WriteBuffer & out) const
    {
        JSONObjectWriter w(out, "Collation");
        w.writeChild("collation", collation);
    }

    void ASTCollation::readJSON(const Poco::JSON::Object & json)
    {
        JSONObjectReader r(json);

        /// `ParserCollation` always stores the collation name as an `ASTIdentifier`; `clone`
        /// dereferences it and `formatImpl` prints it, so require a present identifier child.
        auto child = r.readChildOfType<ASTIdentifier>("collation");
        if (!child)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "`Collation` requires an identifier 'collation' child during AST JSON deserialization");
        collation = child;
        children.push_back(collation);
    }

}
