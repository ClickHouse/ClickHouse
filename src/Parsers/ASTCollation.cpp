#include <Parsers/ASTCollation.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
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

        auto child = r.readChild("collation");
        if (child)
        {
            collation = child;
            children.push_back(collation);
        }
    }

}
