#include <Parsers/ASTCollation.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <Parsers/IAST_fwd.h>
#include <Common/SipHash.h>

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

    void ASTCollation::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
    {
        /// The collation name is not a child (see `ParserCollation`), so without this it is
        /// absent from the hash and column declarations differing only in collation compare equal.
        /// The expected size is for 64-bit targets; the layout differs on 32-bit ones (the wasm parser build).
        static_assert(sizeof(void *) != 8 || sizeof(*this) == 40, "If members were added to ASTCollation, hash them here unless they are purely cosmetic.");
        hash_state.update(collation != nullptr);
        if (collation)
            collation->updateTreeHash(hash_state, ignore_aliases);
        IAST::updateTreeHashImpl(hash_state, ignore_aliases);
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
        /// `ParserCollation` keeps the name out of `children` (and `clone` copies `children`
        /// before reassigning `collation`), so adding it here would make JSON-built nodes hash
        /// differently from parser-built ones and leave a stale child in clones.
        collation = child;
    }

}
