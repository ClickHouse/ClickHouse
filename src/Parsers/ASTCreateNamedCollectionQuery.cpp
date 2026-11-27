#include <Common/quoteString.h>
#include <Parsers/updateTreeHashField.h>
#include <IO/Operators.h>
#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Parsers/formatSettingName.h>
#include <Parsers/ASTExpressionList.h>
#include <Common/FieldVisitorToString.h>
#include <Common/HiddenSecret.h>
#include <Common/SipHash.h>


namespace DB
{

ASTPtr ASTCreateNamedCollectionQuery::clone() const
{
    return make_intrusive<ASTCreateNamedCollectionQuery>(*this);
}

void ASTCreateNamedCollectionQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    /// `getID` is constant and `children` is empty for this query, so every `CREATE NAMED
    /// COLLECTION` collides in the base tree hash. The rewrite-rule matcher uses the tree hash
    /// for semantic equality, so fold every semantic field the formatter emits.
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);

    updateTreeHashField(hash_state, collection_name);
    hash_state.update(or_replace);
    hash_state.update(if_not_exists);
    updateTreeHashField(hash_state, cluster);

    hash_state.update(changes.size());
    for (const auto & change : changes)
    {
        updateTreeHashField(hash_state, change.name);
        /// Fold exactly what the formatter emits for the value with secrets shown (the debug-build
        /// consistency check formats with `show_secrets = true`), so different values do not
        /// collide and the hash stays stable across the format -> parse round-trip.
        updateTreeHashField(hash_state, applyVisitor(FieldVisitorToString(), change.value));
        /// `overridability` is emitted per change (`[NOT ]OVERRIDABLE`), keyed by name.
        auto it = overridability.find(change.name);
        hash_state.update(it != overridability.end());
        if (it != overridability.end())
            hash_state.update(it->second);
    }
}

void ASTCreateNamedCollectionQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << "CREATE ";
    if (or_replace)
        ostr << "OR REPLACE ";
    ostr << "NAMED COLLECTION ";
    if (if_not_exists)
        ostr << "IF NOT EXISTS ";
    ostr << backQuoteIfNeed(collection_name);

    formatOnCluster(ostr, settings);

    ostr << " AS ";
    bool first = true;
    for (const auto & change : changes)
    {
        if (!first)
            ostr << ", ";
        else
            first = false;

        formatSettingName(change.name, ostr);

        if (settings.show_secrets)
            ostr << " = " << applyVisitor(FieldVisitorToString(), change.value);
        else
            ostr << " = " << HIDDEN_SECRET_LITERAL;
        auto override_value = overridability.find(change.name);
        if (override_value != overridability.end())
            ostr << " " << (override_value->second ? "" : "NOT ") << "OVERRIDABLE";
    }
}

}
