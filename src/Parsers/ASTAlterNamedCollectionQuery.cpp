#include <Common/quoteString.h>
#include <Common/FieldVisitorToString.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>
#include <Parsers/ASTAlterNamedCollectionQuery.h>
#include <Parsers/formatSettingName.h>

namespace DB
{

ASTPtr ASTAlterNamedCollectionQuery::clone() const
{
    return make_intrusive<ASTAlterNamedCollectionQuery>(*this);
}

void ASTAlterNamedCollectionQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
    /// Fold the semantic fields kept outside `children`. See the header comment for why the
    /// rewrite-rule matcher needs this. Each field is produced by the formatter, so it survives
    /// the format -> parse round-trip that the debug-build AST consistency check requires.
    hash_state.update(collection_name);
    hash_state.update(if_exists);
    hash_state.update(cluster);

    hash_state.update(changes.size());
    for (const auto & change : changes)
    {
        hash_state.update(change.name);
        /// Fold exactly what the formatter emits for the value with secrets shown (the debug-build
        /// consistency check formats with `show_secrets = true`), so different values do not
        /// collide and the hash stays stable across the format -> parse round-trip.
        hash_state.update(applyVisitor(FieldVisitorToString(), change.value));
        /// `overridability` is emitted per change (`[NOT ]OVERRIDABLE`), keyed by name.
        auto it = overridability.find(change.name);
        hash_state.update(it != overridability.end());
        if (it != overridability.end())
            hash_state.update(it->second);
    }

    hash_state.update(delete_keys.size());
    for (const auto & key : delete_keys)
        hash_state.update(key);
}

void ASTAlterNamedCollectionQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << "ALTER NAMED COLLECTION ";
    if (if_exists)
        ostr << "IF EXISTS ";
    ostr << backQuoteIfNeed(collection_name);
    formatOnCluster(ostr, settings);
    if (!changes.empty())
    {
        ostr << " SET ";
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
                ostr << " = '[HIDDEN]'";
            auto override_value = overridability.find(change.name);
            if (override_value != overridability.end())
                ostr << " " << (override_value->second ? "" : "NOT ") << "OVERRIDABLE";
        }
    }
    if (!delete_keys.empty())
    {
        ostr << " DELETE ";
        bool first = true;
        for (const auto & key : delete_keys)
        {
            if (!first)
                ostr << ", ";
            else
                first = false;

            formatSettingName(key, ostr);
        }
    }
}

}
