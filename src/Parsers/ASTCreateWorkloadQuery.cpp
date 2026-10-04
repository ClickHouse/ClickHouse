#include <Common/quoteString.h>
#include <Common/FieldVisitorToString.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>
#include <Parsers/ASTCreateWorkloadQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

ASTPtr ASTCreateWorkloadQuery::clone() const
{
    auto res = make_intrusive<ASTCreateWorkloadQuery>(*this);
    res->children.clear();

    res->workload_name = workload_name->clone();
    res->children.push_back(res->workload_name);

    if (workload_parent)
    {
        res->workload_parent = workload_parent->clone();
        res->children.push_back(res->workload_parent);
    }

    res->changes = changes;

    return res;
}

void ASTCreateWorkloadQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
    /// Fold the semantic fields kept outside `children`. See the header comment for why the
    /// rewrite-rule matcher needs this. Each field is produced by the formatter, so it survives
    /// the format -> parse round-trip that the debug-build AST consistency check requires.
    hash_state.update(or_replace);
    hash_state.update(if_not_exists);
    hash_state.update(cluster);

    hash_state.update(changes.size());
    for (const auto & change : changes)
    {
        hash_state.update(change.name);
        /// Fold exactly what the formatter emits for the value so different values do not collide
        /// and the hash stays stable across the format -> parse round-trip.
        hash_state.update(applyVisitor(FieldVisitorToString(), change.value));
        hash_state.update(change.resource);
    }
}

void ASTCreateWorkloadQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & format, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << "CREATE ";

    if (or_replace)
        ostr << "OR REPLACE ";

    ostr << "WORKLOAD ";

    if (if_not_exists)
        ostr << "IF NOT EXISTS ";

    ostr << backQuoteIfNeed(getWorkloadName());

    formatOnCluster(ostr, format);

    if (hasParent())
    {
        ostr << " IN ";
        ostr << backQuoteIfNeed(getWorkloadParent());
    }

    if (!changes.empty())
    {
        ostr << ' ' << "SETTINGS" << ' ';

        bool first = true;

        for (const auto & change : changes)
        {
            if (!first)
                ostr << ", ";
            else
                first = false;
            ostr << change.name << " = " << applyVisitor(FieldVisitorToString(), change.value);
            if (!change.resource.empty())
            {
                ostr << ' ' << "FOR" << ' ';
                ostr << backQuoteIfNeed(change.resource);
            }
        }
    }
}

String ASTCreateWorkloadQuery::getWorkloadName() const
{
    String name;
    tryGetIdentifierNameInto(workload_name, name);
    return name;
}

bool ASTCreateWorkloadQuery::hasParent() const
{
    return workload_parent != nullptr;
}

String ASTCreateWorkloadQuery::getWorkloadParent() const
{
    String name;
    tryGetIdentifierNameInto(workload_parent, name);
    return name;
}

}
