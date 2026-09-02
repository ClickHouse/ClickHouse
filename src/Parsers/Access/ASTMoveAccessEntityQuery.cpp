#include <Parsers/Access/ASTMoveAccessEntityQuery.h>
#include <Parsers/Access/ASTRowPolicyName.h>
#include <Common/SipHash.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{
namespace
{
    void formatNames(const Strings & names, WriteBuffer & ostr)
    {
        bool need_comma = false;
        for (const auto & name : names)
        {
            if (std::exchange(need_comma, true))
                ostr << ',';
            ostr << ' ' << backQuoteIfNeed(name);
        }
    }
}

String ASTMoveAccessEntityQuery::getID(char) const
{
    return String("MOVE ") + toString(type) + " query";
}

ASTPtr ASTMoveAccessEntityQuery::clone() const
{
    auto res = make_intrusive<ASTMoveAccessEntityQuery>(*this);

    if (row_policy_names)
        res->row_policy_names = boost::static_pointer_cast<ASTRowPolicyNames>(row_policy_names->clone());

    return res;
}

void ASTMoveAccessEntityQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
    /// Fold the semantic fields kept outside `children`. See the header comment for why the
    /// rewrite-rule matcher needs this. Mirror the formatter exactly — it emits `row_policy_names`
    /// for `ROW_POLICY` and the plain `names` otherwise — so every folded field survives the
    /// format -> parse round-trip that the debug-build AST consistency check requires.
    hash_state.update(type);

    if (type == AccessEntityType::ROW_POLICY)
    {
        hash_state.update(static_cast<bool>(row_policy_names));
        if (row_policy_names)
            row_policy_names->updateTreeHash(hash_state, ignore_aliases);
    }
    else
    {
        hash_state.update(names.size());
        for (const auto & name : names)
            hash_state.update(name);
    }

    hash_state.update(storage_name);
    hash_state.update(cluster);
}

void ASTMoveAccessEntityQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    ostr
                  << "MOVE " << AccessEntityTypeInfo::get(type).name
                 ;

    if (type == AccessEntityType::ROW_POLICY)
    {
        ostr << " ";
        row_policy_names->format(ostr, settings);
    }
    else
        formatNames(names, ostr);

    ostr
                  << " TO "
                  << backQuoteIfNeed(storage_name);

    formatOnCluster(ostr, settings);
}

void ASTMoveAccessEntityQuery::replaceEmptyDatabase(const String & current_database) const
{
    if (row_policy_names)
        row_policy_names->replaceEmptyDatabase(current_database);
}
}
