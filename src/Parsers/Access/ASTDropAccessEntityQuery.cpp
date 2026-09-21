#include <Parsers/Access/ASTDropAccessEntityQuery.h>
#include <Parsers/Access/ASTRowPolicyName.h>
#include <Access/MaskingPolicy.h>
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


String ASTDropAccessEntityQuery::getID(char) const
{
    return String("DROP ") + toString(type) + " query";
}


ASTPtr ASTDropAccessEntityQuery::clone() const
{
    auto res = make_intrusive<ASTDropAccessEntityQuery>(*this);

    if (row_policy_names)
        res->row_policy_names = boost::static_pointer_cast<ASTRowPolicyNames>(row_policy_names->clone());

    if (masking_policy_name)
        res->masking_policy_name = std::make_shared<MaskingPolicyName>(*masking_policy_name);

    return res;
}


void ASTDropAccessEntityQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
    /// Fold the semantic fields kept outside `children`. See the header comment for why the
    /// rewrite-rule matcher needs this. Mirror the formatter exactly — it emits `row_policy_names`
    /// for `ROW_POLICY`, `masking_policy_name` for `MASKING_POLICY` and the plain `names`
    /// otherwise — so every folded field survives the format -> parse round-trip that the
    /// debug-build AST consistency check requires.
    hash_state.update(type);
    hash_state.update(if_exists);

    if (type == AccessEntityType::ROW_POLICY)
    {
        hash_state.update(static_cast<bool>(row_policy_names));
        if (row_policy_names)
            row_policy_names->updateTreeHash(hash_state, ignore_aliases);
    }
    else if (type == AccessEntityType::MASKING_POLICY)
    {
        hash_state.update(static_cast<bool>(masking_policy_name));
        if (masking_policy_name)
            hash_state.update(masking_policy_name->toString());
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


void ASTDropAccessEntityQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    ostr
                  << "DROP " << AccessEntityTypeInfo::get(type).name
                  << (if_exists ? " IF EXISTS" : "")
                 ;

    if (type == AccessEntityType::ROW_POLICY)
    {
        ostr << " ";
        row_policy_names->format(ostr, settings);
    }
    else if (type == AccessEntityType::MASKING_POLICY)
    {
        ostr << " " << masking_policy_name->toString();
    }
    else
        formatNames(names, ostr);

    if (!storage_name.empty())
        ostr
                      << " FROM "
                      << backQuoteIfNeed(storage_name);

    formatOnCluster(ostr, settings);
}


void ASTDropAccessEntityQuery::replaceEmptyDatabase(const String & current_database) const
{
    if (row_policy_names)
        row_policy_names->replaceEmptyDatabase(current_database);

    if (masking_policy_name && masking_policy_name->database.empty())
        masking_policy_name->database = current_database;
}
}
