#include <Parsers/Access/ASTDropAccessEntityQuery.h>
#include <Parsers/Access/ASTRowPolicyName.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{
namespace
{
    void formatNames(const Strings & names, IAST::FormattingBuffer out)
    {
        bool need_comma = false;
        for (const auto & name : names)
        {
            if (std::exchange(need_comma, true))
                out.ostr << ',';
            out.ostr << ' ' << backQuoteIfNeed(name);
        }
    }
}


String ASTDropAccessEntityQuery::getID(char) const
{
    return String("DROP ") + toString(type) + " query";
}


ASTPtr ASTDropAccessEntityQuery::clone() const
{
    auto res = std::make_shared<ASTDropAccessEntityQuery>(*this);

    if (row_policy_names)
        res->row_policy_names = std::static_pointer_cast<ASTRowPolicyNames>(row_policy_names->clone());

    return res;
}


void ASTDropAccessEntityQuery::formatImpl(FormattingBuffer out) const
{
    out.writeKeyword("DROP ");
    out.writeKeyword(AccessEntityTypeInfo::get(type).name);
    out.writeKeyword(if_exists ? " IF EXISTS" : "");

    if (type == AccessEntityType::ROW_POLICY)
    {
        out.ostr << " ";
        row_policy_names->formatImpl(out);
    }
    else
        formatNames(names, out);

    formatOnCluster(out);
}


void ASTDropAccessEntityQuery::replaceEmptyDatabase(const String & current_database) const
{
    if (row_policy_names)
        row_policy_names->replaceEmptyDatabase(current_database);
}
}
