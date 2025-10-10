#include <Parsers/Access/ASTDropAccessEntityQuery.h>
#include <Parsers/Access/ASTRowPolicyName.h>
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
    auto res = std::make_shared<ASTDropAccessEntityQuery>(*this);

    if (row_policy_names)
        res->row_policy_names = std::static_pointer_cast<ASTRowPolicyNames>(row_policy_names->clone());

    return res;
}


void ASTDropAccessEntityQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    ostr << (settings.hilite ? hilite_keyword : "")
                  << "DROP " << AccessEntityTypeInfo::get(type).name
                  << (if_exists ? " IF EXISTS" : "")
                  << (settings.hilite ? hilite_none : "");

    if (type == AccessEntityType::ROW_POLICY)
    {
        ostr << " ";
        row_policy_names->format(ostr, settings);
    }
    else
        formatNames(names, ostr);

    if (!storage_name.empty())
        ostr << (settings.hilite ? hilite_keyword : "")
                      << " FROM " << (settings.hilite ? hilite_none : "")
                      << backQuoteIfNeed(storage_name);

    formatOnCluster(ostr, settings);
}


void ASTDropAccessEntityQuery::replaceEmptyDatabase(const String & current_database) const
{
    if (row_policy_names)
        row_policy_names->replaceEmptyDatabase(current_database);
}
}
