#include <Parsers/Access/ASTDropAccessEntityQuery.h>
#include <Parsers/Access/ASTRowPolicyName.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Access/MaskingPolicy.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{
namespace
{
    void formatNames(const ASTUserNamesWithHost & names, WriteBuffer & ostr, const IAST::FormatSettings & settings)
    {
        bool need_comma = false;
        for (const auto & name : names)
        {
            if (std::exchange(need_comma, true))
                ostr << ",";
            ostr << " ";

            const auto & user_name = name->as<const ASTUserNameWithHost &>();
            if (user_name.usernameWasQueryParameter())
                user_name.format(ostr, settings);
            else
                ostr << backQuoteIfNeed(user_name.toString());
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

    res->children.clear();

    if (names)
    {
        res->names = boost::static_pointer_cast<ASTUserNamesWithHost>(names->clone());
        if (res->names->hasQueryParameters())
            res->children.push_back(res->names);
    }

    return res;
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
        formatNames(*names, ostr, settings);

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
