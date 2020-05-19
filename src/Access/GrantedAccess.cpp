#include <Access/GrantedAccess.h>


namespace DB
{

GrantedAccess::GrantsAndPartialRevokes GrantedAccess::getGrantsAndPartialRevokes() const
{
    GrantsAndPartialRevokes res;
    res.grants_with_grant_option = access_with_grant_option.getGrants();
    AccessRights access_without_gg = access;
    access_without_gg.revoke(res.grants_with_grant_option);
    auto gr = access_without_gg.getGrantsAndPartialRevokes();
    res.grants = std::move(gr.grants);
    res.revokes = std::move(gr.revokes);
    AccessRights access_with_grant_options_without_r = access_with_grant_option;
    access_with_grant_options_without_r.grant(res.revokes);
    res.revokes_grant_option = access_with_grant_options_without_r.getPartialRevokes();
    return res;
}

}
