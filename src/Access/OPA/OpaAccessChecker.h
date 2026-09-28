#pragma once

#include <Access/OPA/OpaClient.h>
#include <Access/OPA/OpaConfiguration.h>
#include <Access/OPA/OpaDecisionCache.h>
#include <Access/OPA/OpaRequest.h>
#include <Access/EnabledRowPolicies.h>


namespace DB
{

struct AccessRightsElement;

/** Decides whether a policy governs a particular access check, and asks OPA about the ones it does.
  *
  * A decision from OPA can only take access away. Native grants are evaluated first and OPA is
  * consulted afterwards, so a policy narrows what a user was already granted and can never widen it.
  */
class OpaAccessChecker
{
public:
    explicit OpaAccessChecker(OpaConfigurationPtr configuration_);

    /// Whether a check on this element has to be sent to OPA at all. Makes no request, so it is
    /// cheap enough to call on every access check.
    bool governs(const String & user_name, const AccessRightsElement & element) const;

    /// Asks OPA about a check that native grants have already allowed. Any failure propagates.
    /// A `cache` memoizes the answer for the rest of the query; passing null asks every time.
    bool isAllowed(
        const AccessRightsElement & element,
        const OpaRequestContext & request_context,
        const OpaDecisionCachePtr & cache) const;

    /// The row filter a policy applies to a table, or null when it applies none. Multiple filters are
    /// combined with AND, so each one can only remove rows.
    RowPolicyFilterPtr getRowFilter(
        const String & database,
        const String & table,
        const OpaRequestContext & request_context,
        const OpaDecisionCachePtr & cache) const;

private:
    const OpaConfigurationPtr configuration;
    const OpaClient client;
};

}
