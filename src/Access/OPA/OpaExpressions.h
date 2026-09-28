#pragma once

#include <base/types.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

/// An expression a policy returned, together with the identity it should be resolved under.
struct OpaViewExpression
{
    String expression;
    /// The user whose privileges the expression is resolved and authorized with. Empty means the
    /// requesting user.
    String identity;
};

/** Parses an expression a policy returned.
  *
  * `description` names what is being parsed and appears in the error, because a policy author reading
  * a failure needs to know which rule produced the broken text. A parse failure propagates: an
  * expression that cannot be understood must not be quietly dropped, since dropping a row filter
  * shows more rows than the policy intended.
  */
ASTPtr parseOpaExpression(const String & expression, const String & description);

/// Parses an expression that will be applied as a row filter, rejecting the constructs a filter
/// cannot contain.
ASTPtr parseOpaRowFilterExpression(const String & expression, const String & description);

}
