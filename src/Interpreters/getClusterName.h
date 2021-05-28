#pragma once

#include <string>
#include <Parsers/IAST_fwd.h>

namespace DB
{

/// Get the cluster name from AST.
/** The name of the cluster is the name of the tag in the xml configuration.
  * Usually it is parsed as an identifier. That is, it can contain underscores, but can not contain hyphens,
  *  provided that the identifier is not in backquotes.
  * But in xml, as a tag name, it's more common to use hyphens.
  * This name will be parsed as an expression with an operator minus - not at all what you need.
  * Therefore, consider this case separately.
  */
String getClusterName(const IAST & node);

String getClusterNameAndMakeLiteral(ASTPtr & node);

}
