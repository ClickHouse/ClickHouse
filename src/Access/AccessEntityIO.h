#pragma once

#include <base/types.h>
#include <Parsers/IAST_fwd.h>
#include <memory>

namespace DB
{

struct IAccessEntity;
using AccessEntityPtr = std::shared_ptr<const IAccessEntity>;

String serializeAccessEntity(const IAccessEntity & entity);

AccessEntityPtr deserializeAccessEntity(const String & definition, const String & file_path = "");

/// Access entity definitions (DiskAccessStorage files, grant queries in users.xml) are never passed
/// through query parameter substitution, so query parameters in them must be rejected.
void checkAccessEntityHasNoQueryParameters(const ASTPtr & query);

}
