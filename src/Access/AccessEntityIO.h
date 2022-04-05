#pragma once

#include <base/types.h>
#include <memory>

namespace DB
{
struct IAccessEntity;
using AccessEntityPtr = std::shared_ptr<const IAccessEntity>;

String serializeAccessEntity(const IAccessEntity & entity);

AccessEntityPtr deserializeAccessEntity(const String & definition, const String & file_path = "");

}
