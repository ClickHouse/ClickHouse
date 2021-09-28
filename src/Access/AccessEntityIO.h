#pragma once

#include <Access/IAccessEntity.h>

namespace DB
{

String serializeAccessEntity(const IAccessEntity & entity);

AccessEntityPtr deserializeAccessEntity(const String & definition, const String & path);

}
