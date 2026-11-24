#pragma once

#include <Common/FactoryRestrictions.h>

namespace DB
{

/// Whether `name` (or an alias) is registered in the factory of `kind`, ignoring the case.
/// Used to reject unknown names in the `factory_restrictions` config section.
bool isRegisteredInFactory(FactoryRestrictions::Kind kind, const String & name);

}
