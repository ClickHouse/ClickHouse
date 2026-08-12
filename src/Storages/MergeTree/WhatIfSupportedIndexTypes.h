#pragma once

#include <base/types.h>

namespace DB
{

/// index types the estimator can model, fail closed so a new type is rejected until checked
bool isIndexTypeSupportedByWhatIf(const String & index_type);

/// comma-separated, for error messages
String getIndexTypesSupportedByWhatIf();

}
