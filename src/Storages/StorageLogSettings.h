#pragma once

#include <base/types.h>

namespace DB
{
    class ASTStorage;

    String getDiskName(ASTStorage & storage_def);
}
