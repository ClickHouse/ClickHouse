#pragma once

#include <common/types.h>

namespace DB
{
    class ASTStorage;

    String getDiskName(ASTStorage & storage_def);
}
