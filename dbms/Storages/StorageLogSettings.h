#pragma once

#include <Core/Types.h>

namespace DB
{
    class ASTStorage;

    String getDiskName(ASTStorage & storage_def);
}
