#pragma once
#include <Core/Field.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

struct DataLakeObjectMetadata
{
    std::shared_ptr<ActionsDAG> transform;
};
}
