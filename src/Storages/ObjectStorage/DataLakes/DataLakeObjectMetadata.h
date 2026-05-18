#pragma once
#include <Interpreters/ActionsDAG.h>
#include <Core/Field.h>

namespace DB
{

struct DataLakeObjectMetadata
{
    std::shared_ptr<ActionsDAG> transform;
};

}
