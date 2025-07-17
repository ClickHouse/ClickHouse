#pragma once
#include <Interpreters/ActionsDAG.h>
#include <Core/Field.h>

namespace ffi
{
    struct Expression;
}

namespace DB
{

struct DataLakeObjectMetadata
{
    std::shared_ptr<ActionsDAG> transform;
    std::vector<std::pair<NameAndTypePair, DB::Field>> partition_values;
    const ffi::Expression * transformm;
};

}
