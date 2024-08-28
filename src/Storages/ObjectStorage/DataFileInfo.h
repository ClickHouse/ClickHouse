#pragma once

#include <optional>
#include <vector>

#include "Interpreters/ActionsDAG.h"

namespace DB
{

struct DataFileInfo
{
    String data_path;
    std::shared_ptr<NamesAndTypesList> initial_schema;
    std::shared_ptr<ActionsDAG> schema_transform;

    DataFileInfo(
        String data_path_,
        std::shared_ptr<NamesAndTypesList> initial_schema_ = nullptr,
        std::shared_ptr<ActionsDAG> schema_transform_ = nullptr)
        : data_path(data_path_), initial_schema(initial_schema_), schema_transform(schema_transform_)
    {
    }

    bool operator==(const DataFileInfo & other_info) const { return data_path == other_info.data_path; }
};

using DataFileInfos = std::vector<DataFileInfo>;

}
