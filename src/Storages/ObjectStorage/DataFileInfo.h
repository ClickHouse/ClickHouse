#include <optional>
#include <vector>

#include "Interpreters/ActionsDAG.h"

namespace DB
{

struct DataFileInfo
{
    String data_path;
    std::optional<NamesAndTypes> initial_schema;
    std::optional<ActionsDAG> schema_transform;

    bool operator==(DataFileInfo & other_info) { return data_path == other_info.data_path; }
};

using DataFileInfos = std::vector<DataFileInfo>;

}