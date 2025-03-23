#pragma once
#include <Core/NamesAndTypes.h>
#include <Core/Field.h>

namespace DB
{

struct DataLakePartitionColumn
{
    NameAndTypePair name_and_type;
    Field value;

    bool operator ==(const DataLakePartitionColumn & other) const = default;
};

/// Data file -> partition columns
using DataLakePartitionColumns = std::unordered_map<std::string, std::vector<DataLakePartitionColumn>>;

}
