#include <Storages/MergeTree/ColumnIdHelper.h>

#include <Storages/MergeTree/ColumnIdMapping.h>

namespace DB
{

ColumnId getColumnIdOrPartName(const NameAndTypePair & requested_column, const String & name_in_part)
{
    if (requested_column.column_id.empty())
        return ColumnId{name_in_part};
    return requested_column.getColumnId();
}

std::optional<String> tryGetCurrentColumnName(const NameAndTypePair & part_column, const ColumnIdMapping * mapping)
{
    if (mapping && !part_column.column_id.empty())
        return mapping->tryGetColumnName(part_column.getColumnId());
    return part_column.name;
}

}
