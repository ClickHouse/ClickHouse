#include <Columns/IColumn.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemContribs.h>


extern const char * library_contribs[];

namespace DB
{
ColumnsDescription StorageSystemContribs::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Name of the library (directory basename under contrib/)."},
        {"path", std::make_shared<DataTypeString>(), "Repository-relative path to the contrib directory."},
        {"commit", std::make_shared<DataTypeString>(), "Pinned submodule commit SHA."},
        {"submodule_url", std::make_shared<DataTypeString>(), "Submodule URL as recorded in .gitmodules."},
        {"upstream_url", std::make_shared<DataTypeString>(), "Original upstream project URL when known; empty otherwise."},
        {"is_fork", std::make_shared<DataTypeUInt8>(), "1 if the submodule points to a ClickHouse-owned fork, 0 otherwise."},
        {"version", std::make_shared<DataTypeString>(), "Best-effort version string derived from the submodule's branch name or git tag."},
    };
}

void StorageSystemContribs::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    for (const auto * it = library_contribs; *it; it += 7)
    {
        res_columns[0]->insert(String(it[0]));
        res_columns[1]->insert(String(it[1]));
        res_columns[2]->insert(String(it[2]));
        res_columns[3]->insert(String(it[3]));
        res_columns[4]->insert(String(it[4]));
        res_columns[5]->insert(UInt8(it[5][0] == '1' ? 1 : 0));
        res_columns[6]->insert(String(it[6]));
    }
}
}
