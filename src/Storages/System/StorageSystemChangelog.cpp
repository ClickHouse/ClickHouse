#include <Storages/System/StorageSystemChangelog.h>
#include <Storages/System/SystemTableSourceRegistry.h>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>

#include <charconv>


namespace DB
{

ColumnsDescription StorageSystemChangelog::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"version",
         std::make_shared<DataTypeTuple>(
             DataTypes{std::make_shared<DataTypeUInt16>(), std::make_shared<DataTypeUInt8>(), std::make_shared<DataTypeUInt16>()},
             Names{"major", "minor", "patch"}),
         "Version of the release the entry belongs to, as a comparable tuple. The curated changelog describes feature releases, so `patch` is 0."},
        {"release_date", std::make_shared<DataTypeDate>(), "Date of the release."},
        {"is_lts", std::make_shared<DataTypeUInt8>(), "1 if the release is a long-term support release."},
        {"category", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
         "Changelog section the entry belongs to, e.g. 'New Feature', 'Performance Improvement', 'Bug Fix'."},
        {"description", std::make_shared<DataTypeString>(), "Text of the changelog entry."},
        {"pull_requests", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>()),
         "Numbers of the pull requests the entry references."},
        {"authors", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Authors of the referenced pull requests."},
    };
}

void StorageSystemChangelog::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    for (size_t i = 0; i < auto_changelog_size; ++i)
    {
        const auto & entry = auto_changelog[i];

        res_columns[0]->insert(Tuple{entry.version_major, entry.version_minor, entry.version_patch});
        res_columns[1]->insert(entry.release_date_day_num);
        res_columns[2]->insert(entry.is_lts);
        res_columns[3]->insert(String(entry.category));
        res_columns[4]->insert(String(entry.description));

        Array pull_requests;
        {
            std::string_view list(entry.pull_requests);
            while (!list.empty())
            {
                size_t comma = list.find(',');
                std::string_view token = list.substr(0, comma);
                UInt32 number = 0;
                std::from_chars(token.data(), token.data() + token.size(), number);
                pull_requests.push_back(number);
                list = (comma == std::string_view::npos) ? std::string_view{} : list.substr(comma + 1);
            }
        }
        res_columns[5]->insert(pull_requests);

        Array authors;
        {
            std::string_view list(entry.authors);
            while (!list.empty())
            {
                size_t tab = list.find('\t');
                authors.push_back(String(list.substr(0, tab)));
                list = (tab == std::string_view::npos) ? std::string_view{} : list.substr(tab + 1);
            }
        }
        res_columns[6]->insert(authors);
    }
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemChangelog) }
