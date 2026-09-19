#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// One entry of the curated changelog, as produced by StorageSystemChangelog.sh from CHANGELOG.md.
struct AutoChangelogEntry
{
    UInt16 version_major;
    UInt8 version_minor;
    UInt16 version_patch;
    UInt16 release_date_day_num;
    UInt8 is_lts;
    const char * category;
    const char * description;
    const char * pull_requests; /// comma-separated numbers
    const char * authors; /// tab-separated names
};

extern const AutoChangelogEntry auto_changelog[];
extern const size_t auto_changelog_size;


/** System table "changelog" with the entries of the curated ClickHouse changelog.
  */
class StorageSystemChangelog final : public IStorageSystemOneBlock
{
protected:
    void fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override
    {
        return "SystemChangelog";
    }

    static ColumnsDescription getColumnsDescription();
};

}
