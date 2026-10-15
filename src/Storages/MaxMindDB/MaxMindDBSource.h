#pragma once

#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

#include <memory>

namespace DB
{
struct MaxMindDBSettings;
struct StorageID;

struct MaxMindDBFile
{
    String path;
    String version;

    MaxMindDBFile();
    ~MaxMindDBFile();
};

/// File metadata and mapping are pinned by each reader generation.
class MaxMindDBSource : public WithContext
{
public:
    MaxMindDBSource(ASTs & arguments, ContextPtr context_, const StorageID & table_id, bool check_access);
    ~MaxMindDBSource();

    /// A null result means the file version has not changed.
    std::unique_ptr<MaxMindDBFile> load(const String & current_version, const MaxMindDBSettings & settings) const;
    void checkVersion(const MaxMindDBFile & file) const;

private:
    String local_path;
};
}
