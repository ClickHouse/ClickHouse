#pragma once

#include <Interpreters/SystemLog.h>


namespace DB
{

struct PartLogElement
{
    enum Type
    {
        NEW_PART = 1,
        MERGE_PARTS = 2,
        DOWNLOAD_PART = 3,
        REMOVE_PART = 4,
        MUTATE_PART = 5,
    };

    Type event_type = NEW_PART;

    time_t event_time = 0;
    UInt64 duration_ms = 0;

    String database_name;
    String table_name;
    String part_name;

    /// Size of the part
    UInt64 rows = 0;

    /// Size of files in filesystem
    UInt64 bytes_compressed_on_disk = 0;

    /// Makes sense for merges and mutations.
    Strings source_part_names;
    UInt64 bytes_uncompressed = 0;
    UInt64 rows_read = 0;
    UInt64 bytes_read_uncompressed = 0;

    /// Was the operation successful?
    UInt16 error = 0;
    String exception;

    static std::string name() { return "PartLog"; }

    static Block createBlock();
    void appendToBlock(Block & block) const;
};

struct MergeTreeDataPart;


/// Instead of typedef - to allow forward declaration.
class PartLog : public SystemLog<PartLogElement>
{
    using SystemLog<PartLogElement>::SystemLog;

    using MutableDataPartPtr = std::shared_ptr<MergeTreeDataPart>;
    using MutableDataPartsVector = std::vector<MutableDataPartPtr>;

public:
    /// Add a record about creation of new part.
    static bool addNewPart(Context & context, const MutableDataPartPtr & part, UInt64 elapsed_ns,
                           const ExecutionStatus & execution_status = {});
    static bool addNewParts(Context & context, const MutableDataPartsVector & parts, UInt64 elapsed_ns,
                            const ExecutionStatus & execution_status = {});
};

}
