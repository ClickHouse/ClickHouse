#pragma once

#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Storages/MergeTree/MergeType.h>
#include <Storages/MergeTree/MergeAlgorithm.h>


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
        MOVE_PART = 6,
    };

    /// Copy of MergeAlgorithm since values are written to disk.
    enum PartMergeAlgorithm
    {
        UNDECIDED = 0,
        VERTICAL = 1,
        HORIZONTAL = 2,
    };

    enum MergeReasonType
    {
        /// merge_reason is relevant only for event_type = 'MERGE_PARTS', in other cases it is NOT_A_MERGE
        NOT_A_MERGE = 1,
        /// Just regular merge
        REGULAR_MERGE = 2,
        /// Merge assigned to delete some data from parts (with TTLMergeSelector)
        TTL_DELETE_MERGE = 3,
        /// Merge with recompression
        TTL_RECOMPRESS_MERGE = 4,
    };

    String query_id;

    Type event_type = NEW_PART;
    MergeReasonType merge_reason = NOT_A_MERGE;
    PartMergeAlgorithm merge_algorithm = UNDECIDED;

    time_t event_time = 0;
    Decimal64 event_time_microseconds = 0;
    UInt64 duration_ms = 0;

    String database_name;
    String table_name;
    String part_name;
    String partition_id;
    String disk_name;
    String path_on_disk;

    MergeTreeDataPartType part_type;

    /// Size of the part
    UInt64 rows = 0;

    /// Size of files in filesystem
    UInt64 bytes_compressed_on_disk = 0;

    /// Makes sense for merges and mutations.
    Strings source_part_names;
    UInt64 bytes_uncompressed = 0;
    UInt64 rows_read = 0;
    UInt64 bytes_read_uncompressed = 0;
    UInt64 peak_memory_usage = 0;

    /// Was the operation successful?
    UInt16 error = 0;
    String exception;

    static std::string name() { return "PartLog"; }

    static MergeReasonType getMergeReasonType(MergeType merge_type);
    static PartMergeAlgorithm getMergeAlgorithm(MergeAlgorithm merge_algorithm_);

    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
    static const char * getCustomColumnList() { return nullptr; }
};

class IMergeTreeDataPart;


/// Instead of typedef - to allow forward declaration.
class PartLog : public SystemLog<PartLogElement>
{
    using SystemLog<PartLogElement>::SystemLog;

    using MutableDataPartPtr = std::shared_ptr<IMergeTreeDataPart>;
    using MutableDataPartsVector = std::vector<MutableDataPartPtr>;

public:
    /// Add a record about creation of new part.
    static bool addNewPart(ContextPtr context, const MutableDataPartPtr & part, UInt64 elapsed_ns,
                           const ExecutionStatus & execution_status = {});
    static bool addNewParts(ContextPtr context, const MutableDataPartsVector & parts, UInt64 elapsed_ns,
                            const ExecutionStatus & execution_status = {});
};

}
