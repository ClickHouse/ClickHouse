#pragma once

#include <cstddef>
#include <memory>
#include <Processors/Chunk.h>
#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageIDMaybeEmpty.h>
#include <Core/Block_fwd.h>

#include <Common/Logger.h>
#include <base/defines.h>


namespace DB
{
enum class InsertDeduplicationVersions : uint8_t;

class InsertDependenciesBuilder;
using InsertDependenciesBuilderConstPtr = std::shared_ptr<const InsertDependenciesBuilder>;

struct DeduplicationHash
{
    enum class HashType : uint8_t
    {
        SYNC = 0,
        ASYNC = 1,
        UNIFIED = 2,
    };

    DeduplicationHash(UInt128 hash_, std::string partition_id_, HashType htype);

    static DeduplicationHash createUnifiedHash(UInt128 hash, std::string partition_id);
    static DeduplicationHash createSyncHash(UInt128 hash, std::string partition_id);
    static DeduplicationHash createAsyncHash(UInt128 hash, std::string partition_id);

    DeduplicationHash(const DeduplicationHash & other) = default;
    DeduplicationHash(DeduplicationHash && other) = default;
    DeduplicationHash & operator =(const DeduplicationHash & other) = default;
    DeduplicationHash & operator =(DeduplicationHash && other) = default;

    /// It returns string representation of the hash
    std::string getBlockId() const;
    /// It returns full path to the hash file on keeper
    std::string getPath(const std::string & storage_path) const;

    void setConflictPartName(const std::string & part_name);
    bool hasConflictPartName() const;
    std::string getConflictPartName() const;

    UInt128 hash = 0;
    std::string partition_id;
    HashType hash_type = HashType::UNIFIED;

    std::optional<std::string> conflicted_part_name;
};


std::vector<std::string> getDeduplicationBlockIds(const std::vector<DeduplicationHash> & deduplication_hashes);
std::vector<std::string> getDeduplicationPaths(std::string storage_path, const std::vector<DeduplicationHash> & deduplication_hashes);


class DeduplicationInfo : public ChunkInfo
{
protected:
    // InsertDependenciesBuilder::createChainForDeduplicationRetry needs access to private members
    friend class InsertDependenciesBuilder;
    /// src/Storages/MergeTree/tests/gtest_async_inserts.cpp
    friend std::vector<Int64> testSelfDeduplicate(std::vector<Int64> data, std::vector<size_t> offsets, std::vector<String> hashes);

public:
    using Ptr = std::shared_ptr<DeduplicationInfo>;

    DeduplicationInfo(const DeduplicationInfo & other);
    DeduplicationInfo(DeduplicationInfo && other) = default;

    static Ptr create(bool async_insert_, InsertDeduplicationVersions unification_stage);

    ChunkInfo::Ptr merge(const ChunkInfo::Ptr & right) const override;
    Ptr mergeSelf(const Ptr & right) const;

    ChunkInfo::Ptr clone() const override;
    Ptr cloneSelf() const;

    bool isAsyncInsert() const { return is_async_insert; }
    bool isDisabled() const { return disabled; }
    struct FilterResult
    {
        std::shared_ptr<Block> filtered_block = nullptr;
        Ptr deduplication_info = nullptr;
        size_t removed_rows = 0;
        size_t removed_tokens = 0;
    };
    FilterResult deduplicateSelf(bool deduplication_enabled, const std::string & partition_id, ContextPtr context) const;
    FilterResult deduplicateBlock(const std::vector<std::string> & existing_block_ids, const std::string & partition_id, ContextPtr context) const;

    std::vector<DeduplicationHash> getDeduplicationHashes(const std::string & partition_id, bool deduplication_enabled) const;

    size_t getCount() const;
    size_t getRows() const;

    std::pair<std::string, size_t> debug(size_t offset) const;
    std::string debug() const;

    // for sync insert: if user token is empty then by_part_writer token would be calculated later by part writer
    // for async insert: if user token is empty then by_data_hash token would be calculated later
    void setUserToken(const String & token, size_t count);
    void setSourceBlockNumber(size_t block_number);
    void setRootViewID(const StorageIDMaybeEmpty & id);

    /// use for provide deduplication hash for the part from one partition
    void setPartWriterHashForPartition(UInt128 hash, size_t count) const;
    /// use for provide deduplication hash for the chunk with maybe multiple partitions in it
    void setPartWriterHashes(const std::vector<UInt128> & partitions_hashes, size_t count) const;
    /// hash from part writer would be used as user token for dependent views if no user token has been set before
    void redefineTokensWithDataHash(const Block & block);

    void setViewID(const StorageID & id);
    void setViewBlockNumber(size_t block_number);

    void setInsertDependencies(InsertDependenciesBuilderConstPtr insert_dependencies_);
    void updateOriginalBlock(const Chunk & chunk, SharedHeader header);

    const std::vector<StorageIDMaybeEmpty> & getVisitedViews() const;

private:
    DeduplicationInfo(bool async_insert_, InsertDeduplicationVersions unification_stage_);

    UInt128 calculateDataHash(size_t offset, const Block & block) const;
    // the old one hash
    DeduplicationHash getBlockHash(size_t offset, const std::string & partition_) const;
    // the new unified hash
    DeduplicationHash getBlockUnifiedHash(size_t offset, const std::string & partition_) const;
    std::vector<DeduplicationHash> chooseDeduplicationHashes(size_t offset, const std::string & partition_id) const;


    Ptr cloneSelfFilterImpl() const;
    std::set<size_t> filterSelf(const String & partition_id) const;
    std::set<size_t> filterOriginal(const std::vector<std::string> & collisions, const String & partition_id) const;
    FilterResult filterImpl(const std::set<size_t> & collision_offsets) const;

    Ptr cloneMergeImpl() const;

    FilterResult recalculateBlock(FilterResult && filtered, const std::string & partition_id, ContextPtr context) const;
    void truncateTokensForRetry();
    Block goRetry(SharedHeader && header, Chunk && filtered_data, Ptr filtered_info, const std::string & partition_id, ContextPtr context) const;

    size_t getTokenBegin(size_t pos) const;
    size_t getTokenEnd(size_t pos) const;
    size_t getTokenRows(size_t pos) const;

    std::unordered_map<std::string, std::vector<size_t>> buildBlockIdToOffsetsMap(const std::string & partition_id) const;

    enum class Level
    {
        SOURCE,
        VIEW,
    };

    LoggerPtr logger = getLogger("DedupInfo");
    size_t instance_id = 0;
    bool is_async_insert = false;
    InsertDeduplicationVersions unification_stage;


    InsertDependenciesBuilderConstPtr insert_dependencies;

    /// When true, no deduplication is performed
    bool disabled = false;

    mutable Level level = Level::SOURCE;

    struct TokenDefinition
    {
        // there is a difference how block ids are generated from these two types of tokens
        // if by_part_writer is set then it is used as is
        // if by_user is set then block id is calculated as a hash of this string extended with extra tokens
        // When both are empty then data hash is calculated and used as by_user token
        std::string by_user;
        std::optional<UInt128> by_part_writer;

        std::optional<UInt128> data_hash;

        struct Extra
        {
            enum Type
            {
                SOURCE_ID,
                SOURCE_NUMBER,
                VIEW_ID,
                VIEW_NUMBER,
            };

            Type type;

            using Range = std::pair<size_t, size_t>;
            std::variant<Range, StorageIDMaybeEmpty> value_variant;

            bool operator==(const Extra & other) const;
            static Extra asSourceID(const StorageIDMaybeEmpty & id);
            static Extra asSourceNumber(uint64_t number);
            static Extra asViewID(const StorageIDMaybeEmpty & id);
            static Extra asViewNumber(uint64_t number);
            std::string debug() const;
            std::string toString() const;
            // when debug=false it takes the right boundary for Range types
            // that supports backward compatibility with previous squasing/mergind logic
            // when the right token was used
            std::string toStringImpl(bool debug = false) const;
        };

        std::vector<Extra> extra_tokens;

        static TokenDefinition asUserToken(std::string token);

        std::string debug() const;
        void setDataToken(UInt128 token);
        bool empty() const;
        bool canBeExtended(const TokenDefinition & right) const;
        void doExtend(const TokenDefinition & right);
        bool operator==(const TokenDefinition & other) const;
    };

    void addExtraPart(const TokenDefinition::Extra & extra);

    mutable std::vector<TokenDefinition> tokens;
    std::vector<size_t> offsets; // points to the last row for each offset

    std::shared_ptr<Block> original_block;
    StorageIDMaybeEmpty original_block_view_id;

    std::vector<StorageIDMaybeEmpty> visited_views;

    StorageIDMaybeEmpty retried_view_id;
};

}
