#include <Storages/MergeTree/UniqueKeySet.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Columns/ColumnVector.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

UniqueKeySet::Hash128 UniqueKeySet::hashRow(const Block & block, const Names & key_columns, size_t row_idx)
{
    SipHash hash;

    for (const auto & col_name : key_columns)
    {
        const auto & column = block.getByName(col_name).column;
        /// IColumn::updateHashWithValue handles ALL ClickHouse data types:
        /// String, FixedString, Int/UInt, Float, Decimal, Nullable, Array, Tuple, Map, etc.
        column->updateHashWithValue(row_idx, hash);
    }

    return hash.get128();
}

std::vector<UniqueKeySet::Hash128> UniqueKeySet::hashAllRows(const Block & block, const Names & key_columns)
{
    const size_t num_rows = block.rows();
    std::vector<Hash128> hashes(num_rows);

    for (size_t i = 0; i < num_rows; ++i)
        hashes[i] = hashRow(block, key_columns, i);

    return hashes;
}

ColumnUInt8::MutablePtr UniqueKeySet::findDuplicates(const Block & block, const Names & key_columns) const
{
    const size_t num_rows = block.rows();
    auto result = ColumnUInt8::create(num_rows, 0);
    auto & result_data = result->getData();

    auto hashes = hashAllRows(block, key_columns);

    std::shared_lock lock(rw_mutex);

    for (size_t i = 0; i < num_rows; ++i)
    {
        if (set.find(hashes[i]) != set.end())
            result_data[i] = 1;
    }

    return result;
}

ColumnUInt8::MutablePtr UniqueKeySet::findDuplicatesWithSelf(const Block & block, const Names & key_columns) const
{
    const size_t num_rows = block.rows();
    auto result = ColumnUInt8::create(num_rows, 0);
    auto & result_data = result->getData();

    auto hashes = hashAllRows(block, key_columns);

    std::shared_lock lock(rw_mutex);

    /// Use a temporary set for intra-block deduplication.
    /// We track which hashes we've already seen within THIS block.
    HashSet<Hash128, UInt128Hash> seen_in_block;

    for (size_t i = 0; i < num_rows; ++i)
    {
        /// Check 1: Does this key already exist in the persistent set?
        if (set.find(hashes[i]) != set.end())
        {
            result_data[i] = 1;
            continue;
        }

        /// Check 2: Did we already see this key earlier in the same block?
        if (!seen_in_block.insert(hashes[i]).second)
        {
            result_data[i] = 1;
            continue;
        }
    }

    return result;
}

void UniqueKeySet::addFromBlock(const Block & block, const Names & key_columns)
{
    auto hashes = hashAllRows(block, key_columns);

    std::unique_lock lock(rw_mutex);

    for (const auto & hash : hashes)
        set.insert(hash);
}

void UniqueKeySet::removeFromBlock(const Block & block, const Names & key_columns)
{
    auto hashes = hashAllRows(block, key_columns);

    std::unique_lock lock(rw_mutex);

    for (const auto & hash : hashes)
        set.erase(hash);
}

void UniqueKeySet::rebuild(const MergeTreeData & storage, const Names & key_columns)
{
    rebuilding.store(true, std::memory_order_release);
    ready.store(false, std::memory_order_release);

    {
        std::unique_lock lock(rw_mutex);
        set.clear();
    }

    auto log = getLogger("UniqueKeySet");
    LOG_INFO(log, "Rebuilding unique key set for constraint with {} key columns", key_columns.size());

    /// Get all active data parts
    auto parts = storage.getDataPartsVectorForInternalUsage();

    size_t total_rows = 0;

    /// The actual column reading from data parts is delegated to the storage layer.
    /// MergeTreeData::initUniqueConstraints() calls rebuild() and then feeds
    /// the set with data by calling addFromBlock() for each part's key columns.
    ///
    /// TODO: Implement inline part reading here once the MergeTreeDataPartReader
    /// integration is finalized. For now, this clears the set and marks it as
    /// ready for addFromBlock() calls.

    ready.store(true, std::memory_order_release);
    rebuilding.store(false, std::memory_order_release);

    LOG_INFO(log, "Unique key set rebuilt: {} unique hashes from {} rows across {} parts",
        set.size(), total_rows, parts.size());
}

void UniqueKeySet::saveSnapshot(WriteBuffer & out) const
{
    std::shared_lock lock(rw_mutex);

    /// Write the number of entries
    writeIntBinary(static_cast<UInt64>(set.size()), out);

    /// Write each hash
    for (const auto & hash : set)
    {
        writeIntBinary(hash.items[0], out);
        writeIntBinary(hash.items[1], out);
    }
}

bool UniqueKeySet::loadSnapshot(ReadBuffer & in, const String & /*parts_checksum*/)
{
    std::unique_lock lock(rw_mutex);
    set.clear();

    try
    {
        UInt64 count;
        readIntBinary(count, in);

        for (UInt64 i = 0; i < count; ++i)
        {
            Hash128 hash;
            readIntBinary(hash.items[0], in);
            readIntBinary(hash.items[1], in);
            set.insert(hash);
        }

        ready.store(true, std::memory_order_release);
        return true;
    }
    catch (...)
    {
        set.clear();
        ready.store(false, std::memory_order_release);
        return false;
    }
}

size_t UniqueKeySet::size() const
{
    std::shared_lock lock(rw_mutex);
    return set.size();
}

size_t UniqueKeySet::getBufferSizeInBytes() const
{
    std::shared_lock lock(rw_mutex);
    return set.getBufferSizeInBytes();
}

} // namespace DB
