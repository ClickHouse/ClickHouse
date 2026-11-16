#pragma once

#include <Core/Defines.h>
#include <IO/ReadBuffer.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Common/PODArray.h>
#include <base/sort.h>

#include <algorithm>
#include <limits>
#include <memory>
#include <queue>
#include <span>
#include <type_traits>
#include <vector>

/// Merges runs of sorted distinct keys of a set on disk into the distinct keys in ascending order. `Key` is
/// an unsigned integer of 8 to 256 bits. `makeChains` groups runs that follow each other into chains, and
/// `merge` reads each chain with a `ChainReader` and passes the keys to `OutputBatches`, which collects them
/// into batches. A single chain is the source of the keys as it is, and the source of two or more chains is a
/// `Tournament` of them. Both sources pass their keys with `passTo`.
namespace DB::DiskSetRunMerge
{

/// A read of a file takes this many keys at once, and so does a write of the keys that a merge passes to a
/// file. Batches of 64 KiB keep the reads and writes of a file large and the memory of a wide merge small.
template <typename Key>
constexpr size_t keys_per_batch = 64 * 1024 / sizeof(Key);

/// A run to merge: its sorted distinct keys in memory or the temporary file that holds them, and its first
/// and last keys. The keys and the file outlive the merge.
template <typename Key>
struct Run
{
    std::span<const Key> keys; /// Empty for a run in a file
    TemporaryDataBuffer * file = nullptr; /// Null for a run in memory
    Key first;
    Key last;

    static Run inMemory(std::span<const Key> keys)
    {
        chassert(!keys.empty());
        return {keys, nullptr, keys.front(), keys.back()};
    }

    static Run inFile(TemporaryDataBuffer & file, const Key & first, const Key & last) { return {{}, &file, first, last}; }

    /// Returns the number of keys of the run. A file holds raw keys, so its size gives their number.
    [[nodiscard]] size_t size() const { return file ? file->getStat().uncompressed_size / sizeof(Key) : keys.size(); }
};

/// Runs that follow each other, each starting where or after the previous one ends.
template <typename Key>
using Chain = std::vector<Run<Key>>;

/// Reads a chain in batches of keys: a run in memory is one batch, and a run in a file is read a batch at a
/// time. A run that starts with the last key of the previous run repeats that key, which the chain passes
/// once. A chain reads one file at a time.
///
/// A single chain passes its keys a batch at a time with `passTo`, and a tournament takes them one at a time
/// with `current` and `advance`.
template <typename Key>
class ChainReader
{
public:
    /// Prepares to read `runs_`, which hold keys, and reads the first batch.
    explicit ChainReader(Chain<Key> runs_)
        : runs(std::move(runs_))
    {
        readBatch(); /// Makes the first key current
    }

    [[nodiscard]] const Key & firstKey() const { return runs.front().first; }
    [[nodiscard]] const Key & lastKey() const { return runs.back().last; }

    /// Whether the chain has no more keys.
    [[nodiscard]] bool ended() const { return position == end; }

    /// The next key of the chain, which exists until the chain ends.
    [[nodiscard]] const Key & current() const { return *position; }

    /// Takes the current key and returns whether the chain has another.
    bool advance() { return ++position != end || readBatch(); } /// Reads the next batch when this one ends

    /// Passes the keys that the merge has not taken, which ascend without repeats, to `out`, and returns
    /// false once `out` stops the merge.
    template <typename Output>
    bool passTo(Output & out)
    {
        do
        {
            if (!out.add(batch()))
                return false;
        } while (readBatch());
        return true;
    }

private:
    /// The keys of the current batch that the merge has not taken, which are empty once the chain ends.
    [[nodiscard]] std::span<const Key> batch() const { return {position, end}; }

    /// Moves to the next batch of the file being read or to the first batch of the next run, and returns
    /// whether the chain has one.
    bool readBatch()
    {
        while (true)
        {
            if (in)
            {
                const size_t bytes = in->read(reinterpret_cast<char *>(buffer.data()), buffer.size() * sizeof(Key));
                chassert(bytes % sizeof(Key) == 0); /// A file holds whole keys
                if (!bytes)
                {
                    in.reset(); /// Frees the buffers of the file that ended
                    skip_first_key = false;
                    continue;
                }
                setBatch({buffer.data(), bytes / sizeof(Key)}); /// Fewer keys only at the end of the file
            }
            else if (next_run < runs.size())
            {
                const auto & run = runs[next_run];
                skip_first_key = next_run > 0 && run.first == runs[next_run - 1].last;
                ++next_run;
                if (run.file)
                {
                    in = run.file->read(); /// Opens the file of the run
                    buffer.resize(keys_per_batch<Key>); /// No-op after the first file of the chain
                    continue; /// Reads the first batch of the file
                }
                setBatch(run.keys); /// A run in memory is one batch
            }
            else
            {
                setBatch({}); /// Makes `ended` true
                return false;
            }

            if (skip_first_key) /// The run starts with the last key of the previous run
            {
                ++position;
                skip_first_key = false;
            }
            if (position != end) /// Otherwise, the batch held only the repeated key
                return true;
        }
    }

    void setBatch(std::span<const Key> keys)
    {
        position = keys.data();
        end = keys.data() + keys.size();
    }

    std::vector<Run<Key>> runs;
    size_t next_run = 0; /// The next run to read
    bool skip_first_key = false; /// Set for a run that repeats the last key of the previous run
    std::unique_ptr<ReadBuffer> in; /// The file being read, if any
    PODArray<Key> buffer; /// The batch read from the file

    /// The keys of the current batch that the merge has not taken.
    const Key * position = nullptr;
    const Key * end = nullptr;
};

/// Splits runs into as few chains as possible, so that a merge compares keys only between runs that overlap.
/// The runs are taken in the order of their first keys. Each joins the chain that ends first, if that chain
/// ends before or where the run starts, and starts a chain otherwise. Any chain that has ended would do, and
/// a heap of the chains by their last keys finds the one that ends first, so the split takes O(R log R) time
/// for R runs. Runs of keys that arrive in order make one chain, even when a key repeats across the boundary
/// of two runs.
///
/// For example, these five runs, in any order, each drawn from its first to its last key:
///
///     keys  0                   10                  20                  30                  40
///     R1      [1-------------9]
///     R2          [3--------------------------------------------------30]
///     R3                        [10---------------20]
///     R4                                            [20-----25]
///     R5                                                                  [31-------------40]
///
/// make two chains, which a merge reads from left to right, each as one run:
///
///     chain 1   R1 [1-9]  R3 [10-20]  R4 [20-25]  R5 [31-40]
///     chain 2   R2 [3-30]
///
/// Taken by their first keys, R1 starts chain 1, and R2 starts chain 2, as it starts before R1 ends. Then
/// R3, R4 and R5 join chain 1: R3 and R5 start after the chain ends, and R4 starts with the last key of the
/// chain, which the chain passes once.
///
/// This saves comparisons: a merge concatenates the runs of a chain without comparing their keys, so it
/// compares keys only between the two chains instead of between the five runs.
template <typename Key>
std::vector<Chain<Key>> makeChains(std::vector<Run<Key>> runs)
{
    ::sort(runs.begin(), runs.end(), [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });

    /// The last key of a chain, with the index of the chain.
    struct ChainEnd
    {
        Key last;
        size_t chain;

        bool operator>(const ChainEnd & other) const { return last > other.last; }
    };

    /// The ends of the chains in a heap, with the chain that ends first on top.
    std::priority_queue<ChainEnd, std::vector<ChainEnd>, std::greater<>> ends;

    std::vector<Chain<Key>> chains;
    for (const auto & run : runs)
    {
        if (!ends.empty() && ends.top().last <= run.first) /// The chain that ends first has ended
        {
            const size_t chain = ends.top().chain;
            ends.pop();
            chains[chain].push_back(run);
            ends.push({run.last, chain}); /// The chain now ends with the run
        }
        else
        {
            chains.push_back({run}); /// A run that overlaps every chain starts a new one
            ends.push({run.last, chains.size() - 1});
        }
    }
    return chains;
}

/// Collects the keys that a merge passes on into batches of `batch_size` keys and gives each full batch to
/// `consume`, until `consume` returns false. Keys that come as a span pass in slices without a copy when they
/// fill whole batches, so the runs in memory of a single chain pass without being copied.
template <typename Key, typename Consume>
class OutputBatches
{
public:
    OutputBatches(size_t batch_size_, Consume & consume_)
        : batch_size(batch_size_)
        , consume(consume_)
    {
    }

    /// Adds `key` and returns false once `consume` stops the merge.
    bool add(const Key & key)
    {
        if (pending.empty())
            pending.reserve_exact(batch_size); /// Allocates once: the batch keeps its capacity
        pending.push_back(key);
        return pending.size() < batch_size || passPending(); /// Passes a full batch
    }

    /// Adds `keys` and returns false once `consume` stops the merge.
    bool add(std::span<const Key> keys)
    {
        while (!keys.empty())
        {
            if (pending.empty() && keys.size() >= batch_size) /// A whole batch passes without a copy
            {
                if (!consume(keys.first(batch_size)))
                    return false;
                keys = keys.subspan(batch_size);
                continue;
            }

            pending.reserve_exact(batch_size); /// No-op once allocated
            const size_t count = std::min(keys.size(), batch_size - pending.size());
            pending.insert(keys.begin(), keys.begin() + count);
            keys = keys.subspan(count);
            if (pending.size() == batch_size && !passPending())
                return false;
        }
        return true;
    }

    /// Passes the keys that do not fill a batch, which make the last batch.
    void finish()
    {
        if (!pending.empty())
            consume(std::span<const Key>(pending.data(), pending.size())); /// Ignores the result: nothing follows
    }

private:
    bool passPending()
    {
        const bool more = consume(std::span<const Key>(pending.data(), pending.size()));
        pending.resize(0); /// Keeps the capacity for the next batch
        return more;
    }

    const size_t batch_size;
    Consume & consume;

    /// The keys of the batch being filled.
    PODArray<Key> pending;
};

/// A tournament of two or more chains, which takes their keys in ascending order. Each inner node of a loser
/// tree keeps the key and the chain that lost the match at the node, and the chain that won the final holds
/// the smallest key. When the winner takes its next key, it replays the matches on its path to the root
/// against the stored losers, one per level, reading only the nodes on the path.
///
/// For example, with four chains whose current keys are 7, 3, 9 and 5, the matches go from the chains on the
/// left to the root on the right, and each node keeps the key that lost there, with its chain:
///
///     chain 0: 7 --+
///                  +-- node 2 keeps 7 (chain 0) --+
///     chain 1: 3 --+                              |
///                                                 +-- node 1 keeps 5 (chain 3) --> winner: 3 (chain 1)
///     chain 2: 9 --+                              |
///                  +-- node 3 keeps 9 (chain 2) --+
///     chain 3: 5 --+
///
/// When chain 1 takes its next key, 8, it replays only the matches on its path, at nodes 2 and 1: 8 loses
/// to the 7 of node 2, and 7 goes on and loses to the 5 of node 1. Node 3 is not read:
///
///     chain 0: 7 --+
///                  +-- node 2 keeps 8 (chain 1) --+
///     chain 1: 8 --+                              |
///                                                 +-- node 1 keeps 7 (chain 0) --> winner: 5 (chain 3)
///     chain 2: 9 --+                              |
///                  +-- node 3 keeps 9 (chain 2) --+
///     chain 3: 5 --+
///
/// The nodes keep the keys themselves when they fit in a native integer: keys of up to 64 bits as they are,
/// and 128-bit keys as `unsigned __int128`, which compares and selects as two machine words. A 256-bit key
/// costs more to copy between nodes than to look up, so the nodes keep one 64-bit word of it: the first word
/// that differs between the smallest and the largest key of the merge, as all its keys share the words before
/// it. For hashes, that word decides almost every match, and a match of equal words compares the whole keys.
/// The matches select with masks instead of branches, because the outcome of comparing random keys is
/// unpredictable.
template <typename Key>
class Tournament
{
public:
    /// Prepares the tournament of `chains_`, which outlive it and hold keys.
    explicit Tournament(std::vector<ChainReader<Key>> & chains_)
        : chains(chains_)
        , current(chains.size())
    {
        chassert(chains.size() > 1);
        if constexpr (!tree_holds_keys)
            tree_word = firstDifferingWord();

        const size_t chain_count = chains.size();
        loser_keys.resize(chain_count);
        loser_chains.resize(chain_count);

        /// Nodes `1` to `chain_count - 1` are inner nodes, and node `chain_count + chain` is the leaf of
        /// `chain`. The children of node `n` are nodes `2n` and `2n + 1`, so the leaves sit on one or two
        /// levels. In the example above, the leaves of chains 0 to 3 are nodes 4 to 7. With three chains, the
        /// leaf of chain 0 is node 3, a child of the root:
        ///
        ///     chain 1, node 4 --+
        ///                       +-- node 2 --+
        ///     chain 2, node 5 --+            |
        ///                                    +-- node 1 --> winner
        ///     chain 0, node 3 ---------------+
        ///
        /// The loop plays the matches from the last inner node to the root, so that each node plays the
        /// winners of its children.
        std::vector<TreeKey> winner_keys(2 * chain_count);
        std::vector<UInt32> winner_chains(2 * chain_count);
        for (size_t chain = 0; chain < chain_count; ++chain)
        {
            current[chain] = chains[chain].current(); /// Each chain has a key, as its runs hold keys
            winner_keys[chain_count + chain] = toTreeKey(current[chain]);
            winner_chains[chain_count + chain] = static_cast<UInt32>(chain);
        }
        for (size_t node = chain_count - 1; node > 0; --node) /// Plays the matches from the leaves up
        {
            const size_t lhs = 2 * node;
            const size_t rhs = 2 * node + 1;
            const bool lhs_wins = less(winner_keys[lhs], winner_chains[lhs], winner_keys[rhs], winner_chains[rhs]);
            winner_keys[node] = lhs_wins ? winner_keys[lhs] : winner_keys[rhs];
            winner_chains[node] = lhs_wins ? winner_chains[lhs] : winner_chains[rhs];
            loser_keys[node] = lhs_wins ? winner_keys[rhs] : winner_keys[lhs];
            loser_chains[node] = lhs_wins ? winner_chains[rhs] : winner_chains[lhs];
        }
        winner_key = winner_keys[1]; /// Node 1 is the root
        winner_chain = winner_chains[1];
    }

    /// Passes each key of the chains once, in ascending order, to `out`, and returns false once `out` stops
    /// the merge. For example, chains [1, 4] and [4, 6] make `pop` take 1, 4, 4 and 6, and the second 4 does
    /// not pass.
    template <typename Output>
    bool passTo(Output & out)
    {
        bool has_last = false;
        Key last{};
        for (Key key{}; pop(key);)
        {
            /// Runs hold distinct keys, so a key that several chains hold comes out of `pop` several times in
            /// a row.
            if (has_last && key == last)
                continue;
            has_last = true;
            last = key;
            if (!out.add(key))
                return false;
        }
        return true;
    }

private:
    static constexpr bool tree_holds_keys = sizeof(Key) <= sizeof(UInt128);
    using TreeKey = std::conditional_t<sizeof(Key) <= sizeof(UInt64), Key, std::conditional_t<tree_holds_keys, unsigned __int128, UInt64>>;

    /// Takes the smallest key of the chains into `key` and returns false when the chains have no more keys. A
    /// key that several chains hold comes out once for each of them, in a row, except the largest key, which
    /// comes out once.
    ///
    /// A chain that ends holds the largest key from then on, so it loses every match. When the winner holds
    /// the largest key, either every chain has ended, or a chain holds it as its last key. For example, with
    /// chains [2, L] and [3], where L is the largest key, the current keys of the chains and the calls go:
    ///
    ///     current keys    the call
    ///     2, 3            takes 2, and chain 0 moves to L
    ///     L, 3            takes 3, and chain 1 ends, so it holds L
    ///     L, L            takes L, as chain 0 has not ended
    ///     L, L            returns false, as L came out already
    ///
    /// With chains [2] and [3], the third call returns false, as both chains hold L only because they ended.
    bool pop(Key & key)
    {
        key = current[winner_chain]; /// The smallest key of the chains
        if (key == largest_key) [[unlikely]] /// Ended chains hold the largest key
        {
            /// A chain that ends takes the largest key, so now every chain holds it, and a chain that has not
            /// ended holds it as its last key.
            if (largest_key_taken || std::ranges::all_of(chains, &ChainReader<Key>::ended))
                return false;
            largest_key_taken = true; /// A real largest key comes out once
            return true;
        }

        const UInt32 chain = winner_chain;
        current[chain] = chains[chain].advance() ? chains[chain].current() : largest_key; /// Ended chains lose
        winner_key = toTreeKey(current[chain]);
        for (size_t node = (chains.size() + chain) / 2; node > 0; node /= 2) /// From the leaf's parent to the root
        {
            const TreeKey loser_key = loser_keys[node];
            const UInt32 loser_chain = loser_chains[node];
            const bool loser_wins = less(loser_key, loser_chain, winner_key, winner_chain);
            loser_keys[node] = selectWithoutBranch(loser_wins, winner_key, loser_key);
            loser_chains[node] = selectWithoutBranch(loser_wins, winner_chain, loser_chain);
            winner_key = selectWithoutBranch(loser_wins, loser_key, winner_key);
            winner_chain = selectWithoutBranch(loser_wins, loser_chain, winner_chain);
        }
        return true;
    }

    /// Returns `lhs` if `condition` holds and `rhs` otherwise, without a branch.
    template <typename T>
    static T selectWithoutBranch(bool condition, T lhs, T rhs)
    {
        if constexpr (sizeof(T) <= sizeof(UInt64))
        {
            const UInt64 mask = -static_cast<UInt64>(condition); /// All ones if `condition` holds, else zero
            return static_cast<T>((static_cast<UInt64>(lhs) & mask) | (static_cast<UInt64>(rhs) & ~mask));
        }
        else
        {
            const T mask = -static_cast<T>(condition); /// Two words for 128-bit keys
            return (lhs & mask) | (rhs & ~mask);
        }
    }

    /// Returns the storage index of the word of 256-bit keys that the nodes keep: the first word that differs
    /// between the smallest first key and the largest last key of the chains, which all keys of the merge
    /// share the words before. For example, keys below 2^64 share their three upper words, which are 0:
    ///
    ///     words, from the most significant     1    2    3    4
    ///     smallest first key                   0    0    0    42
    ///     largest last key                     0    0    0    9000
    ///                                                         ^ the nodes keep the fourth word
    ///
    /// For hashes, the most significant word differs already, and the nodes keep it.
    [[nodiscard]] unsigned firstDifferingWord() const
    {
        Key smallest_first = chains.front().firstKey();
        Key largest_last = chains.front().lastKey();
        for (const auto & chain : chains)
        {
            smallest_first = std::min(smallest_first, chain.firstKey());
            largest_last = std::max(largest_last, chain.lastKey());
        }
        unsigned shared_words = 0;
        while (shared_words + 1 < Key::_impl::item_count /// Stops at the last word, which may be shared too
               && smallest_first.items[Key::_impl::big(shared_words)] == largest_last.items[Key::_impl::big(shared_words)])
            ++shared_words;
        return Key::_impl::big(shared_words); /// `big` counts words from the most significant
    }

    [[nodiscard]] TreeKey toTreeKey(const Key & key) const
    {
        if constexpr (sizeof(Key) <= sizeof(UInt64))
            return key;
        else if constexpr (tree_holds_keys)
            return (static_cast<unsigned __int128>(key.items[Key::_impl::big(0)]) << 64) | key.items[Key::_impl::big(1)];
        else
            return key.items[tree_word]; /// One word of a 256-bit key
    }

    [[nodiscard]] bool less(TreeKey lhs, UInt32 lhs_chain, TreeKey rhs, UInt32 rhs_chain) const
    {
        if constexpr (tree_holds_keys)
            return lhs < rhs; /// The tree keeps whole keys
        else
        {
            if (lhs != rhs) [[likely]] /// The kept words decide most matches
                return lhs < rhs;
            return current[lhs_chain] < current[rhs_chain]; /// Equal words: compares the whole keys
        }
    }

    std::vector<ChainReader<Key>> & chains;

    /// The current key of each chain, or the largest key for a chain that has ended. The tournament passes it
    /// on, and a match of equal words of 256-bit keys compares it.
    std::vector<Key> current;
    const Key largest_key = std::numeric_limits<Key>::max(); /// What an ended chain holds
    bool largest_key_taken = false; /// Whether a real largest key came out

    /// The storage index of the word of 256-bit keys that the nodes keep.
    unsigned tree_word = 0;

    std::vector<TreeKey> loser_keys; /// The key that lost the match at each inner node
    std::vector<UInt32> loser_chains; /// The chain of that key
    TreeKey winner_key; /// The key of the chain that won the final
    UInt32 winner_chain = 0; /// The chain that holds the smallest key
};

/// Reads `chains`, which come from `makeChains`, and passes each of their keys once, in ascending order, to
/// `consume`, in batches of `batch_size` keys but the last one, until `consume` returns false. A batch is
/// valid only until `consume` returns.
template <typename Key, typename Consume>
void merge(std::vector<Chain<Key>> chains, size_t batch_size, Consume && consume)
{
    chassert(!chains.empty());

    /// The runs hold every key that the merge passes, so a batch never takes more keys than they do. A merge
    /// of fewer keys than `batch_size` then reserves room only for its keys.
    size_t run_keys = 0;
    for (const auto & chain : chains)
        for (const auto & run : chain)
            run_keys += run.size();

    std::vector<ChainReader<Key>> readers;
    readers.reserve(chains.size());
    for (auto & chain : chains)
        readers.emplace_back(std::move(chain)); /// Reads the first batch of each chain

    OutputBatches<Key, std::remove_reference_t<Consume>> out(std::min(batch_size, run_keys), consume);

    /// A single chain needs no comparisons, and two or more play a tournament.
    const bool passed_all = readers.size() == 1 ? readers.front().passTo(out) : Tournament<Key>(readers).passTo(out);
    if (passed_all)
        out.finish(); /// The last batch, unless `consume` stopped the merge
}

/// Returns the most memory that `merge` takes besides the runs in memory, when `file_runs` runs are in files
/// that buffers of `buffer_size` bytes wrote, and batches take `batch_size` keys. A chain reads one file at a
/// time, so the files take at most a reader each: a batch of keys, the compressed and the decompressed blocks
/// of the buffer size, and a file buffer of at most `DBMS_DEFAULT_BUFFER_SIZE`. The batch being filled takes
/// at most `batch_size` keys.
template <typename Key>
size_t estimateMemoryToMerge(size_t file_runs, size_t batch_size, size_t buffer_size)
{
    const size_t reader_bytes = keys_per_batch<Key> * sizeof(Key) + 2 * buffer_size + DBMS_DEFAULT_BUFFER_SIZE;
    return file_runs * reader_bytes + batch_size * sizeof(Key);
}

}
