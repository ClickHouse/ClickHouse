#pragma once

#include <pcg-random/pcg_random.hpp>
#include <Common/CacheLine.h>

#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

/// A set of znode paths that request generators draw from (and, when dynamic,
/// update on the fly). One is created for every setup `tag`, every `children_of`
/// parent, and every literal path list in the generator config. Each path is
/// stored once, shared by all generators and worker threads.
struct PathSet
{
    /// Display name for logs and errors, e.g. `tag "leaves"` or `children of /test`.
    std::string name;

    /// How the set is defined (i.e. what populates it). Exactly one of these is
    /// set at creation; `used_as_output` may additionally be set later.
    bool is_setup_tag = false;
    bool is_children_of = false;
    bool is_literal = false;

    /// Parent path, for `is_children_of` sets.
    std::string children_of_parent;

    /// Some request generator reads paths from this set. Sets nobody reads are
    /// not populated and not updated.
    bool used_as_input = false;
    /// Some request generator adds paths to or removes paths from this set.
    bool used_as_output = false;

    /// True if the contents may change while the benchmark runs. Dynamic sets are
    /// sharded by worker thread: each thread reads and updates only its own shard,
    /// so different threads operate on disjoint subsets of paths.
    bool is_dynamic = false;

    struct Shard
    {
        alignas(DB::CH_CACHE_LINE_SIZE) mutable std::mutex mutex;
        std::vector<std::string> paths;
    };

    /// One shard if `!is_dynamic`, one per worker thread otherwise. Allocated by
    /// `finalize` once all generators are parsed and the flags above are final.
    std::vector<Shard> shards;

    /// Allocate shards and move any staged paths into them.
    void finalize(size_t num_threads);

    /// Add a path. Not thread-safe: only for populating the set before the worker
    /// threads start. Before `finalize` the path is staged; after, it is placed
    /// into a shard round-robin.
    void populate(std::string path);

    /// Uniformly random path from the calling thread's shard, or nullopt if it is empty.
    std::optional<std::string> samplePath(pcg64 & rng, size_t thread_idx) const;

    /// Add a path to the calling thread's shard. Thread-safe; for dynamic sets.
    void add(std::string path, size_t thread_idx);

    /// Remove and return a uniformly random path from the calling thread's shard,
    /// or nullopt if it is empty. Thread-safe; for dynamic sets.
    std::optional<std::string> takeRandom(pcg64 & rng, size_t thread_idx);

    /// If the set is a literal list with exactly one path, returns it.
    /// Only meaningful before `finalize`.
    std::optional<std::string> singleStagedPath() const;

    size_t totalSize() const;

    size_t shardFor(size_t thread_idx) const { return is_dynamic ? thread_idx % shards.size() : 0; }

private:
    /// Paths added before `finalize` allocated the shards (e.g. literal path lists
    /// known at config parse time).
    std::vector<std::string> staged_paths;

    size_t next_populate_shard = 0;
};

using PathSetPtr = std::shared_ptr<PathSet>;
