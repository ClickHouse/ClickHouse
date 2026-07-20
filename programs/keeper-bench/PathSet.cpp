#include <PathSet.h>

#include <random>

#include <Common/Exception.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void PathSet::finalize(size_t num_threads)
{
    if (!shards.empty())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Path set {} is finalized twice", name);

    shards = std::vector<Shard>(is_dynamic ? num_threads : 1);

    auto staged = std::move(staged_paths);
    staged_paths.clear();
    for (auto & path : staged)
        populate(std::move(path));
}

void PathSet::populate(std::string path)
{
    if (shards.empty())
    {
        staged_paths.push_back(std::move(path));
        return;
    }

    shards[next_populate_shard].paths.push_back(std::move(path));
    next_populate_shard = (next_populate_shard + 1) % shards.size();
}

std::optional<std::string> PathSet::samplePath(pcg64 & rng, size_t thread_idx) const
{
    if (shards.empty())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Path set {} is not finalized", name);

    const Shard & shard = shards[shardFor(thread_idx)];

    std::unique_lock lock(shard.mutex, std::defer_lock);
    if (is_dynamic)
        lock.lock();

    if (shard.paths.empty())
        return std::nullopt;

    return shard.paths[std::uniform_int_distribution<size_t>(0, shard.paths.size() - 1)(rng)];
}

size_t PathSet::totalSize() const
{
    size_t total = staged_paths.size();
    for (const auto & shard : shards)
    {
        std::unique_lock lock(shard.mutex, std::defer_lock);
        if (is_dynamic)
            lock.lock();
        total += shard.paths.size();
    }
    return total;
}
