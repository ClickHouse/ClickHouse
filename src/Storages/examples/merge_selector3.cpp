#include <list>
#include <iostream>
#include <unordered_map>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/Operators.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreeDataFormatVersion.h>
#include <Storages/MergeTree/MergeSelectors/SimpleMergeSelector.h>
#include <Common/formatReadable.h>
#include <base/interpolate.h>

using namespace DB;


enum TaskType
{
    INSERT = 0,
    MERGE = 1,
    FETCH = 2,
};

struct ReplicaState
{
    IMergeSelector::PartsRange parts_without_currently_merging_parts;
    size_t total_parts_count;
    std::vector<std::string> parts_names_cache;
};

using AllReplicasState = std::unordered_map<std::string, ReplicaState>;

using CurrentlyMergingPartsNames = std::unordered_set<std::string>;

struct SharedState
{
    AllReplicasState all_replicas;
    CurrentlyMergingPartsNames merging_parts;
    size_t currently_running_merges{0};
};

class ITask
{
protected:
    std::string replica_name;
public:
    explicit ITask(const std::string & replica_name_)
        : replica_name(replica_name_)
    {}
    virtual uint64_t getFinishTime() const = 0;
    bool operator<(const ITask & o) const
    {
        return getFinishTime() < o.getFinishTime();
    }

    std::string getReplicaName() const { return replica_name; }
    virtual ~ITask() = default;
    virtual void updatePartsStateOnTaskStart(SharedState & state) = 0;
    virtual void updatePartsStateOnTaskFinish(SharedState & state) = 0;
    virtual bool isFinished(uint64_t current_time) const = 0;
    virtual TaskType getTaskType() const = 0;
};

class FetchTask final : public ITask
{
    IMergeSelector::Part part;
    uint64_t fetch_time;
public:
    FetchTask(const IMergeSelector::Part & part_, uint64_t current_time, const std::string & replica_name_)
        : ITask(replica_name_)
        , part(part_)
        , fetch_time(current_time)
    {}

    uint64_t getFinishTime() const override
    {
        return fetch_time;
    }

    bool isFinished(uint64_t current_time) const override
    {
        return fetch_time <= current_time;
    }

    void updatePartsStateOnTaskStart(SharedState & state) override
    {
        auto & replica_state = state.all_replicas[replica_name];
        replica_state.total_parts_count += 1;
        auto & replica_parts = replica_state.parts_without_currently_merging_parts;
        MergeTreePartInfo new_part_info = MergeTreePartInfo::fromPartName(reinterpret_cast<const char *>(part.data), MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
        std::optional<size_t> begin;
        std::optional<size_t> end;
        for (size_t i = 0; i < replica_parts.size(); ++i)
        {
            MergeTreePartInfo part_info = MergeTreePartInfo::fromPartName(reinterpret_cast<const char *>(replica_parts[i].data), MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
            if (new_part_info.contains(part_info))
            {
                if (!begin)
                    begin.emplace(i);

                end = i;
            }
            else if (begin && end)
            {
                break;
            }
        }

        if (begin && end && *begin != *end)
        {
            replica_parts[*begin] = part;
            replica_parts.erase(replica_parts.begin() + *begin + 1, replica_parts.begin() + *end);
        }
    }

    void updatePartsStateOnTaskFinish(SharedState &) override
    {
    }

    TaskType getTaskType() const override
    {
        return FETCH;
    }
};


class InsertTask final : public ITask
{
public:
    uint64_t getFinishTime() const override
    {
        return insert_time;
    }

    IMergeSelector::Part part;
    uint64_t insert_time;

    InsertTask(const IMergeSelector::Part & part_, uint64_t insert_time_, const std::string & replica_name_)
        : ITask(replica_name_)
        , part(part_)
        , insert_time(insert_time_)
    {
    }

    bool isFinished(uint64_t current_time) const override
    {
        return insert_time <= current_time;
    }

    void updatePartsStateOnTaskStart(SharedState & state) override
    {
        state.all_replicas[replica_name].parts_without_currently_merging_parts.push_back(part);
        state.all_replicas[replica_name].total_parts_count += 1;
    }

    void updatePartsStateOnTaskFinish(SharedState &) override
    {
    }

    TaskType getTaskType() const override
    {
        return INSERT;
    }
};

class MergeTask final : public ITask
{
    IMergeSelector::Part part_after_merge;
public:
    uint64_t getFinishTime() const override
    {
        return start_time + duration;
    }

    std::vector<IMergeSelector::Part> parts_to_merge;
    uint64_t merge_size_bytes{0};
    uint64_t merge_size_rows{0};
    uint64_t start_time{0};
    uint64_t duration{0};
    uint32_t max_level{0};

    MergeTask(const std::vector<IMergeSelector::Part> & parts_to_merge_, uint64_t current_time, uint64_t merge_speed, const std::string & replica_name_)
        : ITask(replica_name_)
        , parts_to_merge(parts_to_merge_)
        , start_time(current_time)
    {
        for (const auto & part : parts_to_merge)
        {
            merge_size_rows += part.rows;
            merge_size_bytes += part.size;
            max_level = std::max<uint32_t>(part.level, max_level);
        }

        duration = merge_size_rows / merge_speed * 1000;
        //std::cerr << "Merge duration: " << duration / 1000 << std::endl;
    }

    bool isFinished(uint64_t current_time) const override
    {
        return (current_time - start_time) >= duration;
    }

    void updatePartsStateOnTaskStart(SharedState & state) override
    {
        for (const auto & part_to_merge : parts_to_merge)
            state.merging_parts.insert(reinterpret_cast<const char *>(part_to_merge.data));
    }

    void updatePartsStateOnTaskFinish(SharedState & state) override
    {
        for (const auto & part_to_merge : parts_to_merge)
            state.merging_parts.erase(reinterpret_cast<const char *>(part_to_merge.data));

        auto & replica_state = state.all_replicas[replica_name];
        auto & replica_parts = replica_state.parts_without_currently_merging_parts;

        uint64_t start_index = 0;

        for (uint64_t i = 0, size = replica_parts.size(); i < size; ++i)
        {
            if (replica_parts[i].data == parts_to_merge.front().data)
            {
                start_index = i;
                break;
            }
        }
        replica_parts[start_index].size = merge_size_bytes;
        replica_parts[start_index].level = max_level + 1;
        replica_parts[start_index].age = 0;
        replica_parts[start_index].rows = merge_size_rows;
        MergeTreePartInfo first_info = MergeTreePartInfo::fromPartName(reinterpret_cast<const char *>(parts_to_merge[0].data), MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
        MergeTreePartInfo last_info = MergeTreePartInfo::fromPartName(reinterpret_cast<const char *>(parts_to_merge.back().data), MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);

        MergeTreePartInfo final_part_info = first_info;
        final_part_info.min_block = first_info.min_block;
        final_part_info.max_block = last_info.max_block;
        final_part_info.level = max_level + 1;
        replica_state.parts_names_cache.push_back(final_part_info.getPartNameV1());
        replica_parts[start_index].data = replica_state.parts_names_cache.data();
        replica_parts.erase(replica_parts.begin() + start_index + 1, replica_parts.begin() + start_index + parts_to_merge.size());

        replica_state.total_parts_count += (1 - parts_to_merge.size());
    }

    TaskType getTaskType() const override
    {
        return MERGE;
    }

};

struct Comparator
{
    bool operator()(const std::unique_ptr<ITask> & f, const std::unique_ptr<ITask> & s) const
    {
        return f->getFinishTime() > s->getFinishTime();
    }
};

struct PartWithActionTypeAndReplica
{
    IMergeSelector::Part part;
    std::string replica_name;
    TaskType task;
};

using PartsWithTypeAndReplicas = std::vector<PartWithActionTypeAndReplica>;

class Simulator
{
private:
    std::priority_queue<std::unique_ptr<ITask>, std::vector<std::unique_ptr<ITask>>, Comparator> tasks;
    SimpleMergeSelector::Settings settings;
    uint64_t current_time{0};
    uint64_t background_pool_size;
    uint64_t max_part_uint64_to_merge;
    uint64_t min_part_uint64_to_merge = 1024 * 1024;
    uint64_t number_of_free_entries_in_pool_to_lower_max_size_of_merge = 8;
    uint64_t merge_speed;
    uint64_t too_many_parts;
    uint64_t currently_running_merges{0};
    SharedState state;
    IMergeSelector::PartsRanges partitions;
    IMergeSelector::PartsRange & parts_state;
    std::set<std::string> replicas;

    SimpleMergeSelector selector;
    uint64_t total_parts{0};
public:
    Simulator(const PartsWithTypeAndReplicas & parts,
              std::vector<uint64_t> insertion_times,
              uint64_t start_time,
              SimpleMergeSelector::Settings settings_,
              uint64_t background_pool_size_,
              uint64_t max_part_uint64_to_merge_,
              uint64_t merge_speed_,
              uint64_t too_many_parts_)
        : settings(settings_)
        , background_pool_size(background_pool_size_)
        , max_part_uint64_to_merge(max_part_uint64_to_merge_)
        , merge_speed(merge_speed_)
        , too_many_parts(too_many_parts_)
        , partitions(1)
        , parts_state(partitions.back())
        , selector(settings)
    {
        for (uint64_t i = 0; i < parts.size(); ++i)
        {
            replicas.insert(parts[i].replica_name);

            if (parts[i].task == INSERT)
            {
                tasks.push(std::make_unique<InsertTask>(parts[i].part, insertion_times[i] - start_time, parts[i].replica_name));
            }
            else if (parts[i].task == FETCH)
            {
                tasks.push(std::make_unique<FetchTask>(parts[i].part, insertion_times[i] - start_time, parts[i].replica_name));
            }
            else
            {
                std::terminate();
            }
            //std::cerr << "Insert time: " << insertion_times[i] - start_time << " start time:" << start_time << std::endl;
        }
    }

    uint64_t getMaxSizeToMerge() const
    {
        uint64_t free_entries = background_pool_size - currently_running_merges;
        UInt64 max_size = 0;
        if (currently_running_merges <= 1 || free_entries >= number_of_free_entries_in_pool_to_lower_max_size_of_merge)
            max_size = max_part_uint64_to_merge;
        else
            max_size = static_cast<UInt64>(interpolateExponential(
                min_part_uint64_to_merge,
                max_part_uint64_to_merge,
                static_cast<double>(free_entries) / number_of_free_entries_in_pool_to_lower_max_size_of_merge));
        return max_size;
    }

    void step()
    {
        if (current_time % 1000 == 0)
        {
            std::cerr << "Total parts count: " << total_parts << " merges running:" << currently_running_merges << std::endl;
            for (auto & part : parts_state)
                part.age++;
        }

        while (!tasks.empty())
        {
            const std::unique_ptr<ITask> & top_task = tasks.top();
            if (top_task->isFinished(current_time))
            {
                if (top_task->getTaskType() == MERGE)
                {
                    top_task->updatePartsStateOnTaskFinish(state);
                }
                else if (top_task->getTaskType() == INSERT)
                {
                    if (total_parts < too_many_parts)
                    {
                        top_task->updatePartsStateOnTaskStart(state);
                    }
                    else
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Too many parts");
                }
                else if (top_task->getTaskType() == FETCH)
                {
                    top_task->updatePartsStateOnTaskStart(state);
                }

                tasks.pop();
            }
            else
            {
                break;
            }
        }

        if (current_time % 67 == 0)
        {
            for (const auto & replica_name : replicas)
            {
                auto parts_for_replica = state.all_replicas[replica_name].parts_without_currently_merging_parts;
                for (auto itr = parts_for_replica.begin(); itr != parts_for_replica.end();)
                {
                    if (state.merging_parts.contains(static_cast<const char *>(itr->data)))
                        itr = parts_for_replica.erase(itr);
                    else
                        ++itr;
                }

                IMergeSelector::PartsRange selected_parts = selector.select({parts_for_replica}, getMaxSizeToMerge());

                if (!selected_parts.empty())
                {
                    auto new_merge = std::make_unique<MergeTask>(selected_parts, current_time, merge_speed, replica_name);
                    new_merge->updatePartsStateOnTaskStart(state);
                    tasks.push(std::move(new_merge));
                    currently_running_merges++;
                }
            }

        }

        current_time++;
    }

    uint64_t getTime() const
    {
        return current_time;
    }

};

int main(int, char **)
{
    uint64_t max_part_size = 300UL * 1024 * 1024 * 1024;
    uint64_t background_pool_size = 64 * 2 * 30;
    uint64_t merge_rows_per_second = 1'000'000;
    uint64_t max_parts_to_merge_at_once = 100;
    uint64_t too_many_parts = 30'000;

    ReadBufferFromFileDescriptor in(STDIN_FILENO);

    std::list<std::string> part_names;

    uint64_t start_time = std::numeric_limits<uint64_t>::max();
    std::vector<uint64_t> insertion_times;
    PartsWithTypeAndReplicas parts;
    while (!in.eof())
    {
        part_names.emplace_back();
        IMergeSelector::Part part;
        uint64_t event_time;
        std::string replica_name;
        uint64_t event_type;
        in >> part.size >> "\t" >> part.rows >> "\t" >> part.level >> "\t" >> part_names.back() >> "\t" >> event_time >> "\t" >> event_type >> "\t" >> replica_name >> "\n";
        part.data = part_names.back().data();
        part.age = 0;
        parts.emplace_back(PartWithActionTypeAndReplica{part, replica_name, static_cast<TaskType>(event_type)});
        start_time = std::min(start_time, event_time);
        insertion_times.push_back(event_time);
    }

    std::cerr << "Parsed: \n";
    SimpleMergeSelector::Settings settings;
    settings.max_parts_to_merge_at_once = max_parts_to_merge_at_once;

    Simulator simulator(
        parts,
        insertion_times,
        start_time,
        settings,
        background_pool_size,
        max_part_size,
        merge_rows_per_second,
        too_many_parts
    );
    std::cerr << "Simulator created\n";

    while (true)
    {
        try {
            simulator.step();
        }
        catch (...)
        {
            std::cerr << "Failed after: " << simulator.getTime() / 1000 << " seconds" << std::endl;
            return 1;
        }

        if (simulator.getTime() > 20 * 3600 * 1000)
            break;
    }

    return 0;
}
