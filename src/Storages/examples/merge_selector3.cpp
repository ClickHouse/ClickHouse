#include <list>
#include <iostream>
#include <unordered_map>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/Operators.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreeDataFormatVersion.h>
#include <Storages/MergeTree/MergeSelectors/SimpleMergeSelector.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Common/formatReadable.h>
#include <base/interpolate.h>
#include <absl/container/flat_hash_set.h>

using namespace DB;


enum TaskType
{
    INSERT = 0,
    MERGE = 1,
    FETCH = 2,
};

struct ReplicaState
{
    ActiveDataPartSet parts_without_currently_merging_parts{MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING};
    std::unordered_map<std::string_view, std::pair<std::list<std::string>::iterator, IMergeSelector::Part>> parts_data;
    std::list<std::string> names_holder;

    std::list<std::string> parts_names_cache;

    uint64_t getTotalPartsCount() const
    {
        return parts_without_currently_merging_parts.size();
    }

    void tickAge()
    {
        for (auto & [_, data] : parts_data)
            data.second.age++;
    }

    void addPart(const std::string & part_name, IMergeSelector::Part & part_data)
    {
        //std::cerr << "Add part: " << part_name << std::endl;
        Strings replaced_parts;
        parts_without_currently_merging_parts.add(part_name, &replaced_parts);
        for (const auto & replaced_part : replaced_parts)
        {
            //auto list_it = parts_data[replaced_part].first;
            //names_holder.erase(list_it);
            parts_data.erase(replaced_part);
        }

        names_holder.push_back(part_name);
        auto it = names_holder.end();
        parts_data[names_holder.back()] = std::make_pair(std::prev(it), part_data);
    }

    IMergeSelector::PartsRanges getPartRangesForMerge(const absl::flat_hash_set<std::string> & currently_merging_parts, const ActiveDataPartSet & shared_state)
    {
        IMergeSelector::PartsRanges parts_ranges;
        const MergeTreePartInfo * prev_part = nullptr;
        const auto & parts = parts_without_currently_merging_parts.getPartNamesWithInfos();
        std::cerr << "Currently merging parts: " << currently_merging_parts.size() << std::endl;
        std::cerr << "Total parts: " << parts.size() << std::endl;
        for (const auto & [part_info, part_name] : parts)
        {
            if (currently_merging_parts.contains(part_name))
            {
                std::cerr << "Skip\n";
                continue;
            }
            else
            {
                std::cerr << "No Skip\n";
            }

            auto containing_part = shared_state.getContainingPart(part_info);
            if (!containing_part.empty() && containing_part != part_name)
            {
                continue;
            }

            if (!prev_part)
            {
                if (parts_ranges.empty() || !parts_ranges.back().empty())
                    parts_ranges.emplace_back();
            }
            else
            {
                if (part_info.min_block != prev_part->max_block + 1)
                {
                    //std::cerr << "New range because left: " << prev_part->getPartNameV1() << " | right: " << part_info.getPartNameV1() << std::endl;
                    prev_part = nullptr;
                    parts_ranges.emplace_back();
                }
            }


            //std::cerr << "Parts_data has: " << part_name << std::endl;
            auto it = parts_data.find(part_name);
            if (it == parts_data.end())
                std::terminate();
            auto [list_it, part] = it->second;
            part.data = reinterpret_cast<const void *>(list_it->data());
            parts_ranges.back().emplace_back(part);

            prev_part = &part_info;
        }

        return parts_ranges;
    }

};

using AllReplicasState = std::unordered_map<std::string, ReplicaState>;

using CurrentlyMergingPartsNames = std::unordered_set<std::string>;

struct SharedState
{
    ActiveDataPartSet shared_state{MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING};
    absl::flat_hash_set<std::string> merging_parts;
    AllReplicasState all_replicas;
    size_t currently_running_merges{0};
    uint64_t total_parts{0};
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
    virtual std::vector<std::unique_ptr<ITask>> updatePartsStateOnTaskFinish(SharedState & state, uint64_t) = 0;
    virtual bool isFinished(uint64_t current_time) const = 0;
    virtual TaskType getTaskType() const = 0;
};

class FetchTask final : public ITask
{
    IMergeSelector::Part part;
    uint64_t fetch_time;
    std::string part_name;
public:
    FetchTask(const IMergeSelector::Part & part_, uint64_t current_time, const std::string & part_name_, const std::string & replica_name_)
        : ITask(replica_name_)
        , part(part_)
        , fetch_time(current_time)
        , part_name(part_name_)
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
        //std::cerr << "Fetching part: " << part_name << " for replica " << replica_name << std::endl;
        replica_state.addPart(part_name, part);
    }

    std::vector<std::unique_ptr<ITask>> updatePartsStateOnTaskFinish(SharedState &, uint64_t) override
    {
        return {};
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
        //std::cerr << "Inserting part: " << reinterpret_cast<const char *>(part.data) << " for replica " << replica_name << std::endl;
        state.all_replicas[replica_name].addPart(reinterpret_cast<const char *>(part.data), part);
        state.shared_state.add(reinterpret_cast<const char *>(part.data));
        state.total_parts += 1;
    }

    std::vector<std::unique_ptr<ITask>> updatePartsStateOnTaskFinish(SharedState &, uint64_t) override
    {
        return {};
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

            //std::cerr << "Starting merge range: " << reinterpret_cast<const char *>(part.data) << std::endl;
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

        state.currently_running_merges++;
    }

    std::vector<std::unique_ptr<ITask>> updatePartsStateOnTaskFinish(SharedState & state, uint64_t current_time) override
    {
        for (const auto & part_to_merge : parts_to_merge)
        {
            //std::cerr << "Finishing merge range: " << reinterpret_cast<const char *>(part_to_merge.data) << std::endl;
            state.merging_parts.erase(reinterpret_cast<const char *>(part_to_merge.data));
        }

        auto & replica_state = state.all_replicas[replica_name];

        IMergeSelector::Part merged_part;
        merged_part.size = merge_size_bytes;
        merged_part.level = max_level + 1;
        merged_part.age = 0;
        merged_part.rows = merge_size_rows;
        //std::cerr << "Merge first part: " << reinterpret_cast<const char *>(parts_to_merge[0].data) << std::endl;
        //std::cerr << "Merge last part: " << reinterpret_cast<const char *>(parts_to_merge.back().data) << std::endl;
        MergeTreePartInfo first_info = MergeTreePartInfo::fromPartName(reinterpret_cast<const char *>(parts_to_merge[0].data), MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
        MergeTreePartInfo last_info = MergeTreePartInfo::fromPartName(reinterpret_cast<const char *>(parts_to_merge.back().data), MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);

        MergeTreePartInfo final_part_info = first_info;
        final_part_info.min_block = first_info.min_block;
        final_part_info.max_block = last_info.max_block;
        final_part_info.level = max_level + 1;


        //std::cerr << "Merged part: " << final_part_info.getPartNameV1() << " for replica " << replica_name << std::endl;
        replica_state.addPart(final_part_info.getPartNameV1(), merged_part);
        state.shared_state.add(final_part_info);

        state.total_parts += (1 - parts_to_merge.size());
        state.currently_running_merges--;

        std::vector<std::unique_ptr<ITask>> tasks;
        for (const auto & [replica_name, _] : state.all_replicas)
            tasks.push_back(std::make_unique<FetchTask>(merged_part, current_time + 5, final_part_info.getPartNameV1(), replica_name));
        return tasks;
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
    SharedState state;
    std::set<std::string> replicas;

    SimpleMergeSelector selector;
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
                tasks.push(std::make_unique<FetchTask>(parts[i].part, insertion_times[i] - start_time, reinterpret_cast<const char *>(parts[i].part.data), parts[i].replica_name));
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
        uint64_t free_entries = background_pool_size - state.currently_running_merges;
        UInt64 max_size = 0;
        if (state.currently_running_merges <= 1 || free_entries >= number_of_free_entries_in_pool_to_lower_max_size_of_merge)
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
            std::cerr << "Total parts count: " << state.total_parts << " merges running:" << state.currently_running_merges << " time passed " << current_time / 1000 << std::endl;
            //for (auto & replica : state.all_replicas)
            //    replica.second.tickAge();
        }

        while (!tasks.empty())
        {
            const std::unique_ptr<ITask> & top_task = tasks.top();
            if (top_task->isFinished(current_time))
            {
                if (top_task->getTaskType() == MERGE)
                {
                    auto new_tasks = top_task->updatePartsStateOnTaskFinish(state, current_time);
                    for (auto && new_task : new_tasks)
                        tasks.push(std::move(new_task));
                }
                else if (top_task->getTaskType() == INSERT)
                {
                    if (state.total_parts < too_many_parts)
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

        //std::cerr << "Will try to assign merge\n";
        if (current_time % 30 == 0)
        {
            for (const auto & replica_name : replicas)
            {
                //std::cerr << "Trying for replica: " << replica_name << " merging parts: " << state.merging_parts.size() << std::endl;
                auto ranges = state.all_replicas[replica_name].getPartRangesForMerge(state.merging_parts, state.shared_state);

                if (ranges.empty())
                {
                    std::cerr << "No ranges\n";
                    continue;
                }

                IMergeSelector::PartsRange selected_parts = selector.select(ranges, getMaxSizeToMerge());

                if (!selected_parts.empty())
                {
                    auto new_merge = std::make_unique<MergeTask>(selected_parts, current_time, merge_speed, replica_name);
                    new_merge->updatePartsStateOnTaskStart(state);
                    tasks.push(std::move(new_merge));
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
    size_t counter = 0;
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
        counter++;
        if (counter % 1000 == 0)
            std::cerr << "Loaded:" << counter << " events\n";
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
        try
        {
            simulator.step();
        }
        catch (...)
        {
            std::cerr << getCurrentExceptionMessage(true) << std::endl;
            std::cerr << "Failed after: " << simulator.getTime() / 1000 << " seconds" << std::endl;
            return 1;
        }

        if (simulator.getTime() > 20 * 3600 * 1000)
            break;
    }

    return 0;
}
