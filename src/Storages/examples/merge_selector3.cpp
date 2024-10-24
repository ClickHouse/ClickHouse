#include <list>
#include <iostream>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/Operators.h>
#include <Storages/MergeTree/MergeSelectors/SimpleMergeSelector.h>
#include <Common/formatReadable.h>
#include <base/interpolate.h>

using namespace DB;


enum TaskType
{
    INSERT = 0,
    MERGE = 1,
};

class ITask
{
public:
    virtual uint64_t getFinishTime() const = 0;
    bool operator<(const ITask & o) const
    {
        return getFinishTime() < o.getFinishTime();
    }

    virtual ~ITask() = default;
    virtual void updatePartsState(IMergeSelector::PartsRange & parts) = 0;
    virtual int64_t getTotalPartsUpdate() const = 0;
    virtual bool isFinished(uint64_t current_time) const = 0;
    virtual TaskType getTaskType() const = 0;
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
    InsertTask(const IMergeSelector::Part & part_, uint64_t insert_time_)
        : part(part_)
        , insert_time(insert_time_)
    {
    }

    bool isFinished(uint64_t current_time) const override
    {
        return insert_time <= current_time;
    }

    void updatePartsState(IMergeSelector::PartsRange & parts) override
    {
        parts.push_back(part);
    }

    int64_t getTotalPartsUpdate() const override
    {
        return +1;
    }

    TaskType getTaskType() const override
    {
        return INSERT;
    }
};

class MergeTask final : public ITask
{
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

    MergeTask(const std::vector<IMergeSelector::Part> & parts_to_merge_, uint64_t current_time, uint64_t merge_speed)
        : parts_to_merge(parts_to_merge_)
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

    void updatePartsState(IMergeSelector::PartsRange & parts) override
    {
        uint64_t start_index = 0;

        for (uint64_t i = 0, size = parts.size(); i < size; ++i)
        {
            if (parts[i].data == parts_to_merge.front().data)
            {
                start_index = i;
                break;
            }
        }
        parts[start_index].size = merge_size_bytes;
        parts[start_index].level = max_level + 1;
        parts[start_index].age = 0;
        parts[start_index].rows = merge_size_rows;
        parts.erase(parts.begin() + start_index + 1, parts.begin() + start_index + parts_to_merge.size());
    }

    int64_t getTotalPartsUpdate() const override
    {
        return 1 - parts_to_merge.size();
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
    IMergeSelector::PartsRanges partitions;
    IMergeSelector::PartsRange & parts_state;
    SimpleMergeSelector selector;
    uint64_t total_parts{0};
public:
    Simulator(const IMergeSelector::PartsRange & inserted_parts,
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
        for (uint64_t i = 0; i < inserted_parts.size(); ++i)
        {
            //std::cerr << "Insert time: " << insertion_times[i] - start_time << " start time:" << start_time << std::endl;
            tasks.push(std::make_unique<InsertTask>(inserted_parts[i], insertion_times[i] - start_time));
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
                    --currently_running_merges;
                }

                if (top_task->getTaskType() == INSERT)
                {
                    if (total_parts < too_many_parts)
                    {
                        top_task->updatePartsState(parts_state);
                    }
                    else
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Too many parts");
                }

                total_parts += top_task->getTotalPartsUpdate();
                tasks.pop();
            }
            else
            {
                break;
            }
        }

        if (current_time % 67 == 0)
        {
            IMergeSelector::PartsRange selected_parts = selector.select(partitions, getMaxSizeToMerge());

            if (!selected_parts.empty())
            {
                auto new_merge = std::make_unique<MergeTask>(selected_parts, current_time, merge_speed);
                new_merge->updatePartsState(parts_state);
                tasks.push(std::move(new_merge));
                currently_running_merges++;
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

    IMergeSelector::PartsRange parts;
    uint64_t start_time = std::numeric_limits<uint64_t>::max();
    std::vector<uint64_t> insertion_times;
    while (!in.eof())
    {
        part_names.emplace_back();
        IMergeSelector::Part part;
        uint64_t event_time;
        in >> part.size >> "\t" >> part.rows >> "\t" >> part.level >> "\t" >> part_names.back() >> "\t" >> event_time >> "\n";
        part.data = part_names.back().data();
        part.age = 0;
        parts.emplace_back(part);
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
