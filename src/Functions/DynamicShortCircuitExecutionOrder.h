#pragma once

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <Columns/ColumnFunction.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Functions/IFunction.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <boost/algorithm/string/split.hpp>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>
#include <Common/StackTrace.h>

namespace DB
{
/** For functions that support short-circuit calculations, if the execution order of the function's
  * arguments can be adjusted, then by calculating the execution cost of each argument, the
  * execution order of the argument can be adjusted to minimize the overall execution cost.
  */
class DynamicShortCircuitExecutionOrder
{
public:
    using ColumnsWithTypeAndName = std::vector<ColumnWithTypeAndName>;
    struct ProfileData
    {
        std::mutex mutex;
        size_t seen_rows = 0;
        size_t selected_rows = 0;
        size_t elapsed_ns = 0;
        double rank = 0.0;
        ProfileData() = default;
        ProfileData(ProfileData && b)
        {
            seen_rows = b.seen_rows;
            selected_rows = b.selected_rows;
            elapsed_ns = b.elapsed_ns;
            rank = b.rank;
        }
    };

    DynamicShortCircuitExecutionOrder(ContextPtr context_, bool inverted_rank_ = false) : context(context_), inverted_rank(inverted_rank_) { }

    /// Need to keep the types of all columns the same between each batch.
    ALWAYS_INLINE std::shared_ptr<std::vector<size_t>> getAdjustedExecutionOrder(const ColumnsWithTypeAndName & arguments)
    {
        // arguments could be empty when `getSampleBlockImpl` is called.
        if (arguments.empty() || !arguments[0].column->size()) [[unlikely]]
            return nullptr;

        trySetup(arguments);
        if (arguments.size() != num_arguments.load())
        {
            auto block = Block(arguments).cloneEmpty();
            LOG_ERROR(
                &Poco::Logger::get("DynamicShortCircuitExecutionOrder"),
                "Mismatch arguments size. arguments size: {}, num_arguments: {}. headers: {}; {}",
                arguments.size(),
                num_arguments.load(),
                header.dumpStructure(),
                block.dumpStructure());
            return nullptr;
        }
        if (!could_adjust)
          return nullptr;
        std::shared_lock lock(mutex);
        return adjusted_arguments_execution_order;
    }

    ALWAYS_INLINE void addProfileData(size_t pos, const ProfileData & profile_data)
    {
        auto & data = arguments_profile_data[pos];
        std::lock_guard lock(data.mutex);
        data.seen_rows += profile_data.seen_rows;
        data.selected_rows += profile_data.selected_rows;
        data.elapsed_ns += profile_data.elapsed_ns;
    }

    void readjustExecutionOrder(size_t rows)
    {
        current_executed_rows += rows;
        if (current_executed_rows.load() < last_adjust_rows + 65536)
            return;
        auto flag = in_adjusting_process_count.fetch_add(1);
        if (flag)
        {
            in_adjusting_process_count.fetch_sub(1);
            return;
        }
        last_adjust_rows = current_executed_rows.load();
        std::vector<std::pair<size_t, double>> arguments_rank;
        for (size_t i = 0, args_size = num_arguments.load(); i < args_size; ++i)
            arguments_rank.emplace_back(i, nextRank(i));
        std::sort(arguments_rank.begin(), arguments_rank.end(), [](const auto & a, const auto & b) { return a.second < b.second; });
        auto new_adjusted_arguments_execution_order = std::make_shared<std::vector<size_t>>();
        WriteBufferFromOwnString log_str;
        for (auto & [pos, rank] : arguments_rank)
        {
            log_str << pos << std::string(",");
            new_adjusted_arguments_execution_order->emplace_back(pos);
        }
        std::unique_lock lock(mutex);
        adjusted_arguments_execution_order = new_adjusted_arguments_execution_order;
        in_adjusting_process_count.fetch_sub(1);
    }

    String dumpProfileData()
    {
        WriteBufferFromOwnString out;
        for (size_t i = 0; i < arguments_profile_data.size(); ++i)
        {
            if (i)
              out << std::string(",");
            auto & profile_data = arguments_profile_data[i];
            std::lock_guard lock(profile_data.mutex);
            out << std::string("(seen_rows:") << profile_data.seen_rows;
            out << std::string(", selected_rows:") << profile_data.selected_rows;
            out << std::string(", elapsed_ns:") << profile_data.elapsed_ns;
            out << std::string(", rank:") << profile_data.rank << std::string(")");
        }
        return out.str();
    }
private:
    ContextPtr context;
    size_t inverted_rank;
    std::atomic<size_t> num_arguments = 0;
    size_t last_adjust_rows = 0;
    std::atomic<size_t> current_executed_rows = 0;
    bool could_adjust = true;
    std::atomic<bool> has_setup = false;
    std::atomic<size_t> in_adjusting_process_count = 0;
    Block header;
    Poco::Logger * logger = &Poco::Logger::get("DynamicShortCircuitExecutionOrder");

    std::shared_mutex mutex;
    std::shared_ptr<std::vector<size_t>> adjusted_arguments_execution_order;
    std::vector<ProfileData> arguments_profile_data;

    ALWAYS_INLINE void trySetup(const ColumnsWithTypeAndName & arguments)
    {
        if (has_setup)
          return;
        std::unique_lock lock(mutex);
        if (has_setup)
          return;
        header = Block(arguments).cloneEmpty();
        num_arguments = arguments.size();
        adjusted_arguments_execution_order = std::make_shared<std::vector<size_t>>();
        for (size_t i = 0; i < num_arguments; ++i)
        {
            arguments_profile_data.emplace_back(ProfileData());
            adjusted_arguments_execution_order->emplace_back(i);
        }

        for (const auto & argument : arguments)
        {
          if (checkAndGetShortCircuitArgument(argument.column))
          {
              continue;
          }
          else
          {
              LOG_INFO(
                  logger,
                  "Disable dynamic adjustment of arguments execution order. Since {} is not a lazy execution column",
                  argument.name);
              could_adjust = false;
          }
        }
        has_setup = true;
    }

    /**
      * How the rank values come
      * a.elapsed + a.selectivity * b.elapsed < b.elapsed + b.selectivity * a.elapsed =>
      * a.elapsed * (1 - b.selectivity) < b.elapsed * (1 - a.selectivity) =>
      * a.elapsed / (1 - a.selectivity) < b.elapsed / (1 - b.selectivity)
      *
      * For inverted rank, it should be a.elapsed / a.selectivity < b.elapsed / b.selectivity
      */
    ALWAYS_INLINE double nextRank(size_t pos)
    {
        auto & data = arguments_profile_data[pos];
        if (!inverted_rank)
        {
            std::lock_guard lock(data.mutex);
            if (!data.seen_rows)
                return data.elapsed_ns;
            data.rank
                = static_cast<double>(data.elapsed_ns) / (1 - static_cast<double>(data.selected_rows) / data.seen_rows) + 0.1 * data.rank;
            return data.rank;
        }
        else
        {
            if (!data.seen_rows)
                return 0.0;
            data.rank
                = static_cast<double>(data.elapsed_ns) / (static_cast<double>(data.selected_rows) / data.seen_rows) + 0.1 * data.rank;
            return data.rank;
        }
    }
};
}
