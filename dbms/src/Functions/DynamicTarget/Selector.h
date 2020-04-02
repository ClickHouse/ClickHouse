#pragma once

#include "Target.h"

namespace DB::DynamicTarget
{

class PerformanceStatistic
{};

template <typename... Params>
class SelectorExecutor
{
public:
    using Executor = std::function<void(Params...)>;
    // Should register all executors before 
    void registerExecutor(std::optional<TargetArch> arch, Executor executor)
    {
        if (!arch || IsArchSupported(*arch)) {
            executors_.emplace_back(std::move(executor));
        }
    }

    void execute(Params... params)
    {
        if (executors_.empty()) {
            throw "There are no realizations for this arch Arch";
        }
        int impl = 0;
        // TODO: choose implementation.
        executors_[impl](params...);
    }

private:
    std::vector<Executor> executors_;
    PerformanceStatistic statistic_;
};

} // namespace DB::DynamicTarget