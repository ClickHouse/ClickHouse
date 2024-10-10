#include <chrono>
#include <Storages/System/StorageSystemAsyncLoader.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Interpreters/Context.h>
#include <base/EnumReflection.h>


namespace DB
{

using TimePoint = LoadJob::TimePoint;
using TimeDuration = LoadJob::TimePoint::duration;

static constexpr auto TIME_SCALE = 6;

namespace
{
    template <typename Type>
    DataTypeEnum8::Values getTypeEnumValues()
    {
        DataTypeEnum8::Values enum_values;
        for (auto value : magic_enum::enum_values<Type>())
            enum_values.emplace_back(magic_enum::enum_name(value), magic_enum::enum_integer(value));
        return enum_values;
    }

    auto timeInMicroseconds(const TimePoint & time_point)
    {
        auto time_us = std::chrono::duration_cast<std::chrono::microseconds>(time_point.time_since_epoch()).count();
        DecimalUtils::DecimalComponents<DateTime64> components{time_us / 1'000'000, time_us % 1'000'000};
        return DecimalField(DecimalUtils::decimalFromComponents<DateTime64>(components, TIME_SCALE), TIME_SCALE);
    }

    Field optionalTimeInMicroseconds(const TimePoint & time_point)
    {
        if (time_point == TimePoint{})
            return {};
        return timeInMicroseconds(time_point);
    }
}

ColumnsDescription StorageSystemAsyncLoader::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"job", std::make_shared<DataTypeString>(), "Job name (may be not unique)."},
        {"job_id", std::make_shared<DataTypeUInt64>(), "Unique ID of the job."},
        {"dependencies", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), "List of IDs of jobs that should be done before this job."},
        {"dependencies_left", std::make_shared<DataTypeUInt64>(), "Current number of dependencies left to be done."},
        {"status", std::make_shared<DataTypeEnum8>(getTypeEnumValues<LoadStatus>()), "Current load status of a job: PENDING: Load job is not started yet. OK: Load job executed and was successful. FAILED: Load job executed and failed. CANCELED: Load job is not going to be executed due to removal or dependency failure."},
        {"is_executing", std::make_shared<DataTypeUInt8>(), "The job is currently being executed by a worker."},
        {"is_blocked", std::make_shared<DataTypeUInt8>(), "The job waits for its dependencies to be done."},
        {"is_ready", std::make_shared<DataTypeUInt8>(), "The job is ready to be executed and waits for a worker."},
        {"elapsed", std::make_shared<DataTypeFloat64>(), "Seconds elapsed since start of execution. Zero if job is not started. Total execution time if job finished."},
        {"pool_id", std::make_shared<DataTypeUInt64>(), "ID of a pool currently assigned to the job."},
        {"pool", std::make_shared<DataTypeString>(), "Name of `pool_id` pool."},
        {"priority", std::make_shared<DataTypeInt64>(), "Priority of `pool_id` pool."},
        {"execution_pool_id", std::make_shared<DataTypeUInt64>(), "ID of a pool the job is executed in. Equals initially assigned pool before execution starts."},
        {"execution_pool", std::make_shared<DataTypeString>(), "Name of `execution_pool_id` pool."},
        {"execution_priority", std::make_shared<DataTypeInt64>(), "Priority of execution_pool_id pool."},
        {"ready_seqno", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "Not null for ready jobs. Worker pulls the next job to be executed from a ready queue of its pool. If there are multiple ready jobs, then job with the lowest value of `ready_seqno` is picked."},
        {"waiters", std::make_shared<DataTypeUInt64>(), "The number of threads waiting on this job."},
        {"exception", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Not null for failed and canceled jobs. Holds error message raised during query execution or error leading to cancelling of this job along with dependency failure chain of job names."},
        {"schedule_time", std::make_shared<DataTypeDateTime64>(TIME_SCALE), "Time when job was created and scheduled to be executed (usually with all its dependencies)."},
        {"enqueue_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(TIME_SCALE)), "Time when job became ready and was enqueued into a ready queue of it's pool. Null if the job is not ready yet."},
        {"start_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(TIME_SCALE)), "Time when worker dequeues the job from ready queue and start its execution. Null if the job is not started yet."},
        {"finish_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(TIME_SCALE)), "Time when job execution is finished. Null if the job is not finished yet."},
    };
}

void StorageSystemAsyncLoader::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    TimePoint now = std::chrono::system_clock::now();

    AsyncLoader & async_loader = context->getAsyncLoader();

    for (const auto & state : async_loader.getJobStates())
    {
        Array dependencies;
        dependencies.reserve(state.job->dependencies.size());
        for (const auto & dep : state.job->dependencies)
            dependencies.emplace_back(dep->jobId());

        TimePoint started = state.job->startTime();
        TimePoint finished = state.job->finishTime();
        TimePoint last = finished != TimePoint{} ? finished : now;
        TimeDuration elapsed = started != TimePoint{} ? last - started : TimeDuration{0};
        double elapsed_sec = std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count() * 1e-9;

        Field ready_seqno;
        if (state.ready_seqno)
            ready_seqno = state.ready_seqno;

        Field exception;
        if (state.job->exception())
        {
            try
            {
                std::rethrow_exception(state.job->exception());
            }
            catch (...)
            {
                exception = getCurrentExceptionMessage(false);
            }
        }

        size_t i = 0;
        res_columns[i++]->insert(state.job->name);
        res_columns[i++]->insert(state.job->jobId());
        res_columns[i++]->insert(dependencies);
        res_columns[i++]->insert(state.dependencies_left);
        res_columns[i++]->insert(static_cast<Int8>(state.job->status()));
        res_columns[i++]->insert(state.is_executing);
        res_columns[i++]->insert(state.is_blocked);
        res_columns[i++]->insert(state.is_ready);
        res_columns[i++]->insert(elapsed_sec);
        res_columns[i++]->insert(state.job->pool());
        res_columns[i++]->insert(async_loader.getPoolName(state.job->pool()));
        res_columns[i++]->insert(async_loader.getPoolPriority(state.job->pool()).value);
        res_columns[i++]->insert(state.job->executionPool());
        res_columns[i++]->insert(async_loader.getPoolName(state.job->executionPool()));
        res_columns[i++]->insert(async_loader.getPoolPriority(state.job->executionPool()).value);
        res_columns[i++]->insert(ready_seqno);
        res_columns[i++]->insert(state.job->waitersCount());
        res_columns[i++]->insert(exception);
        res_columns[i++]->insert(timeInMicroseconds(state.job->scheduleTime()));
        res_columns[i++]->insert(optionalTimeInMicroseconds(state.job->enqueueTime()));
        res_columns[i++]->insert(optionalTimeInMicroseconds(state.job->startTime()));
        res_columns[i++]->insert(optionalTimeInMicroseconds(state.job->finishTime()));
    }
}

}
