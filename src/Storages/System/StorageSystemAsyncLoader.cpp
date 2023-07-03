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
        else
            return timeInMicroseconds(time_point);
    }
}

NamesAndTypesList StorageSystemAsyncLoader::getNamesAndTypes()
{
    return {
        { "job",                std::make_shared<DataTypeString>() },
        { "job_id",             std::make_shared<DataTypeUInt64>() },
        { "dependencies",       std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()) },
        { "dependencies_left",  std::make_shared<DataTypeUInt64>() },
        { "status",             std::make_shared<DataTypeEnum8>(getTypeEnumValues<LoadStatus>()) },
        { "is_executing",       std::make_shared<DataTypeUInt8>() },
        { "is_blocked",         std::make_shared<DataTypeUInt8>() },
        { "is_ready",           std::make_shared<DataTypeUInt8>() },
        { "elapsed",            std::make_shared<DataTypeFloat64>()},
        { "pool_id",            std::make_shared<DataTypeInt64>() },
        { "pool",               std::make_shared<DataTypeString>() },
        { "priority",           std::make_shared<DataTypeInt64>() },
        { "execution_pool_id",  std::make_shared<DataTypeInt64>() },
        { "execution_pool",     std::make_shared<DataTypeString>() },
        { "execution_priority", std::make_shared<DataTypeInt64>() },
        { "ready_seqno",        std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()) },
        { "waiters",            std::make_shared<DataTypeUInt64>() },
        { "exception",          std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()) },
        { "schedule_time",      std::make_shared<DataTypeDateTime64>(TIME_SCALE) },
        { "enqueue_time",       std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(TIME_SCALE)) },
        { "start_time",         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(TIME_SCALE)) },
        { "finish_time",        std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(TIME_SCALE)) },
    };
}

void StorageSystemAsyncLoader::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
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
            catch (Exception & e)
            {
                exception = e.message();
            }
            catch (...) // just in case
            {
                exception = String("unknown error");
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
