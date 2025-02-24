#include <Processors/Transforms/LimitsCheckingTransform.h>
#include <Access/EnabledQuota.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_ROWS_OR_BYTES;
    extern const int TIMEOUT_EXCEEDED;
}


void ProcessorProfileInfo::update(const Chunk & block)
{
    ++blocks;
    rows += block.getNumRows();
    bytes += block.bytes();
}

LimitsCheckingTransform::LimitsCheckingTransform(const Block & header_, StreamLocalLimits limits_)
    : ISimpleTransform(header_, header_, false)
    , limits(std::move(limits_))
{
}

void LimitsCheckingTransform::transform(Chunk & chunk)
{
    if (!info.started)
    {
        info.total_stopwatch.start();
        info.started = true;
    }

    if (const UInt64 & max_execution_time = limits.speed_limits.max_execution_time.totalMilliseconds(); max_execution_time != 0)
    {
        // Check for elapsed time or if the query has been marked as timed out.
        if (info.total_stopwatch.elapsedMilliseconds() > static_cast<UInt64>(max_execution_time) ||
            (process_list_elem && process_list_elem->isTimedOut()))
        {
            ExecutionSpeedLimits::handleOverflowMode(
                limits.timeout_overflow_mode,
                ErrorCodes::TIMEOUT_EXCEEDED,
                "Timeout exceeded: elapsed {} ms, maximum: {} ms",
                static_cast<double>(info.total_stopwatch.elapsed()) / 1000000ULL,
                max_execution_time);

            stopReading();
            return;
        }
    }

    if (chunk)
    {
        info.update(chunk);

        if (limits.mode == LimitsMode::LIMITS_CURRENT &&
            !limits.size_limits.check(info.rows, info.bytes, "result", ErrorCodes::TOO_MANY_ROWS_OR_BYTES))
        {
            stopReading();
        }

        if (quota)
            checkQuota(chunk);
    }
}

void LimitsCheckingTransform::checkQuota(Chunk & chunk)
{
    switch (limits.mode)
    {
        case LimitsMode::LIMITS_TOTAL:
            /// Checked in ISource::progress method.
            break;

        case LimitsMode::LIMITS_CURRENT:
        {
            UInt64 total_elapsed = info.total_stopwatch.elapsedNanoseconds();
            quota->used(
                {QuotaType::RESULT_ROWS, chunk.getNumRows()},
                {QuotaType::RESULT_BYTES, chunk.bytes()},
                {QuotaType::EXECUTION_TIME, total_elapsed - prev_elapsed});
            prev_elapsed = total_elapsed;
            break;
        }
    }
}

}
