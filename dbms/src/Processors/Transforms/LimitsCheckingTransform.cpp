#include <Processors/Transforms/LimitsCheckingTransform.h>
#include <Interpreters/Quota.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_ROWS;
    extern const int TOO_MANY_BYTES;
    extern const int TOO_MANY_ROWS_OR_BYTES;
    extern const int TIMEOUT_EXCEEDED;
    extern const int TOO_SLOW;
    extern const int LOGICAL_ERROR;
    extern const int BLOCKS_HAVE_DIFFERENT_STRUCTURE;
    extern const int TOO_DEEP_PIPELINE;
}


void ProcessorProfileInfo::update(const Chunk & block)
{
    ++blocks;
    rows += block.getNumRows();
    bytes += block.bytes();
}

LimitsCheckingTransform::LimitsCheckingTransform(const Block & header_, LocalLimits limits_)
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

    if (!limits.speed_limits.checkTimeLimit(info.total_stopwatch.elapsed(), limits.timeout_overflow_mode))
    {
        stopReading();
        return;
    }

    if (chunk)
    {
        info.update(chunk);

        if (limits.mode == LimitsMode::LIMITS_CURRENT &&
            !limits.size_limits.check(info.rows, info.bytes, "result", ErrorCodes::TOO_MANY_ROWS_OR_BYTES))
            stopReading();

        if (quota != nullptr)
            checkQuota(chunk);
    }
}

void LimitsCheckingTransform::checkQuota(Chunk & chunk)
{
    switch (limits.mode)
    {
        case LimitsMode::LIMITS_TOTAL:
            /// Checked in `progress` method.
            break;

        case LimitsMode::LIMITS_CURRENT:
        {
            time_t current_time = time(nullptr);
            double total_elapsed = info.total_stopwatch.elapsedSeconds();

            quota->checkAndAddResultRowsBytes(current_time, chunk.getNumRows(), chunk.bytes());
            quota->checkAndAddExecutionTime(current_time, Poco::Timespan((total_elapsed - prev_elapsed) * 1000000.0));

            prev_elapsed = total_elapsed;
            break;
        }
    }
}

}
