#include <Processors/Transforms/LimitsCheckingTransform.h>
#include <Access/QuotaContext.h>

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


static bool handleOverflowMode(OverflowMode mode, const String & message, int code)
{
    switch (mode)
    {
        case OverflowMode::THROW:
            throw Exception(message, code);
        case OverflowMode::BREAK:
            return false;
        default:
            throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
    }
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

//LimitsCheckingTransform::LimitsCheckingTransform(const Block & header, LocalLimits limits, QueryStatus * process_list_elem)
//    : ISimpleTransform(header, header, false)
//    , limits(std::move(limits))
//    , process_list_elem(process_list_elem)
//{
//}

void LimitsCheckingTransform::transform(Chunk & chunk)
{
    if (!info.started)
    {
        info.total_stopwatch.start();
        info.started = true;
    }

    if (!checkTimeLimit())
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

        if (quota)
            checkQuota(chunk);
    }
}

bool LimitsCheckingTransform::checkTimeLimit()
{
    if (limits.speed_limits.max_execution_time != 0
        && info.total_stopwatch.elapsed() > static_cast<UInt64>(limits.speed_limits.max_execution_time.totalMicroseconds()) * 1000)
        return handleOverflowMode(limits.timeout_overflow_mode,
                                  "Timeout exceeded: elapsed " + toString(info.total_stopwatch.elapsedSeconds())
                                  + " seconds, maximum: " + toString(limits.speed_limits.max_execution_time.totalMicroseconds() / 1000000.0),
                                  ErrorCodes::TIMEOUT_EXCEEDED);

    return true;
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
            UInt64 total_elapsed = info.total_stopwatch.elapsedNanoseconds();
            quota->used({Quota::RESULT_ROWS, chunk.getNumRows()}, {Quota::RESULT_BYTES, chunk.bytes()}, {Quota::EXECUTION_TIME, total_elapsed - prev_elapsed});
            prev_elapsed = total_elapsed;
            break;
        }
    }
}

}
