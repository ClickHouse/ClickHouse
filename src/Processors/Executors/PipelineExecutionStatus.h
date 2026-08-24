#pragma once

namespace DB
{

enum class PipelineExecutionStatus
{
    NotStarted,
    Executing,
    Finished,
    Exception,
    CancelledByUser,
    CancelledByTimeout,
};

}
