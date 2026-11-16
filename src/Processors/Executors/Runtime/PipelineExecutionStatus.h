#pragma once

namespace DB
{

enum class PipelineExecutionStatus
{
    NotStarted,
    Executing,
    Exception,
    CancelledByUser,
    CancelledByTimeout,
};

}
