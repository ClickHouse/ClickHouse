#pragma once

namespace DB
{

/// How an executor handles a cancellation callback returning true.
/// Exceptions from the callback always abort execution.
enum class CancelCallbackMode
{
    Cancel,
    PartialResult,
};

}
