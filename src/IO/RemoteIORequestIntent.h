#pragma once

namespace DB
{

/// Cleanup may finish deleting remote resources after its owning operation is cancelled.
/// This describes one request, not permission for other I/O performed by the same thread.
enum class RemoteIORequestIntent
{
    Regular,
    Cleanup,
};

}
