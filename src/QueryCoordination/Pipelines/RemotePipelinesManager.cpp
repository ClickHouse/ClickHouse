#include <Interpreters/Context.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <QueryCoordination/RemotePipelinesManager.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/scope_guard_safe.h>
#include <Common/setThreadName.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <QueryPipeline/ProfileInfo.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYSTEM_ERROR;
}

void RemotePipelinesManager::receiveReporter(ThreadGroupPtr thread_group)
{
    SCOPE_EXIT_SAFE(
        if (thread_group)
            CurrentThread::detachFromGroupIfNotDetached();
    );
    setThreadName("receReporter");

    try
    {
        if (thread_group)
            CurrentThread::attachToGroup(thread_group);

        while (!cancelled_reading.load() && !cancelled.load())
        {
            /// TODO select or epoll
            for (auto & node : managed_nodes)
            {
                if (node.is_finished)
                    continue;

                Packet packet = node.connection->receivePacket();
                switch (packet.type)
                {
                    case Protocol::Server::ProfileInfo:
                    {
                        if (profile_info_callback)
                            profile_info_callback(packet.profile_info);
                        break;
                    }
                    case Protocol::Server::Log:
                    {
                        /// Pass logs from remote server to client
                        if (auto log_queue = CurrentThread::getInternalTextLogsQueue())
                            log_queue->pushBlock(std::move(packet.block));
                        break;
                    }
                    case Protocol::Server::Progress:
                    {
                        /// update progress
                        if (progress_callback)
                        {
                            LOG_DEBUG(log, "Update progress read_rows {}", packet.progress.read_rows);
                            LOG_DEBUG(log, "Update progress read_bytes {}", packet.progress.read_bytes);
                            LOG_DEBUG(log, "Update progress total_rows_to_read {}", packet.progress.total_rows_to_read);
                            LOG_DEBUG(log, "Update progress total_bytes_to_read {}", packet.progress.total_bytes_to_read);
                            LOG_DEBUG(log, "Update progress written_rows {}", packet.progress.written_rows);
                            LOG_DEBUG(log, "Update progress written_bytes {}", packet.progress.written_bytes);
                            LOG_DEBUG(log, "Update progress result_rows {}", packet.progress.result_rows);
                            LOG_DEBUG(log, "Update progress result_bytes {}", packet.progress.result_bytes);
                            LOG_DEBUG(log, "Update progress elapsed_ns {}", packet.progress.elapsed_ns);
                            progress_callback(packet.progress);
                            LOG_DEBUG(log, "Updated progress from {}", node.host_port);
                        }
                        break;
                    }
                    case Protocol::Server::ProfileEvents:
                    {
                        /// Pass profile events from remote server to client
                        if (auto profile_queue = CurrentThread::getInternalProfileEventsQueue())
                            if (!profile_queue->emplace(std::move(packet.block)))
                                throw Exception(ErrorCodes::SYSTEM_ERROR, "Could not push into profile queue");
                        break;
                    }
                    case Protocol::Server::Exception:
                    {
                        packet.exception->rethrow();
                        break;
                    }
                    case Protocol::Server::EndOfStream:
                    {
                        node.is_finished = true;

                        LOG_DEBUG(log, "{} is finished", node.host_port);
                        break;
                    }

                    default:
                        throw;
                }
            }
        }
    }
    catch (...)
    {
        exception_callback(std::current_exception());
    }
}


void RemotePipelinesManager::asyncReceiveReporter()
{
    auto func = [this, thread_group = CurrentThread::getGroup()]() { receiveReporter(thread_group); };

    receive_reporter_thread = ThreadFromGlobalPool(std::move(func));
}


void RemotePipelinesManager::cancel()
{
    if (cancelled)
        return;

    LOG_DEBUG(log, "cancel");

    cancelled = true;

    for (auto & node : managed_nodes)
    {
        node.connection->sendCancel();
    }

    if (receive_reporter_thread.joinable())
        receive_reporter_thread.join();

    LOG_DEBUG(log, "cancelled");
}


RemotePipelinesManager::~RemotePipelinesManager()
{
    try
    {
        cancel();
    }
    catch (...)
    {
        tryLogCurrentException("RemotePipelinesManager");
    }
}

}
