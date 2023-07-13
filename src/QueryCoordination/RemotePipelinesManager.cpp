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
                if (node.is_local || node.is_finished)
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
                            progress_callback(packet.progress);
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
        if (node.is_local)
            continue;

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
