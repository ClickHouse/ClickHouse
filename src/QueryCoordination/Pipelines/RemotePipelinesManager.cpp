#include <Interpreters/Context.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <QueryCoordination/Pipelines/RemotePipelinesManager.h>
#include <QueryPipeline/ProfileInfo.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/scope_guard_safe.h>
#include <Common/setThreadName.h>

namespace DB
{

namespace ErrorCodes
{
extern const int SYSTEM_ERROR;
}

void RemotePipelinesManager::processPacket(Packet & packet, ManagedNode & node)
{
    switch (packet.type)
    {
        case Protocol::Server::ProfileInfo: {
            if (profile_info_callback)
                profile_info_callback(packet.profile_info);
            break;
        }
        case Protocol::Server::Log: {
            /// Pass logs from remote server to client
            if (auto log_queue = CurrentThread::getInternalTextLogsQueue())
                log_queue->pushBlock(std::move(packet.block));
            break;
        }
        case Protocol::Server::Progress: {
            /// update progress
            if (read_progress_callback)
            {
                LOG_DEBUG(log, "{} update progress read_rows {}", node.host_port, packet.progress.read_rows);
                LOG_DEBUG(log, "{} update progress read_bytes {}", node.host_port, packet.progress.read_bytes);
                LOG_DEBUG(log, "{} update progress total_rows_to_read {}", node.host_port, packet.progress.total_rows_to_read);
                LOG_DEBUG(log, "{} update progress total_bytes_to_read {}", node.host_port, packet.progress.total_bytes_to_read);
                LOG_DEBUG(log, "{} update progress written_rows {}", node.host_port, packet.progress.written_rows);
                LOG_DEBUG(log, "{} update progress written_bytes {}", node.host_port, packet.progress.written_bytes);
                LOG_DEBUG(log, "{} update progress result_rows {}", node.host_port, packet.progress.result_rows);
                LOG_DEBUG(log, "{} update progress result_bytes {}", node.host_port, packet.progress.result_bytes);
                LOG_DEBUG(log, "{} update progress elapsed_ns {}", node.host_port, packet.progress.elapsed_ns);

                if (packet.progress.total_rows_to_read)
                    read_progress_callback->addTotalRowsApprox(packet.progress.total_rows_to_read);

                if (!read_progress_callback->onProgress(packet.progress.read_rows, packet.progress.read_bytes, storage_limits))
                    LOG_WARNING(log, "Check Limit failed");

                LOG_DEBUG(log, "Updated progress from {}", node.host_port);
            }
            break;
        }
        case Protocol::Server::ProfileEvents: {
            /// Pass profile events from remote server to client
            if (auto profile_queue = CurrentThread::getInternalProfileEventsQueue())
                if (!profile_queue->emplace(std::move(packet.block)))
                    throw Exception(ErrorCodes::SYSTEM_ERROR, "Could not push into profile queue");
            break;
        }
        case Protocol::Server::Exception: {
            packet.exception->rethrow();
            break;
        }
        case Protocol::Server::EndOfStream: {
            node.is_finished = true;

            LOG_DEBUG(log, "{} is finished", node.host_port);
            break;
        }

        default:
            throw;
    }
}

void RemotePipelinesManager::receiveReporter(ThreadGroupPtr thread_group)
{
    SCOPE_EXIT_SAFE(if (thread_group) CurrentThread::detachFromGroupIfNotDetached(););
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

                auto packet = node.connection->receivePacket();
                processPacket(packet, node);
            }

            if (allFinished())
            {
                finish_event.set();
                break;
            }
        }
    }
    catch (...)
    {
        exception_callback(std::current_exception());
    }
}

bool RemotePipelinesManager::allFinished()
{
    std::lock_guard lock(finish_mutex);
    for (auto & node : managed_nodes)
        if (!node.is_finished)
            return false;
    return true;
}


void RemotePipelinesManager::asyncReceiveReporter()
{
    auto func = [this, thread_group = CurrentThread::getGroup()]() { receiveReporter(thread_group); };

    receive_reporter_thread = ThreadFromGlobalPool(std::move(func));
}


void RemotePipelinesManager::waitFinish()
{
    if (!allFinished())
        finish_event.wait();
}

void RemotePipelinesManager::cancel()
{
    if (cancelled)
        return;

    LOG_DEBUG(log, "cancel");

    cancelled = true;

    if (receive_reporter_thread.joinable())
        receive_reporter_thread.join();

    if (!allFinished())
    {
        for (auto & node : managed_nodes)
        {
            /// drain
            while (node.connection->hasReadPendingData() && !node.is_finished)
            {
                auto packet = node.connection->receivePacket();
                processPacket(packet, node);
            }

            if (!node.is_finished)
                node.connection->sendCancel();

            /// wait EndOfStream or Exception
            Packet packet;
            while (!node.is_finished && !packet.exception)
            {
                packet = node.connection->receivePacket();
                processPacket(packet, node);
            }
        }

        for (auto & node : managed_nodes)
            node.connection->disconnect();
    }

    finish_event.set();

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
