#pragma once

#include <atomic>
#include <list>
#include <mutex>
#include <Columns/IColumn.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Names.h>
#include <IO/WriteBuffer.h>
#include <Storages/NATS/NATSConnection.h>
#include <Common/ConcurrentBoundedQueue.h>

namespace DB
{

class WriteBufferToNATSProducer : public WriteBuffer
{
public:
    WriteBufferToNATSProducer(
        const NATSConfiguration & configuration_,
        ContextPtr global_context,
        const String & subject_,
        std::atomic<bool> & shutdown_called_,
        Poco::Logger * log_,
        std::optional<char> delimiter,
        size_t rows_per_message,
        size_t chunk_size_);

    ~WriteBufferToNATSProducer() override;

    void countRow();
    void activateWriting() { writing_task->activateAndSchedule(); }
    void updateMaxWait() { wait_payloads.store(true); }

private:
    void nextImpl() override;
    void addChunk();
    void reinitializeChunks();

    void iterateEventLoop();
    void writingFunc();
    void publish();

    static void publishThreadFunc(void * arg);

    NATSConnectionManager connection;
    const String subject;

    /* false: when shutdown is called
     * true: in all other cases
     */
    std::atomic<bool> & shutdown_called;

    BackgroundSchedulePool::TaskHolder writing_task;

    /* payloads.queue:
     *      - payloads are pushed to queue in countRow and popped by another thread in writingFunc, each payload gets into queue only once
     */
    ConcurrentBoundedQueue<String> payloads;

    /* false: message delivery successfully ended: publisher received confirm from server that all published
     *  1) persistent messages were written to disk
     *  2) non-persistent messages reached the queue
     * true: continue to process deliveries and returned messages
     */
    bool wait_all = true;

    /* false: until writeSuffix is called
     * true: means payloads.queue will not grow anymore
     */
    std::atomic<bool> wait_payloads = false;

    Poco::Logger * log;
    const std::optional<char> delim;
    const size_t max_rows;
    const size_t chunk_size;
    size_t rows = 0;
    std::list<std::string> chunks;
};

}
