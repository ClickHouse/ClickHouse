#pragma once

#include <Core/Block.h>
#include <Core/StreamingHandleErrorMode.h>
#include <Core/Streaming/Settings.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/ISource.h>
#include <Storages/NATS/NATSConnection.h>
#include <Storages/StorageSnapshot.h>
#include <Common/Logger.h>

#include <nats.h>

#include <memory>
#include <optional>


namespace DB
{

/// Reads the JetStream stream of a `NATS` table for a streaming query (`SELECT ... FROM nats_table STREAM`).
///
/// Unlike a direct `SELECT` from the table, it does not use the durable consumer of the table and does not
/// acknowledge the messages. It creates an ephemeral ordered consumer of the subjects of the table, starting
/// after the stream sequence in the cursor of the query:
///     {'<stream name>': <stream sequence of the last read message>}
/// Without the cursor, the stream is read from the beginning.
///
/// A bounded stream (`STREAM BOUNDED`) finishes once it has read all the messages of the stream at its start;
/// the messages published later are not read.
class NATSStreamSource final : public ISource
{
public:
    struct Params
    {
        NATSConnectionPtr connection;
        String stream_name;
        std::vector<String> subjects;
        String format_name;
        StreamingHandleErrorMode handle_error_mode = StreamingHandleErrorMode::DEFAULT;
        size_t max_block_size = 0;
        size_t poll_timeout_ms = 0;
        String table_name;
    };

    NATSStreamSource(
        Params params_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        const Names & columns,
        const StreamSettings & stream_settings_,
        LoggerPtr log_);

    ~NATSStreamSource() override;

    String getName() const override { return "NATSStreamSource"; }

    Chunk generate() override;
    std::optional<Chunk> tryGenerate() override;

private:
    void start();
    bool hasNothingPending();
    void reportFinalCursor() const;
    void finish();

    Params params;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
    const StreamSettings stream_settings;
    LoggerPtr log;

    const Block non_virtual_header;
    const Block virtual_header;

    std::unique_ptr<jsCtx, decltype(&jsCtx_Destroy)> jet_stream_ctx;
    std::unique_ptr<natsSubscription, decltype(&natsSubscription_Destroy)> subscription;

    /// The name of the ephemeral consumer created for this read.
    String consumer_name;

    /// The stream sequence of the last read message (or of the cursor), 0 if nothing was read.
    UInt64 last_sequence = 0;
    /// For a bounded read, the last stream sequence at its start: later messages are not read.
    UInt64 end_sequence = 0;
    /// The number of messages received by the consumer.
    UInt64 received_messages = 0;
    bool started = false;
    bool stream_finished = false;
};

}
