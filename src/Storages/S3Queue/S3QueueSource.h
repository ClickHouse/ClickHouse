//#pragma once
//
//#include <Processors/ISource.h>
//#include <Storages/S3Queue/ReadBufferFromS3.h>
//#include <Storages/S3Queue/StorageS3Queue.h>
//
//namespace Poco
//{
//class Logger;
//}
//namespace DB
//{
//class S3QueueSource : public ISource
//{
//public:
//    S3QueueSource(
//        StorageS3Queue & storage_,
//        const StorageSnapshotPtr & storage_snapshot_,
//        const ContextPtr & context_,
//        const Names & columns,
//        size_t max_block_size_,
//        size_t poll_time_out_,
//        size_t stream_number_,
//        size_t max_streams_number_);
//
//    String getName() const override { return "S3Queue"; }
//
//    bool noRecords() { return !buffer || buffer->noRecords(); }
//
//    void onFinish();
//
//    virtual ~S3QueueSource() override;
//
//protected:
//    Chunk generate() override;
//
//private:
//    StorageS3Queue & storage;
//    StorageSnapshotPtr storage_snapshot;
//    ContextPtr context;
//    Names column_names;
//    UInt64 max_block_size;
//
//    size_t poll_time_out;
//
//    size_t stream_number;
//    size_t max_streams_number;
//
//    std::unique_ptr<ReadBufferFromS3> buffer;
//
//    Block non_virtual_header;
//    Block virtual_header;
//};
//
//}
