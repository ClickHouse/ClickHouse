#include <Storages/Distributed/DistributedAsyncInsertHelpers.h>
#include <Storages/Distributed/DistributedAsyncInsertHeader.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CheckingCompressedReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <QueryPipeline/RemoteInserter.h>
#include <Formats/NativeReader.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int UNKNOWN_CODEC;
    extern const int CANNOT_DECOMPRESS;
    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int TOO_LARGE_SIZE_COMPRESSED;
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int EMPTY_DATA_PASSED;
    extern const int DISTRIBUTED_BROKEN_BATCH_INFO;
    extern const int DISTRIBUTED_BROKEN_BATCH_FILES;
}

/// 'remote_error' argument is used to decide whether some errors should be
/// ignored or not, in particular:
///
/// - ATTEMPT_TO_READ_AFTER_EOF should not be ignored
///   if we receive it from remote (receiver), since:
///   - the sender will got ATTEMPT_TO_READ_AFTER_EOF when the client just go away,
///     i.e. server had been restarted
///   - since #18853 the file will be checked on the sender locally, and
///     if there is something wrong with the file itself, we will receive
///     ATTEMPT_TO_READ_AFTER_EOF not from the remote at first
///     and mark batch as broken.
bool isDistributedSendBroken(int code, bool remote_error)
{
    return code == ErrorCodes::CHECKSUM_DOESNT_MATCH
        || code == ErrorCodes::EMPTY_DATA_PASSED
        || code == ErrorCodes::TOO_LARGE_SIZE_COMPRESSED
        || code == ErrorCodes::CANNOT_READ_ALL_DATA
        || code == ErrorCodes::UNKNOWN_CODEC
        || code == ErrorCodes::CANNOT_DECOMPRESS
        || code == ErrorCodes::DISTRIBUTED_BROKEN_BATCH_INFO
        || code == ErrorCodes::DISTRIBUTED_BROKEN_BATCH_FILES
        || (!remote_error && code == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF);
}

void writeAndConvert(RemoteInserter & remote, const DistributedAsyncInsertHeader & distributed_header, ReadBufferFromFile & in)
{
    CompressedReadBuffer decompressing_in(in);
    NativeReader block_in(decompressing_in, distributed_header.revision);

    while (Block block = block_in.read())
    {
        auto converting_dag = ActionsDAG::makeConvertingActions(
            block.cloneEmpty().getColumnsWithTypeAndName(),
            remote.getHeader().getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name);

        auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));
        converting_actions->execute(block);
        remote.write(block);
    }
}

void writeRemoteConvert(
    const DistributedAsyncInsertHeader & distributed_header,
    RemoteInserter & remote,
    bool compression_expected,
    ReadBufferFromFile & in,
    LoggerPtr log)
{
    if (!remote.getHeader())
    {
        CheckingCompressedReadBuffer checking_in(in);
        remote.writePrepared(checking_in);
        return;
    }

    /// This is old format, that does not have header for the block in the file header,
    /// applying ConvertingTransform in this case is not a big overhead.
    ///
    /// Anyway we can get header only from the first block, which contain all rows anyway.
    if (!distributed_header.block_header)
    {
        LOG_TRACE(log, "Processing batch {} with old format (no header)", in.getFileName());

        writeAndConvert(remote, distributed_header, in);
        return;
    }

    if (!blocksHaveEqualStructure(distributed_header.block_header, remote.getHeader()))
    {
        LOG_WARNING(log,
            "Structure does not match (remote: {}, local: {}), implicit conversion will be done",
            remote.getHeader().dumpStructure(), distributed_header.block_header.dumpStructure());

        writeAndConvert(remote, distributed_header, in);
        return;
    }

    /// If connection does not use compression, we have to uncompress the data.
    if (!compression_expected)
    {
        writeAndConvert(remote, distributed_header, in);
        return;
    }

    if (distributed_header.revision != remote.getServerRevision())
    {
        writeAndConvert(remote, distributed_header, in);
        return;
    }

    /// Otherwise write data as it was already prepared (more efficient path).
    CheckingCompressedReadBuffer checking_in(in);
    remote.writePrepared(checking_in);
}

}
