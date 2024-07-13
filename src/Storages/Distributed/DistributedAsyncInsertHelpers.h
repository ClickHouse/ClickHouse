#pragma once

#include <Common/Logger.h>


namespace DB
{

struct DistributedAsyncInsertHeader;
class ReadBufferFromFile;
class RemoteInserter;

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
bool isDistributedSendBroken(int code, bool remote_error);

void writeRemoteConvert(
    const DistributedAsyncInsertHeader & distributed_header,
    RemoteInserter & remote,
    bool compression_expected,
    ReadBufferFromFile & in,
    LoggerPtr log);

}
