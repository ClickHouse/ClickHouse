#pragma once

#include <Common/Logger.h>
#include <base/types.h>

#include <utility>


namespace DB
{

struct DistributedAsyncInsertHeader;
class ClientInfo;
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

/// The identity the shard authenticates a queued insert as (with the interserver `<secret>`): `initial_user`
/// and the host of `initial_address`, which is checked against the allowed hosts of that user.
/// A batch is sent with the `ClientInfo` of its first file, so all files of a batch must have the same identity.
std::pair<String, String> getRemoteAuthenticationIdentity(const ClientInfo & client_info);

void writeRemoteConvert(
    const DistributedAsyncInsertHeader & distributed_header,
    RemoteInserter & remote,
    bool compression_expected,
    ReadBufferFromFile & in,
    LoggerPtr log);

}
