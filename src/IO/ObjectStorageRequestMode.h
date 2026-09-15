#pragma once

#include <cstdint>

namespace DB
{

/// How a request is issued to an object storage. `NativeConditional` marks a request as carrying (or
/// eligible to carry) a storage-native conditional header, which lets a provider-specific client
/// translate `If-Match`/`ETag` into its own vocabulary (e.g. a GCS generation) on the request and the
/// response. Reads use it so that a plain GET answers with the same incarnation identity a HEAD does.
enum class ObjectStorageRequestMode : uint8_t
{
    Default,
    NativeConditional,
};

}
