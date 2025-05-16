#pragma once
#include <Core/Types.h>

namespace DB::S3
{

/// Client settings.
inline static constexpr uint64_t DEFAULT_EXPIRATION_WINDOW_SECONDS = 120;
inline static constexpr uint64_t DEFAULT_CONNECT_TIMEOUT_MS = 1000;
inline static constexpr uint64_t DEFAULT_REQUEST_TIMEOUT_MS = 30000;
inline static constexpr uint64_t DEFAULT_MAX_CONNECTIONS = 1024;
inline static constexpr uint64_t DEFAULT_KEEP_ALIVE_TIMEOUT = 5;
inline static constexpr uint64_t DEFAULT_KEEP_ALIVE_MAX_REQUESTS = 100;

inline static constexpr bool DEFAULT_USE_ENVIRONMENT_CREDENTIALS = true;
inline static constexpr bool DEFAULT_NO_SIGN_REQUEST = false;
inline static constexpr bool DEFAULT_DISABLE_CHECKSUM = false;
inline static constexpr bool DEFAULT_USE_ADAPTIVE_TIMEOUTS = true;

/// Upload settings.
inline static constexpr uint64_t DEFAULT_MIN_UPLOAD_PART_SIZE = 16 * 1024 * 1024;
inline static constexpr uint64_t DEFAULT_MAX_UPLOAD_PART_SIZE = 5ull * 1024 * 1024 * 1024;
inline static constexpr uint64_t DEFAULT_MAX_SINGLE_PART_UPLOAD_SIZE = 32 * 1024 * 1024;
inline static constexpr uint64_t DEFAULT_STRICT_UPLOAD_PART_SIZE = 0;
inline static constexpr uint64_t DEFAULT_UPLOAD_PART_SIZE_MULTIPLY_FACTOR = 2;
inline static constexpr uint64_t DEFAULT_UPLOAD_PART_SIZE_MULTIPLY_PARTS_COUNT_THRESHOLD = 500;
inline static constexpr uint64_t DEFAULT_MAX_PART_NUMBER = 10000;

/// Other settings.
inline static constexpr uint64_t DEFAULT_MAX_SINGLE_OPERATION_COPY_SIZE = 32 * 1024 * 1024;
inline static constexpr uint64_t DEFAULT_MAX_INFLIGHT_PARTS_FOR_ONE_FILE = 20;
inline static constexpr uint64_t DEFAULT_LIST_OBJECT_KEYS_SIZE = 1000;
inline static constexpr uint64_t DEFAULT_MAX_SINGLE_READ_TRIES = 4;
inline static constexpr uint64_t DEFAULT_MAX_UNEXPECTED_WRITE_ERROR_RETRIES = 4;
inline static constexpr uint64_t DEFAULT_MAX_REDIRECTS = 10;
inline static constexpr uint64_t DEFAULT_RETRY_ATTEMPTS = 100;

inline static constexpr bool DEFAULT_ALLOW_NATIVE_COPY = true;
inline static constexpr bool DEFAULT_CHECK_OBJECTS_AFTER_UPLOAD = false;

}
