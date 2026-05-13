#pragma once

#include <Common/Logger.h>
#include <Core/Field.h>
#include <Interpreters/Context_fwd.h>
#include <base/types.h>

#include <chrono>
#include <memory>
#include <optional>
#include <string_view>

namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;
class ReadBuffer;
class WriteBuffer;

enum class StoredValueStatus : UInt8
{
    Empty,
    Valid,
    StaleAfterFailure,
};

struct StoredValue
{
    static std::shared_ptr<const StoredValue> fromEvaluationSuccess(
        DataTypePtr type,
        Field value,
        const String & hostname,
        std::chrono::system_clock::time_point now);

    static std::shared_ptr<const StoredValue> fromEvaluationFailure(
        const StoredValue * prev,
        std::string_view error_message,
        std::string_view error_type,
        const String & hostname,
        std::chrono::system_clock::time_point now);

    static std::optional<StoredValue> deserialize(ReadBuffer & in);
    void serialize(WriteBuffer & out) const;

    DataTypePtr type;
    Field value;
    std::chrono::system_clock::time_point last_update_time;
    std::chrono::system_clock::time_point last_successful_update_time;
    String last_update_hostname;
    String last_error;
    String last_error_type;
    StoredValueStatus state = StoredValueStatus::Empty;

    bool has_value() const { return state != StoredValueStatus::Empty; }
    bool is_valid() const { return state == StoredValueStatus::Valid; }
};

/// Serialize StoredValue to a blob and enforce the configured max size
/// (`named_scalar_max_value_size`, default 1 MiB). Throws
/// NAMED_SCALAR_VALUE_TOO_LARGE if the encoded blob exceeds the limit.
String encodeNamedScalarValueAndCheckSize(const StoredValue & snapshot, const ContextPtr & context);

/// Decode a value blob; returns nullopt and logs a warning if the blob
/// uses an unsupported format version or fails to decode. Never throws.
std::optional<StoredValue> tryDecodeNamedScalarValueBlob(
    const String & value_blob,
    const String & name,
    LoggerPtr log);

}
