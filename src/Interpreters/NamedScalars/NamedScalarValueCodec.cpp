#include <Interpreters/NamedScalars/NamedScalarValueCodec.h>

#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/FieldBinaryEncoding.h>
#include <Common/logger_useful.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>

#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <Interpreters/Context.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NAMED_SCALAR_VALUE_TOO_LARGE;
}

namespace
{

constexpr UInt64 stored_value_format_version = 1;

String storedValueStatusToString(StoredValueStatus state)
{
    switch (state)
    {
        case StoredValueStatus::Empty:
            return "empty";
        case StoredValueStatus::Valid:
            return "valid";
        case StoredValueStatus::StaleAfterFailure:
            return "stale_after_failure";
    }
    UNREACHABLE();
}

StoredValueStatus parseStoredValueStatus(const String & state)
{
    if (state == "empty")
        return StoredValueStatus::Empty;
    if (state == "valid")
        return StoredValueStatus::Valid;
    if (state == "stale_after_failure")
        return StoredValueStatus::StaleAfterFailure;
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown named scalar stored value state: {}", state);
}

Int64 timePointToUnixSeconds(std::chrono::system_clock::time_point timepoint)
{
    return std::chrono::duration_cast<std::chrono::seconds>(timepoint.time_since_epoch()).count();
}

std::chrono::system_clock::time_point timePointFromUnixSeconds(Int64 seconds)
{
    return std::chrono::system_clock::time_point{std::chrono::seconds(seconds)};
}

}

void StoredValue::serialize(WriteBuffer & out) const
{
    String value_blob;
    if (has_value())
    {
        WriteBufferFromOwnString value_out;
        encodeField(value, value_out);
        value_blob = value_out.str();
    }

    out << "format version: " << stored_value_format_version << "\n";
    out << "state: " << storedValueStatusToString(state) << "\n";
    out << "type: " << escape << (type ? type->getName() : String{}) << "\n";
    out << "last_update_time: " << timePointToUnixSeconds(last_update_time) << "\n";
    out << "last_successful_update_time: " << timePointToUnixSeconds(last_successful_update_time) << "\n";
    out << "last_update_hostname: " << escape << last_update_hostname << "\n";
    out << "last_error_type: " << escape << last_error_type << "\n";
    out << "last_error: " << escape << last_error << "\n";
    out << "value: " << escape << value_blob << "\n";
}

std::optional<StoredValue> StoredValue::deserialize(ReadBuffer & in)
{
    UInt64 version = 0;
    in >> "format version: " >> version >> "\n";
    if (version != stored_value_format_version)
        return std::nullopt;

    StoredValue snapshot;
    String state;
    String type_name;
    String value_blob;
    Int64 last_update_time_seconds = 0;
    Int64 last_successful_update_time_seconds = 0;

    in >> "state: " >> state >> "\n";
    snapshot.state = parseStoredValueStatus(state);

    in >> "type: " >> escape >> type_name >> "\n";
    if (!type_name.empty())
        snapshot.type = DataTypeFactory::instance().get(type_name);

    in >> "last_update_time: " >> last_update_time_seconds >> "\n";
    snapshot.last_update_time = timePointFromUnixSeconds(last_update_time_seconds);

    in >> "last_successful_update_time: " >> last_successful_update_time_seconds >> "\n";
    snapshot.last_successful_update_time = timePointFromUnixSeconds(last_successful_update_time_seconds);

    in >> "last_update_hostname: " >> escape >> snapshot.last_update_hostname >> "\n";
    in >> "last_error_type: " >> escape >> snapshot.last_error_type >> "\n";
    in >> "last_error: " >> escape >> snapshot.last_error >> "\n";
    in >> "value: " >> escape >> value_blob >> "\n";

    if (snapshot.has_value())
    {
        ReadBufferFromString value_in(value_blob);
        snapshot.value = decodeField(value_in);
    }
    return snapshot;
}

std::shared_ptr<const StoredValue> StoredValue::fromEvaluationSuccess(
    DataTypePtr type_,
    Field value_,
    const String & hostname,
    std::chrono::system_clock::time_point now)
{
    auto snapshot = std::make_shared<StoredValue>();
    snapshot->type = std::move(type_);
    snapshot->value = std::move(value_);
    snapshot->last_update_time = now;
    snapshot->last_successful_update_time = now;
    snapshot->last_update_hostname = hostname;
    snapshot->state = StoredValueStatus::Valid;
    return snapshot;
}

std::shared_ptr<const StoredValue> StoredValue::fromEvaluationFailure(
    const StoredValue * prev,
    std::string_view error_message,
    std::string_view error_type,
    const String & hostname,
    std::chrono::system_clock::time_point now)
{
    auto snapshot = std::make_shared<StoredValue>();
    if (prev && prev->has_value())
    {
        snapshot->type = prev->type;
        snapshot->value = prev->value;
        snapshot->last_successful_update_time = prev->last_successful_update_time;
        snapshot->state = StoredValueStatus::StaleAfterFailure;
    }

    snapshot->last_update_time = now;
    snapshot->last_update_hostname = hostname;
    snapshot->last_error = String(error_message);
    snapshot->last_error_type = String(error_type);
    return snapshot;
}

String encodeNamedScalarValueAndCheckSize(const StoredValue & snapshot, const ContextPtr & context)
{
    static constexpr UInt64 default_max = 1ULL << 20;
    const UInt64 max_size = context
        ? context->getConfigRef().getUInt64("named_scalar_max_value_size", default_max)
        : default_max;

    WriteBufferFromOwnString buf;
    snapshot.serialize(buf);
    String payload = buf.str();
    if (payload.size() > max_size)
        throw Exception(
            ErrorCodes::NAMED_SCALAR_VALUE_TOO_LARGE,
            "Named scalar value is too large ({} bytes, max {} bytes)",
            payload.size(),
            max_size);
    return payload;
}

std::optional<StoredValue> tryDecodeNamedScalarValueBlob(
    const String & value_blob,
    const String & name,
    LoggerPtr log)
{
    try
    {
        ReadBufferFromString rb(value_blob);
        auto snapshot = StoredValue::deserialize(rb);
        if (!snapshot)
            LOG_WARNING(
                log,
                "Ignoring persisted value for named scalar '{}' because it uses an unsupported format; it will be rebuilt",
                name);
        return snapshot;
    }
    catch (...)
    {
        LOG_WARNING(
            log,
            "Ignoring persisted value for named scalar '{}' because decoding failed: {}; it will be rebuilt",
            name,
            getCurrentExceptionMessage(false));
        return std::nullopt;
    }
}

}
