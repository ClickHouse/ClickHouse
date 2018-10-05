#include <Core/Field.h>

#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/FieldVisitors.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Interpreters/SettingsCommon.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int UNKNOWN_LOAD_BALANCING;
    extern const int UNKNOWN_OVERFLOW_MODE;
    extern const int ILLEGAL_OVERFLOW_MODE;
    extern const int UNKNOWN_TOTALS_MODE;
    extern const int UNKNOWN_COMPRESSION_METHOD;
    extern const int UNKNOWN_DISTRIBUTED_PRODUCT_MODE;
    extern const int UNKNOWN_GLOBAL_SUBQUERIES_METHOD;
    extern const int UNKNOWN_JOIN_STRICTNESS;
    extern const int SIZE_OF_FIXED_STRING_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

template <typename IntType>
String SettingInt<IntType>::toString() const
{
    return DB::toString(value);
}

template <typename IntType>
void SettingInt<IntType>::set(IntType x)
{
    value = x;
    changed = true;
}

template <typename IntType>
void SettingInt<IntType>::set(const Field & x)
{
    set(applyVisitor(FieldVisitorConvertToNumber<IntType>(), x));
}

template <typename IntType>
void SettingInt<IntType>::set(const String & x)
{
    set(parse<IntType>(x));
}

template <typename IntType>
void SettingInt<IntType>::set(ReadBuffer & buf)
{
    IntType x = 0;
    readVarT(x, buf);
    set(x);
}

template <typename IntType>
void SettingInt<IntType>::write(WriteBuffer & buf) const
{
    writeVarT(value, buf);
}

template struct SettingInt<UInt64>;
template struct SettingInt<Int64>;


String SettingMaxThreads::toString() const
{
    /// Instead of the `auto` value, we output the actual value to make it easier to see.
    return DB::toString(value);
}

void SettingMaxThreads::set(UInt64 x)
{
    value = x ? x : getAutoValue();
    is_auto = x == 0;
    changed = true;
}

void SettingMaxThreads::set(const Field & x)
{
    if (x.getType() == Field::Types::String)
        set(safeGet<const String &>(x));
    else
        set(safeGet<UInt64>(x));
}

void SettingMaxThreads::set(const String & x)
{
    if (x == "auto")
        setAuto();
    else
        set(parse<UInt64>(x));
}

void SettingMaxThreads::set(ReadBuffer & buf)
{
    UInt64 x = 0;
    readVarUInt(x, buf);
    set(x);
}

void SettingMaxThreads::write(WriteBuffer & buf) const
{
    writeVarUInt(is_auto ? 0 : value, buf);
}

void SettingMaxThreads::setAuto()
{
    value = getAutoValue();
    is_auto = true;
}

UInt64 SettingMaxThreads::getAutoValue() const
{
    static auto res = getAutoValueImpl();
    return res;
}

/// Executed once for all time. Executed from one thread.
UInt64 SettingMaxThreads::getAutoValueImpl() const
{
    return getNumberOfPhysicalCPUCores();
}


String SettingSeconds::toString() const
{
    return DB::toString(totalSeconds());
}

void SettingSeconds::set(const Poco::Timespan & x)
{
    value = x;
    changed = true;
}

void SettingSeconds::set(UInt64 x)
{
    set(Poco::Timespan(x, 0));
}

void SettingSeconds::set(const Field & x)
{
    set(safeGet<UInt64>(x));
}

void SettingSeconds::set(const String & x)
{
    set(parse<UInt64>(x));
}

void SettingSeconds::set(ReadBuffer & buf)
{
    UInt64 x = 0;
    readVarUInt(x, buf);
    set(x);
}

void SettingSeconds::write(WriteBuffer & buf) const
{
    writeVarUInt(value.totalSeconds(), buf);
}


String SettingMilliseconds::toString() const
{
    return DB::toString(totalMilliseconds());
}

void SettingMilliseconds::set(const Poco::Timespan & x)
{
    value = x;
    changed = true;
}

void SettingMilliseconds::set(UInt64 x)
{
    set(Poco::Timespan(x * 1000));
}

void SettingMilliseconds::set(const Field & x)
{
    set(safeGet<UInt64>(x));
}

void SettingMilliseconds::set(const String & x)
{
    set(parse<UInt64>(x));
}

void SettingMilliseconds::set(ReadBuffer & buf)
{
    UInt64 x = 0;
    readVarUInt(x, buf);
    set(x);
}

void SettingMilliseconds::write(WriteBuffer & buf) const
{
    writeVarUInt(value.totalMilliseconds(), buf);
}


String SettingFloat::toString() const
{
    return DB::toString(value);
}

void SettingFloat::set(float x)
{
    value = x;
    changed = true;
}

void SettingFloat::set(const Field & x)
{
    set(applyVisitor(FieldVisitorConvertToNumber<float>(), x));
}

void SettingFloat::set(const String & x)
{
    set(parse<float>(x));
}

void SettingFloat::set(ReadBuffer & buf)
{
    String x;
    readBinary(x, buf);
    set(x);
}

void SettingFloat::write(WriteBuffer & buf) const
{
    writeBinary(toString(), buf);
}


LoadBalancing SettingLoadBalancing::getLoadBalancing(const String & s)
{
    if (s == "random")           return LoadBalancing::RANDOM;
    if (s == "nearest_hostname") return LoadBalancing::NEAREST_HOSTNAME;
    if (s == "in_order")         return LoadBalancing::IN_ORDER;

    throw Exception("Unknown load balancing mode: '" + s + "', must be one of 'random', 'nearest_hostname', 'in_order'",
        ErrorCodes::UNKNOWN_LOAD_BALANCING);
}

String SettingLoadBalancing::toString() const
{
    const char * strings[] = {"random", "nearest_hostname", "in_order"};
    if (value < LoadBalancing::RANDOM || value > LoadBalancing::IN_ORDER)
        throw Exception("Unknown load balancing mode", ErrorCodes::UNKNOWN_LOAD_BALANCING);
    return strings[static_cast<size_t>(value)];
}

void SettingLoadBalancing::set(LoadBalancing x)
{
    value = x;
    changed = true;
}

void SettingLoadBalancing::set(const Field & x)
{
    set(safeGet<const String &>(x));
}

void SettingLoadBalancing::set(const String & x)
{
    set(getLoadBalancing(x));
}

void SettingLoadBalancing::set(ReadBuffer & buf)
{
    String x;
    readBinary(x, buf);
    set(x);
}

void SettingLoadBalancing::write(WriteBuffer & buf) const
{
    writeBinary(toString(), buf);
}


JoinStrictness SettingJoinStrictness::getJoinStrictness(const String & s)
{
    if (s == "")       return JoinStrictness::Unspecified;
    if (s == "ALL")    return JoinStrictness::ALL;
    if (s == "ANY")    return JoinStrictness::ANY;

    throw Exception("Unknown join strictness mode: '" + s + "', must be one of '', 'ALL', 'ANY'",
        ErrorCodes::UNKNOWN_JOIN_STRICTNESS);
}

String SettingJoinStrictness::toString() const
{
    const char * strings[] = {"", "ALL", "ANY"};
    if (value < JoinStrictness::Unspecified || value > JoinStrictness::ANY)
        throw Exception("Unknown join strictness mode", ErrorCodes::UNKNOWN_JOIN_STRICTNESS);
    return strings[static_cast<size_t>(value)];
}

void SettingJoinStrictness::set(JoinStrictness x)
{
    value = x;
    changed = true;
}

void SettingJoinStrictness::set(const Field & x)
{
    set(safeGet<const String &>(x));
}

void SettingJoinStrictness::set(const String & x)
{
    set(getJoinStrictness(x));
}

void SettingJoinStrictness::set(ReadBuffer & buf)
{
    String x;
    readBinary(x, buf);
    set(x);
}

void SettingJoinStrictness::write(WriteBuffer & buf) const
{
    writeBinary(toString(), buf);
}


TotalsMode SettingTotalsMode::getTotalsMode(const String & s)
{
    if (s == "before_having")          return TotalsMode::BEFORE_HAVING;
    if (s == "after_having_exclusive") return TotalsMode::AFTER_HAVING_EXCLUSIVE;
    if (s == "after_having_inclusive") return TotalsMode::AFTER_HAVING_INCLUSIVE;
    if (s == "after_having_auto")      return TotalsMode::AFTER_HAVING_AUTO;

    throw Exception("Unknown totals mode: '" + s + "', must be one of 'before_having', 'after_having_exclusive', 'after_having_inclusive', 'after_having_auto'", ErrorCodes::UNKNOWN_TOTALS_MODE);
}

String SettingTotalsMode::toString() const
{
    switch (value)
    {
        case TotalsMode::BEFORE_HAVING:          return "before_having";
        case TotalsMode::AFTER_HAVING_EXCLUSIVE: return "after_having_exclusive";
        case TotalsMode::AFTER_HAVING_INCLUSIVE: return "after_having_inclusive";
        case TotalsMode::AFTER_HAVING_AUTO:      return "after_having_auto";

        default:
            throw Exception("Unknown TotalsMode enum value", ErrorCodes::UNKNOWN_TOTALS_MODE);
    }
}

void SettingTotalsMode::set(TotalsMode x)
{
    value = x;
    changed = true;
}

void SettingTotalsMode::set(const Field & x)
{
    set(safeGet<const String &>(x));
}

void SettingTotalsMode::set(const String & x)
{
    set(getTotalsMode(x));
}

void SettingTotalsMode::set(ReadBuffer & buf)
{
    String x;
    readBinary(x, buf);
    set(x);
}

void SettingTotalsMode::write(WriteBuffer & buf) const
{
    writeBinary(toString(), buf);
}


template <bool enable_mode_any>
OverflowMode SettingOverflowMode<enable_mode_any>::getOverflowModeForGroupBy(const String & s)
{
    if (s == "throw") return OverflowMode::THROW;
    if (s == "break") return OverflowMode::BREAK;
    if (s == "any")   return OverflowMode::ANY;

    throw Exception("Unknown overflow mode: '" + s + "', must be one of 'throw', 'break', 'any'", ErrorCodes::UNKNOWN_OVERFLOW_MODE);
}

template <bool enable_mode_any>
OverflowMode SettingOverflowMode<enable_mode_any>::getOverflowMode(const String & s)
{
    OverflowMode mode = getOverflowModeForGroupBy(s);

    if (mode == OverflowMode::ANY && !enable_mode_any)
        throw Exception("Illegal overflow mode: 'any' is only for 'group_by_overflow_mode'", ErrorCodes::ILLEGAL_OVERFLOW_MODE);

    return mode;
}

template <bool enable_mode_any>
String SettingOverflowMode<enable_mode_any>::toString() const
{
    const char * strings[] = { "throw", "break", "any" };

    if (value < OverflowMode::THROW || value > OverflowMode::ANY)
        throw Exception("Unknown overflow mode", ErrorCodes::UNKNOWN_OVERFLOW_MODE);

    return strings[static_cast<size_t>(value)];
}

template <bool enable_mode_any>
void SettingOverflowMode<enable_mode_any>::set(OverflowMode x)
{
    value = x;
    changed = true;
}

template <bool enable_mode_any>
void SettingOverflowMode<enable_mode_any>::set(const Field & x)
{
    set(safeGet<const String &>(x));
}

template <bool enable_mode_any>
void SettingOverflowMode<enable_mode_any>::set(const String & x)
{
    set(getOverflowMode(x));
}

template <bool enable_mode_any>
void SettingOverflowMode<enable_mode_any>::set(ReadBuffer & buf)
{
    String x;
    readBinary(x, buf);
    set(x);
}

template <bool enable_mode_any>
void SettingOverflowMode<enable_mode_any>::write(WriteBuffer & buf) const
{
    writeBinary(toString(), buf);
}

template struct SettingOverflowMode<false>;
template struct SettingOverflowMode<true>;


CompressionMethod SettingCompressionMethod::getCompressionMethod(const String & s)
{
    if (s == "lz4")
        return CompressionMethod::LZ4;
    if (s == "lz4hc")
        return CompressionMethod::LZ4HC;
    if (s == "zstd")
        return CompressionMethod::ZSTD;

    throw Exception("Unknown compression method: '" + s + "', must be one of 'lz4', 'lz4hc', 'zstd'", ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
}

String SettingCompressionMethod::toString() const
{
    const char * strings[] = { nullptr, "lz4", "lz4hc", "zstd" };

    if (value < CompressionMethod::LZ4 || value > CompressionMethod::ZSTD)
        throw Exception("Unknown compression method", ErrorCodes::UNKNOWN_COMPRESSION_METHOD);

    return strings[static_cast<size_t>(value)];
}

void SettingCompressionMethod::set(CompressionMethod x)
{
    value = x;
    changed = true;
}

void SettingCompressionMethod::set(const Field & x)
{
    set(safeGet<const String &>(x));
}

void SettingCompressionMethod::set(const String & x)
{
    set(getCompressionMethod(x));
}

void SettingCompressionMethod::set(ReadBuffer & buf)
{
    String x;
    readBinary(x, buf);
    set(x);
}

void SettingCompressionMethod::write(WriteBuffer & buf) const
{
    writeBinary(toString(), buf);
}


DistributedProductMode SettingDistributedProductMode::getDistributedProductMode(const String & s)
{
    if (s == "deny")   return DistributedProductMode::DENY;
    if (s == "local")  return DistributedProductMode::LOCAL;
    if (s == "global") return DistributedProductMode::GLOBAL;
    if (s == "allow")  return DistributedProductMode::ALLOW;

    throw Exception("Unknown distributed product mode: '" + s + "', must be one of 'deny', 'local', 'global', 'allow'",
        ErrorCodes::UNKNOWN_DISTRIBUTED_PRODUCT_MODE);
}

String SettingDistributedProductMode::toString() const
{
    const char * strings[] = {"deny", "local", "global", "allow"};
    if (value < DistributedProductMode::DENY || value > DistributedProductMode::ALLOW)
        throw Exception("Unknown distributed product mode", ErrorCodes::UNKNOWN_DISTRIBUTED_PRODUCT_MODE);
    return strings[static_cast<size_t>(value)];
}

void SettingDistributedProductMode::set(DistributedProductMode x)
{
    value = x;
    changed = true;
}

void SettingDistributedProductMode::set(const Field & x)
{
    set(safeGet<const String &>(x));
}

void SettingDistributedProductMode::set(const String & x)
{
    set(getDistributedProductMode(x));
}

void SettingDistributedProductMode::set(ReadBuffer & buf)
{
    String x;
    readBinary(x, buf);
    set(x);
}

void SettingDistributedProductMode::write(WriteBuffer & buf) const
{
    writeBinary(toString(), buf);
}


String SettingString::toString() const
{
    return value;
}

void SettingString::set(const String & x)
{
    value = x;
    changed = true;
}

void SettingString::set(const Field & x)
{
    set(safeGet<const String &>(x));
}

void SettingString::set(ReadBuffer & buf)
{
    String x;
    readBinary(x, buf);
    set(x);
}

void SettingString::write(WriteBuffer & buf) const
{
    writeBinary(value, buf);
}


void SettingChar::checkStringIsACharacter(const String & x) const
{
    if (x.size() != 1)
        throw Exception("A setting's value string has to be an exactly one character long", ErrorCodes::SIZE_OF_FIXED_STRING_DOESNT_MATCH);
}

String SettingChar::toString() const
{
    return String(1, value);
}

void SettingChar::set(char x)
{
    value = x;
    changed = true;
}

void SettingChar::set(const String & x)
{
    checkStringIsACharacter(x);
    value = x[0];
    changed = true;
}

void SettingChar::set(const Field & x)
{
    const String & s = safeGet<const String &>(x);
    set(s);
}

void SettingChar::set(ReadBuffer & buf)
{
    String x;
    readBinary(x, buf);
    checkStringIsACharacter(x);
    set(x);
}

void SettingChar::write(WriteBuffer & buf) const
{
    writeBinary(toString(), buf);
}


SettingDateTimeInputFormat::Value SettingDateTimeInputFormat::getValue(const String & s)
{
    if (s == "basic")  return Value::Basic;
    if (s == "best_effort")  return Value::BestEffort;

    throw Exception("Unknown DateTime input format: '" + s + "', must be one of 'basic', 'best_effort'", ErrorCodes::BAD_ARGUMENTS);
}

String SettingDateTimeInputFormat::toString() const
{
    const char * strings[] = {"basic", "best_effort"};
    if (value < Value::Basic || value > Value::BestEffort)
        throw Exception("Unknown DateTime input format", ErrorCodes::BAD_ARGUMENTS);
    return strings[static_cast<size_t>(value)];
}

void SettingDateTimeInputFormat::set(Value x)
{
    value = x;
    changed = true;
}

void SettingDateTimeInputFormat::set(const Field & x)
{
    set(safeGet<const String &>(x));
}

void SettingDateTimeInputFormat::set(const String & x)
{
    set(getValue(x));
}

void SettingDateTimeInputFormat::set(ReadBuffer & buf)
{
    String x;
    readBinary(x, buf);
    set(x);
}

void SettingDateTimeInputFormat::write(WriteBuffer & buf) const
{
    writeBinary(toString(), buf);
}

}
