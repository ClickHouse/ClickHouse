#include "SettingsCommon.h"

#include <Core/Field.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/FieldVisitors.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>



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
    extern const int UNKNOWN_LOG_LEVEL;
    extern const int SIZE_OF_FIXED_STRING_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_SETTING;
}

template <typename IntType>
String SettingInt<IntType>::toString() const
{
    return DB::toString(value);
}

template <typename IntType>
Field SettingInt<IntType>::toField() const
{
    return value;
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
    if (x.getType() == Field::Types::String)
        set(get<const String &>(x));
    else
        set(applyVisitor(FieldVisitorConvertToNumber<IntType>(), x));
}

template <typename IntType>
void SettingInt<IntType>::set(const String & x)
{
    set(parse<IntType>(x));
}

template <typename IntType>
void SettingInt<IntType>::serialize(WriteBuffer & buf) const
{
    writeVarT(value, buf);
}

template <typename IntType>
void SettingInt<IntType>::deserialize(ReadBuffer & buf)
{
    IntType x = 0;
    readVarT(x, buf);
    set(x);
}

template struct SettingInt<UInt64>;
template struct SettingInt<Int64>;


String SettingMaxThreads::toString() const
{
    /// Instead of the `auto` value, we output the actual value to make it easier to see.
    return DB::toString(value);
}

Field SettingMaxThreads::toField() const
{
    return is_auto ? 0 : value;
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
        set(get<const String &>(x));
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

void SettingMaxThreads::serialize(WriteBuffer & buf) const
{
    writeVarUInt(is_auto ? 0 : value, buf);
}

void SettingMaxThreads::deserialize(ReadBuffer & buf)
{
    UInt64 x = 0;
    readVarUInt(x, buf);
    set(x);
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

Field SettingSeconds::toField() const
{
    return totalSeconds();
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
    if (x.getType() == Field::Types::String)
        set(get<const String &>(x));
    else
        set(safeGet<UInt64>(x));
}

void SettingSeconds::set(const String & x)
{
    set(parse<UInt64>(x));
}

void SettingSeconds::serialize(WriteBuffer & buf) const
{
    writeVarUInt(value.totalSeconds(), buf);
}

void SettingSeconds::deserialize(ReadBuffer & buf)
{
    UInt64 x = 0;
    readVarUInt(x, buf);
    set(x);
}


String SettingMilliseconds::toString() const
{
    return DB::toString(totalMilliseconds());
}

Field SettingMilliseconds::toField() const
{
    return totalMilliseconds();
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
    if (x.getType() == Field::Types::String)
        set(get<const String &>(x));
    else
        set(safeGet<UInt64>(x));
}

void SettingMilliseconds::set(const String & x)
{
    set(parse<UInt64>(x));
}

void SettingMilliseconds::serialize(WriteBuffer & buf) const
{
    writeVarUInt(value.totalMilliseconds(), buf);
}

void SettingMilliseconds::deserialize(ReadBuffer & buf)
{
    UInt64 x = 0;
    readVarUInt(x, buf);
    set(x);
}


String SettingFloat::toString() const
{
    return DB::toString(value);
}

Field SettingFloat::toField() const
{
    return value;
}

void SettingFloat::set(float x)
{
    value = x;
    changed = true;
}

void SettingFloat::set(const Field & x)
{
    if (x.getType() == Field::Types::String)
        set(get<const String &>(x));
    else
        set(applyVisitor(FieldVisitorConvertToNumber<float>(), x));
}

void SettingFloat::set(const String & x)
{
    set(parse<float>(x));
}

void SettingFloat::serialize(WriteBuffer & buf) const
{
    writeBinary(toString(), buf);
}

void SettingFloat::deserialize(ReadBuffer & buf)
{
    String x;
    readBinary(x, buf);
    set(x);
}


LoadBalancing SettingLoadBalancing::getLoadBalancing(const String & s)
{
    if (s == "random")           return LoadBalancing::RANDOM;
    if (s == "nearest_hostname") return LoadBalancing::NEAREST_HOSTNAME;
    if (s == "in_order")         return LoadBalancing::IN_ORDER;
    if (s == "first_or_random")  return LoadBalancing::FIRST_OR_RANDOM;

    throw Exception("Unknown load balancing mode: '" + s + "', must be one of 'random', 'nearest_hostname', 'in_order', 'first_or_random'",
        ErrorCodes::UNKNOWN_LOAD_BALANCING);
}

String SettingLoadBalancing::toString() const
{
    const char * strings[] = {"random", "nearest_hostname", "in_order", "first_or_random"};
    if (value < LoadBalancing::RANDOM || value > LoadBalancing::FIRST_OR_RANDOM)
        throw Exception("Unknown load balancing mode", ErrorCodes::UNKNOWN_LOAD_BALANCING);
    return strings[static_cast<size_t>(value)];
}

Field SettingLoadBalancing::toField() const
{
    return toString();
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

void SettingLoadBalancing::serialize(WriteBuffer & buf) const
{
    writeBinary(toString(), buf);
}

void SettingLoadBalancing::deserialize(ReadBuffer & buf)
{
    String s;
    readBinary(s, buf);
    set(s);
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

Field SettingJoinStrictness::toField() const
{
    return toString();
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

void SettingJoinStrictness::serialize(WriteBuffer & buf) const
{
    writeBinary(toString(), buf);
}

void SettingJoinStrictness::deserialize(ReadBuffer & buf)
{
    String s;
    readBinary(s, buf);
    set(s);
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
    }

    __builtin_unreachable();
}

Field SettingTotalsMode::toField() const
{
    return toString();
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

void SettingTotalsMode::serialize(WriteBuffer & buf) const
{
    writeBinary(toString(), buf);
}

void SettingTotalsMode::deserialize(ReadBuffer & buf)
{
    String s;
    readBinary(s, buf);
    set(s);
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
Field SettingOverflowMode<enable_mode_any>::toField() const
{
    return toString();
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
void SettingOverflowMode<enable_mode_any>::serialize(WriteBuffer & buf) const
{
    writeBinary(toString(), buf);
}

template <bool enable_mode_any>
void SettingOverflowMode<enable_mode_any>::deserialize(ReadBuffer & buf)
{
    String s;
    readBinary(s, buf);
    set(s);
}


template struct SettingOverflowMode<false>;
template struct SettingOverflowMode<true>;

DistributedProductMode SettingDistributedProductMode::getDistributedProductMode(const String & s)
{
    if (s == "deny") return DistributedProductMode::DENY;
    if (s == "local") return DistributedProductMode::LOCAL;
    if (s == "global") return DistributedProductMode::GLOBAL;
    if (s == "allow") return DistributedProductMode::ALLOW;

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

Field SettingDistributedProductMode::toField() const
{
    return toString();
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

void SettingDistributedProductMode::serialize(WriteBuffer & buf) const
{
    writeBinary(toString(), buf);
}

void SettingDistributedProductMode::deserialize(ReadBuffer & buf)
{
    String s;
    readBinary(s, buf);
    set(s);
}


String SettingString::toString() const
{
    return value;
}

Field SettingString::toField() const
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

void SettingString::serialize(WriteBuffer & buf) const
{
    writeBinary(value, buf);
}

void SettingString::deserialize(ReadBuffer & buf)
{
    String s;
    readBinary(s, buf);
    set(s);
}


String SettingChar::toString() const
{
    return String(1, value);
}

Field SettingChar::toField() const
{
    return toString();
}

void SettingChar::set(char x)
{
    value = x;
    changed = true;
}

void SettingChar::set(const String & x)
{
    if (x.size() > 1)
        throw Exception("A setting's value string has to be an exactly one character long", ErrorCodes::SIZE_OF_FIXED_STRING_DOESNT_MATCH);
    char c = (x.size() == 1) ? x[0] : '\0';
    set(c);
}

void SettingChar::set(const Field & x)
{
    const String & s = safeGet<const String &>(x);
    set(s);
}

void SettingChar::serialize(WriteBuffer & buf) const
{
    writeBinary(toString(), buf);
}

void SettingChar::deserialize(ReadBuffer & buf)
{
    String s;
    readBinary(s, buf);
    set(s);
}


SettingDateTimeInputFormat::Value SettingDateTimeInputFormat::getValue(const String & s)
{
    if (s == "basic") return Value::Basic;
    if (s == "best_effort") return Value::BestEffort;

    throw Exception("Unknown DateTime input format: '" + s + "', must be one of 'basic', 'best_effort'", ErrorCodes::BAD_ARGUMENTS);
}

String SettingDateTimeInputFormat::toString() const
{
    const char * strings[] = {"basic", "best_effort"};
    if (value < Value::Basic || value > Value::BestEffort)
        throw Exception("Unknown DateTime input format", ErrorCodes::BAD_ARGUMENTS);
    return strings[static_cast<size_t>(value)];
}

Field SettingDateTimeInputFormat::toField() const
{
    return toString();
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

void SettingDateTimeInputFormat::serialize(WriteBuffer & buf) const
{
    writeBinary(toString(), buf);
}

void SettingDateTimeInputFormat::deserialize(ReadBuffer & buf)
{
    String s;
    readBinary(s, buf);
    set(s);
}


SettingLogsLevel::Value SettingLogsLevel::getValue(const String & s)
{
    if (s == "none") return Value::none;
    if (s == "error") return Value::error;
    if (s == "warning") return Value::warning;
    if (s == "information") return Value::information;
    if (s == "debug") return Value::debug;
    if (s == "trace") return Value::trace;

    throw Exception("Unknown logs level: '" + s + "', must be one of: none, error, warning, information, debug, trace", ErrorCodes::BAD_ARGUMENTS);
}

String SettingLogsLevel::toString() const
{
    const char * strings[] = {"none", "error", "warning", "information", "debug", "trace"};
    return strings[static_cast<size_t>(value)];
}

Field SettingLogsLevel::toField() const
{
    return toString();
}

void SettingLogsLevel::set(Value x)
{
    value = x;
    changed = true;
}

void SettingLogsLevel::set(const Field & x)
{
    set(safeGet<const String &>(x));
}

void SettingLogsLevel::set(const String & x)
{
    set(getValue(x));
}

void SettingLogsLevel::serialize(WriteBuffer & buf) const
{
    writeBinary(toString(), buf);
}

void SettingLogsLevel::deserialize(ReadBuffer & buf)
{
    String s;
    readBinary(s, buf);
    set(s);
}


namespace details
{
    String SettingsCollectionUtils::deserializeName(ReadBuffer & buf)
    {
        String name;
        readBinary(name, buf);
        return name;
    }

    void SettingsCollectionUtils::serializeName(const StringRef & name, WriteBuffer & buf) { writeBinary(name, buf); }

    void SettingsCollectionUtils::throwNameNotFound(const StringRef & name)
    {
        throw Exception("Unknown setting " + name.toString(), ErrorCodes::UNKNOWN_SETTING);
    }
}
}
