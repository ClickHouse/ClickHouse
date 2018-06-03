#pragma once

#include <Core/Field.h>
#include <Poco/Timespan.h>

#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/FieldVisitors.h>

#include <DataStreams/SizeLimits.h>

#include <IO/CompressedStream.h>
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
    extern const int SIZE_OF_FIXED_STRING_DOESNT_MATCH;
}


/** One setting for any type.
  * Stores a value within itself, as well as a flag - whether the value was changed.
  * This is done so that you can send to the remote servers only changed settings (or explicitly specified in the config) values.
  * That is, if the configuration was not specified in the config and was not dynamically changed, it is not sent to the remote server,
  *  and the remote server will use its default value.
  */

template <typename IntType>
struct SettingInt
{
    IntType value;
    bool changed = false;

    SettingInt(IntType x = 0) : value(x) {}

    operator IntType() const { return value; }
    SettingInt & operator= (IntType x) { set(x); return *this; }

    String toString() const
    {
        return DB::toString(value);
    }

    void set(IntType x)
    {
        value = x;
        changed = true;
    }

    void set(const Field & x)
    {
        set(applyVisitor(FieldVisitorConvertToNumber<IntType>(), x));
    }

    void set(const String & x)
    {
        set(parse<IntType>(x));
    }

    void set(ReadBuffer & buf)
    {
        IntType x = 0;
        readVarT(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const
    {
        writeVarT(value, buf);
    }
};

using SettingUInt64 = SettingInt<UInt64>;
using SettingInt64 = SettingInt<Int64>;
using SettingBool = SettingUInt64;


/** Unlike SettingUInt64, supports the value of 'auto' - the number of processor cores without taking into account SMT.
  * A value of 0 is also treated as auto.
  * When serializing, `auto` is written in the same way as 0.
  */
struct SettingMaxThreads
{
    UInt64 value;
    bool is_auto;
    bool changed = false;

    SettingMaxThreads(UInt64 x = 0) : value(x ? x : getAutoValue()), is_auto(x == 0) {}

    operator UInt64() const { return value; }
    SettingMaxThreads & operator= (UInt64 x) { set(x); return *this; }

    String toString() const
    {
        /// Instead of the `auto` value, we output the actual value to make it easier to see.
        return DB::toString(value);
    }

    void set(UInt64 x)
    {
        value = x ? x : getAutoValue();
        is_auto = x == 0;
        changed = true;
    }

    void set(const Field & x)
    {
        if (x.getType() == Field::Types::String)
            set(safeGet<const String &>(x));
        else
            set(safeGet<UInt64>(x));
    }

    void set(const String & x)
    {
        if (x == "auto")
            setAuto();
        else
            set(parse<UInt64>(x));
    }

    void set(ReadBuffer & buf)
    {
        UInt64 x = 0;
        readVarUInt(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const
    {
        writeVarUInt(is_auto ? 0 : value, buf);
    }

    void setAuto()
    {
        value = getAutoValue();
        is_auto = true;
    }

    UInt64 getAutoValue() const
    {
        static auto res = getAutoValueImpl();
        return res;
    }

    /// Executed once for all time. Executed from one thread.
    UInt64 getAutoValueImpl() const
    {
        return getNumberOfPhysicalCPUCores();
    }
};


struct SettingSeconds
{
    Poco::Timespan value;
    bool changed = false;

    SettingSeconds(UInt64 seconds = 0) : value(seconds, 0) {}

    operator Poco::Timespan() const { return value; }
    SettingSeconds & operator= (const Poco::Timespan & x) { set(x); return *this; }

    Poco::Timespan::TimeDiff totalSeconds() const { return value.totalSeconds(); }

    String toString() const
    {
        return DB::toString(totalSeconds());
    }

    void set(const Poco::Timespan & x)
    {
        value = x;
        changed = true;
    }

    void set(UInt64 x)
    {
        set(Poco::Timespan(x, 0));
    }

    void set(const Field & x)
    {
        set(safeGet<UInt64>(x));
    }

    void set(const String & x)
    {
        set(parse<UInt64>(x));
    }

    void set(ReadBuffer & buf)
    {
        UInt64 x = 0;
        readVarUInt(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const
    {
        writeVarUInt(value.totalSeconds(), buf);
    }
};


struct SettingMilliseconds
{
    Poco::Timespan value;
    bool changed = false;

    SettingMilliseconds(UInt64 milliseconds = 0) : value(milliseconds * 1000) {}

    operator Poco::Timespan() const { return value; }
    SettingMilliseconds & operator= (const Poco::Timespan & x) { set(x); return *this; }

    Poco::Timespan::TimeDiff totalMilliseconds() const { return value.totalMilliseconds(); }

    String toString() const
    {
        return DB::toString(totalMilliseconds());
    }

    void set(const Poco::Timespan & x)
    {
        value = x;
        changed = true;
    }

    void set(UInt64 x)
    {
        set(Poco::Timespan(x * 1000));
    }

    void set(const Field & x)
    {
        set(safeGet<UInt64>(x));
    }

    void set(const String & x)
    {
        set(parse<UInt64>(x));
    }

    void set(ReadBuffer & buf)
    {
        UInt64 x = 0;
        readVarUInt(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const
    {
        writeVarUInt(value.totalMilliseconds(), buf);
    }
};


struct SettingFloat
{
    float value;
    bool changed = false;

    SettingFloat(float x = 0) : value(x) {}

    operator float() const { return value; }
    SettingFloat & operator= (float x) { set(x); return *this; }

    String toString() const
    {
        return DB::toString(value);
    }

    void set(float x)
    {
        value = x;
        changed = true;
    }

    void set(const Field & x)
    {
        if (x.getType() == Field::Types::UInt64)
        {
            set(safeGet<UInt64>(x));
        }
        else if (x.getType() == Field::Types::Int64)
        {
            set(safeGet<Int64>(x));
        }
        else if (x.getType() == Field::Types::Float64)
        {
            set(safeGet<Float64>(x));
        }
        else
            throw Exception(std::string("Bad type of setting. Expected UInt64, Int64 or Float64, got ") + x.getTypeName(), ErrorCodes::TYPE_MISMATCH);
    }

    void set(const String & x)
    {
        set(parse<float>(x));
    }

    void set(ReadBuffer & buf)
    {
        String x;
        readBinary(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(toString(), buf);
    }
};


enum class LoadBalancing
{
    /// among replicas with a minimum number of errors selected randomly
    RANDOM = 0,
    /// a replica is selected among the replicas with the minimum number of errors
    /// with the minimum number of distinguished characters in the replica name and local hostname
    NEAREST_HOSTNAME,
    /// replicas are walked through strictly in order; the number of errors does not matter
    IN_ORDER,
};

struct SettingLoadBalancing
{
    LoadBalancing value;
    bool changed = false;

    SettingLoadBalancing(LoadBalancing x) : value(x) {}

    operator LoadBalancing() const { return value; }
    SettingLoadBalancing & operator= (LoadBalancing x) { set(x); return *this; }

    static LoadBalancing getLoadBalancing(const String & s)
    {
        if (s == "random")                 return LoadBalancing::RANDOM;
        if (s == "nearest_hostname")     return LoadBalancing::NEAREST_HOSTNAME;
        if (s == "in_order")             return LoadBalancing::IN_ORDER;

        throw Exception("Unknown load balancing mode: '" + s + "', must be one of 'random', 'nearest_hostname', 'in_order'",
            ErrorCodes::UNKNOWN_LOAD_BALANCING);
    }

    String toString() const
    {
        const char * strings[] = {"random", "nearest_hostname", "in_order"};
        if (value < LoadBalancing::RANDOM || value > LoadBalancing::IN_ORDER)
            throw Exception("Unknown load balancing mode", ErrorCodes::UNKNOWN_LOAD_BALANCING);
        return strings[static_cast<size_t>(value)];
    }

    void set(LoadBalancing x)
    {
        value = x;
        changed = true;
    }

    void set(const Field & x)
    {
        set(safeGet<const String &>(x));
    }

    void set(const String & x)
    {
        set(getLoadBalancing(x));
    }

    void set(ReadBuffer & buf)
    {
        String x;
        readBinary(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(toString(), buf);
    }
};


/// Which rows should be included in TOTALS.
enum class TotalsMode
{
    BEFORE_HAVING            = 0, /// Count HAVING for all read rows;
                                  ///  including those not in max_rows_to_group_by
                                  ///  and have not passed HAVING after grouping.
    AFTER_HAVING_INCLUSIVE    = 1, /// Count on all rows except those that have not passed HAVING;
                                   ///  that is, to include in TOTALS all the rows that did not pass max_rows_to_group_by.
    AFTER_HAVING_EXCLUSIVE    = 2, /// Include only the rows that passed and max_rows_to_group_by, and HAVING.
    AFTER_HAVING_AUTO         = 3, /// Automatically select between INCLUSIVE and EXCLUSIVE,
};

struct SettingTotalsMode
{
    TotalsMode value;
    bool changed = false;

    SettingTotalsMode(TotalsMode x) : value(x) {}

    operator TotalsMode() const { return value; }
    SettingTotalsMode & operator= (TotalsMode x) { set(x); return *this; }

    static TotalsMode getTotalsMode(const String & s)
    {
        if (s == "before_having")             return TotalsMode::BEFORE_HAVING;
        if (s == "after_having_exclusive")    return TotalsMode::AFTER_HAVING_EXCLUSIVE;
        if (s == "after_having_inclusive")    return TotalsMode::AFTER_HAVING_INCLUSIVE;
        if (s == "after_having_auto")        return TotalsMode::AFTER_HAVING_AUTO;

        throw Exception("Unknown totals mode: '" + s + "', must be one of 'before_having', 'after_having_exclusive', 'after_having_inclusive', 'after_having_auto'", ErrorCodes::UNKNOWN_TOTALS_MODE);
    }

    String toString() const
    {
        switch (value)
        {
            case TotalsMode::BEFORE_HAVING:                return "before_having";
            case TotalsMode::AFTER_HAVING_EXCLUSIVE:    return "after_having_exclusive";
            case TotalsMode::AFTER_HAVING_INCLUSIVE:    return "after_having_inclusive";
            case TotalsMode::AFTER_HAVING_AUTO:            return "after_having_auto";

            default:
                throw Exception("Unknown TotalsMode enum value", ErrorCodes::UNKNOWN_TOTALS_MODE);
        }
    }

    void set(TotalsMode x)
    {
        value = x;
        changed = true;
    }

    void set(const Field & x)
    {
        set(safeGet<const String &>(x));
    }

    void set(const String & x)
    {
        set(getTotalsMode(x));
    }

    void set(ReadBuffer & buf)
    {
        String x;
        readBinary(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(toString(), buf);
    }
};


template <bool enable_mode_any>
struct SettingOverflowMode
{
    OverflowMode value;
    bool changed = false;

    SettingOverflowMode(OverflowMode x = OverflowMode::THROW) : value(x) {}

    operator OverflowMode() const { return value; }
    SettingOverflowMode & operator= (OverflowMode x) { set(x); return *this; }

    static OverflowMode getOverflowModeForGroupBy(const String & s)
    {
        if (s == "throw")     return OverflowMode::THROW;
        if (s == "break")     return OverflowMode::BREAK;
        if (s == "any")        return OverflowMode::ANY;

        throw Exception("Unknown overflow mode: '" + s + "', must be one of 'throw', 'break', 'any'", ErrorCodes::UNKNOWN_OVERFLOW_MODE);
    }

    static OverflowMode getOverflowMode(const String & s)
    {
        OverflowMode mode = getOverflowModeForGroupBy(s);

        if (mode == OverflowMode::ANY && !enable_mode_any)
            throw Exception("Illegal overflow mode: 'any' is only for 'group_by_overflow_mode'", ErrorCodes::ILLEGAL_OVERFLOW_MODE);

        return mode;
    }

    String toString() const
    {
        const char * strings[] = { "throw", "break", "any" };

        if (value < OverflowMode::THROW || value > OverflowMode::ANY)
            throw Exception("Unknown overflow mode", ErrorCodes::UNKNOWN_OVERFLOW_MODE);

        return strings[static_cast<size_t>(value)];
    }

    void set(OverflowMode x)
    {
        value = x;
        changed = true;
    }

    void set(const Field & x)
    {
        set(safeGet<const String &>(x));
    }

    void set(const String & x)
    {
        set(getOverflowMode(x));
    }

    void set(ReadBuffer & buf)
    {
        String x;
        readBinary(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(toString(), buf);
    }
};

struct SettingCompressionMethod
{
    CompressionMethod value;
    bool changed = false;

    SettingCompressionMethod(CompressionMethod x = CompressionMethod::LZ4) : value(x) {}

    operator CompressionMethod() const { return value; }
    SettingCompressionMethod & operator= (CompressionMethod x) { set(x); return *this; }

    static CompressionMethod getCompressionMethod(const String & s)
    {
        if (s == "lz4")
            return CompressionMethod::LZ4;
        if (s == "lz4hc")
            return CompressionMethod::LZ4HC;
        if (s == "zstd")
            return CompressionMethod::ZSTD;

        throw Exception("Unknown compression method: '" + s + "', must be one of 'lz4', 'lz4hc', 'zstd'", ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
    }

    String toString() const
    {
        const char * strings[] = { nullptr, "lz4", "lz4hc", "zstd" };

        if (value < CompressionMethod::LZ4 || value > CompressionMethod::ZSTD)
            throw Exception("Unknown compression method", ErrorCodes::UNKNOWN_COMPRESSION_METHOD);

        return strings[static_cast<size_t>(value)];
    }

    void set(CompressionMethod x)
    {
        value = x;
        changed = true;
    }

    void set(const Field & x)
    {
        set(safeGet<const String &>(x));
    }

    void set(const String & x)
    {
        set(getCompressionMethod(x));
    }

    void set(ReadBuffer & buf)
    {
        String x;
        readBinary(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(toString(), buf);
    }
};

/// The setting for executing distributed subqueries inside IN or JOIN sections.
enum class DistributedProductMode
{
    DENY = 0,    /// Disable
    LOCAL,       /// Convert to local query
    GLOBAL,      /// Convert to global query
    ALLOW        /// Enable
};

struct SettingDistributedProductMode
{
    DistributedProductMode value;
    bool changed = false;

    SettingDistributedProductMode(DistributedProductMode x) : value(x) {}

    operator DistributedProductMode() const { return value; }
    SettingDistributedProductMode & operator= (DistributedProductMode x) { set(x); return *this; }

    static DistributedProductMode getDistributedProductMode(const String & s)
    {
        if (s == "deny")     return DistributedProductMode::DENY;
        if (s == "local")     return DistributedProductMode::LOCAL;
        if (s == "global")     return DistributedProductMode::GLOBAL;
        if (s == "allow")    return DistributedProductMode::ALLOW;

        throw Exception("Unknown distributed product mode: '" + s + "', must be one of 'deny', 'local', 'global', 'allow'",
            ErrorCodes::UNKNOWN_DISTRIBUTED_PRODUCT_MODE);
    }

    String toString() const
    {
        const char * strings[] = {"deny", "local", "global", "allow"};
        if (value < DistributedProductMode::DENY || value > DistributedProductMode::ALLOW)
            throw Exception("Unknown distributed product mode", ErrorCodes::UNKNOWN_DISTRIBUTED_PRODUCT_MODE);
        return strings[static_cast<size_t>(value)];
    }

    void set(DistributedProductMode x)
    {
        value = x;
        changed = true;
    }

    void set(const Field & x)
    {
        set(safeGet<const String &>(x));
    }

    void set(const String & x)
    {
        set(getDistributedProductMode(x));
    }

    void set(ReadBuffer & buf)
    {
        String x;
        readBinary(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(toString(), buf);
    }
};


struct SettingString
{
    String value;
    bool changed = false;

    SettingString(const String & x = String{}) : value(x) {}

    operator String() const { return value; }
    SettingString & operator= (const String & x) { set(x); return *this; }

    String toString() const
    {
        return value;
    }

    void set(const String & x)
    {
        value = x;
        changed = true;
    }

    void set(const Field & x)
    {
        set(safeGet<const String &>(x));
    }

    void set(ReadBuffer & buf)
    {
        String x;
        readBinary(x, buf);
        set(x);
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(value, buf);
    }
};


struct SettingChar
{
private:
    void checkStringIsACharacter(const String & x) const
    {
        if (x.size() != 1)
            throw Exception("A setting's value string has to be an exactly one character long", ErrorCodes::SIZE_OF_FIXED_STRING_DOESNT_MATCH);
    }
public:
    char value;
    bool changed = false;

    SettingChar(char x = '\0') : value(x) {}

    operator char() const { return value; }
    SettingChar & operator= (char x) { set(x); return *this; }

    String toString() const
    {
        return String(1, value);
    }

    void set(char x)
    {
        value = x;
        changed = true;
    }

    void set(const String & x)
    {
        checkStringIsACharacter(x);
        value = x[0];
        changed = true;
    }

    void set(const Field & x)
    {
        const String & s = safeGet<const String &>(x);
        set(s);
    }

    void set(ReadBuffer & buf)
    {
        String x;
        readBinary(x, buf);
        checkStringIsACharacter(x);
        set(x);
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(toString(), buf);
    }
};
}
