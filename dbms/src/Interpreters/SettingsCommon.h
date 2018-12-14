#pragma once

#include <Poco/Timespan.h>
#include <DataStreams/SizeLimits.h>
#include <Formats/FormatSettings.h>
#include <IO/CompressedStream.h>
#include <Core/Types.h>


namespace DB
{

class Field;
class ReadBuffer;
class WriteBuffer;


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

    /// Serialize to a test string.
    String toString() const;

    /// Serialize to binary stream suitable for transfer over network.
    void write(WriteBuffer & buf) const;

    void set(IntType x);

    /// Read from SQL literal.
    void set(const Field & x);

    /// Read from text string.
    void set(const String & x);

    /// Read from binary stream.
    void set(ReadBuffer & buf);
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

    String toString() const;

    void set(UInt64 x);
    void set(const Field & x);
    void set(const String & x);
    void set(ReadBuffer & buf);

    void write(WriteBuffer & buf) const;

    void setAuto();
    UInt64 getAutoValue() const;

    /// Executed once for all time. Executed from one thread.
    UInt64 getAutoValueImpl() const;
};


struct SettingSeconds
{
    Poco::Timespan value;
    bool changed = false;

    SettingSeconds(UInt64 seconds = 0) : value(seconds, 0) {}

    operator Poco::Timespan() const { return value; }
    SettingSeconds & operator= (const Poco::Timespan & x) { set(x); return *this; }

    Poco::Timespan::TimeDiff totalSeconds() const { return value.totalSeconds(); }

    String toString() const;

    void set(const Poco::Timespan & x);

    void set(UInt64 x);
    void set(const Field & x);
    void set(const String & x);
    void set(ReadBuffer & buf);

    void write(WriteBuffer & buf) const;
};


struct SettingMilliseconds
{
    Poco::Timespan value;
    bool changed = false;

    SettingMilliseconds(UInt64 milliseconds = 0) : value(milliseconds * 1000) {}

    operator Poco::Timespan() const { return value; }
    SettingMilliseconds & operator= (const Poco::Timespan & x) { set(x); return *this; }

    Poco::Timespan::TimeDiff totalMilliseconds() const { return value.totalMilliseconds(); }

    String toString() const;

    void set(const Poco::Timespan & x);
    void set(UInt64 x);
    void set(const Field & x);
    void set(const String & x);
    void set(ReadBuffer & buf);
    void write(WriteBuffer & buf) const;
};


struct SettingFloat
{
    float value;
    bool changed = false;

    SettingFloat(float x = 0) : value(x) {}

    operator float() const { return value; }
    SettingFloat & operator= (float x) { set(x); return *this; }

    String toString() const;

    void set(float x);
    void set(const Field & x);
    void set(const String & x);
    void set(ReadBuffer & buf);

    void write(WriteBuffer & buf) const;
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

    static LoadBalancing getLoadBalancing(const String & s);

    String toString() const;

    void set(LoadBalancing x);
    void set(const Field & x);
    void set(const String & x);
    void set(ReadBuffer & buf);

    void write(WriteBuffer & buf) const;
};


enum class JoinStrictness
{
    Unspecified = 0, /// Query JOIN without strictness will throw Exception.
    ALL, /// Query JOIN without strictness -> ALL JOIN ...
    ANY, /// Query JOIN without strictness -> ANY JOIN ...
};


struct SettingJoinStrictness
{
    JoinStrictness value;
    bool changed = false;

    SettingJoinStrictness(JoinStrictness x) : value(x) {}

    operator JoinStrictness() const { return value; }
    SettingJoinStrictness & operator= (JoinStrictness x) { set(x); return *this; }

    static JoinStrictness getJoinStrictness(const String & s);

    String toString() const;

    void set(JoinStrictness x);
    void set(const Field & x);
    void set(const String & x);
    void set(ReadBuffer & buf);

    void write(WriteBuffer & buf) const;
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

    static TotalsMode getTotalsMode(const String & s);

    String toString() const;

    void set(TotalsMode x);
    void set(const Field & x);
    void set(const String & x);
    void set(ReadBuffer & buf);

    void write(WriteBuffer & buf) const;
};


template <bool enable_mode_any>
struct SettingOverflowMode
{
    OverflowMode value;
    bool changed = false;

    SettingOverflowMode(OverflowMode x = OverflowMode::THROW) : value(x) {}

    operator OverflowMode() const { return value; }
    SettingOverflowMode & operator= (OverflowMode x) { set(x); return *this; }

    static OverflowMode getOverflowModeForGroupBy(const String & s);
    static OverflowMode getOverflowMode(const String & s);

    String toString() const;

    void set(OverflowMode x);
    void set(const Field & x);
    void set(const String & x);
    void set(ReadBuffer & buf);

    void write(WriteBuffer & buf) const;
};


struct SettingCompressionMethod
{
    CompressionMethod value;
    bool changed = false;

    SettingCompressionMethod(CompressionMethod x = CompressionMethod::LZ4) : value(x) {}

    operator CompressionMethod() const { return value; }
    SettingCompressionMethod & operator= (CompressionMethod x) { set(x); return *this; }

    static CompressionMethod getCompressionMethod(const String & s);

    String toString() const;

    void set(CompressionMethod x);
    void set(const Field & x);
    void set(const String & x);
    void set(ReadBuffer & buf);

    void write(WriteBuffer & buf) const;
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

    static DistributedProductMode getDistributedProductMode(const String & s);

    String toString() const;

    void set(DistributedProductMode x);
    void set(const Field & x);
    void set(const String & x);
    void set(ReadBuffer & buf);
    void write(WriteBuffer & buf) const;
};


struct SettingString
{
    String value;
    bool changed = false;

    SettingString(const String & x = String{}) : value(x) {}

    operator String() const { return value; }
    SettingString & operator= (const String & x) { set(x); return *this; }

    String toString() const;

    void set(const String & x);
    void set(const Field & x);
    void set(ReadBuffer & buf);

    void write(WriteBuffer & buf) const;
};


struct SettingChar
{
private:
    void checkStringIsACharacter(const String & x) const;

public:
    char value;
    bool changed = false;

    SettingChar(char x = '\0') : value(x) {}

    operator char() const { return value; }
    SettingChar & operator= (char x) { set(x); return *this; }

    String toString() const;

    void set(char x);
    void set(const String & x);
    void set(const Field & x);
    void set(ReadBuffer & buf);

    void write(WriteBuffer & buf) const;
};


struct SettingDateTimeInputFormat
{
    using Value = FormatSettings::DateTimeInputFormat;

    Value value;
    bool changed = false;

    SettingDateTimeInputFormat(Value x) : value(x) {}

    operator Value() const { return value; }
    SettingDateTimeInputFormat & operator= (Value x) { set(x); return *this; }

    static Value getValue(const String & s);

    String toString() const;

    void set(Value x);
    void set(const Field & x);
    void set(const String & x);
    void set(ReadBuffer & buf);
    void write(WriteBuffer & buf) const;
};

}
