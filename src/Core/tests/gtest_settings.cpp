#include <gtest/gtest.h>

#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Core/BaseSettings.h>
#include <Core/Settings.h>
#include <Core/SettingsFields.h>
#include <Core/SettingsEnums.h>
#include <Core/Field.h>
#include <IO/ReadBufferFromString.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>

#include <functional>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
}

namespace
{
using namespace DB;
using SettingMySQLDataTypesSupport = SettingFieldMultiEnum<MySQLDataTypesSupport, SettingFieldMySQLDataTypesSupportTraits>;
}

namespace DB
{

template <typename Enum, typename Traits>
bool operator== (const SettingFieldMultiEnum<Enum, Traits> & setting, const Field & f)
{
    return Field(setting) == f;
}

template <typename Enum, typename Traits>
bool operator== (const Field & f, const SettingFieldMultiEnum<Enum, Traits> & setting)
{
    return f == Field(setting);
}

}

GTEST_TEST(SettingMySQLDataTypesSupport, WithDefault)
{
    // Setting can be default-initialized and that means all values are unset.
    const SettingMySQLDataTypesSupport setting;
    ASSERT_EQ(std::vector<MySQLDataTypesSupport>{}, setting.value);
    ASSERT_EQ("", setting.toString());
    ASSERT_EQ(setting, Field(""));

    ASSERT_FALSE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_FALSE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DATETIME64));
}

GTEST_TEST(SettingMySQLDataTypesSupport, WithDECIMAL)
{
    // Setting can be initialized with MySQLDataTypesSupport::DECIMAL
    // and this value can be obtained in varios forms with getters.
    const SettingMySQLDataTypesSupport setting(MySQLDataTypesSupport::DECIMAL);
    ASSERT_EQ(std::vector<MySQLDataTypesSupport>{MySQLDataTypesSupport::DECIMAL}, setting.value);

    ASSERT_TRUE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_FALSE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DATETIME64));

    ASSERT_EQ("decimal", setting.toString());
    ASSERT_EQ(Field("decimal"), setting);
}

GTEST_TEST(SettingMySQLDataTypesSupport, WithDATE)
{
    SettingMySQLDataTypesSupport setting;
    setting = String("date2Date32");
    ASSERT_EQ(std::vector<MySQLDataTypesSupport>{MySQLDataTypesSupport::DATE2DATE32}, setting.value);

    ASSERT_TRUE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DATE2DATE32));
    ASSERT_FALSE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_FALSE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DATETIME64));

    ASSERT_EQ("date2Date32", setting.toString());
    ASSERT_EQ(Field("date2Date32"), setting);

    setting = String("date2String");
    ASSERT_EQ(std::vector<MySQLDataTypesSupport>{MySQLDataTypesSupport::DATE2STRING}, setting.value);

    ASSERT_TRUE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DATE2STRING));
    ASSERT_FALSE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DATE2DATE32));

    ASSERT_EQ("date2String", setting.toString());
    ASSERT_EQ(Field("date2String"), setting);
}

GTEST_TEST(SettingMySQLDataTypesSupport, SetString)
{
    SettingMySQLDataTypesSupport setting;
    setting = String("decimal");
    ASSERT_TRUE(setting.changed);

    ASSERT_TRUE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_FALSE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DATETIME64));
    ASSERT_EQ("decimal", setting.toString());
    ASSERT_EQ(Field("decimal"), setting);

    setting = "datetime64,decimal";
    ASSERT_TRUE(setting.changed);
    ASSERT_TRUE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_TRUE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DATETIME64));
    ASSERT_EQ("datetime64,decimal", setting.toString());
    ASSERT_EQ(Field("datetime64,decimal"), setting);

    // comma with spaces
    setting = " datetime64 ,    decimal "; /// bad punctuation is ok here
    ASSERT_TRUE(setting.changed);
    ASSERT_TRUE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_TRUE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DATETIME64));
    ASSERT_EQ("datetime64,decimal", setting.toString());
    ASSERT_EQ(Field("datetime64,decimal"), setting);

    setting = String(",,,,,,,, ,decimal");
    ASSERT_TRUE(setting.changed);
    ASSERT_TRUE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_FALSE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DATETIME64));
    ASSERT_EQ("decimal", setting.toString());
    ASSERT_EQ(Field("decimal"), setting);

    setting = String(",decimal,decimal,decimal,decimal,decimal,decimal,decimal,decimal,decimal,");
    ASSERT_TRUE(setting.changed); //since previous value was DECIMAL
    ASSERT_TRUE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_FALSE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DATETIME64));
    ASSERT_EQ("decimal", setting.toString());
    ASSERT_EQ(Field("decimal"), setting);

    setting = String("");
    ASSERT_TRUE(setting.changed);
    ASSERT_FALSE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DECIMAL));
    ASSERT_FALSE(MultiEnum<MySQLDataTypesSupport>(setting).isSet(MySQLDataTypesSupport::DATETIME64));
    ASSERT_EQ("", setting.toString());
    ASSERT_EQ(Field(""), setting);
}

GTEST_TEST(SettingMySQLDataTypesSupport, SetInvalidString)
{
    // Setting can be initialized with int value corresponding to (DECIMAL | DATETIME64)
    SettingMySQLDataTypesSupport setting;
    EXPECT_THROW(setting = String("FOOBAR"), Exception);
    ASSERT_FALSE(setting.changed);
    ASSERT_EQ(std::vector<MySQLDataTypesSupport>{}, setting.value);

    EXPECT_THROW(setting = String("decimal,datetime64,123"), Exception);
    ASSERT_FALSE(setting.changed);
    ASSERT_EQ(std::vector<MySQLDataTypesSupport>{}, setting.value);

    EXPECT_NO_THROW(setting = String(", "));
    ASSERT_TRUE(setting.changed);
    ASSERT_EQ(std::vector<MySQLDataTypesSupport>{}, setting.value);
}

GTEST_TEST(QueryParameters, RoundTrip)
{
    NameToNameMap parameters{{"max_threads", "0"}, {"name", "John's \\ Doe"}, {"empty", ""}};

    WriteBufferFromOwnString out;
    writeQueryParameters(parameters, out);

    ReadBufferFromString in(out.str());
    ASSERT_EQ(readQueryParameters(in), parameters);
}

GTEST_TEST(QueryParameters, DuplicateNameOnTheWireLastOccurrenceWins)
{
    /// A driver that appends an override without deduplicating first relies on the last
    /// occurrence of a repeated Parameter[] key winning, matching the pre-existing behavior
    /// of routing parameters through a Settings object (BaseSettings::read overwrites the
    /// same custom setting entry on each occurrence of its name).
    WriteBufferFromOwnString out;
    BaseSettingsHelpers::writeString("x", out);
    BaseSettingsHelpers::writeFlags(BaseSettingsHelpers::Flags::CUSTOM, out);
    BaseSettingsHelpers::writeString(SettingFieldCustom(Field(String("1"))).toString(), out);
    BaseSettingsHelpers::writeString("x", out);
    BaseSettingsHelpers::writeFlags(BaseSettingsHelpers::Flags::CUSTOM, out);
    BaseSettingsHelpers::writeString(SettingFieldCustom(Field(String("2"))).toString(), out);
    BaseSettingsHelpers::writeString(std::string_view{}, out);

    ReadBufferFromString in(out.str());
    NameToNameMap parameters = readQueryParameters(in);
    ASSERT_EQ(parameters.at("x"), "2");
}

namespace
{

/// Restores the global hard limit and the operator-new throw threshold on scope exit.
struct MemoryLimitGuard
{
    Int64 prev_hard_limit;
    MemoryLimitGuard() : prev_hard_limit(total_memory_tracker.getHardLimit()) {}
    ~MemoryLimitGuard()
    {
        CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(0);
        total_memory_tracker.setHardLimit(prev_hard_limit);
    }
};

/// Makes the next allocation of a megabyte or more overshoot the global limit, while smaller
/// allocations (the exception being built, the settings object's own bookkeeping) still succeed.
void armMemoryLimitOnAllocationsOfAtLeastAMegabyte()
{
    MainThreadStatus::getInstance();
    CurrentThread::flushUntrackedMemory();
    CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(1ULL << 20);
    total_memory_tracker.setHardLimit(total_memory_tracker.get() + 1024);
}

}

GTEST_TEST(Settings, ADeclaredLengthOnTheSettingsWireIsNotAnAllocation)
{
    /// Each entry declares a large string length on the settings wire and then sends no payload, so
    /// a read that grows with the bytes that actually arrive runs out of buffer immediately, while
    /// one that sizes its destination from the declared length allocates before it can fail.
    constexpr size_t declared_size = 64 * 1024 * 1024;

    /// `writeBinary(const Map &)` writes a fixed width element count, then one tagged field per
    /// element, and puts no type constraint on an element.
    const auto write_map_element_tag = [](WriteBuffer & out, Field::Types::Which field_type)
    {
        BaseSettingsHelpers::writeString("additional_table_filters", out);
        writeBinary(static_cast<size_t>(1), out);
        writeBinary(static_cast<UInt8>(field_type), out);
    };

    struct Case
    {
        std::string_view covers;
        SettingsWriteFormat format;
        std::function<void(WriteBuffer &)> write_wire;
    };

    const std::vector<Case> cases = {
        {"the setting name, i.e. every BaseSettingsHelpers::readString carrier",
         SettingsWriteFormat::STRINGS_WITH_FLAGS,
         [&](WriteBuffer & out) { writeVarUInt(declared_size, out); }},
        {"SettingFieldString::readBinary",
         SettingsWriteFormat::BINARY,
         [&](WriteBuffer & out)
         {
             BaseSettingsHelpers::writeString("log_comment", out);
             writeVarUInt(declared_size, out);
         }},
        {"SettingFieldNumber<Float64>::readBinary",
         SettingsWriteFormat::BINARY,
         [&](WriteBuffer & out)
         {
             BaseSettingsHelpers::writeString("totals_auto_threshold", out);
             writeVarUInt(declared_size, out);
         }},
        {"a String inside a Map setting",
         SettingsWriteFormat::BINARY,
         [&](WriteBuffer & out)
         {
             write_map_element_tag(out, Field::Types::String);
             writeVarUInt(declared_size, out);
         }},
        {"an AggregateFunctionState name inside a Map setting",
         SettingsWriteFormat::BINARY,
         [&](WriteBuffer & out)
         {
             write_map_element_tag(out, Field::Types::AggregateFunctionState);
             writeVarUInt(declared_size, out);
         }},
        {"an Object key inside a Map setting",
         SettingsWriteFormat::BINARY,
         [&](WriteBuffer & out)
         {
             write_map_element_tag(out, Field::Types::Object);
             writeBinary(static_cast<size_t>(1), out);
             writeBinary(static_cast<UInt8>(Field::Types::String), out);
             writeVarUInt(declared_size, out);
         }},
    };

    for (const auto & test_case : cases)
    {
        SCOPED_TRACE(test_case.covers);

        /// Build the wire and the settings object before clamping, so only the read runs clamped.
        WriteBufferFromOwnString out;
        test_case.write_wire(out);
        const std::string wire = out.str();
        Settings settings;
        ReadBufferFromString in(wire);

        try
        {
            MemoryLimitGuard guard;
            armMemoryLimitOnAllocationsOfAtLeastAMegabyte();
            settings.read(in, test_case.format);
            ADD_FAILURE() << "reading a declared length of " << declared_size << " did not throw";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF) << e.displayText();
        }
    }
}

GTEST_TEST(Settings, WellFormedSettingsStillReadUnderAClampedMemoryLimit)
{
    /// The in-range control: the clamp itself does not make a read fail, and the empty string end
    /// marker still terminates the loop.
    Settings sent;
    sent.set("log_comment", "hello");
    sent.set("max_block_size", UInt64(4096));
    sent.set("totals_auto_threshold", 0.25);

    WriteBufferFromOwnString out;
    sent.write(out, SettingsWriteFormat::STRINGS_WITH_FLAGS);
    const std::string wire = out.str();

    Settings settings;
    ReadBufferFromString in(wire);
    {
        MemoryLimitGuard guard;
        armMemoryLimitOnAllocationsOfAtLeastAMegabyte();
        ASSERT_NO_THROW(settings.read(in, SettingsWriteFormat::STRINGS_WITH_FLAGS));
    }

    ASSERT_EQ(settings.get("log_comment"), Field(String("hello")));
    ASSERT_EQ(settings.get("max_block_size"), Field(UInt64(4096)));
    ASSERT_EQ(settings.get("totals_auto_threshold"), Field(0.25));
    ASSERT_TRUE(in.eof());
}
