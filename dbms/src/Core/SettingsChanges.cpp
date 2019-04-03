#include <Core/SettingsChanges.h>

#include <Core/Settings.h>
#include <Common/FieldVisitors.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace
{

    template<typename T>
    struct Serialization;


    template <typename T>
    struct Serialization<SettingNumber<T>>
    {
        static Field read(ReadBuffer & buf)
        {
            T value;
            if constexpr (std::is_integral_v<T>)
            {
                value = 0;
                readVarT(value, buf);
            }
            else
            {
                String s;
                readBinary(s, buf);
                value = parse<T>(s);
            }
            return value;
        }

        static void write(const Field & field, WriteBuffer & buf)
        {
            T value;
            if (field.getType() == Field::Types::String)
                value = parse<T>(field.get<String>());
            else
                value = applyVisitor(FieldVisitorConvertToNumber<T>(), field);
            if constexpr (std::is_integral_v<T>)
                writeVarT(value, buf);
            else
                writeBinary(toString(value), buf);
        }
    };


    template<>
    struct Serialization<SettingMaxThreads>
    {
        static Field read(ReadBuffer & buf)
        {
            UInt64 max_threads = 0;
            readVarUInt(max_threads, buf);
            return max_threads;
        }

        static void write(const Field & field, WriteBuffer & buf)
        {
            UInt64 max_threads;
            if (field.getType() == Field::Types::String)
            {
                const String & s = field.get<String>();
                if (s == "auto")
                    max_threads = 0;
                else
                    max_threads = parse<UInt64>(s);
            }
            else
                 max_threads = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), field);
            writeVarUInt(max_threads, buf);
        }
    };


    template<>
    struct Serialization<SettingSeconds>
    {
        static Field read(ReadBuffer & buf)
        {
            UInt64 seconds = 0;
            readVarUInt(seconds, buf);
            return seconds;
        }

        static void write(const Field & field, WriteBuffer & buf)
        {
            UInt64 seconds;
            if (field.getType() == Field::Types::String)
                seconds = parse<UInt64>(field.get<String>());
            else
                seconds = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), field);
            writeVarUInt(seconds, buf);
        }
    };


    template<>
    struct Serialization<SettingMilliseconds>
    {
        static Field read(ReadBuffer & buf)
        {
            UInt64 milliseconds = 0;
            readVarUInt(milliseconds, buf);
            return milliseconds;
        }

        static void write(const Field & field, WriteBuffer & buf)
        {
            UInt64 milliseconds;
            if (field.getType() == Field::Types::String)
                milliseconds = parse<UInt64>(field.get<String>());
            else
                milliseconds = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), field);
            writeVarUInt(milliseconds, buf);
        }
    };


    template<>
    struct Serialization<SettingString>
    {
        static Field read(ReadBuffer & buf)
        {
            String s;
            readBinary(s, buf);
            return s;
        }

        static void write(const Field & field, WriteBuffer & buf)
        {
            writeBinary(field.safeGet<String>(), buf);
        }
    };


    template<>
    struct Serialization<SettingChar> : public Serialization<SettingString> {};

    template <>
    struct Serialization<SettingLoadBalancing> : public Serialization<SettingString> {};

    template <>
    struct Serialization<SettingJoinStrictness> : public Serialization<SettingString> {};

    template <>
    struct Serialization<SettingTotalsMode> : public Serialization<SettingString> {};

    template <bool enable_mode_any>
    struct Serialization<SettingOverflowMode<enable_mode_any>> : public Serialization<SettingString> {};

    template <>
    struct Serialization<SettingDistributedProductMode> : public Serialization<SettingString> {};

    template <>
    struct Serialization<SettingDateTimeInputFormat> : public Serialization<SettingString> {};

    template <>
    struct Serialization<SettingLogsLevel> : public Serialization<SettingString> {};


    class SerializationDispatcher : public ext::singleton<SerializationDispatcher>
    {
    public:
        SerializationDispatcher()
        {
#define GET_READ_WRITE_FUNCTIONS(TYPE, NAME, DEFAULT, DESCRIPTION) \
            map[StringRef(#NAME, strlen(#NAME))] = {&Serialization<TYPE>::read, &Serialization<TYPE>::write };

            APPLY_FOR_SETTINGS(GET_READ_WRITE_FUNCTIONS)
#undef GET_READ_WRITE_FUNCTIONS
        }

        Field read(const String & name, ReadBuffer & buf) const
        {
            auto it = map.find(name);
            if (it == map.end())
                throw Exception("Not found reader for setting " + name, ErrorCodes::LOGICAL_ERROR);
            return (it->second.read)(buf);
        }

        void write(const String & name, const Field & field, WriteBuffer & buf) const
        {
            auto it = map.find(name);
            if (it == map.end())
                return;
            (it->second.write)(field, buf);
        }

    private:
        typedef Field (*ReadFunction)(ReadBuffer &);
        typedef void (*WriteFunction)(const Field &, WriteBuffer &);
        struct Functions { ReadFunction read; WriteFunction write; };
        std::unordered_map<StringRef, Functions> map;
    };
}


SettingsChanges::SettingsChanges()
{
}

SettingsChanges::~SettingsChanges() = default;

void SettingsChanges::deserialize(ReadBuffer & buf)
{
    while (true)
    {
        String name;
        readBinary(name, buf);

        /// An empty string is the marker for the end of the settings.
        if (name.empty())
            break;

        Field field = SerializationDispatcher::instance().read(name, buf);
        push_back(name, field);
    }
}

void SettingsChanges::serialize(WriteBuffer & buf) const
{
    for (const SettingChange & change : changes)
    {
        writeStringBinary(change.name, buf);
        SerializationDispatcher::instance().write(change.name, change.value, buf);
    }

    /// An empty string is a marker for the end of the settings.
    writeStringBinary("", buf);
}

}
