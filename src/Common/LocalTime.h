#pragma once

#include <string>
#include <cstring>
#include <stdexcept>
#include <Common/DateLUT.h>
#include <Common/LocalDate.h>

/**
 * Stores time in a broken-down format (hours, minutes, seconds).
 * Can be initialized from a time in text form like "HHHH:MM:SS" or from a `time_t` value.
 * Can be implicitly cast to `time_t`, but remember that `time_t` usually includes a date component.
 * NOTE: Transforming between `time_t` and `LocalTime` is done in the local time zone!
 *
 * When local time is shifted backward (due to daylight saving time or other reasons),
 * to resolve the ambiguity of transforming to `time_t`, the lowest of the two possible values is selected.
 */
class LocalTime
{
private:
    uint64_t m_hour; /// NOLINT             /// Hours (0-999)
    unsigned char m_minute;                 /// Minutes (0-59)
    unsigned char m_second;                 /// Seconds (0-59)

    /// Padding to fill the structure to 8 bytes and ensure safe invocation of `memcmp`.
    unsigned char pad[4] = {0};

    /**
     * Initializes time from `time_t` and time zone.
     *
     * @param time Time in `time_t` format.
     * @param time_zone Time zone information.
     */
    void init(time_t time, const DateLUTImpl & time_zone)
    {
        DateLUTImpl::TimeComponents components = time_zone.toTimeComponents(static_cast<DateLUTImpl::Time>(time));

        m_hour = components.hour;
        m_minute = components.minute;
        m_second = components.second;

        // Initialize padding
        std::memset(pad, 0, sizeof(pad));
    }

    /**
     * Initializes time from a string.
     *
     * @param s Pointer to the beginning of the string.
     * @param length Length of the string.
     */
    void init(const char * s, size_t length)
    {
        if (length < 9)
            throw std::runtime_error("Cannot parse LocalTime: " + std::string(s, length));

        // Expected format "HHHHH:MM:SS"
        if (s[3] != ':' || s[6] != ':')
            throw std::runtime_error("Invalid format for LocalTime: " + std::string(s, length));

        // Parse hours: s[0] to s[2]
        m_hour = 0;
        for (int i = 0; i < 3; ++i)
        {
            if (s[i] < '0' || s[i] > '9')
                throw std::runtime_error("Invalid hour digit in LocalTime: " + std::string(s, length));
            m_hour = m_hour * 10 + static_cast<unsigned int>(s[i] - '0');
        }

        // Parse minutes: s[4] to s[5]
        if (s[4] < '0' || s[4] > '9' || s[5] < '0' || s[5] > '9')
            throw std::runtime_error("Invalid minute digits in LocalTime: " + std::string(s, length));
        m_minute = static_cast<unsigned char>((s[4] - '0') * 10 + (s[5] - '0'));

        // Parse seconds: s[7] to s[8]
        if (s[7] < '0' || s[7] > '9' || s[8] < '0' || s[8] > '9')
            throw std::runtime_error("Invalid second digits in LocalTime: " + std::string(s, length));
        m_second = static_cast<unsigned char>((s[7] - '0') * 10 + (s[8] - '0'));

        // Initialize padding
        std::memset(pad, 0, sizeof(pad));
    }

public:
    /**
     * Constructor from `time_t` and time zone.
     *
     * @param time Time in `time_t` format.
     * @param time_zone Time zone information.
     */
    explicit LocalTime(time_t time, const DateLUTImpl & time_zone = DateLUT::instance())
    {
        init(time, time_zone);
    }

    /**
     * Constructor with individual time components.
     *
     * @param hour_ Hours (0-65535).
     * @param minute_ Minutes (0-59).
     * @param second_ Seconds (0-59).
     */
    LocalTime(unsigned short hour_, unsigned char minute_, unsigned char second_) /// NOLINT
        : m_hour(hour_), m_minute(minute_), m_second(second_)
    {
        std::memset(pad, 0, sizeof(pad));
    }

    /**
     * Constructor from a string.
     *
     * @param s String in the format "HHHH:MM:SS".
     */
    explicit LocalTime(const std::string & s)
    {
        if (s.size() < 9)
            throw std::runtime_error("Cannot parse LocalTime: " + s);

        init(s.data(), s.size());
    }

    /**
     * Default constructor initializes time to 0000:00:00.
     */
    LocalTime() : m_hour(0), m_minute(0), m_second(0)
    {
        std::memset(pad, 0, sizeof(pad));
    }

    /**
     * Constructor from data pointer and length.
     *
     * @param data Pointer to the beginning of the data.
     * @param length Length of the data.
     */
    LocalTime(const char * data, size_t length)
    {
        init(data, length);
    }

    // Copy constructor and assignment operator
    LocalTime(const LocalTime &) noexcept = default;
    LocalTime & operator= (const LocalTime &) noexcept = default;

    // Accessor methods
    unsigned short hour() const { return m_hour; } /// NOLINT
    unsigned char minute() const { return m_minute; }
    unsigned char second() const { return m_second; }

    // Mutator methods
    void hour(unsigned short x) { m_hour = x; } /// NOLINT
    void minute(unsigned char x) { m_minute = x; }
    void second(unsigned char x) { m_second = x; }

    // Comparison operators
    bool operator< (const LocalTime & other) const
    {
        return std::memcmp(this, &other, sizeof(*this)) < 0;
    }

    bool operator> (const LocalTime & other) const
    {
        return std::memcmp(this, &other, sizeof(*this)) > 0;
    }

    bool operator<= (const LocalTime & other) const
    {
        return std::memcmp(this, &other, sizeof(*this)) <= 0;
    }

    bool operator>= (const LocalTime & other) const
    {
        return std::memcmp(this, &other, sizeof(*this)) >= 0;
    }

    bool operator== (const LocalTime & other) const
    {
        return std::memcmp(this, &other, sizeof(*this)) == 0;
    }

    bool operator!= (const LocalTime & other) const
    {
        return !(*this == other);
    }
};

// static_assert(sizeof(LocalTime) == 8, "LocalTime must be 8 bytes in size.");
