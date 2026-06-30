#pragma once

#include <string>
#include <cstring>
#include <stdexcept>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/LocalDate.h>

/**
 * Stores time in a broken-down format (hours, minutes, seconds).
 * Can be initialized from a time in text form like "HHH:MM:SS" or from a `time_t` value.
 * Can be implicitly cast to `time_t`, but remember that `time_t` usually includes a date component.
 * NOTE: Transforming between `time_t` and `LocalTime` is done in the local time zone!
 *
 * When local time is shifted backward (due to daylight saving time or other reasons),
 * to resolve the ambiguity of transforming to `time_t`, the lowest of the two possible values is selected.
 */
class LocalTime
{
private:
    bool is_negative = false;
    uint64_t m_hour; /// NOLINT             /// Hours (0-999)
    unsigned char m_minute;                 /// Minutes (0-59)
    unsigned char m_second;                 /// Seconds (0-59)

    /**
     * Initializes time from `time_t` and time zone.
     *
     * @param time Time in `time_t` format.
     * @param time_zone Time zone information.
     */
    void init(time_t time, const DateLUTImpl & time_zone)
    {
        if (time < 0)
            is_negative = true;

        auto time_to_split = is_negative ? time * (-1) : time;

        const auto components = time_zone.toTimeComponents(static_cast<DateLUTImpl::Time>(time_to_split));

        m_hour = components.hour;
        m_minute = components.minute;
        m_second = components.second;
    }

    /**
     * Initializes time from a string.
     *
     * @param s Pointer to the beginning of the string.
     * @param length Length of the string.
     */
    void init(const char * s, size_t length)
    {
        if (length < 7)
            throw std::runtime_error("Cannot parse LocalTime: " + std::string(s, length));

        std::string s_copy;
        if (s[0] == '-') // if we have a minus sign at the beginning
        {
            is_negative = true;
            s_copy = std::string(s + 1, length - 1);
        }
        else
        {
            s_copy = std::string(s, length);
        }

        /// Here we should consider three cases: HHH:MM::SS, HH:MM:SS and H:MM:SS
        if (s_copy[3] == ':' && s[6] == ':') /// Case 1
        {
            // Parse hours: s_copy[0] to s_copy[2]
            m_hour = 0;
            for (int i = 0; i < 3; ++i)
            {
                if (s_copy[i] < '0' || s_copy[i] > '9')
                    throw std::runtime_error("Invalid hour digit in LocalTime: " + std::string(s, length));
                m_hour = m_hour * 10 + static_cast<unsigned int>(s_copy[i] - '0');
            }

            // Parse minutes: s_copy[4] to s_copy[5]
            if (s_copy[4] < '0' || s_copy[4] > '9' || s_copy[5] < '0' || s_copy[5] > '9')
                throw std::runtime_error("Invalid minute digits in LocalTime: " + std::string(s, length));
            m_minute = static_cast<unsigned char>((s_copy[4] - '0') * 10 + (s_copy[5] - '0'));

            // Parse seconds: s_copy[7] to s_copy[8]
            if (s_copy[7] < '0' || s_copy[7] > '9' || s_copy[8] < '0' || s_copy[8] > '9')
                throw std::runtime_error("Invalid second digits in LocalTime: " + std::string(s, length));
            m_second = static_cast<unsigned char>((s_copy[7] - '0') * 10 + (s_copy[8] - '0'));
        }
        else if (s_copy[2] == ':' && s_copy[5] == ':') /// Case 2
        {
            // Parse hours: s[0] to s[1]
            m_hour = 0;
            for (int i = 0; i < 2; ++i)
            {
                if (s_copy[i] < '0' || s_copy[i] > '9')
                    throw std::runtime_error("Invalid hour digit in LocalTime: " + std::string(s, length));
                m_hour = m_hour * 10 + static_cast<unsigned int>(s_copy[i] - '0');
            }

            // Parse minutes: s_copy[3] to s_copy[4]
            if (s_copy[3] < '0' || s_copy[3] > '9' || s_copy[4] < '0' || s_copy[4] > '9')
                throw std::runtime_error("Invalid minute digits in LocalTime: " + std::string(s, length));
            m_minute = static_cast<unsigned char>((s_copy[3] - '0') * 10 + (s_copy[4] - '0'));

            // Parse seconds: s_copy[6] to s_copy[7]
            if (s_copy[6] < '0' || s_copy[6] > '9' || s_copy[7] < '0' || s_copy[7] > '9')
                throw std::runtime_error("Invalid second digits in LocalTime: " + std::string(s, length));
            m_second = static_cast<unsigned char>((s_copy[6] - '0') * 10 + (s_copy[7] - '0'));
        }
        else if (s_copy[1] == ':' && s_copy[4] == ':') /// Case 3
        {
            // Parse hours: s_copy[0]
            if (s_copy[0] < '0' || s_copy[0] > '9')
                throw std::runtime_error("Invalid hour digit in LocalTime: " + std::string(s, length));
            m_hour = static_cast<unsigned int>(s_copy[0] - '0');

            // Parse minutes: s_copy[2] to s_copy[3]
            if (s_copy[2] < '0' || s_copy[2] > '9' || s_copy[3] < '0' || s_copy[3] > '9')
                throw std::runtime_error("Invalid minute digits in LocalTime: " + std::string(s, length));
            m_minute = static_cast<unsigned char>((s_copy[2] - '0') * 10 + (s_copy[3] - '0'));

            // Parse seconds: s_copy[5] to s_copy[6]
            if (s_copy[5] < '0' || s_copy[5] > '9' || s_copy[6] < '0' || s_copy[6] > '9')
                throw std::runtime_error("Invalid second digits in LocalTime: " + std::string(s, length));
            m_second = static_cast<unsigned char>((s_copy[5] - '0') * 10 + (s_copy[6] - '0'));
        }
        else
            throw std::runtime_error("Cannot parse LocalTime: " + std::string(s, length));
    }

    // Helper function to compute the total seconds represented by the LocalTime.
    // For negative times, the total seconds is negative.
    inline int64_t totalSeconds() const
    {
        // Compute total seconds from hours, minutes, and seconds.
        int64_t total = static_cast<int64_t>(m_hour) * 3600
                      + static_cast<int64_t>(m_minute) * 60
                      + static_cast<int64_t>(m_second);
        return is_negative ? -total : total;
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
     * @param hour_ Hours (0-999).
     * @param minute_ Minutes (0-59).
     * @param second_ Seconds (0-59).
     */
    LocalTime(bool is_negative_, uint64_t hour_, unsigned char minute_, unsigned char second_) /// NOLINT
        : is_negative(is_negative_), m_hour(hour_), m_minute(minute_), m_second(second_)
    {}

    /**
     * Constructor from a string.
     *
     * @param s String in the format "HHH:MM:SS".
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
    {}

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
    bool negative() const { return is_negative; }
    uint64_t hour() const { return m_hour; } /// NOLINT
    unsigned char minute() const { return m_minute; }
    unsigned char second() const { return m_second; }

    // Mutator methods
    void negative(bool b) { is_negative = b; }
    void hour(uint64_t x) { m_hour = x; } /// NOLINT
    void minute(unsigned char x) { m_minute = x; }
    void second(unsigned char x) { m_second = x; }

    // Comparison operators
    bool operator< (const LocalTime & other) const
    {
        return totalSeconds() < other.totalSeconds();
    }

    bool operator> (const LocalTime & other) const
    {
        return totalSeconds() > other.totalSeconds();
    }

    bool operator<= (const LocalTime & other) const
    {
        return totalSeconds() <= other.totalSeconds();
    }

    bool operator>= (const LocalTime & other) const
    {
        return totalSeconds() >= other.totalSeconds();
    }

    bool operator== (const LocalTime & other) const
    {
        return totalSeconds() == other.totalSeconds();
    }

    bool operator!= (const LocalTime & other) const
    {
        return totalSeconds() != other.totalSeconds();
    }
};
