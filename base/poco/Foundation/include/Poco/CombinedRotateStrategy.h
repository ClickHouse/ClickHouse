#pragma once
#include <Poco/AutoPtr.h>
#include <Poco/LocalDateTime.h>
#include <Poco/RotateStrategy.h>
#include <Poco/Ascii.h>
#include <vector>


namespace Poco
{

/// Combined rotation strategies
class CombinedRotateStrategy : public RotateStrategy
{
public:
    CombinedRotateStrategy(const std::string & _rotation, const std::string & _times)
    {
        rotation = parseRotation(_rotation); /// Parse rotation size and time or interval

        if (rotation.size)
            addRotateStrategy(new RotateBySizeStrategy(*rotation.size));

        if (rotation.interval)
            addRotateStrategy(new RotateByIntervalStrategy(*rotation.interval));

        if (rotation.time)
        {
            if (_times == "utc")
                addRotateStrategy(new RotateAtTimeStrategy<DateTime>(*rotation.time));
            else
                addRotateStrategy(new RotateAtTimeStrategy<LocalDateTime>(*rotation.time));
        }
    }

    void addRotateStrategy(RotateStrategy * strategy)
    {
        _pRotateStrategies.emplace_back(strategy);
    }

    bool mustRotate(LogFile * pFile) override
    {
        for (auto & strategy : _pRotateStrategies)
        {
            if (strategy->mustRotate(pFile))
                return true;
        }

        return false;
    }

private:
    struct RotationInfo
    {
        std::optional<UInt64> size = std::nullopt;
        std::optional<Timespan> interval = std::nullopt;
        std::optional<std::string> time = std::nullopt;
    };

    /// <n> / <n> K/M/G
    static std::optional<UInt64> parseSize(std::string_view str, size_t & pos)
    {
        auto it = str.begin() + pos;
        auto end = str.end();
        UInt64 n = 0;
        while (it != end && Ascii::isSpace(*it)) ++it;
        while (it != end && Ascii::isDigit(*it)) { n *= 10; n += *it++ - '0'; }
        while (it != end && Ascii::isSpace(*it)) ++it;
        std::string unit;
        while (it != end && Ascii::isAlpha(*it)) unit += *it++;

        UInt64 size = 0;
        if (unit == "K")
            size = n * 1024;
        else if (unit == "M")
            size = n * 1024 * 1024;
        else if (unit == "G")
            size = n * 1024 * 1024 * 1024;
        else if (unit.empty())
            size = n;
        else
            return std::nullopt; /// Return std::nullopt if fails to parse

        /// Try to skip ','
        while (it != end && Ascii::isSpace(*it)) ++it;
        if (it != end && *it == ',') ++it;

        pos = it - str.begin();
        return size;
    }

    /// daily / weekly / monthly / <n> minutes
    static std::optional<Timespan> parseInterval(std::string_view str, size_t & pos)
    {
        auto it = str.begin() + pos;
        auto end = str.end();
        UInt64 n = 0;
        while (it != end && Ascii::isSpace(*it)) ++it;
        while (it != end && Ascii::isDigit(*it)) { n *= 10; n += *it++ - '0'; }
        while (it != end && Ascii::isSpace(*it)) ++it;
        std::string unit;
        while (it != end && Ascii::isAlpha(*it)) unit += *it++;

        Timespan interval;

        if (unit == "daily")
            interval = Timespan(1 * Timespan::DAYS);
        else if (unit == "weekly")
            interval = Timespan(7 * Timespan::DAYS);
        else if (unit == "monthly")
            interval = Timespan(30 * Timespan::DAYS);
        else if (unit == "seconds") // for testing only
            interval = Timespan(n * Timespan::SECONDS);
        else if (unit == "milliseconds") // for testing only
            interval = Timespan(n * Timespan::MILLISECONDS);
        else if (unit == "minutes")
            interval = Timespan(n * Timespan::MINUTES);
        else if (unit == "hours")
            interval = Timespan(n * Timespan::HOURS);
        else if (unit == "days")
            interval = Timespan(n * Timespan::DAYS);
        else if (unit == "weeks")
            interval = Timespan(n * 7 * Timespan::DAYS);
        else if (unit == "months")
            interval = Timespan(n * 30 * Timespan::DAYS);
        else
            return std::nullopt; /// Return std::nullopt if fails to parse

        /// Try to skip ','
        while (it != end && Ascii::isSpace(*it)) ++it;
        if (it != end && *it == ',') ++it;

        pos = it - str.begin();
        return interval;
    }

    /// [day,][hh]:mm
    static std::optional<std::string> parseTime(std::string_view str, size_t & pos)
    {
        size_t colon_pos = str.find(':', pos);
        if (colon_pos == std::string::npos)
            return std::nullopt;

        size_t comma_after_colon = str.find(',', colon_pos + 1);

        /// Simple rotation by time
        if (comma_after_colon == std::string::npos)
        {
            auto res = std::string(str.substr(pos));
            pos = str.size();
            return res;
        }

        auto res = std::string(str.substr(pos, comma_after_colon));
        pos = comma_after_colon + 1;
        return res;
    }

    /// Returns true if it's "never" or false if not.
    static bool parseNever(std::string_view str)
    {
        return str == "never";
    }

    /// Support 100M/10 minutes/100M,10 minutes
    RotationInfo parseRotation(std::string_view rotation)
    {
        RotationInfo res;
        size_t pos = 0;

        res.size = parseSize(rotation, pos);
        res.interval = parseInterval(rotation, pos);
        res.time = parseTime(rotation, pos);

        if (!res.size)
            res.size = parseSize(rotation, pos); /// To handle the case when an interval is specified before a size

        if (!res.size && !res.interval && !res.time && !parseNever(rotation))
            throw InvalidArgumentException(
                "Invalid rotation format '" + std::string(rotation) +
                "'. Expected formats: <size>[K|M|G], <interval>, [<size>,]<interval>, or 'never'.");

        return res;
    }

    RotationInfo rotation;
    std::vector<std::unique_ptr<RotateStrategy>> _pRotateStrategies;
};

}
