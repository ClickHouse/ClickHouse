#pragma once
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Poco/AutoPtr.h>
#include <Poco/FileChannel.h>
#include <Poco/LocalDateTime.h>
#include <Poco/RotateStrategy.h>
#include <Poco/Ascii.h>
#include <vector>


namespace DB
{
/// Combined rotation strategies
class OwnRotateStrategy : public Poco::RotateStrategy
{
public:
    void addRotateStrategy(Poco::RotateStrategy * strategy)
    {
        _pRotateStrategies.emplace_back(strategy);
    }

    bool mustRotate(Poco::LogFile * pFile) override
    {
        for (auto & strategy : _pRotateStrategies)
        {
            if (strategy->mustRotate(pFile))
                return true;
        }

        return false;
    }

private:
    std::vector<std::unique_ptr<Poco::RotateStrategy>> _pRotateStrategies;
};


/// Like Poco::FileChannel but supports combined rotation strategies
class OwnFileChannel final : public Poco::FileChannel
{
public:
    void setProperty(const std::string & name, const std::string & value) override
    {
        if (name == Poco::FileChannel::PROP_ROTATION)
        {
            bool composite = false;
            parseRotation(value, composite); /// Parse rotation size and time or interval

            if (composite)
                updateStrategy();
            else
                Poco::FileChannel::setRotation(value);
        }
        else
        {
            Poco::FileChannel::setProperty(name, value);

            if (name == Poco::FileChannel::PROP_TIMES)
                updateStrategy();
        }
    }

private:
    static inline std::string trim(const std::string & s)
    {
        auto start = s.find_first_not_of(" \t\r\n");
        auto end   = s.find_last_not_of(" \t\r\n");
        if (start == std::string::npos)
            return "";
        return s.substr(start, end - start + 1);
    }

    static inline bool isSizeRotation(const std::string & elem)
    {
        if (elem.empty() || elem.find(':') != std::string::npos)
            return false;

        char last = elem.back();
        return (last == 'K' || last == 'M' || last == 'G' || std::isdigit(last));
    }

    /// Example of rotation: 100M,daily
    void parseRotation(const std::string & rotation, bool & composite)
    {
        /// Simple rotation
        if (rotation.find(',') == std::string::npos)
            return;

        /// Split size and time or interval strategies
        /// 100M,daily / daily,100M
        /// 100M,Mon,00:00 / Mon,00:00,100M
        std::string rotation_size;
        std::string rotation_interval;
        std::vector<std::string> elems;
        std::string item;

        ReadBufferFromString in(rotation);

        do
        {
            readStringUntilComma(item, in);

            item = trim(item);
            if (!item.empty())
                elems.push_back(item);

            if (in.eof())
                break;

            in.ignore();
        } while (true);

        for (const auto & elem : elems)
        {
            if (isSizeRotation(elem))
            {
                if (!rotation_size.empty())
                    throw Poco::InvalidArgumentException("rotation", rotation);

                rotation_size = elem;
            }
            else
                rotation_interval += rotation_interval.empty() ? elem : "," + elem;
        }

        /// Simple rotation strategy: size or time/interval
        if (rotation_size.empty() || rotation_interval.empty())
            return;

        /// The combined rotation strategies should contain size and (time or interval)
        _rotationSize = rotation_size;
        _rotationInterval = rotation_interval;
        composite = true;
    }

    Poco::UInt64 parseSize(const std::string & rotation_size)
    {
        /// Reference from FileChannel::setRotation() to find unit
        std::string::const_iterator it  = rotation_size.begin();
        std::string::const_iterator end = rotation_size.end();
        Poco::UInt64 n = 0;
        while (it != end && Poco::Ascii::isSpace(*it)) ++it;
        while (it != end && Poco::Ascii::isDigit(*it)) { n *= 10; n += *it++ - '0'; }
        while (it != end && Poco::Ascii::isSpace(*it)) ++it;
        std::string unit;
        while (it != end && Poco::Ascii::isAlpha(*it)) unit += *it++;

        if (unit == "K")
            return n * 1024;
        if (unit == "M")
            return n * 1024 * 1024;
        if (unit == "G")
            return n * 1024 * 1024 * 1024;
        if (unit.empty())
            return n;

        throw Poco::InvalidArgumentException("rotation", rotation_size);
    }

    Poco::Timespan parseInterval(const std::string & rotation_interval)
    {
        /// Reference from FileChannel::setRotation() to find unit
        std::string::const_iterator it  = rotation_interval.begin();
        std::string::const_iterator end = rotation_interval.end();
        Poco::UInt64 n = 0;
        while (it != end && Poco::Ascii::isSpace(*it)) ++it;
        while (it != end && Poco::Ascii::isDigit(*it)) { n *= 10; n += *it++ - '0'; }
        while (it != end && Poco::Ascii::isSpace(*it)) ++it;
        std::string unit;
        while (it != end && Poco::Ascii::isAlpha(*it)) unit += *it++;

        if (unit == "daily")
            return Poco::Timespan(1 * Poco::Timespan::DAYS);
        if (unit == "weekly")
            return Poco::Timespan(7 *  Poco::Timespan::DAYS);
        if (unit == "monthly")
            return Poco::Timespan(30 * Poco::Timespan::DAYS);
        if (unit == "seconds") // for testing only
            return Poco::Timespan(n * Poco::Timespan::SECONDS);
        if (unit == "minutes")
            return Poco::Timespan(n * Poco::Timespan::MINUTES);
        if (unit == "hours")
            return Poco::Timespan(n * Poco::Timespan::HOURS);
        if (unit == "days")
            return Poco::Timespan(n * Poco::Timespan::DAYS);
        if (unit == "weeks")
            return Poco::Timespan(n * 7 * Poco::Timespan::DAYS);
        if (unit == "months")
            return Poco::Timespan(n * 30 * Poco::Timespan::DAYS);

        throw Poco::InvalidArgumentException("rotation", rotation_interval);
    }

    void updateStrategy()
    {
        /// Simple rotation strategy
        if (_rotationSize.empty() && _rotationInterval.empty())
            return;

        auto * comp = new OwnRotateStrategy();

        if (!_rotationSize.empty() && _rotationSize != "never")
        {
            Poco::UInt64 size = parseSize(_rotationSize);
            comp->addRotateStrategy(new Poco::RotateBySizeStrategy(size));
        }

        if (!_rotationInterval.empty() && _rotationInterval != "never")
        {
            if ((_rotationInterval.find(',') != std::string::npos) || (_rotationInterval.find(':') != std::string::npos))
            {
                std::string times = getProperty(Poco::FileChannel::PROP_TIMES);
                if (times == "utc")
                    comp->addRotateStrategy(new Poco::RotateAtTimeStrategy<Poco::DateTime>(_rotationInterval));
                else
                    comp->addRotateStrategy(new Poco::RotateAtTimeStrategy<Poco::LocalDateTime>(_rotationInterval));
            }
            else
            {
                Poco::Timespan span = parseInterval(_rotationInterval);
                comp->addRotateStrategy(new Poco::RotateByIntervalStrategy(span));
            }
        }

        setRotationStrategy(comp);
    }

    std::string _rotationSize;
    std::string _rotationInterval;
};

}
