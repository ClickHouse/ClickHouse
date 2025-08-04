#include <Functions/UUIDv7.h>
#include <ctime>

namespace DB
{

namespace UUIDv7Helpers
{
    uint64_t getTimestampMillisecond()
    {
        timespec tp;
        clock_gettime(CLOCK_REALTIME, &tp);/// NOLINT(cert-err33-c)
        const uint64_t sec = tp.tv_sec;
        return sec * 1000 + tp.tv_nsec / 1000000;
    }

    uint64_t dateTimeToMillisecond(UInt32 date_time)
    {
        return static_cast<uint64_t>(date_time) * 1000;
    }

    void setTimestampAndVersion(UUID & uuid, uint64_t timestamp)
    {
        UUIDHelpers::getHighBytes(uuid) = (UUIDHelpers::getHighBytes(uuid) & rand_a_bits_mask) | (timestamp << 16) | 0x7000;
    }

    void setVariant(UUID & uuid)
    {
        UUIDHelpers::getLowBytes(uuid) = (UUIDHelpers::getLowBytes(uuid) & rand_b_bits_mask) | variant_2_mask;
    }

    void CounterFields::resetCounter(const UUID & uuid)
    {
        const uint64_t counter_low_bits = (UUIDHelpers::getLowBytes(uuid) >> rand_b_low_bits_count) & counter_low_bits_mask;
        const uint64_t counter_high_bits = UUIDHelpers::getHighBytes(uuid) & counter_high_bits_mask;
        counter = (counter_high_bits << 30) | counter_low_bits;
    }

    void CounterFields::incrementCounter(UUID & uuid)
    {
        if (++counter == counter_limit) [[unlikely]]
        {
            ++last_timestamp;
            resetCounter(uuid);
            setTimestampAndVersion(uuid, last_timestamp);
            setVariant(uuid);
        }
        else
        {
            UUIDHelpers::getHighBytes(uuid) = (last_timestamp << 16) | 0x7000 | (counter >> counter_low_bits_count);
            UUIDHelpers::getLowBytes(uuid) = (UUIDHelpers::getLowBytes(uuid) & rand_b_with_counter_bits_mask) | variant_2_mask | ((counter & counter_low_bits_mask) << rand_b_low_bits_count);
        }
    }

    void CounterFields::generate(UUID & uuid, uint64_t timestamp)
    {
        const bool need_to_increment_counter = (last_timestamp == timestamp) || ((last_timestamp > timestamp) & (last_timestamp < timestamp + 10000));
        if (need_to_increment_counter)
        {
            incrementCounter(uuid);
        }
        else
        {
            last_timestamp = timestamp;
            resetCounter(uuid);
            setTimestampAndVersion(uuid, last_timestamp);
            setVariant(uuid);
        }
    }

    void Data::generate(UUID & uuid, uint64_t timestamp)
    {
        fields.generate(uuid, timestamp);
    }
}
}
