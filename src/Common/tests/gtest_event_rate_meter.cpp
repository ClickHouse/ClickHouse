#include <gtest/gtest.h>

#include <Common/EventRateMeter.h>

#include <cmath>


TEST(EventRateMeter, ExponentiallySmoothedAverage)
{
    double target = 100.0;

    // The test is only correct for timestep of 1 second because of
    // how sum of weights is implemented inside `ExponentiallySmoothedAverage`
    double time_step = 1.0;

    for (double half_decay_time : { 0.1, 1.0, 10.0, 100.0})
    {
        DB::ExponentiallySmoothedAverage esa;

        int steps = static_cast<int>(half_decay_time * 30 / time_step);
        for (int i = 1; i <= steps; ++i)
            esa.add(target * time_step, i * time_step, half_decay_time);
        double measured = esa.get(half_decay_time);
        ASSERT_LE(std::fabs(measured - target), 1e-5 * target);
    }
}

TEST(EventRateMeter, ConstantRate)
{
    double target = 100.0;

    for (double period : {0.1, 1.0, 10.0})
    {
        for (double time_step : {0.001, 0.01, 0.1, 1.0})
        {
            DB::EventRateMeter erm(0.0, period);

            int steps = static_cast<int>(period * 30 / time_step);
            for (int i = 1; i <= steps; ++i)
                erm.add(i * time_step, target * time_step);
            double measured = erm.rate(steps * time_step);
            // std::cout << "T=" << period << " dt=" << time_step << " measured=" << measured << std::endl;
            ASSERT_LE(std::fabs(measured - target), 1e-5 * target);
        }
    }
}

TEST(EventRateMeter, PreciseStart)
{
    double target = 100.0;

    for (double period : {0.1, 1.0, 10.0})
    {
        for (double time_step : {0.001, 0.01, 0.1, 1.0})
        {
            DB::EventRateMeter erm(0.0, period);

            int steps = static_cast<int>(period / time_step);
            for (int i = 1; i <= steps; ++i)
            {
                erm.add(i * time_step, target * time_step);
                double measured = erm.rate(i * time_step);
                // std::cout << "T=" << period << " dt=" << time_step << " measured=" << measured << std::endl;
                ASSERT_LE(std::fabs(measured - target), 1e-5 * target);
            }
        }
    }
}

TEST(EventRateMeter, NonZeroClockEpoch)
{
    double target = 1000.0;
    double time_step = 0.001;

    // Production meters are fed absolute instants of a clock that counts from boot
    // (`clock_gettime_ns()`), so the epoch is far from zero. The zero epoch is kept as a
    // control arm: it is the only shape the tests above cover, and it must not regress.
    for (double start : {0.0, 3600.0, 1e6})
    {
        for (size_t heating : {0UL, 1UL, 4UL})
        {
            for (double period : {0.1, 2.0})
            {
                DB::EventRateMeter erm(start, period, heating);

                double now = start;
                int steps = static_cast<int>(period / time_step);
                for (int i = 1; i <= steps; ++i)
                {
                    now += time_step;
                    erm.add(now, target * time_step);
                    double measured = erm.rate(now);
                    ASSERT_LE(std::fabs(measured - target), 1e-3 * target)
                        << "start=" << start << " heating=" << heating
                        << " period=" << period << " step=" << i;
                }
            }
        }
    }
}
