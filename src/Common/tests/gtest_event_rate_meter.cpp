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
