#pragma once

#include <string>

/**
 * About:
 * This is implementation of One-sample t-test
 * Read about it on https://en.wikipedia.org/wiki/Student%27s_t-test (One-sample t-test)
 *
 * Usage:
 * It's used to test whether the mean of a single sample differs from a known population mean.
 * Values can be added with t_test.add(value) and after compared and reported with compareAndReport().
 */
class StudentTTestOneSample
{
private:
    struct SampleData
    {
        size_t size = 0;
        double sum = 0;
        double squares_sum = 0;
        double population_mean = 0.0;

        void add(double value)
        {
            ++size;
            sum += value;
            squares_sum += value * value;
        }

        double avg() const
        {
            return sum / static_cast<double>(size);
        }

        double var() const
        {
            return (squares_sum - (sum * sum / size)) / static_cast<double>(size - 1);
        }

        void clear()
        {
            size = 0;
            sum = 0;
            squares_sum = 0;
            population_mean = 0.0;
        }
    };

    SampleData data;

public:
    void clear();

    void setPopulationMean(double mean);

    void add(double value);

    std::pair<bool, std::string> compareAndReport(size_t confidence_level_index = 5) const;
};
