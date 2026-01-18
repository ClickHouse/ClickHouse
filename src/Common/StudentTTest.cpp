#include <Common/StudentTTest.h>
#include <Common/StudentTTable.h>

#include <cmath>
#include <iomanip>
#include <sstream>
#include <stdexcept>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

void StudentTTest::clear()
{
    data[0].clear();
    data[1].clear();
}

void StudentTTest::add(size_t distribution, double value)
{
    if (distribution > 1)
        throw std::logic_error("Distribution number for Student's T-Test must be eigther 0 or 1");
    data[distribution].add(value);
}

/// Confidence_level_index can be set in range [0, 5]. Corresponding values can be found above.
std::pair<bool, std::string> StudentTTest::compareAndReport(size_t confidence_level_index) const
{
    confidence_level_index = std::min<size_t>(confidence_level_index, 5);

    if (data[0].size == 0 || data[1].size == 0)
        return {true, ""};

    size_t degrees_of_freedom = (data[0].size - 1) + (data[1].size - 1);

    double table_value = impl::students_table[degrees_of_freedom > 100 ? 0 : degrees_of_freedom][confidence_level_index];

    double pooled_standard_deviation = sqrt(((data[0].size - 1) * data[0].var() + (data[1].size - 1) * data[1].var()) / degrees_of_freedom);

    double t_statistic = pooled_standard_deviation * sqrt(1.0 / data[0].size + 1.0 / data[1].size);

    double mean_difference = fabs(data[0].avg() - data[1].avg());

    double mean_confidence_interval = table_value * t_statistic;

    DB::WriteBufferFromOwnString out;

    if (mean_difference > mean_confidence_interval && (mean_difference - mean_confidence_interval > 0.0001)) /// difference must be more than 0.0001, to take into account connection latency.
    {
        out << "Difference at " << impl::confidence_level[confidence_level_index] <<  "% confidence: ";
        out << "mean difference is " << mean_difference << ", but confidence interval is " << mean_confidence_interval;
        return {false, out.str()};
    }

    out << "No difference proven at " << impl::confidence_level[confidence_level_index] << "% confidence";
    return {true, out.str()};
}
