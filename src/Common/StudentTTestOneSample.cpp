#include <Common/StudentTTestOneSample.h>
#include <Common/StudentTTable.h>

#include <cmath>
#include <iomanip>
#include <sstream>
#include <stdexcept>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


void StudentTTestOneSample::clear()
{
    data.clear();
}

void StudentTTestOneSample::setPopulationMean(double mean)
{
    data.population_mean = mean;
}

void StudentTTestOneSample::add(double value)
{
    data.add(value);
}

std::pair<bool, std::string> StudentTTestOneSample::compareAndReport(size_t confidence_level_index) const
{
    confidence_level_index = std::min<size_t>(confidence_level_index, 5);

    if (data.size == 0)
        return {true, ""};

    if (data.size == 1)
        return {true, "Insufficient data for t-test (need at least 2 observations)"};

    size_t degrees_of_freedom = data.size - 1;

    double table_value = impl::students_table[degrees_of_freedom > 100 ? 0 : degrees_of_freedom][confidence_level_index];

    double sample_mean = data.avg();
    double sample_variance = data.var();
    double sample_std_dev = sqrt(sample_variance);
    double standard_error = sample_std_dev / sqrt(static_cast<double>(data.size));

    // t-statistic is not needed for compare/report path; confidence interval is based on standard error.

    double mean_difference = fabs(sample_mean - data.population_mean);
    double mean_confidence_interval = table_value * standard_error;

    DB::WriteBufferFromOwnString out;

    if (mean_difference > mean_confidence_interval && (mean_difference - mean_confidence_interval > 0.0001))
    {
        out << "Difference at " << impl::confidence_level[confidence_level_index] <<  "% confidence: ";
        out << "sample mean is " << sample_mean << ", population mean is " << data.population_mean;
        out << ", difference is " << mean_difference << ", but confidence interval is " << mean_confidence_interval;
        return {false, out.str()};
    }

    out << "No difference proven at " << impl::confidence_level[confidence_level_index] << "% confidence";
    return {true, out.str()};
}
