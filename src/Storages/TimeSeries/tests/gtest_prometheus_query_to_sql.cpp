#include <gtest/gtest.h>

#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationSettings.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/Converter.h>

#include <memory>
#include <string_view>
#include <utility>

using namespace DB;

namespace
{
String getHistogramQuantileSQL(std::string_view compatibility)
{
    auto promql_tree = std::make_shared<PrometheusQueryTree>("histogram_quantile(0.5, http_request_duration_seconds_bucket)");

    PrometheusQueryEvaluationSettings settings;
    settings.time_series_storage_id = StorageID{"default", "prometheus"};
    settings.timestamp_data_type = std::make_shared<DataTypeDateTime64>(3);
    settings.scalar_data_type = std::make_shared<DataTypeFloat64>();
    settings.use_current_time = true;
    settings.use_quantile_prometheus_histogram_array = useQuantilePrometheusHistogramArray(compatibility);

    return PrometheusQueryToSQL::Converter{std::move(promql_tree), std::move(settings)}.getSQL()->formatForLogging();
}

}

TEST(PrometheusQueryToSQL, HistogramQuantileCompatibility)
{
    const auto old_sql = getHistogramQuantileSQL("26.8");
    EXPECT_NE(old_sql.find("quantilePrometheusHistogramForEach"), String::npos);
    EXPECT_EQ(old_sql.find("quantilePrometheusHistogramArray"), String::npos);

    const auto new_sql = getHistogramQuantileSQL("26.9");
    EXPECT_NE(new_sql.find("quantilePrometheusHistogramArray"), String::npos);
    EXPECT_EQ(new_sql.find("quantilePrometheusHistogramForEach"), String::npos);
}
