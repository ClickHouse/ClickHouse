#include <gtest/gtest.h>

#include <Parsers/Prometheus/PrometheusQueryTree.h>

using namespace DB;

namespace
{
    String parseAndDumpTree(std::string_view input)
    {
        return PrometheusQueryTree{input}.dumpTree();
    }
}


/// Parse queries from https://github.com/prometheus/compliance/blob/main/promql/promql-test-queries.yml
TEST(PrometheusQueryTree, ParseComplianceQueries)
{
    /// Scalar literals.
    EXPECT_EQ(parseAndDumpTree("42"), R"(
PrometheusQueryTree(SCALAR):
    Scalar(42)
)");

    EXPECT_EQ(parseAndDumpTree("1.234"), R"(
PrometheusQueryTree(SCALAR):
    Scalar(1.234)
)");

    EXPECT_EQ(parseAndDumpTree(".123"), R"(
PrometheusQueryTree(SCALAR):
    Scalar(0.123)
)");

    EXPECT_EQ(parseAndDumpTree("1.23e-3"), R"(
PrometheusQueryTree(SCALAR):
    Scalar(0.00123)
)");

    EXPECT_EQ(parseAndDumpTree("0x3d"), R"(
PrometheusQueryTree(SCALAR):
    Scalar(61)
)");

    EXPECT_EQ(parseAndDumpTree("Inf"), R"(
PrometheusQueryTree(SCALAR):
    Scalar(inf)
)");

    EXPECT_EQ(parseAndDumpTree("-Inf"), R"(
PrometheusQueryTree(SCALAR):
    UnaryOperator(-)
        Scalar(inf)
)");

    EXPECT_EQ(parseAndDumpTree("NaN"), R"(
PrometheusQueryTree(SCALAR):
    Scalar(nan)
)");

    /// Vector selectors.
    EXPECT_EQ(parseAndDumpTree("demo_memory_usage_bytes"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree(R"(
        {__name__="demo_memory_usage_bytes"}
        )"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree(R"(
        demo_memory_usage_bytes{type="free"}
        )"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'demo_memory_usage_bytes'
        type EQ 'free'
)");

    EXPECT_EQ(parseAndDumpTree(R"(
        demo_memory_usage_bytes{type!="free"}
        )"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'demo_memory_usage_bytes'
        type NE 'free'
)");

    EXPECT_EQ(parseAndDumpTree(R"(
        demo_memory_usage_bytes{instance=~"demo.promlabs.com:.*"}
        )"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'demo_memory_usage_bytes'
        instance RE 'demo.promlabs.com:.*'
)");

    EXPECT_EQ(parseAndDumpTree(R"(
        demo_memory_usage_bytes{instance=~"host"}
        )"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'demo_memory_usage_bytes'
        instance RE 'host'
)");

    EXPECT_EQ(parseAndDumpTree(R"(
        demo_memory_usage_bytes{instance!~".*:10000"}
        )"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'demo_memory_usage_bytes'
        instance NRE '.*:10000'
)");

    EXPECT_EQ(parseAndDumpTree(R"(
        demo_memory_usage_bytes{type="free", instance!="demo.promlabs.com:10000"}
        )"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'demo_memory_usage_bytes'
        type EQ 'free'
        instance NE 'demo.promlabs.com:10000'
)");

    EXPECT_EQ(parseAndDumpTree(R"(
        {type="free", instance!="demo.promlabs.com:10000"}
        )"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        type EQ 'free'
        instance NE 'demo.promlabs.com:10000'
)");

    EXPECT_EQ(parseAndDumpTree(R"(
        {__name__=~".*"}
        )"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ RE '.*'
)");

    /// Aggregation operators.
    EXPECT_EQ(parseAndDumpTree("sum(demo_memory_usage_bytes)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(sum)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("sum by() (demo_memory_usage_bytes)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(sum)
        by
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("sum by(instance) (demo_memory_usage_bytes)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(sum)
        by instance
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("sum by(instance, type) (demo_memory_usage_bytes)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(sum)
        by instance, type
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("sum without() (demo_memory_usage_bytes)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(sum)
        without
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("sum without(instance) (demo_memory_usage_bytes)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(sum)
        without instance
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("sum without(instance, type) (demo_memory_usage_bytes)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(sum)
        without instance, type
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("topk (3, demo_memory_usage_bytes)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(topk)
        Scalar(3)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("topk by(instance) (2, demo_memory_usage_bytes)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(topk)
        by instance
        Scalar(2)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("topk without(instance) (2, demo_memory_usage_bytes)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(topk)
        without instance
        Scalar(2)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("topk without() (2, demo_memory_usage_bytes)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(topk)
        without
        Scalar(2)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("quantile(0.5, demo_memory_usage_bytes)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(quantile)
        Scalar(0.5)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("avg(max by(type) (demo_memory_usage_bytes))"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(avg)
        AggregationOperator(max)
            by type
            InstantSelector:
                __name__ EQ 'demo_memory_usage_bytes'
)");

    /// Binary operators.
    EXPECT_EQ(parseAndDumpTree("1 * 2 + 4 / 6 - 10 % 2 ^ 2"), R"(
PrometheusQueryTree(SCALAR):
    BinaryOperator(-)
        BinaryOperator(+)
            BinaryOperator(*)
                Scalar(1)
                Scalar(2)
            BinaryOperator(/)
                Scalar(4)
                Scalar(6)
        BinaryOperator(%)
            Scalar(10)
            BinaryOperator(^)
                Scalar(2)
                Scalar(2)
)");

    EXPECT_EQ(parseAndDumpTree("demo_num_cpus + (1 == bool 2)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(+)
        InstantSelector:
            __name__ EQ 'demo_num_cpus'
        BinaryOperator(==)
            bool
            Scalar(1)
            Scalar(2)
)");

    EXPECT_EQ(parseAndDumpTree("demo_memory_usage_bytes + 1.2345"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(+)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
        Scalar(1.2345)
)");

    EXPECT_EQ(parseAndDumpTree("demo_memory_usage_bytes == bool 1.2345"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(==)
        bool
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
        Scalar(1.2345)
)");

    EXPECT_EQ(parseAndDumpTree("1.2345 == bool demo_memory_usage_bytes"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(==)
        bool
        Scalar(1.2345)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("0.12345 + demo_memory_usage_bytes"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(+)
        Scalar(0.12345)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("(1 * 2 + 4 / 6 - (10%7)^2) + demo_memory_usage_bytes"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(+)
        BinaryOperator(-)
            BinaryOperator(+)
                BinaryOperator(*)
                    Scalar(1)
                    Scalar(2)
                BinaryOperator(/)
                    Scalar(4)
                    Scalar(6)
            BinaryOperator(^)
                BinaryOperator(%)
                    Scalar(10)
                    Scalar(7)
                Scalar(2)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("demo_memory_usage_bytes + (1 * 2 + 4 / 6 - 10)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(+)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
        BinaryOperator(-)
            BinaryOperator(+)
                BinaryOperator(*)
                    Scalar(1)
                    Scalar(2)
                BinaryOperator(/)
                    Scalar(4)
                    Scalar(6)
            Scalar(10)
)");

    EXPECT_EQ(parseAndDumpTree("timestamp(demo_memory_usage_bytes * 1)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(timestamp):
        BinaryOperator(*)
            InstantSelector:
                __name__ EQ 'demo_memory_usage_bytes'
            Scalar(1)
)");

    EXPECT_EQ(parseAndDumpTree("timestamp(-demo_memory_usage_bytes)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(timestamp):
        UnaryOperator(-)
            InstantSelector:
                __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("demo_memory_usage_bytes + on(instance, job, type) demo_memory_usage_bytes"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(+)
        on instance, job, type
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("sum by(instance, type) (demo_memory_usage_bytes) + on(instance, type) group_left(job) demo_memory_usage_bytes"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(+)
        on instance, type
        group_left job
        AggregationOperator(sum)
            by instance, type
            InstantSelector:
                __name__ EQ 'demo_memory_usage_bytes'
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("demo_memory_usage_bytes == bool on(instance, job, type) demo_memory_usage_bytes"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(==)
        bool
        on instance, job, type
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("demo_memory_usage_bytes / on(instance, job, type, __name__) demo_memory_usage_bytes"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(/)
        on instance, job, type, __name__
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("sum without(job) (demo_memory_usage_bytes) / on(instance, type) demo_memory_usage_bytes"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(/)
        on instance, type
        AggregationOperator(sum)
            without job
            InstantSelector:
                __name__ EQ 'demo_memory_usage_bytes'
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("sum without(job) (demo_memory_usage_bytes) / on(instance, type) group_left demo_memory_usage_bytes"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(/)
        on instance, type
        group_left
        AggregationOperator(sum)
            without job
            InstantSelector:
                __name__ EQ 'demo_memory_usage_bytes'
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("sum without(job) (demo_memory_usage_bytes) / on(instance, type) group_left(job) demo_memory_usage_bytes"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(/)
        on instance, type
        group_left job
        AggregationOperator(sum)
            without job
            InstantSelector:
                __name__ EQ 'demo_memory_usage_bytes'
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("demo_memory_usage_bytes / on(instance, job) group_left demo_num_cpus"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(/)
        on instance, job
        group_left
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
        InstantSelector:
            __name__ EQ 'demo_num_cpus'
)");

    EXPECT_EQ(parseAndDumpTree("demo_memory_usage_bytes / on(instance, type, job, non_existent) demo_memory_usage_bytes"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(/)
        on instance, type, job, non_existent
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    /// NaN/Inf/-Inf support.
    EXPECT_EQ(parseAndDumpTree("demo_num_cpus * Inf"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(*)
        InstantSelector:
            __name__ EQ 'demo_num_cpus'
        Scalar(inf)
)");

    EXPECT_EQ(parseAndDumpTree("demo_num_cpus * -Inf"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(*)
        InstantSelector:
            __name__ EQ 'demo_num_cpus'
        UnaryOperator(-)
            Scalar(inf)
)");

    EXPECT_EQ(parseAndDumpTree("demo_num_cpus * NaN"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(*)
        InstantSelector:
            __name__ EQ 'demo_num_cpus'
        Scalar(nan)
)");

    /// Unary expressions.
    EXPECT_EQ(parseAndDumpTree("demo_memory_usage_bytes + -(1)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(+)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
        UnaryOperator(-)
            Scalar(1)
)");

    EXPECT_EQ(parseAndDumpTree("-demo_memory_usage_bytes"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    UnaryOperator(-)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    /// Check precedence.
    EXPECT_EQ(parseAndDumpTree("-1 ^ 2"), R"(
PrometheusQueryTree(SCALAR):
    UnaryOperator(-)
        BinaryOperator(^)
            Scalar(1)
            Scalar(2)
)");

    /// Binops involving non-const scalars.
    EXPECT_EQ(parseAndDumpTree("1 + time()"), R"(
PrometheusQueryTree(SCALAR):
    BinaryOperator(+)
        Scalar(1)
        Function(time)
)");

    EXPECT_EQ(parseAndDumpTree("time() + 1"), R"(
PrometheusQueryTree(SCALAR):
    BinaryOperator(+)
        Function(time)
        Scalar(1)
)");

    EXPECT_EQ(parseAndDumpTree("time() == bool 1"), R"(
PrometheusQueryTree(SCALAR):
    BinaryOperator(==)
        bool
        Function(time)
        Scalar(1)
)");

    EXPECT_EQ(parseAndDumpTree("1 == bool time()"), R"(
PrometheusQueryTree(SCALAR):
    BinaryOperator(==)
        bool
        Scalar(1)
        Function(time)
)");

    EXPECT_EQ(parseAndDumpTree("time() + demo_memory_usage_bytes"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(+)
        Function(time)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("demo_memory_usage_bytes + time()"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(+)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
        Function(time)
)");

    /// Functions.
    EXPECT_EQ(parseAndDumpTree("avg_over_time(demo_memory_usage_bytes[20m])"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(avg_over_time):
        RangeSelector:
            range: 1200
            InstantSelector:
                __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("quantile_over_time(0.5, demo_memory_usage_bytes[20m])"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(quantile_over_time):
        Scalar(0.5)
        RangeSelector:
            range: 1200
            InstantSelector:
                __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("timestamp(demo_num_cpus)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(timestamp):
        InstantSelector:
            __name__ EQ 'demo_num_cpus'
)");

    EXPECT_EQ(parseAndDumpTree("timestamp(timestamp(demo_num_cpus))"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(timestamp):
        Function(timestamp):
            InstantSelector:
                __name__ EQ 'demo_num_cpus'
)");

    EXPECT_EQ(parseAndDumpTree("abs(demo_memory_usage_bytes)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(abs):
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("abs(-demo_memory_usage_bytes)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(abs):
        UnaryOperator(-)
            InstantSelector:
                __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("rate(demo_cpu_usage_seconds_total[20m])"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(rate):
        RangeSelector:
            range: 1200
            InstantSelector:
                __name__ EQ 'demo_cpu_usage_seconds_total'
)");

    EXPECT_EQ(parseAndDumpTree("deriv(demo_disk_usage_bytes[20m])"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(deriv):
        RangeSelector:
            range: 1200
            InstantSelector:
                __name__ EQ 'demo_disk_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree("predict_linear(demo_disk_usage_bytes[20m], 600)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(predict_linear):
        RangeSelector:
            range: 1200
            InstantSelector:
                __name__ EQ 'demo_disk_usage_bytes'
        Scalar(600)
)");

    EXPECT_EQ(parseAndDumpTree("time()"), R"(
PrometheusQueryTree(SCALAR):
    Function(time)
)");

    EXPECT_EQ(parseAndDumpTree(R"s(
        label_replace(demo_num_cpus, "job", "destination-value-$1", "instance", "demo.promlabs.com:(.*)")
    )s"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(label_replace):
        InstantSelector:
            __name__ EQ 'demo_num_cpus'
        StringLiteral('job')
        StringLiteral('destination-value-$1')
        StringLiteral('instance')
        StringLiteral('demo.promlabs.com:(.*)')
)");

    EXPECT_EQ(parseAndDumpTree(R"s(
        label_join(demo_num_cpus, "new_label", "-", "instance", "job")
    )s"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(label_join):
        InstantSelector:
            __name__ EQ 'demo_num_cpus'
        StringLiteral('new_label')
        StringLiteral('-')
        StringLiteral('instance')
        StringLiteral('job')
)");

    EXPECT_EQ(parseAndDumpTree("day_of_week(demo_batch_last_success_timestamp_seconds)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(day_of_week):
        InstantSelector:
            __name__ EQ 'demo_batch_last_success_timestamp_seconds'
)");

    EXPECT_EQ(parseAndDumpTree("irate(demo_cpu_usage_seconds_total[20m])"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(irate):
        RangeSelector:
            range: 1200
            InstantSelector:
                __name__ EQ 'demo_cpu_usage_seconds_total'
)");

    EXPECT_EQ(parseAndDumpTree("clamp_max(demo_memory_usage_bytes, 2)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(clamp_max):
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
        Scalar(2)
)");

    EXPECT_EQ(parseAndDumpTree("clamp(demo_memory_usage_bytes, 0, 1)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(clamp):
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
        Scalar(0)
        Scalar(1)
)");

    EXPECT_EQ(parseAndDumpTree("clamp(demo_memory_usage_bytes, 0, 1000000000000)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(clamp):
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
        Scalar(0)
        Scalar(1000000000000)
)");

    EXPECT_EQ(parseAndDumpTree("resets(demo_cpu_usage_seconds_total[20m])"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(resets):
        RangeSelector:
            range: 1200
            InstantSelector:
                __name__ EQ 'demo_cpu_usage_seconds_total'
)");

    EXPECT_EQ(parseAndDumpTree("changes(demo_batch_last_success_timestamp_seconds[20m])"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(changes):
        RangeSelector:
            range: 1200
            InstantSelector:
                __name__ EQ 'demo_batch_last_success_timestamp_seconds'
)");

    EXPECT_EQ(parseAndDumpTree("vector(1.23)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(vector):
        Scalar(1.23)
)");

    EXPECT_EQ(parseAndDumpTree("vector(time())"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(vector):
        Function(time)
)");

    EXPECT_EQ(parseAndDumpTree("histogram_quantile(0.5, rate(demo_api_request_duration_seconds_bucket[1m]))"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(histogram_quantile):
        Scalar(0.5)
        Function(rate):
            RangeSelector:
                range: 60
                InstantSelector:
                    __name__ EQ 'demo_api_request_duration_seconds_bucket'
)");

    EXPECT_EQ(parseAndDumpTree("histogram_quantile(0.9, demo_memory_usage_bytes)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(histogram_quantile):
        Scalar(0.9)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parseAndDumpTree(R"(
        histogram_quantile(0.9, {__name__=~"demo_api_request_duration_seconds_.+"})
    )"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(histogram_quantile):
        Scalar(0.9)
        InstantSelector:
            __name__ RE 'demo_api_request_duration_seconds_.+'
)");

    EXPECT_EQ(parseAndDumpTree(R"(
        count_values("value", demo_api_request_duration_seconds_bucket)
    )"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(count_values)
        StringLiteral('value')
        InstantSelector:
            __name__ EQ 'demo_api_request_duration_seconds_bucket'
)");

    EXPECT_EQ(parseAndDumpTree("absent(demo_memory_usage_bytes)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(absent):
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    /// Subqueries.
    EXPECT_EQ(parseAndDumpTree("max_over_time((time() - max(demo_batch_last_success_timestamp_seconds) < 1000)[5m:10s] offset 5m)"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(max_over_time):
        Offset:
            offset: 300
            Subquery:
                range: 300
                step: 10
                BinaryOperator(<)
                    BinaryOperator(-)
                        Function(time)
                        AggregationOperator(max)
                            InstantSelector:
                                __name__ EQ 'demo_batch_last_success_timestamp_seconds'
                    Scalar(1000)
)");

    EXPECT_EQ(parseAndDumpTree("avg_over_time(rate(demo_cpu_usage_seconds_total[1m])[2m:10s])"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Function(avg_over_time):
        Subquery:
            range: 120
            step: 10
            Function(rate):
                RangeSelector:
                    range: 60
                    InstantSelector:
                        __name__ EQ 'demo_cpu_usage_seconds_total'
)");

}


TEST(PrometheusQueryTree, ParseOtherQueries)
{
    EXPECT_EQ(parseAndDumpTree("0.74"), R"(
PrometheusQueryTree(SCALAR):
    Scalar(0.74)
)");

    EXPECT_EQ(parseAndDumpTree("2e-5"), R"(
PrometheusQueryTree(SCALAR):
    Scalar(0.00002)
)");

    EXPECT_EQ(parseAndDumpTree("1.5E4"), R"(
PrometheusQueryTree(SCALAR):
    Scalar(15000)
)");

    EXPECT_EQ(parseAndDumpTree("0xABcd"), R"(
PrometheusQueryTree(SCALAR):
    Scalar(43981)
)");

    EXPECT_EQ(parseAndDumpTree("3h20m10s5ms"), R"(
PrometheusQueryTree(SCALAR):
    Scalar(12010.005)
)");

    EXPECT_EQ(parseAndDumpTree("-1"), R"(
PrometheusQueryTree(SCALAR):
    UnaryOperator(-)
        Scalar(1)
)");

    EXPECT_EQ(parseAndDumpTree("Inf+inf+iNf"), R"(
PrometheusQueryTree(SCALAR):
    BinaryOperator(+)
        BinaryOperator(+)
            Scalar(inf)
            Scalar(inf)
        Scalar(inf)
)");

    EXPECT_EQ(parseAndDumpTree("NaN+nan+nAn"), R"(
PrometheusQueryTree(SCALAR):
    BinaryOperator(+)
        BinaryOperator(+)
            Scalar(nan)
            Scalar(nan)
        Scalar(nan)
)");

    EXPECT_EQ(parseAndDumpTree("0x_1_2_3 * 0X_A_B"), R"(
PrometheusQueryTree(SCALAR):
    BinaryOperator(*)
        Scalar(291)
        Scalar(171)
)");

    EXPECT_EQ(parseAndDumpTree(R"(
        http_requests_total{job="prometheus",group="canary"}
    )"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'http_requests_total'
        job EQ 'prometheus'
        group EQ 'canary'
)");

    EXPECT_EQ(parseAndDumpTree(R"(
        http_requests_total{environment=~"staging|testing|development",method!="GET"}
    )"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'http_requests_total'
        environment RE 'staging|testing|development'
        method NE 'GET'
)");

    EXPECT_EQ(parseAndDumpTree(R"(
        http_requests_total{job="prometheus"}[5m]
    )"), R"(
PrometheusQueryTree(RANGE_VECTOR):
    RangeSelector:
        range: 300
        InstantSelector:
            __name__ EQ 'http_requests_total'
            job EQ 'prometheus'
)");

    EXPECT_EQ(parseAndDumpTree("http_requests_total offset 5m @ 1609746000"), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    Offset:
        at: 1609746000
        offset: 300
        InstantSelector:
            __name__ EQ 'http_requests_total'
)");

    EXPECT_EQ(parseAndDumpTree("http_requests_total[5m:1m] offset -10s"), R"(
PrometheusQueryTree(RANGE_VECTOR):
    Offset:
        offset: -10
        Subquery:
            range: 300
            step: 60
            InstantSelector:
                __name__ EQ 'http_requests_total'
)");

}
