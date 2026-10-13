#include <gtest/gtest.h>

#include <Parsers/Prometheus/PrometheusQueryTree.h>

using namespace DB;

namespace
{
    String parse(std::string_view input)
    {
        PrometheusQueryTree query_tree{input};
        return "\n" + query_tree.toString() + "\n" + query_tree.dumpTree();
    }
}


/// Parse queries from https://github.com/prometheus/compliance/blob/main/promql/promql-test-queries.yml
TEST(PromQLParser, ComplianceQueries)
{
    /// Scalar literals.
    EXPECT_EQ(parse("42"), R"(
42

PrometheusQueryTree(SCALAR):
    Scalar(42)
)");

    EXPECT_EQ(parse("1.234"), R"(
1.234

PrometheusQueryTree(SCALAR):
    Scalar(1.234)
)");

    EXPECT_EQ(parse(".123"), R"(
0.123

PrometheusQueryTree(SCALAR):
    Scalar(0.123)
)");

    EXPECT_EQ(parse("1.23e-3"), R"(
0.00123

PrometheusQueryTree(SCALAR):
    Scalar(0.00123)
)");

    EXPECT_EQ(parse("0x3d"), R"(
61

PrometheusQueryTree(SCALAR):
    Scalar(61)
)");

    EXPECT_EQ(parse("Inf"), R"(
Inf

PrometheusQueryTree(SCALAR):
    Scalar(inf)
)");

    EXPECT_EQ(parse("-Inf"), R"(
-Inf

PrometheusQueryTree(SCALAR):
    UnaryOperator(-)
        Scalar(inf)
)");

    EXPECT_EQ(parse("NaN"), R"(
NaN

PrometheusQueryTree(SCALAR):
    Scalar(nan)
)");

    /// Vector selectors.
    EXPECT_EQ(parse("demo_memory_usage_bytes"), R"(
demo_memory_usage_bytes

PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parse(R"(
        {__name__="demo_memory_usage_bytes"}
        )"), R"(
demo_memory_usage_bytes

PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parse(R"(
        demo_memory_usage_bytes{type="free"}
        )"), R"(
demo_memory_usage_bytes{type="free"}

PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'demo_memory_usage_bytes'
        type EQ 'free'
)");

    EXPECT_EQ(parse(R"(
        demo_memory_usage_bytes{type!="free"}
        )"), R"(
demo_memory_usage_bytes{type!="free"}

PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'demo_memory_usage_bytes'
        type NE 'free'
)");

    EXPECT_EQ(parse(R"(
        demo_memory_usage_bytes{instance=~"demo.promlabs.com:.*"}
        )"), R"(
demo_memory_usage_bytes{instance=~"demo.promlabs.com:.*"}

PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'demo_memory_usage_bytes'
        instance RE 'demo.promlabs.com:.*'
)");

    EXPECT_EQ(parse(R"(
        demo_memory_usage_bytes{instance=~"host"}
        )"), R"(
demo_memory_usage_bytes{instance=~"host"}

PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'demo_memory_usage_bytes'
        instance RE 'host'
)");

    EXPECT_EQ(parse(R"(
        demo_memory_usage_bytes{instance!~".*:10000"}
        )"), R"(
demo_memory_usage_bytes{instance!~".*:10000"}

PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'demo_memory_usage_bytes'
        instance NRE '.*:10000'
)");

    EXPECT_EQ(parse(R"(
        demo_memory_usage_bytes{type="free", instance!="demo.promlabs.com:10000"}
        )"), R"(
demo_memory_usage_bytes{type="free",instance!="demo.promlabs.com:10000"}

PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'demo_memory_usage_bytes'
        type EQ 'free'
        instance NE 'demo.promlabs.com:10000'
)");

    EXPECT_EQ(parse(R"(
        {type="free", instance!="demo.promlabs.com:10000"}
        )"), R"(
{type="free",instance!="demo.promlabs.com:10000"}

PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        type EQ 'free'
        instance NE 'demo.promlabs.com:10000'
)");

    EXPECT_EQ(parse(R"(
        {__name__=~".*"}
        )"), R"(
{__name__=~".*"}

PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ RE '.*'
)");

    /// Aggregation operators.
    EXPECT_EQ(parse("sum(demo_memory_usage_bytes)"), R"(
sum(demo_memory_usage_bytes)

PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(sum)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parse("sum by() (demo_memory_usage_bytes)"), R"(
sum by () (demo_memory_usage_bytes)

PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(sum)
        by
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parse("sum by(instance) (demo_memory_usage_bytes)"), R"(
sum by (instance) (demo_memory_usage_bytes)

PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(sum)
        by instance
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parse("sum by(instance, type) (demo_memory_usage_bytes)"), R"(
sum by (instance, type) (demo_memory_usage_bytes)

PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(sum)
        by instance, type
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parse("sum without() (demo_memory_usage_bytes)"), R"(
sum without () (demo_memory_usage_bytes)

PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(sum)
        without
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parse("sum without(instance) (demo_memory_usage_bytes)"), R"(
sum without (instance) (demo_memory_usage_bytes)

PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(sum)
        without instance
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parse("sum without(instance, type) (demo_memory_usage_bytes)"), R"(
sum without (instance, type) (demo_memory_usage_bytes)

PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(sum)
        without instance, type
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parse("topk (3, demo_memory_usage_bytes)"), R"(
topk(3, demo_memory_usage_bytes)

PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(topk)
        Scalar(3)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parse("topk by(instance) (2, demo_memory_usage_bytes)"), R"(
topk by (instance) (2, demo_memory_usage_bytes)

PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(topk)
        by instance
        Scalar(2)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parse("topk without(instance) (2, demo_memory_usage_bytes)"), R"(
topk without (instance) (2, demo_memory_usage_bytes)

PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(topk)
        without instance
        Scalar(2)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parse("topk without() (2, demo_memory_usage_bytes)"), R"(
topk without () (2, demo_memory_usage_bytes)

PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(topk)
        without
        Scalar(2)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parse("quantile(0.5, demo_memory_usage_bytes)"), R"(
quantile(0.5, demo_memory_usage_bytes)

PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(quantile)
        Scalar(0.5)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parse("avg(max by(type) (demo_memory_usage_bytes))"), R"(
avg(max by (type) (demo_memory_usage_bytes))

PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(avg)
        AggregationOperator(max)
            by type
            InstantSelector:
                __name__ EQ 'demo_memory_usage_bytes'
)");

    /// Binary operators.
    EXPECT_EQ(parse("1 * 2 + 4 / 6 - 10 % 2 ^ 2"), R"(
1 * 2 + 4 / 6 - 10 % 2 ^ 2

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

    EXPECT_EQ(parse("demo_num_cpus + (1 == bool 2)"), R"(
demo_num_cpus + (1 == bool 2)

PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(+)
        InstantSelector:
            __name__ EQ 'demo_num_cpus'
        BinaryOperator(==)
            bool
            Scalar(1)
            Scalar(2)
)");

    EXPECT_EQ(parse("demo_memory_usage_bytes + 1.2345"), R"(
demo_memory_usage_bytes + 1.2345

PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(+)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
        Scalar(1.2345)
)");

    EXPECT_EQ(parse("demo_memory_usage_bytes == bool 1.2345"), R"(
demo_memory_usage_bytes == bool 1.2345

PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(==)
        bool
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
        Scalar(1.2345)
)");

    EXPECT_EQ(parse("1.2345 == bool demo_memory_usage_bytes"), R"(
1.2345 == bool demo_memory_usage_bytes

PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(==)
        bool
        Scalar(1.2345)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parse("0.12345 + demo_memory_usage_bytes"), R"(
0.12345 + demo_memory_usage_bytes

PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(+)
        Scalar(0.12345)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parse("(1 * 2 + 4 / 6 - (10%7)^2) + demo_memory_usage_bytes"), R"(
1 * 2 + 4 / 6 - (10 % 7) ^ 2 + demo_memory_usage_bytes

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

    EXPECT_EQ(parse("demo_memory_usage_bytes + (1 * 2 + 4 / 6 - 10)"), R"(
demo_memory_usage_bytes + (1 * 2 + 4 / 6 - 10)

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

    EXPECT_EQ(parse("timestamp(demo_memory_usage_bytes * 1)"), R"(
timestamp(demo_memory_usage_bytes * 1)

PrometheusQueryTree(INSTANT_VECTOR):
    Function(timestamp):
        BinaryOperator(*)
            InstantSelector:
                __name__ EQ 'demo_memory_usage_bytes'
            Scalar(1)
)");

    EXPECT_EQ(parse("timestamp(-demo_memory_usage_bytes)"), R"(
timestamp(-demo_memory_usage_bytes)

PrometheusQueryTree(INSTANT_VECTOR):
    Function(timestamp):
        UnaryOperator(-)
            InstantSelector:
                __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parse("demo_memory_usage_bytes + on(instance, job, type) demo_memory_usage_bytes"), R"(
demo_memory_usage_bytes + on(instance, job, type) demo_memory_usage_bytes

PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(+)
        on instance, job, type
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parse("sum by(instance, type) (demo_memory_usage_bytes) + on(instance, type) group_left(job) demo_memory_usage_bytes"), R"(
sum by (instance, type) (demo_memory_usage_bytes) + on(instance, type) group_left(job) demo_memory_usage_bytes

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

    EXPECT_EQ(parse("demo_memory_usage_bytes == bool on(instance, job, type) demo_memory_usage_bytes"), R"(
demo_memory_usage_bytes == bool on(instance, job, type) demo_memory_usage_bytes

PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(==)
        bool
        on instance, job, type
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parse("demo_memory_usage_bytes / on(instance, job, type, __name__) demo_memory_usage_bytes"), R"(
demo_memory_usage_bytes / on(instance, job, type, __name__) demo_memory_usage_bytes

PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(/)
        on instance, job, type, __name__
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parse("sum without(job) (demo_memory_usage_bytes) / on(instance, type) demo_memory_usage_bytes"), R"(
sum without (job) (demo_memory_usage_bytes) / on(instance, type) demo_memory_usage_bytes

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

    EXPECT_EQ(parse("sum without(job) (demo_memory_usage_bytes) / on(instance, type) group_left demo_memory_usage_bytes"), R"(
sum without (job) (demo_memory_usage_bytes) / on(instance, type) group_left demo_memory_usage_bytes

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

    EXPECT_EQ(parse("sum without(job) (demo_memory_usage_bytes) / on(instance, type) group_left(job) demo_memory_usage_bytes"), R"(
sum without (job) (demo_memory_usage_bytes) / on(instance, type) group_left(job) demo_memory_usage_bytes

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

    EXPECT_EQ(parse("demo_memory_usage_bytes / on(instance, job) group_left demo_num_cpus"), R"(
demo_memory_usage_bytes / on(instance, job) group_left demo_num_cpus

PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(/)
        on instance, job
        group_left
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
        InstantSelector:
            __name__ EQ 'demo_num_cpus'
)");

    EXPECT_EQ(parse("demo_memory_usage_bytes / on(instance, type, job, non_existent) demo_memory_usage_bytes"), R"(
demo_memory_usage_bytes / on(instance, type, job, non_existent) demo_memory_usage_bytes

PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(/)
        on instance, type, job, non_existent
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    /// NaN/Inf/-Inf support.
    EXPECT_EQ(parse("demo_num_cpus * Inf"), R"(
demo_num_cpus * Inf

PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(*)
        InstantSelector:
            __name__ EQ 'demo_num_cpus'
        Scalar(inf)
)");

    EXPECT_EQ(parse("demo_num_cpus * -Inf"), R"(
demo_num_cpus * (-Inf)

PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(*)
        InstantSelector:
            __name__ EQ 'demo_num_cpus'
        UnaryOperator(-)
            Scalar(inf)
)");

    EXPECT_EQ(parse("demo_num_cpus * NaN"), R"(
demo_num_cpus * NaN

PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(*)
        InstantSelector:
            __name__ EQ 'demo_num_cpus'
        Scalar(nan)
)");

    /// Unary expressions.
    EXPECT_EQ(parse("demo_memory_usage_bytes + -(1)"), R"(
demo_memory_usage_bytes + -1

PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(+)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
        UnaryOperator(-)
            Scalar(1)
)");

    EXPECT_EQ(parse("-demo_memory_usage_bytes"), R"(
-demo_memory_usage_bytes

PrometheusQueryTree(INSTANT_VECTOR):
    UnaryOperator(-)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    /// Check precedence.
    EXPECT_EQ(parse("-1 ^ 2"), R"(
-1 ^ 2

PrometheusQueryTree(SCALAR):
    UnaryOperator(-)
        BinaryOperator(^)
            Scalar(1)
            Scalar(2)
)");

    /// Binops involving non-const scalars.
    EXPECT_EQ(parse("1 + time()"), R"(
1 + time()

PrometheusQueryTree(SCALAR):
    BinaryOperator(+)
        Scalar(1)
        Function(time)
)");

    EXPECT_EQ(parse("time() + 1"), R"(
time() + 1

PrometheusQueryTree(SCALAR):
    BinaryOperator(+)
        Function(time)
        Scalar(1)
)");

    EXPECT_EQ(parse("time() == bool 1"), R"(
time() == bool 1

PrometheusQueryTree(SCALAR):
    BinaryOperator(==)
        bool
        Function(time)
        Scalar(1)
)");

    EXPECT_EQ(parse("1 == bool time()"), R"(
1 == bool time()

PrometheusQueryTree(SCALAR):
    BinaryOperator(==)
        bool
        Scalar(1)
        Function(time)
)");

    EXPECT_EQ(parse("time() + demo_memory_usage_bytes"), R"(
time() + demo_memory_usage_bytes

PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(+)
        Function(time)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parse("demo_memory_usage_bytes + time()"), R"(
demo_memory_usage_bytes + time()

PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(+)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
        Function(time)
)");

    /// Functions.
    EXPECT_EQ(parse("avg_over_time(demo_memory_usage_bytes[20m])"), R"(
avg_over_time(demo_memory_usage_bytes[1200])

PrometheusQueryTree(INSTANT_VECTOR):
    Function(avg_over_time):
        RangeSelector:
            range: 1200
            InstantSelector:
                __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parse("quantile_over_time(0.5, demo_memory_usage_bytes[20m])"), R"(
quantile_over_time(0.5, demo_memory_usage_bytes[1200])

PrometheusQueryTree(INSTANT_VECTOR):
    Function(quantile_over_time):
        Scalar(0.5)
        RangeSelector:
            range: 1200
            InstantSelector:
                __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parse("timestamp(demo_num_cpus)"), R"(
timestamp(demo_num_cpus)

PrometheusQueryTree(INSTANT_VECTOR):
    Function(timestamp):
        InstantSelector:
            __name__ EQ 'demo_num_cpus'
)");

    EXPECT_EQ(parse("timestamp(timestamp(demo_num_cpus))"), R"(
timestamp(timestamp(demo_num_cpus))

PrometheusQueryTree(INSTANT_VECTOR):
    Function(timestamp):
        Function(timestamp):
            InstantSelector:
                __name__ EQ 'demo_num_cpus'
)");

    EXPECT_EQ(parse("abs(demo_memory_usage_bytes)"), R"(
abs(demo_memory_usage_bytes)

PrometheusQueryTree(INSTANT_VECTOR):
    Function(abs):
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parse("abs(-demo_memory_usage_bytes)"), R"(
abs(-demo_memory_usage_bytes)

PrometheusQueryTree(INSTANT_VECTOR):
    Function(abs):
        UnaryOperator(-)
            InstantSelector:
                __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parse("rate(demo_cpu_usage_seconds_total[20m])"), R"(
rate(demo_cpu_usage_seconds_total[1200])

PrometheusQueryTree(INSTANT_VECTOR):
    Function(rate):
        RangeSelector:
            range: 1200
            InstantSelector:
                __name__ EQ 'demo_cpu_usage_seconds_total'
)");

    EXPECT_EQ(parse("deriv(demo_disk_usage_bytes[20m])"), R"(
deriv(demo_disk_usage_bytes[1200])

PrometheusQueryTree(INSTANT_VECTOR):
    Function(deriv):
        RangeSelector:
            range: 1200
            InstantSelector:
                __name__ EQ 'demo_disk_usage_bytes'
)");

    EXPECT_EQ(parse("predict_linear(demo_disk_usage_bytes[20m], 600)"), R"(
predict_linear(demo_disk_usage_bytes[1200], 600)

PrometheusQueryTree(INSTANT_VECTOR):
    Function(predict_linear):
        RangeSelector:
            range: 1200
            InstantSelector:
                __name__ EQ 'demo_disk_usage_bytes'
        Scalar(600)
)");

    EXPECT_EQ(parse("time()"), R"(
time()

PrometheusQueryTree(SCALAR):
    Function(time)
)");

    EXPECT_EQ(parse(R"s(
        label_replace(demo_num_cpus, "job", "destination-value-$1", "instance", "demo.promlabs.com:(.*)")
    )s"), R"s(
label_replace(demo_num_cpus, "job", "destination-value-$1", "instance", "demo.promlabs.com:(.*)")

PrometheusQueryTree(INSTANT_VECTOR):
    Function(label_replace):
        InstantSelector:
            __name__ EQ 'demo_num_cpus'
        StringLiteral('job')
        StringLiteral('destination-value-$1')
        StringLiteral('instance')
        StringLiteral('demo.promlabs.com:(.*)')
)s");

    EXPECT_EQ(parse(R"s(
        label_join(demo_num_cpus, "new_label", "-", "instance", "job")
    )s"), R"(
label_join(demo_num_cpus, "new_label", "-", "instance", "job")

PrometheusQueryTree(INSTANT_VECTOR):
    Function(label_join):
        InstantSelector:
            __name__ EQ 'demo_num_cpus'
        StringLiteral('new_label')
        StringLiteral('-')
        StringLiteral('instance')
        StringLiteral('job')
)");

    EXPECT_EQ(parse("day_of_week(demo_batch_last_success_timestamp_seconds)"), R"(
day_of_week(demo_batch_last_success_timestamp_seconds)

PrometheusQueryTree(INSTANT_VECTOR):
    Function(day_of_week):
        InstantSelector:
            __name__ EQ 'demo_batch_last_success_timestamp_seconds'
)");

    EXPECT_EQ(parse("irate(demo_cpu_usage_seconds_total[20m])"), R"(
irate(demo_cpu_usage_seconds_total[1200])

PrometheusQueryTree(INSTANT_VECTOR):
    Function(irate):
        RangeSelector:
            range: 1200
            InstantSelector:
                __name__ EQ 'demo_cpu_usage_seconds_total'
)");

    EXPECT_EQ(parse("clamp_max(demo_memory_usage_bytes, 2)"), R"(
clamp_max(demo_memory_usage_bytes, 2)

PrometheusQueryTree(INSTANT_VECTOR):
    Function(clamp_max):
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
        Scalar(2)
)");

    EXPECT_EQ(parse("clamp(demo_memory_usage_bytes, 0, 1)"), R"(
clamp(demo_memory_usage_bytes, 0, 1)

PrometheusQueryTree(INSTANT_VECTOR):
    Function(clamp):
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
        Scalar(0)
        Scalar(1)
)");

    EXPECT_EQ(parse("clamp(demo_memory_usage_bytes, 0, 1000000000000)"), R"(
clamp(demo_memory_usage_bytes, 0, 1000000000000)

PrometheusQueryTree(INSTANT_VECTOR):
    Function(clamp):
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
        Scalar(0)
        Scalar(1000000000000)
)");

    EXPECT_EQ(parse("resets(demo_cpu_usage_seconds_total[20m])"), R"(
resets(demo_cpu_usage_seconds_total[1200])

PrometheusQueryTree(INSTANT_VECTOR):
    Function(resets):
        RangeSelector:
            range: 1200
            InstantSelector:
                __name__ EQ 'demo_cpu_usage_seconds_total'
)");

    EXPECT_EQ(parse("changes(demo_batch_last_success_timestamp_seconds[20m])"), R"(
changes(demo_batch_last_success_timestamp_seconds[1200])

PrometheusQueryTree(INSTANT_VECTOR):
    Function(changes):
        RangeSelector:
            range: 1200
            InstantSelector:
                __name__ EQ 'demo_batch_last_success_timestamp_seconds'
)");

    EXPECT_EQ(parse("vector(1.23)"), R"(
vector(1.23)

PrometheusQueryTree(INSTANT_VECTOR):
    Function(vector):
        Scalar(1.23)
)");

    EXPECT_EQ(parse("vector(time())"), R"(
vector(time())

PrometheusQueryTree(INSTANT_VECTOR):
    Function(vector):
        Function(time)
)");

    EXPECT_EQ(parse("histogram_quantile(0.5, rate(demo_api_request_duration_seconds_bucket[1m]))"), R"(
histogram_quantile(0.5, rate(demo_api_request_duration_seconds_bucket[60]))

PrometheusQueryTree(INSTANT_VECTOR):
    Function(histogram_quantile):
        Scalar(0.5)
        Function(rate):
            RangeSelector:
                range: 60
                InstantSelector:
                    __name__ EQ 'demo_api_request_duration_seconds_bucket'
)");

    EXPECT_EQ(parse("histogram_quantile(0.9, demo_memory_usage_bytes)"), R"(
histogram_quantile(0.9, demo_memory_usage_bytes)

PrometheusQueryTree(INSTANT_VECTOR):
    Function(histogram_quantile):
        Scalar(0.9)
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    EXPECT_EQ(parse(R"(
        histogram_quantile(0.9, {__name__=~"demo_api_request_duration_seconds_.+"})
    )"), R"(
histogram_quantile(0.9, {__name__=~"demo_api_request_duration_seconds_.+"})

PrometheusQueryTree(INSTANT_VECTOR):
    Function(histogram_quantile):
        Scalar(0.9)
        InstantSelector:
            __name__ RE 'demo_api_request_duration_seconds_.+'
)");

    EXPECT_EQ(parse(R"(
        count_values("value", demo_api_request_duration_seconds_bucket)
    )"), R"(
count_values("value", demo_api_request_duration_seconds_bucket)

PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(count_values)
        StringLiteral('value')
        InstantSelector:
            __name__ EQ 'demo_api_request_duration_seconds_bucket'
)");

    EXPECT_EQ(parse("absent(demo_memory_usage_bytes)"), R"(
absent(demo_memory_usage_bytes)

PrometheusQueryTree(INSTANT_VECTOR):
    Function(absent):
        InstantSelector:
            __name__ EQ 'demo_memory_usage_bytes'
)");

    /// Subqueries.
    EXPECT_EQ(parse("max_over_time((time() - max(demo_batch_last_success_timestamp_seconds) < 1000)[5m:10s] offset 5m)"), R"(
max_over_time((time() - max(demo_batch_last_success_timestamp_seconds) < 1000)[300:10] offset 300)

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

    EXPECT_EQ(parse("avg_over_time(rate(demo_cpu_usage_seconds_total[1m])[2m:10s])"), R"(
avg_over_time(rate(demo_cpu_usage_seconds_total[60])[120:10])

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


TEST(PromQLParser, OtherQueries)
{
    EXPECT_EQ(parse("0.74"), R"(
0.74

PrometheusQueryTree(SCALAR):
    Scalar(0.74)
)");

    EXPECT_EQ(parse("2e-5"), R"(
0.00002

PrometheusQueryTree(SCALAR):
    Scalar(0.00002)
)");

    EXPECT_EQ(parse("1.5E4"), R"(
15000

PrometheusQueryTree(SCALAR):
    Scalar(15000)
)");

    EXPECT_EQ(parse("0xABcd"), R"(
43981

PrometheusQueryTree(SCALAR):
    Scalar(43981)
)");

    EXPECT_EQ(parse("3h20m10s5ms"), R"(
12010.005

PrometheusQueryTree(SCALAR):
    Scalar(12010.005)
)");

    EXPECT_EQ(parse("-1"), R"(
-1

PrometheusQueryTree(SCALAR):
    UnaryOperator(-)
        Scalar(1)
)");

    EXPECT_EQ(parse("Inf+inf+iNf"), R"(
Inf + Inf + Inf

PrometheusQueryTree(SCALAR):
    BinaryOperator(+)
        BinaryOperator(+)
            Scalar(inf)
            Scalar(inf)
        Scalar(inf)
)");

    EXPECT_EQ(parse("NaN+nan+nAn"), R"(
NaN + NaN + NaN

PrometheusQueryTree(SCALAR):
    BinaryOperator(+)
        BinaryOperator(+)
            Scalar(nan)
            Scalar(nan)
        Scalar(nan)
)");

    EXPECT_EQ(parse("0x_1_2_3 * 0X_A_B"), R"(
291 * 171

PrometheusQueryTree(SCALAR):
    BinaryOperator(*)
        Scalar(291)
        Scalar(171)
)");

    EXPECT_EQ(parse(R"(
        http_requests_total{job="prometheus",group="canary"}
    )"), R"(
http_requests_total{job="prometheus",group="canary"}

PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'http_requests_total'
        job EQ 'prometheus'
        group EQ 'canary'
)");

    EXPECT_EQ(parse(R"(
        http_requests_total{environment=~"staging|testing|development",method!="GET"}
    )"), R"(
http_requests_total{environment=~"staging|testing|development",method!="GET"}

PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'http_requests_total'
        environment RE 'staging|testing|development'
        method NE 'GET'
)");

    EXPECT_EQ(parse(R"(
        http_requests_total{job="prometheus"}[5m]
    )"), R"(
http_requests_total{job="prometheus"}[300]

PrometheusQueryTree(RANGE_VECTOR):
    RangeSelector:
        range: 300
        InstantSelector:
            __name__ EQ 'http_requests_total'
            job EQ 'prometheus'
)");

    EXPECT_EQ(parse("http_requests_total offset 5m @ 1609746000"), R"(
http_requests_total @ 1609746000 offset 300

PrometheusQueryTree(INSTANT_VECTOR):
    Offset:
        at: 1609746000
        offset: 300
        InstantSelector:
            __name__ EQ 'http_requests_total'
)");

    EXPECT_EQ(parse("http_requests_total[5m:1m] offset -10s"), R"(
http_requests_total[300:60] offset -10

PrometheusQueryTree(RANGE_VECTOR):
    Offset:
        offset: -10
        Subquery:
            range: 300
            step: 60
            InstantSelector:
                __name__ EQ 'http_requests_total'
)");

    EXPECT_EQ(parse("(2 ^ vector(3))[5m:1m]"), R"(
(2 ^ vector(3))[300:60]

PrometheusQueryTree(RANGE_VECTOR):
    Subquery:
        range: 300
        step: 60
        BinaryOperator(^)
            Scalar(2)
            Function(vector):
                Scalar(3)
)");

    /// Subquery has higher precedence than power '^'
    EXPECT_EQ(parse("2 ^ vector(3)[5m:1m]"), R"(
2 ^ vector(3)[300:60]

PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(^)
        Scalar(2)
        Subquery:
            range: 300
            step: 60
            Function(vector):
                Scalar(3)
)");

}


TEST(PromQLParser, ParseStringLiterals)
{
    EXPECT_EQ(parse(R"(
        "this is a string"
        )"), R"(
"this is a string"

PrometheusQueryTree(STRING):
    StringLiteral('this is a string')
)");

    EXPECT_EQ(parse(R"(
        "\n"
        )"), R"(
"\n"

PrometheusQueryTree(STRING):
    StringLiteral('\n')
)");

    EXPECT_EQ(parse(R"(
        "these are unescaped: \n \\ ' \" ` \t"
        )"), R"(
"these are unescaped: \n \\ ' \" ` \t"

PrometheusQueryTree(STRING):
    StringLiteral('these are unescaped: \n \\ \' " ` \t')
)");

    EXPECT_EQ(parse(R"(
        'these are unescaped: \n \\ \' " ` \t'
        )"), R"(
"these are unescaped: \n \\ ' \" ` \t"

PrometheusQueryTree(STRING):
    StringLiteral('these are unescaped: \n \\ \' " ` \t')
)");

    EXPECT_EQ(parse(R"(
        `these are not unescaped: \n \\ ' " \t`
        )"), R"(
"these are not unescaped: \\n \\\\ ' \" \\t"

PrometheusQueryTree(STRING):
    StringLiteral('these are not unescaped: \\n \\\\ \' " \\t')
)");

    EXPECT_EQ(parse(R"(
        "日本語"
        )"), R"(
"日本語"

PrometheusQueryTree(STRING):
    StringLiteral('日本語')
)");

    EXPECT_EQ(parse(R"(
        "\u65e5\u672c\u8a9e" 
        )"), R"(
"日本語"

PrometheusQueryTree(STRING):
    StringLiteral('日本語')
)");

    EXPECT_EQ(parse(R"(
        "\U000065e5\U0000672c\U00008a9e" 
        )"), R"(
"日本語"

PrometheusQueryTree(STRING):
    StringLiteral('日本語')
)");

    EXPECT_EQ(parse(R"(
        "\xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e"
        )"), R"(
"日本語"

PrometheusQueryTree(STRING):
    StringLiteral('日本語')
)");

}
