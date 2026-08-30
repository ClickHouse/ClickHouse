#include <gtest/gtest.h>

#include <Parsers/Prometheus/PrometheusQueryTree.h>

#include <fmt/format.h>

using namespace DB;

namespace
{
    String parse(std::string_view input)
    {
        PrometheusQueryTree query_tree{input};
        return "\n" + query_tree.toString() + "\n" + query_tree.dumpTree();
    }

    /// Parses a query consisting of a single string literal and returns its unescaped value.
    String parseStringLiteral(std::string_view input)
    {
        PrometheusQueryTree query_tree{input};
        return typeid_cast<const PrometheusQueryTree::StringLiteral &>(*query_tree.getRoot()).string;
    }

    void expectRoundTrip(std::string_view input, std::string_view expected)
    {
        PrometheusQueryTree query_tree{input};
        const auto serialized = query_tree.toString();
        EXPECT_EQ(serialized, expected) << input;

        PrometheusQueryTree reparsed;
        String error_message;
        size_t error_pos = 0;
        ASSERT_TRUE(reparsed.tryParse(serialized, 3, &error_message, &error_pos))
            << input << ": " << error_message << " at position " << error_pos;
        EXPECT_EQ(reparsed.getResultType(), query_tree.getResultType()) << input;
        EXPECT_EQ(reparsed.dumpTree(), query_tree.dumpTree()) << input;
    }
}


TEST(PromQLParser, QuotedSelectorIdentifiers)
{
    EXPECT_EQ(parse(R"({"http.server.request.duration"})"), R"(
{"http.server.request.duration"}

PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'http.server.request.duration'
)");

    EXPECT_EQ(parse(R"({"rpc.server.duration", "service.name"="api"})"), R"(
{"rpc.server.duration","service.name"="api"}

PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'rpc.server.duration'
        service.name EQ 'api'
)");

    EXPECT_EQ(parse(R"(up{"service.name"=~"api.*"})"), R"(
up{"service.name"=~"api.*"}

PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'up'
        service.name RE 'api.*'
)");

    expectRoundTrip(R"({""="value"})", R"({""="value"})");
    expectRoundTrip(R"({"métric.name","服务.name"="api"})", R"({"métric.name","服务.name"="api"})");
    expectRoundTrip(R"({"NaN"})", R"({"NaN"})");
    expectRoundTrip(R"({"Inf"})", R"({"Inf"})");
    expectRoundTrip(R"(up{"NaN"="x"})", R"(up{"NaN"="x"})");
    expectRoundTrip(R"(up{"Inf"="x"})", R"(up{"Inf"="x"})");
}


TEST(PromQLParser, ReservedKeywordMetricNames)
{
    /// A bare keyword in the position of an operand is a binary operator or a modifier,
    /// so such metric names must stay quoted, otherwise the serialized query doesn't parse back.
    for (const auto * const keyword :
         {"and", "or", "unless", "atan2", "by", "without", "on", "ignoring", "group_left", "group_right", "offset", "bool"})
    {
        expectRoundTrip(fmt::format(R"({{"{}"}})", keyword), fmt::format(R"({{"{}"}})", keyword));
        expectRoundTrip(fmt::format(R"({}{{job="x"}})", keyword), fmt::format(R"({{"{}",job="x"}})", keyword));
        expectRoundTrip(fmt::format(R"(up * {{"{}"}})", keyword), fmt::format(R"(up * {{"{}"}})", keyword));
    }
}


TEST(PromQLParser, InvalidQuotedSelectorIdentifiers)
{
    for (const auto * const query : {R"({""})", R"({"\xff"})"})
    {
        PrometheusQueryTree query_tree;
        String error_message;
        size_t error_pos = 0;
        EXPECT_FALSE(query_tree.tryParse(query, 3, &error_message, &error_pos)) << query;
        EXPECT_FALSE(error_message.empty()) << query;
    }
}


TEST(PromQLParser, EmptyMetricNameMatcher)
{
    for (const auto * const query : {R"({__name__="",a="x"})", R"({"__name__"="",a="x"})"})
        expectRoundTrip(query, R"({__name__="",a="x"})");
}


TEST(PromQLParser, MultipleMetricNameMatchers)
{
    expectRoundTrip(R"({"bar",__name__="baz"})", R"({"bar","baz"})");
    expectRoundTrip(R"({"bar",__name__=~"ba.*"})", R"({"bar",__name__=~"ba.*"})");
    expectRoundTrip(R"({"foo","bar"})", R"({"foo","bar"})");
    expectRoundTrip(R"({__name__="foo",__name__="bar"})", R"({"foo","bar"})");
}


TEST(PromQLParser, DuplicateMetricName)
{
    for (const auto * const query : {R"(up{"other.metric"})", R"(up{"up"})", R"(up{"__name__"="other"})"})
    {
        PrometheusQueryTree query_tree;
        String error_message;
        size_t error_pos = 0;
        EXPECT_FALSE(query_tree.tryParse(query, 3, &error_message, &error_pos)) << query;
        EXPECT_NE(error_message.find("metric name must not be set twice"), String::npos) << query;
    }
}


TEST(PromQLParser, CaseInsensitiveAggregationOperators)
{
    EXPECT_EQ(parse("SuM(up)"), R"(
sum(up)

PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(sum)
        InstantSelector:
            __name__ EQ 'up'
)");

    EXPECT_EQ(parse("ToPk BY(job) (1, up)"), R"(
topk by (job) (1, up)

PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(topk)
        by job
        Scalar(1)
        InstantSelector:
            __name__ EQ 'up'
)");

    /// Aggregation operator keywords can also be metric names and must keep their original case.
    EXPECT_EQ(parse("SUM"), R"(
SUM

PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'SUM'
)");
}


TEST(PromQLParser, QuotedGroupingLabels)
{
    EXPECT_EQ(parse(R"(sum by ("service.name", "k8s.namespace.name") (http_requests_total))"), R"(
sum by ("service.name", "k8s.namespace.name") (http_requests_total)

PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(sum)
        by service.name, k8s.namespace.name
        InstantSelector:
            __name__ EQ 'http_requests_total'
)");

    EXPECT_EQ(parse(R"(max without ("deployment.environment") (http_request_duration_seconds))"), R"(
max without ("deployment.environment") (http_request_duration_seconds)

PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(max)
        without deployment.environment
        InstantSelector:
            __name__ EQ 'http_request_duration_seconds'
)");

    EXPECT_EQ(parse(R"(http_requests_total + on ("service.name", "k8s.namespace.name") group_left ("pod.name") target_info)"), R"(
http_requests_total + on("service.name", "k8s.namespace.name") group_left("pod.name") target_info

PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(+)
        on service.name, k8s.namespace.name
        group_left pod.name
        InstantSelector:
            __name__ EQ 'http_requests_total'
        InstantSelector:
            __name__ EQ 'target_info'
)");

    EXPECT_EQ(parse(R"(http_requests_total / ignoring ("cluster.name") group_right ("instance.name") target_info)"), R"(
http_requests_total / ignoring("cluster.name") group_right("instance.name") target_info

PrometheusQueryTree(INSTANT_VECTOR):
    BinaryOperator(/)
        ignoring cluster.name
        group_right instance.name
        InstantSelector:
            __name__ EQ 'http_requests_total'
        InstantSelector:
            __name__ EQ 'target_info'
)");

    EXPECT_EQ(parse(R"(sum by ('service.name', `k8s.namespace.name`) (up))"), R"(
sum by ("service.name", "k8s.namespace.name") (up)

PrometheusQueryTree(INSTANT_VECTOR):
    AggregationOperator(sum)
        by service.name, k8s.namespace.name
        InstantSelector:
            __name__ EQ 'up'
)");
}


TEST(PromQLParser, QuotedGroupingLabelsRoundTrip)
{
    for (const auto *const input : {
             R"(sum by ("a\x00b") (up))",
             R"(sum by ("Inf") (up))",
             R"(sum by ("NaN") (up))",
             R"(sum by ("iNf") (up))",
             R"(sum by ("nAn") (up))",
         })
    {
        PrometheusQueryTree query_tree{input};
        EXPECT_EQ(query_tree.toString(), input);

        PrometheusQueryTree reparsed_query_tree{query_tree.toString()};
        EXPECT_EQ(reparsed_query_tree.toString(), input);
    }
}


TEST(PromQLParser, QuotedMetricNameRoundTrip)
{
    const auto *const input = R"(sum by ("service.name") ({__name__="http.server.duration"}))";
    const auto *const expected = R"(sum by ("service.name") ({"http.server.duration"}))";

    PrometheusQueryTree query_tree{input};
    EXPECT_EQ(query_tree.toString(), expected);

    PrometheusQueryTree reparsed_query_tree{query_tree.toString()};
    EXPECT_EQ(reparsed_query_tree.toString(), expected);
}


TEST(PromQLParser, InvalidQuotedGroupingLabels)
{
    for (const auto *const query : {R"(sum by ("") (up))", R"(sum by ("\xff") (up))"})
    {
        PrometheusQueryTree query_tree;
        String error_message;
        size_t error_pos = 0;
        EXPECT_FALSE(query_tree.tryParse(query, 3, &error_message, &error_pos));
        EXPECT_FALSE(error_message.empty());
    }
}


TEST(PromQLParser, PromQLStringSerializationRoundTrip)
{
    expectRoundTrip(R"("line\n\t\r\b\f\v")", R"("line\n\t\r\b\f\v")");
    expectRoundTrip(R"("invalid \xff")", R"("invalid \xff")");
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

    /// `start` and `end` are also valid metric and label names, even though they are used by the @ modifier.
    EXPECT_EQ(parse("start"), R"(
start

PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'start'
)");

    EXPECT_EQ(parse("end"), R"(
end

PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'end'
)");

    EXPECT_EQ(parse(R"(http_requests_total{start="x", end="y"})"), R"(
http_requests_total{start="x",end="y"}

PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'http_requests_total'
        start EQ 'x'
        end EQ 'y'
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

    EXPECT_EQ(parse("http_requests_total @ start()"), R"(
http_requests_total @ start()

PrometheusQueryTree(INSTANT_VECTOR):
    Offset:
        at: start()
        InstantSelector:
            __name__ EQ 'http_requests_total'
)");

    EXPECT_EQ(parse("http_requests_total @ end() offset 5m"), R"(
http_requests_total @ end() offset 300

PrometheusQueryTree(INSTANT_VECTOR):
    Offset:
        at: end()
        offset: 300
        InstantSelector:
            __name__ EQ 'http_requests_total'
)");

    EXPECT_EQ(parse("http_requests_total offset 5m @ start()"), R"(
http_requests_total @ start() offset 300

PrometheusQueryTree(INSTANT_VECTOR):
    Offset:
        at: start()
        offset: 300
        InstantSelector:
            __name__ EQ 'http_requests_total'
)");

    EXPECT_EQ(parse(R"PROMQL(
        http_requests_total{job="@ start()", instance=~"@ end\\(\\)"} @ end()
        )PROMQL"), R"PROMQL(
http_requests_total{job="@ start()",instance=~"@ end\\(\\)"} @ end()

PrometheusQueryTree(INSTANT_VECTOR):
    Offset:
        at: end()
        InstantSelector:
            __name__ EQ 'http_requests_total'
            job EQ '@ start()'
            instance RE '@ end\\(\\)'
)PROMQL");

    EXPECT_EQ(parse("http_requests_total[5m:1m] @ start()"), R"(
http_requests_total[300:60] @ start()

PrometheusQueryTree(RANGE_VECTOR):
    Offset:
        at: start()
        Subquery:
            range: 300
            step: 60
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


TEST(PromQLParser, TrailingCommasInGroupingLabelLists)
{
    for (const auto * const query : {
             "sum by (job,) (up)",
             "sum by (job, instance,) (up)",
             "sum without (instance,) (up)",
             "up + on(job,) up",
             "up + ignoring(instance,) up",
             "up + on(job,) group_left(instance,) up",
             "up + on(job,) group_right(instance,) up",
         })
    {
        EXPECT_NO_THROW(PrometheusQueryTree{query}) << query;
    }

    for (const auto * const query : {
             "sum by (,) (up)",
             "sum by (job,,) (up)",
         })
    {
        EXPECT_ANY_THROW(PrometheusQueryTree{query}) << query;
    }
}


TEST(PromQLParser, DurationUnitOrder)
{
    for (const auto & [query, expected_error_pos] : std::initializer_list<std::pair<std::string_view, size_t>>{
             {"up[1m1d]", 6},
             {"up[1h2h]", 6},
             {"up offset 1ms1s", 14},
         })
    {
        PrometheusQueryTree query_tree;
        String error_message;
        size_t error_pos = String::npos;

        EXPECT_FALSE(query_tree.tryParse(query, 3, &error_message, &error_pos)) << query;
        EXPECT_EQ(error_pos, expected_error_pos) << query;
    }

    EXPECT_NO_THROW(PrometheusQueryTree{"up[1y2w3d4h5m6s7ms]"});
    EXPECT_NO_THROW(PrometheusQueryTree{"up[1d2h5ms]"});
}


TEST(PromQLParser, ErrorPosition)
{
    for (const auto & [query, expected_error_pos] : std::initializer_list<std::pair<std::string_view, size_t>>{
             {"$metric", 0},
             {"up\n$down", 3},
             {R"("é"$)", 4},
             {R"("é" "x")", 5},
             {R"(label_join(up, "dst", "é", "src", "\q"))", 36},
         })
    {
        PrometheusQueryTree query_tree;
        String error_message;
        size_t error_pos = String::npos;

        EXPECT_FALSE(query_tree.tryParse(query, 3, &error_message, &error_pos));
        EXPECT_EQ(error_pos, expected_error_pos);
    }
}


TEST(PromQLParser, InvalidStringQuoteEscapes)
{
    for (const auto & [query, expected_error_pos] : std::initializer_list<std::pair<std::string_view, size_t>>{
             {R"(up{label="a\'b"})", 11},
             {R"(up{label='a\"b'})", 11},
             {R"("hello\
world")", 6},
             {R"("hello\x
world")", 6},
         })
    {
        PrometheusQueryTree query_tree;
        String error_message;
        size_t error_pos = String::npos;

        EXPECT_FALSE(query_tree.tryParse(query, 3, &error_message, &error_pos));
        EXPECT_EQ(error_pos, expected_error_pos) << query;
        EXPECT_NE(error_message.find("Invalid escape sequence"), String::npos) << query;
    }
}


TEST(PromQLParser, RejectLiteralLFInQuotedStrings)
{
    for (const auto & [query, expected_error_pos] : std::initializer_list<std::pair<std::string_view, size_t>>{
             {R"("hello
world")", 0},
             {R"('hello
world')", 0},
             {R"("hello\\
world")", 0},
             {"\"hello\r\nworld\"", 0},
             {R"("hello
world\q")", 0},
             {R"(up{job="hello
world"})", 7},
             {R"("hello
world" "x")", 0},
             {R"("hello
world" $)", 0},
         })
    {
        PrometheusQueryTree query_tree;
        String error_message;
        size_t error_pos = String::npos;

        EXPECT_FALSE(query_tree.tryParse(query, 3, &error_message, &error_pos)) << query;
        EXPECT_EQ(error_message, "unterminated quoted string") << query;
        EXPECT_EQ(error_pos, expected_error_pos) << query;
    }

    EXPECT_EQ(parseStringLiteral(R"(`hello
world`)"), "hello\nworld");
    EXPECT_EQ(parseStringLiteral(R"("hello\nworld")"), "hello\nworld");
    EXPECT_EQ(parseStringLiteral(R"('hello\nworld')"), "hello\nworld");
    EXPECT_EQ(parseStringLiteral("\"hello\rworld\""), "hello\rworld");
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


TEST(PromQLParser, RejectUnicodeSurrogateEscapes)
{
    auto expect_rejected = [](std::string_view query, size_t expected_error_pos)
    {
        PrometheusQueryTree query_tree;
        String error_message;
        size_t error_pos = String::npos;
        EXPECT_FALSE(query_tree.tryParse(query, /* timestamp_scale = */ 3, &error_message, &error_pos)) << query;
        EXPECT_NE(error_message.find("surrogate range 0xD800-0xDFFF"), String::npos) << query << ": " << error_message;
        EXPECT_EQ(error_pos, expected_error_pos) << query;
    };

    expect_rejected(R"("\uD800")", 1);
    expect_rejected(R"("\uDFFF")", 1);
    expect_rejected(R"("\U0000D800")", 1);
    expect_rejected(R"("\U0000DFFF")", 1);
    expect_rejected(R"("ab\uD800cd")", 3);

    /// Code points right outside the surrogate range are still accepted and encoded as UTF-8.
    EXPECT_EQ(parseStringLiteral(R"("\uD7FF")"), "\xED\x9F\xBF");
    EXPECT_EQ(parseStringLiteral(R"("\uE000")"), "\xEE\x80\x80");
    EXPECT_EQ(parseStringLiteral(R"("\U00010000")"), "\xF0\x90\x80\x80");
    EXPECT_EQ(parseStringLiteral(R"("\U0010FFFF")"), "\xF4\x8F\xBF\xBF");
}
