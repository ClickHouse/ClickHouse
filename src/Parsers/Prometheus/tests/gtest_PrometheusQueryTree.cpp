#include <gtest/gtest.h>

#include <Parsers/Prometheus/PrometheusQueryTree.h>

using namespace DB;


TEST(PrometheusQueryTree, Parse)
{
    EXPECT_EQ(PrometheusQueryTree{"0.74"}.dumpTree(), R"(
PrometheusQueryTree(SCALAR):
    ScalarLiteral(0.74)
)");

    EXPECT_EQ(PrometheusQueryTree{"2e-5"}.dumpTree(), R"(
PrometheusQueryTree(SCALAR):
    ScalarLiteral(0.00002)
)");

    EXPECT_EQ(PrometheusQueryTree{"1.5E4"}.dumpTree(), R"(
PrometheusQueryTree(SCALAR):
    ScalarLiteral(15000)
)");

    EXPECT_EQ(PrometheusQueryTree{"0xABcd"}.dumpTree(), R"(
PrometheusQueryTree(SCALAR):
    ScalarLiteral(43981)
)");

    EXPECT_EQ(PrometheusQueryTree{"3h20m10s5ms"}.dumpTree(), R"(
PrometheusQueryTree(SCALAR):
    IntervalLiteral(12010.005)
)");

    EXPECT_EQ(PrometheusQueryTree{"-1"}.dumpTree(), R"(
PrometheusQueryTree(SCALAR):
    UnaryOperator(-)
        ScalarLiteral(1)
)");

    EXPECT_EQ(PrometheusQueryTree{"Inf+inf+iNf"}.dumpTree(), R"(
PrometheusQueryTree(SCALAR):
    BinaryOperator(+)
        BinaryOperator(+)
            ScalarLiteral(inf)
            ScalarLiteral(inf)
        ScalarLiteral(inf)
)");

    EXPECT_EQ(PrometheusQueryTree{"NaN+nan+nAn"}.dumpTree(), R"(
PrometheusQueryTree(SCALAR):
    BinaryOperator(+)
        BinaryOperator(+)
            ScalarLiteral(nan)
            ScalarLiteral(nan)
        ScalarLiteral(nan)
)");

    EXPECT_EQ(PrometheusQueryTree{"0x_1_2_3 * 0X_A_B"}.dumpTree(), R"(
PrometheusQueryTree(SCALAR):
    BinaryOperator(*)
        ScalarLiteral(291)
        ScalarLiteral(171)
)");

    EXPECT_EQ(PrometheusQueryTree{R"(
        http_requests_total{job="prometheus",group="canary"}
    )"}.dumpTree(), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'http_requests_total'
        job EQ 'prometheus'
        group EQ 'canary'
)");

    EXPECT_EQ(PrometheusQueryTree{R"(
        http_requests_total{environment=~"staging|testing|development",method!="GET"}
    )"}.dumpTree(), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    InstantSelector:
        __name__ EQ 'http_requests_total'
        environment RE 'staging|testing|development'
        method NE 'GET'
)");

    EXPECT_EQ(PrometheusQueryTree{R"(
        http_requests_total{job="prometheus"}[5m]
    )"}.dumpTree(), R"(
PrometheusQueryTree(RANGE_VECTOR):
    RangeSelector:
        range:
            IntervalLiteral(300)
        InstantSelector:
            __name__ EQ 'http_requests_total'
            job EQ 'prometheus'
)");

    EXPECT_EQ(PrometheusQueryTree{"http_requests_total offset 5m @ 1609746000"}.dumpTree(), R"(
PrometheusQueryTree(INSTANT_VECTOR):
    At:
        at:
            ScalarLiteral(1609746000)
        offset:
            IntervalLiteral(300)
        InstantSelector:
            __name__ EQ 'http_requests_total'
)");

    EXPECT_EQ(PrometheusQueryTree{"http_requests_total[5m:1m] offset -10s"}.dumpTree(), R"(
PrometheusQueryTree(RANGE_VECTOR):
    At:
        offset:
            IntervalLiteral(-10)
        Subquery:
            range:
                IntervalLiteral(300)
            resolution:
                IntervalLiteral(60)
            InstantSelector:
                __name__ EQ 'http_requests_total'
)");

}
