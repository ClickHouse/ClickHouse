#pragma once

#include <Common/Exception.h>

#include <utility>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}

/// Marks a valid PromQL expression that failed during evaluation.
/// It deliberately keeps CANNOT_EXECUTE_PROMQL_QUERY so SQL-visible error codes stay stable;
/// the Prometheus HTTP handler uses the dynamic type to select HTTP 422 / "execution".
class PrometheusQueryExecutionException final : public Exception
{
public:
    template <typename... Args>
    explicit PrometheusQueryExecutionException(FormatStringHelper<Args...> fmt, Args &&... args)
        : Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY, std::move(fmt), std::forward<Args>(args)...)
    {
    }

    PrometheusQueryExecutionException * clone() const override { return new PrometheusQueryExecutionException(*this); }
    void rethrow() const override { throw *this; } // NOLINT
};

}
