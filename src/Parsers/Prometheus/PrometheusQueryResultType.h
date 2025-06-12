#pragma once

namespace DB
{

/// A query in prometheus can evaluate to one of the following four types:
enum class PrometheusQueryResultType
{
    INSTANT_VECTOR,
    RANGE_VECTOR,
    SCALAR,
    STRING,
};

}
