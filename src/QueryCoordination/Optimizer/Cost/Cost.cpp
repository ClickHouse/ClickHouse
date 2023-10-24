#include "Cost.h"

#include <fmt/format.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

Float64 Cost::get() const
{
    return cpu_cost * CPU_COST_COEFFICIENT + mem_cost * MEM_COST_COEFFICIENT + net_cost * NET_COST_COEFFICIENT;
}

Cost Cost::infinite()
{
    return Cost(std::numeric_limits<Float64>::max());
}

void Cost::dividedBy(size_t n)
{
    if (n == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "divided by 0");

    cpu_cost /= n;
    mem_cost /= n;
    net_cost /= n;
}

void Cost::multiplyBy(size_t n)
{
    cpu_cost *= n;
    mem_cost *= n;
    net_cost *= n;
}

Cost& Cost::operator=(const Cost & other)
{
    this->cpu_cost = other.cpu_cost;
    this->mem_cost = other.mem_cost;
    this->net_cost = other.net_cost;
    return *this;
}

Cost Cost::operator+(const Cost & other)
{
    Cost result;
    result.cpu_cost = this->cpu_cost + other.cpu_cost;
    result.mem_cost = this->mem_cost + other.mem_cost;
    result.net_cost = this->net_cost + other.net_cost;

    return result;
}

Cost Cost::operator-(const Cost & other)
{
    Cost result;
    result.cpu_cost = this->cpu_cost - other.cpu_cost;
    result.mem_cost = this->mem_cost - other.mem_cost;
    result.net_cost = this->net_cost - other.net_cost;

    result.cpu_cost = std::max(0.0, result.cpu_cost);
    result.mem_cost = std::max(0.0, result.mem_cost);
    result.net_cost = std::max(0.0, result.net_cost);

    return result;
}

Cost& Cost::operator+=(const Cost & other)
{
    this->cpu_cost += other.cpu_cost;
    this->mem_cost += other.mem_cost;
    this->net_cost += other.net_cost;
    return *this;
}

Cost& Cost::operator-=(const Cost & other)
{
    this->cpu_cost -= other.cpu_cost;
    this->mem_cost -= other.mem_cost;
    this->net_cost -= other.net_cost;

    this->cpu_cost = std::max(0.0, other.cpu_cost);
    this->mem_cost = std::max(0.0, other.mem_cost);
    this->net_cost = std::max(0.0, other.net_cost);

    return *this;
}

bool Cost::operator<(const Cost & other) const
{
    return this->get() < other.get();
}

bool Cost::operator>(const Cost & other) const
{
    return this->get() > other.get();
}

bool Cost::operator<=(const Cost & other) const
{
    return this->get() <= other.get();
}

bool Cost::operator>=(const Cost & other) const
{
    return this->get() >= other.get();
}

String Cost::toString()
{
    return fmt::format("[cpu_cost: {}, mem_cost: {}, net_cost: {}]", cpu_cost, mem_cost, net_cost);
}

}
