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
    return cpu_cost * weight.cpu_weight + mem_cost * weight.mem_weight + net_cost * weight.net_weight;
}

Cost Cost::infinite(const Weight & weight_)
{
    return Cost(weight_, std::numeric_limits<Float64>::max());
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

Cost & Cost::operator=(const Cost & other)
{
    this->weight = other.weight;
    this->cpu_cost = other.cpu_cost;
    this->mem_cost = other.mem_cost;
    this->net_cost = other.net_cost;
    return *this;
}

Cost Cost::operator+(const Cost & other) const
{
    checkWeight(other);
    Cost result(other.weight);
    result.cpu_cost = this->cpu_cost + other.cpu_cost;
    result.mem_cost = this->mem_cost + other.mem_cost;
    result.net_cost = this->net_cost + other.net_cost;

    return result;
}

Cost Cost::operator-(const Cost & other) const
{
    checkWeight(other);
    Cost result(other.weight);
    result.cpu_cost = this->cpu_cost - other.cpu_cost;
    result.mem_cost = this->mem_cost - other.mem_cost;
    result.net_cost = this->net_cost - other.net_cost;

    result.cpu_cost = std::max(0.0, result.cpu_cost);
    result.mem_cost = std::max(0.0, result.mem_cost);
    result.net_cost = std::max(0.0, result.net_cost);

    return result;
}

Cost & Cost::operator+=(const Cost & other)
{
    checkWeight(other);
    this->cpu_cost += other.cpu_cost;
    this->mem_cost += other.mem_cost;
    this->net_cost += other.net_cost;
    return *this;
}

Cost & Cost::operator-=(const Cost & other)
{
    checkWeight(other);
    this->cpu_cost -= other.cpu_cost;
    this->mem_cost -= other.mem_cost;
    this->net_cost -= other.net_cost;

    this->cpu_cost = std::max(0.0, cpu_cost);
    this->mem_cost = std::max(0.0, mem_cost);
    this->net_cost = std::max(0.0, net_cost);

    return *this;
}

bool Cost::operator<(const Cost & other) const
{
    checkWeight(other);
    return this->get() < other.get();
}

bool Cost::operator>(const Cost & other) const
{
    checkWeight(other);
    return this->get() > other.get();
}

bool Cost::operator<=(const Cost & other) const
{
    checkWeight(other);
    return this->get() <= other.get();
}

bool Cost::operator>=(const Cost & other) const
{
    checkWeight(other);
    return this->get() >= other.get();
}

void Cost::reset()
{
    cpu_cost = 0.0;
    mem_cost = 0.0;
    net_cost = 0.0;
}

void Cost::checkWeight(const Cost & other) const
{
    if (weight != other.weight)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Weight is not equal when calculating two cost.");
}

String Cost::toString() const
{
    return fmt::format("(summary:{:.2g}, cup:{:.2g}, mem:{:.2g}, net:{:.2g})", get(), cpu_cost, mem_cost, net_cost);
}

bool Cost::Weight::operator!=(const Weight& other) const
{
    return cpu_weight != other.cpu_weight ||
        mem_weight != other.mem_weight ||
        net_weight != other.net_weight;
}

}
