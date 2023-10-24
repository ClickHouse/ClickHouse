#pragma once

#include <base/types.h>

namespace DB
{

class Cost
{
public:
    static constexpr Float64 CPU_COST_COEFFICIENT = 0.1; /// TODO add to settings
    static constexpr Float64 MEM_COST_COEFFICIENT = 0.1;
    static constexpr Float64 NET_COST_COEFFICIENT = 0.8;

    Cost(Float64 cpu_cost_, Float64 mem_cost_, Float64 net_cost_) : cpu_cost(cpu_cost_), mem_cost(mem_cost_), net_cost(net_cost_) { }
    Cost(Float64 cpu_cost_, Float64 mem_cost_) : Cost(cpu_cost_, mem_cost_, 0.0) { }
    Cost(Float64 cpu_cost_) : Cost(cpu_cost_, 0.0, 0.0) { }
    Cost() : Cost(0.0, 0.0, 0.0) { }

    Cost(const Cost & other) : cpu_cost(other.cpu_cost), mem_cost(other.mem_cost), net_cost(other.net_cost) { }

    static Cost infinite();
    Float64 get() const;

    void dividedBy(size_t n);
    void multiplyBy(size_t n);

    Cost operator+(const Cost & other);
    Cost operator-(const Cost & other);

    Cost & operator+=(const Cost & other);
    Cost & operator-=(const Cost & other);

    Cost & operator=(const Cost & other);

    bool operator<(const Cost & other) const;
    bool operator>(const Cost & other) const;

    bool operator<=(const Cost & other) const;
    bool operator>=(const Cost & other) const;

    String toString();

private:
    Float64 cpu_cost;
    Float64 mem_cost;
    Float64 net_cost;
};

}
