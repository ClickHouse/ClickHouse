#pragma once

#include <base/types.h>

namespace DB
{

class Cost
{
public:
    struct Weight
    {
        Float64 cpu_weight;
        Float64 mem_weight;
        Float64 net_weight;

        bool operator!=(const Weight& other) const;
    };

    Cost(Weight weight_, Float64 cpu_cost_, Float64 mem_cost_, Float64 net_cost_)
        : weight(weight_), cpu_cost(cpu_cost_), mem_cost(mem_cost_), net_cost(net_cost_)
    {
    }
    Cost(Weight weight_, Float64 cpu_cost_, Float64 mem_cost_) : Cost(weight_, cpu_cost_, mem_cost_, 0.0) { }
    Cost(Weight weight_, Float64 cpu_cost_) : Cost(weight_, cpu_cost_, 0.0, 0.0) { }
    Cost(Weight weight_ = {0.2, 0.2, 0.6}) : Cost(weight_, 0.0, 0.0, 0.0) { }

    Cost(const Cost & other) : weight(other.weight), cpu_cost(other.cpu_cost), mem_cost(other.mem_cost), net_cost(other.net_cost) { }

    static Cost infinite(const Weight & weight_);
    Float64 get() const;

    void dividedBy(size_t n);
    void multiplyBy(size_t n);

    Cost operator+(const Cost & other) const;
    Cost operator-(const Cost & other) const;

    Cost & operator+=(const Cost & other);
    Cost & operator-=(const Cost & other);

    Cost & operator=(const Cost & other);

    bool operator<(const Cost & other) const;
    bool operator>(const Cost & other) const;

    bool operator<=(const Cost & other) const;
    bool operator>=(const Cost & other) const;

    String toString() const;
    void reset();

private:
    void checkWeight(const Cost & other) const;

    Weight weight;
    Float64 cpu_cost;
    Float64 mem_cost;
    Float64 net_cost;
};

}
