#include <Processors/Executors/Runtime/Wyrd/Strategies/Adversary/Access.h>

#include <memory>

namespace DB::Runtime::Wyrd
{

namespace
{

std::unique_ptr<Adversary> & current()
{
    static std::unique_ptr<Adversary> adversary;
    return adversary;
}

}

Adversary * adversary()
{
    return current().get();
}

void setAdversary(AdversaryParams params)
{
    current() = std::make_unique<Adversary>(params);
}

void resetAdversary()
{
    current() = nullptr;
}

}
