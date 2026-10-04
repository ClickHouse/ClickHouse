#include <Processors/Executors/Runtime/Tools/Wyrd/Adversary/Adversary.h>
#include <Processors/Executors/Runtime/Tools/Wyrd/Adversary/YieldAdversary.h>
#include <Processors/Executors/Runtime/Tools/Wyrd/Adversary/LockFreeAdversary.h>
#include <Processors/Executors/Runtime/Tools/Wyrd/Adversary/NopAdversary.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

}

namespace DB::Runtime::Wyrd
{

namespace
{

AdversaryPtr & current()
{
    static AdversaryPtr adversary = createYieldAdversary({});
    return adversary;
}

}

IAdversary * adversary()
{
    return current().get();
}

void setAdversary(AdversaryPtr adversary)
{
    current() = std::move(adversary);
}

AdversaryPtr createAdversary(std::string_view name)
{
    if (name == "yield")
        return createYieldAdversary({});
    if (name == "lockfree")
        return createLockFreeAdversary({});
    if (name == "nop")
        return createNopAdversary();
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown adversary '{}', expected one of: yield, lockfree, nop", name);
}

}
