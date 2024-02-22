#include <Common/FunctionsFuzzer.h>

#include <atomic>

namespace
{

std::atomic<double> fault_function_type_fault_probability[FaultFunctionType::MAX] = {};

}

std::optional<FaultFunctionType> faultFunctionTypeFromString(std::string_view value)
{
    if (value == "ThreadPoolSchedule")
        return FaultFunctionType::ThreadPoolSchedule;

    return {};
}

std::string toString(FaultFunctionType fault_function_type)
{
    switch (fault_function_type)
    {
        case ThreadPoolSchedule:
            return "ThreadPoolSchedule";
        case MAX:
            return {};
    }
}

void setFunctionFaultProbability(FaultFunctionType fault_function_type, double value)
{
    fault_function_type_fault_probability[static_cast<uint32_t>(fault_function_type)].store(value, std::memory_order_release);
}

double getFunctionFaultProbability(FaultFunctionType fault_function_type)
{
    return fault_function_type_fault_probability[static_cast<uint32_t>(fault_function_type)].load(std::memory_order_acquire);
}
