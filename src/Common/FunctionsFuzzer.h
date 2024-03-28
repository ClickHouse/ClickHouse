#pragma once

#include <cstdint>
#include <optional>
#include <string>

enum FaultFunctionType : uint32_t
{
    ThreadPoolSchedule = 0,
    MAX
};

std::optional<FaultFunctionType> faultFunctionTypeFromString(std::string_view value);

std::string toString(FaultFunctionType fault_function_type);

void setFunctionFaultProbability(FaultFunctionType fault_function_type, double value);

double getFunctionFaultProbability(FaultFunctionType fault_function_type);
