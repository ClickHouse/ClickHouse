#pragma once

#include <Core/Types.h>
#include <memory>

namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<IDataType>;

namespace Conditional
{

/// When performing a multiIf for numeric arguments, the following
/// structure is used to collect all the information needed on
/// the branches (1 or more then branches + 1 else branch) for
/// processing.
struct Branch
{
    size_t index;
    DataTypePtr type;
    bool is_const;
    UInt8 category;
};

using Branches = std::vector<Branch>;

/// The following functions are designed to make the code that handles
/// multiIf parameters a tad more readable and less prone to off-by-one
/// errors.
template <typename T>
inline bool hasValidArgCount(const T & args)
{
    return (args.size() >= 3) && ((args.size() % 2) == 1);
}

inline constexpr size_t firstCond()
{
    return 0;
}

inline constexpr size_t firstThen()
{
    return 1;
}

inline constexpr size_t secondThen()
{
    return 3;
}

/// NOTE: we always use this function after the number of arguments of multiIf
/// has been validated. Therefore there is no need to check for zero size.
template <typename T>
inline size_t elseArg(const T & args)
{
    return args.size() - 1;
}

inline size_t thenFromCond(size_t i)
{
    return i + 1;
}

inline size_t nextCond(size_t i)
{
    return i + 2;
}

inline size_t nextThen(size_t i)
{
    return i + 2;
}

inline bool isCond(size_t i)
{
    return (i % 2) == 0;
}

template <typename T>
inline size_t getCondCount(const T & args)
{
    return args.size() / 2;
}

template <typename T>
inline size_t getBranchCount(const T & args)
{
    return (args.size() / 2) + 1;
}

}

}
