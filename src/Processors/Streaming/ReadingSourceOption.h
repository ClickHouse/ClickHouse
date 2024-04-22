#pragma once

#include <Core/MultiEnum.h>

namespace DB
{

enum class ReadingSourceOption : uint64_t
{
    Storage,
    Subscription,
};

using ReadingSourceOptions = MultiEnum<ReadingSourceOption>;

}
