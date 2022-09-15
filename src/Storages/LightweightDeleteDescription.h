#pragma once
#include <Core/NamesAndTypes.h>
#include "Storages/TTLDescription.h"

namespace DB
{

struct LightweightDeleteDescription
{
    static const NameAndTypePair FILTER_COLUMN;
};

}
