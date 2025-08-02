#pragma once
#include <cstddef>

namespace DB
{

struct SerializationInfoSettings
{
    const double ratio_of_defaults_for_sparse = 1.0;
    const size_t number_of_uniq_for_low_cardinality = 20000;
    const bool choose_kind = false;

    bool isAlwaysDefault() const { return ratio_of_defaults_for_sparse >= 1.0; }
};

}
