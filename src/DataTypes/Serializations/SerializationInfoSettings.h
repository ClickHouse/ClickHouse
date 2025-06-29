#pragma once

namespace DB
{

struct SerializationInfoSettings
{
    double ratio_of_defaults_for_sparse = 1.0;
    bool choose_kind = false;
    bool string_with_size_stream = false;

    bool isAlwaysDefault() const { return ratio_of_defaults_for_sparse >= 1.0; }
};

}
