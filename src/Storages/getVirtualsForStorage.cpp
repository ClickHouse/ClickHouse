#include "getVirtualsForStorage.h"

namespace DB
{

NamesAndTypesList getVirtualsForStorage(const NamesAndTypesList & storage_columns_, const NamesAndTypesList & default_virtuals_)
{
    auto default_virtuals = default_virtuals_;
    auto storage_columns = storage_columns_;
    default_virtuals.sort();
    storage_columns.sort();

    NamesAndTypesList result_virtuals;
    std::set_difference(
        default_virtuals.begin(), default_virtuals.end(), storage_columns.begin(), storage_columns.end(),
        std::back_inserter(result_virtuals),
        [](const NameAndTypePair & lhs, const NameAndTypePair & rhs){ return lhs.name < rhs.name; });

    return result_virtuals;
}

}
