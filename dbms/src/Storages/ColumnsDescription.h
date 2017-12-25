#pragma once

#include <Storages/ColumnDefault.h>
#include <Core/NamesAndTypes.h>


namespace DB
{


template <bool store>
struct ColumnsDescription
{
    template <typename T>
    using by_value_or_cref = std::conditional_t<store, T, const T &>;

    by_value_or_cref<NamesAndTypes> columns;
    by_value_or_cref<NamesAndTypes> materialized;
    by_value_or_cref<NamesAndTypes> alias;
    by_value_or_cref<ColumnDefaults> defaults;

    String toString() const;

    static ColumnsDescription parse(const String & str);
};


}
