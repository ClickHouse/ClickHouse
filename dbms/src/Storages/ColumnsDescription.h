#pragma once

#include <Core/NamesAndTypes.h>
#include <Core/Names.h>
#include <Storages/ColumnDefault.h>
#include <Core/Block.h>


namespace DB
{

struct ColumnsDescription
{
    NamesAndTypesList ordinary;
    NamesAndTypesList materialized;
    NamesAndTypesList aliases;
    ColumnDefaults defaults;

    ColumnsDescription() = default;

    ColumnsDescription(
        NamesAndTypesList ordinary_,
        NamesAndTypesList materialized_,
        NamesAndTypesList aliases_,
        ColumnDefaults defaults_)
        : ordinary(std::move(ordinary_))
        , materialized(std::move(materialized_))
        , aliases(std::move(aliases_))
        , defaults(std::move(defaults_))
    {}

    explicit ColumnsDescription(NamesAndTypesList ordinary_) : ordinary(std::move(ordinary_)) {}

    bool operator==(const ColumnsDescription & other) const
    {
        return ordinary == other.ordinary
            && materialized == other.materialized
            && aliases == other.aliases
            && defaults == other.defaults;
    }

    bool operator!=(const ColumnsDescription & other) const { return !(*this == other); }

    /// ordinary + materialized.
    NamesAndTypesList getAllPhysical() const;

    /// ordinary + materialized + aliases.
    NamesAndTypesList getAll() const;

    Names getNamesOfPhysical() const;

    NameAndTypePair getPhysical(const String & column_name) const;

    bool hasPhysical(const String & column_name) const;


    String toString() const;

    static ColumnsDescription parse(const String & str);
};

}
