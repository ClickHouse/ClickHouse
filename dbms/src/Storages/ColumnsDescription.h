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


    bool operator==(const ColumnsDescription & other) const
    {
        return ordinary == other.ordinary
            && materialized == other.materialized
            && aliases == other.aliases
            && defaults == other.defaults;
    }

    bool operator!=(const ColumnsDescription & other) const { return !(*this == other); }

    /** Get a list of names and table column types, only non-virtual.
     */
    NamesAndTypesList getList() const;
    const NamesAndTypesList & getListNonMaterialized() const { return ordinary; }

    /** Get a list of column names.
     */
    Names getNames() const;

    /** Get a description of any column by its name.
     */
    NameAndTypePair get(const String & column_name) const;

    /** Is there a column with that name.
      */
    bool has(const String & column_name) const;


    String toString() const;

    static ColumnsDescription parse(const String & str);
};

}
