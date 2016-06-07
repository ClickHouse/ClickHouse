#pragma once

#include <DB/Storages/ColumnDefault.h>
#include <DB/Core/NamesAndTypes.h>


namespace DB
{


template <bool store>
struct ColumnsDescription
{
	template <typename T>
	using by_value_or_cref = typename std::conditional<store, T, const T &>::type;

	by_value_or_cref<NamesAndTypesList> columns;
	by_value_or_cref<NamesAndTypesList> materialized;
	by_value_or_cref<NamesAndTypesList> alias;
	by_value_or_cref<ColumnDefaults> defaults;

	String toString() const;

	static ColumnsDescription parse(const String & str);
};


}
