#pragma once

#include <DB/Core/NamesAndTypes.h>


namespace DB
{

class Cluster;
class Context;

/// Узнать имена и типы столбцов таблицы на первом попавшемся сервере кластера.
/// Используется для реализации табличной функции remote и других.
NamesAndTypesList getStructureOfRemoteTable(
	const Cluster & cluster,
	const std::string & database,
	const std::string & table,
	const Context & context);

}
