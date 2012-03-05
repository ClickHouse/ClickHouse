#pragma once

#include <DB/Core/Defines.h>


namespace DB
{

/** Настройки выполнения запроса.
  */
struct Settings
{
	size_t max_block_size;	/// Максимальный размер блока для чтения
	size_t max_threads;		/// Максимальное количество потоков выполнения запроса
	size_t max_query_size;	/// Какую часть запроса можно прочитать в оперативку для парсинга (оставшиеся данные для INSERT, если есть, считываются позже)
	bool asynchronous;		/// Выполнять разные стадии конвейера выполнения запроса параллельно

	Settings() : max_block_size(DEFAULT_BLOCK_SIZE), max_threads(DEFAULT_MAX_THREADS), max_query_size(DEFAULT_MAX_QUERY_SIZE), asynchronous(true) {}
};


}
