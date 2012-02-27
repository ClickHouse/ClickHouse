#pragma once

#include <DB/Parsers/ParserQuery.h>
#include <DB/Interpreters/InterpreterQuery.h>


namespace DB
{


/** Парсит и исполняет запрос.
  */
void executeQuery(
	ReadBuffer & istr,								/// Откуда читать запрос (а также данные для INSERT-а, если есть)
	WriteBuffer & ostr,								/// Куда писать результат
	Context & context,								/// БД, таблицы, типы данных, движки таблиц, функции, агрегатные функции...
	BlockInputStreamPtr & query_plan,				/// Сюда может быть записано описание, как выполнялся запрос
	size_t max_query_size = DEFAULT_MAX_QUERY_SIZE,	/// Какую часть запроса можно прочитать в оперативку для парсинга (оставшиеся данные для INSERT, если есть, считываются позже)
	size_t max_threads = DEFAULT_MAX_THREADS,		/// Максимальное количество потоков выполнения запроса
	size_t max_block_size = DEFAULT_BLOCK_SIZE);	/// Максимальный размер блока при чтении или вставке данных

}
