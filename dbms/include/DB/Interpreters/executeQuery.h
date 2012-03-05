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
	BlockInputStreamPtr & query_plan);				/// Сюда может быть записано описание, как выполнялся запрос

}
