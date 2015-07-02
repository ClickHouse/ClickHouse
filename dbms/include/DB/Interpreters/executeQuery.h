#pragma once

#include <DB/Core/QueryProcessingStage.h>
#include <DB/DataStreams/BlockIO.h>


namespace DB
{


/** Парсит и исполняет запрос.
  */
void executeQuery(
	ReadBuffer & istr,					/// Откуда читать запрос (а также данные для INSERT-а, если есть)
	WriteBuffer & ostr,					/// Куда писать результат
	Context & context,					/// БД, таблицы, типы данных, движки таблиц, функции, агрегатные функции...
	BlockInputStreamPtr & query_plan,	/// Сюда может быть записано описание, как выполнялся запрос
	bool internal = false,				/// Если true - значит запрос порождён из другого запроса, и не нужно его регистировать в ProcessList-е.
	QueryProcessingStage::Enum stage = QueryProcessingStage::Complete);	/// До какой стадии выполнять SELECT запрос.


/** Более низкоуровневая функция для межсерверного взаимодействия.
  * Подготавливает запрос к выполнению, но не выполняет его.
  * Возвращает потоки блоков, при использовании которых, запрос будет выполняться.
  * То есть, вы можете, в некоторой степени, управлять циклом выполнения запроса.
  *
  * Для выполнения запроса:
  * - сначала передайте данные INSERT-а, если есть, в BlockIO::out;
  * - затем читайте результат из BlockIO::in;
  *
  * Если запрос не предполагает записи данных или возврата результата, то out и in,
  *  соответственно, будут равны nullptr.
  *
  * Часть запроса по парсингу и форматированию (секция FORMAT) необходимо выполнить отдельно.
  */
BlockIO executeQuery(
	const String & query,	/// Текст запроса, без данных INSERT-а (если есть). Данные INSERT-а следует писать в BlockIO::out.
	Context & context,
	bool internal = false,
	QueryProcessingStage::Enum stage = QueryProcessingStage::Complete);

}
