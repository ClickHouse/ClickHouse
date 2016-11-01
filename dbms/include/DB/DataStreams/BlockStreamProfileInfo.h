#pragma once

#include <vector>
#include <DB/Common/Stopwatch.h>

#include <DB/Core/Types.h>

#if __APPLE__
#include <common/apple_rt.h>
#endif

namespace DB
{

class Block;
class ReadBuffer;
class WriteBuffer;

/// Информация для профайлинга. См. IProfilingBlockInputStream.h
struct BlockStreamProfileInfo
{
	bool started = false;
	Stopwatch total_stopwatch {CLOCK_MONOTONIC_COARSE};	/// Время с учётом ожидания

	String stream_name;			/// Короткое имя потока, для которого собирается информация

	size_t rows = 0;
	size_t blocks = 0;
	size_t bytes = 0;

	/// Информация о вложенных потоках - для выделения чистого времени работы.
	using BlockStreamProfileInfos = std::vector<const BlockStreamProfileInfo *>;
	BlockStreamProfileInfos nested_infos;

	/// Собрать BlockStreamProfileInfo для ближайших в дереве источников с именем name. Пример; собрать все info для PartialSorting stream-ов.
	void collectInfosForStreamsWithName(const char * name, BlockStreamProfileInfos & res) const;

	/** Получить число строк, если бы не было LIMIT-а.
	  * Если нет LIMIT-а - возвращается 0.
	  * Если запрос не содержит ORDER BY, то число может быть занижено - возвращается количество строк в блоках, которые были прочитаны до LIMIT-а.
	  * Если запрос содержит ORDER BY, то возвращается точное число строк, которое было бы, если убрать LIMIT.
	  */
	size_t getRowsBeforeLimit() const;
	bool hasAppliedLimit() const;

	void update(Block & block);

	/// Методы для бинарной [де]сериализации. Передаются не все поля.
	void read(ReadBuffer & in);
	void write(WriteBuffer & out) const;

	/// Установить поля из другого объекта, но только те, которые передаются по сети (методами выше).
	void setFrom(const BlockStreamProfileInfo & rhs);

private:
	void calculateRowsBeforeLimit() const;

	/// Для этих полей сделаем accessor'ы, т.к. их необходимо предварительно вычислять.
	mutable bool applied_limit = false;					/// Применялся ли LIMIT
	mutable size_t rows_before_limit = 0;
	mutable bool calculated_rows_before_limit = false;	/// Вычислялось ли поле rows_before_limit
};

}
