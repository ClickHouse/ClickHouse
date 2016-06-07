#pragma once

#include <DB/Core/Defines.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>


namespace DB
{


/** Прогресс выполнения запроса.
  * Передаваемые по сети значения представляют собой разницу - сколько было сделано после предыдущего отправленного значения.
  * Тот же объект используется для суммирования полученных значений.
  */
struct Progress
{
	size_t rows = 0;		/// Строк обработано.
	size_t bytes = 0;		/// Байт обработано.

	/** Сколько ещё строк надо обработать, приблизительно. Передаётся не ноль, когда возникает информация о какой-то новой части работы.
	  * Полученные значения надо суммровать, чтобы получить оценку общего количества строк для обработки.
	  * Используется для отображения прогресс-бара на клиенте.
	  */
	size_t total_rows = 0;

	Progress() {}
	Progress(size_t rows_, size_t bytes_, size_t total_rows_ = 0)
		: rows(rows_), bytes(bytes_), total_rows(total_rows_) {}

	void read(ReadBuffer & in, UInt64 server_revision)
	{
		readVarUInt(rows, in);
		readVarUInt(bytes, in);

		if (server_revision >= DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS)
			readVarUInt(total_rows, in);
	}

	void write(WriteBuffer & out, UInt64 client_revision) const
	{
		writeVarUInt(rows, out);
		writeVarUInt(bytes, out);

		if (client_revision >= DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS)
			writeVarUInt(total_rows, out);
	}

	void increment(const Progress & rhs)
	{
		rows += rhs.rows;
		bytes += rhs.bytes;
		total_rows += rhs.total_rows;
	}

	/// Каждое значение по-отдельности изменяется атомарно.
	void incrementPiecewiseAtomically(const Progress & rhs)
	{
		__sync_add_and_fetch(&rows, rhs.rows);
		__sync_add_and_fetch(&bytes, rhs.bytes);
		__sync_add_and_fetch(&total_rows, rhs.total_rows);
	}

	void reset()
	{
		*this = Progress();
	}

	Progress fetchAndResetPiecewiseAtomically()
	{
		Progress res;

		res.rows = __sync_fetch_and_and(&rows, 0);
		res.bytes = __sync_fetch_and_and(&bytes, 0);
		res.total_rows = __sync_fetch_and_and(&total_rows, 0);

		return res;
	}
};


}
