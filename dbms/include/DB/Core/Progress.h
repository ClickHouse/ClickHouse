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
	std::atomic<size_t> rows {0};		/// Строк обработано.
	std::atomic<size_t> bytes {0};		/// Байт обработано.

	/** Сколько ещё строк надо обработать, приблизительно. Передаётся не ноль, когда возникает информация о какой-то новой части работы.
	  * Полученные значения надо суммровать, чтобы получить оценку общего количества строк для обработки.
	  * Используется для отображения прогресс-бара на клиенте.
	  */
	std::atomic<size_t> total_rows {0};

	Progress() {}
	Progress(size_t rows_, size_t bytes_, size_t total_rows_ = 0)
		: rows(rows_), bytes(bytes_), total_rows(total_rows_) {}

	void read(ReadBuffer & in, UInt64 server_revision)
	{
		size_t new_rows = 0;
		size_t new_bytes = 0;
		size_t new_total_rows = 0;

		readVarUInt(new_rows, in);
		readVarUInt(new_bytes, in);

		if (server_revision >= DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS)
			readVarUInt(new_total_rows, in);

		rows = new_rows;
		bytes = new_bytes;
		total_rows = new_total_rows;
	}

	void write(WriteBuffer & out, UInt64 client_revision) const
	{
		writeVarUInt(rows, out);
		writeVarUInt(bytes, out);

		if (client_revision >= DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS)
			writeVarUInt(total_rows, out);
	}

	/// Каждое значение по-отдельности изменяется атомарно.
	void incrementPiecewiseAtomically(const Progress & rhs)
	{
		rows += rhs.rows;
		bytes += rhs.bytes;
		total_rows += rhs.total_rows;
	}

	void reset()
	{
		rows = 0;
		bytes = 0;
		total_rows = 0;
	}

	Progress fetchAndResetPiecewiseAtomically()
	{
		Progress res;

		res.rows = rows.fetch_and(0);
		res.bytes = bytes.fetch_and(0);
		res.total_rows = total_rows.fetch_and(0);

		return res;
	}

	Progress & operator=(Progress && other)
	{
		rows = other.rows.load(std::memory_order_relaxed);
		bytes = other.bytes.load(std::memory_order_relaxed);
		total_rows = other.total_rows.load(std::memory_order_relaxed);

		return *this;
	}

	Progress(Progress && other)
	{
		*this = std::move(other);
	}
};


}
