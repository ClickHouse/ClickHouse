#pragma once

#include <DB/Core/Defines.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>


namespace DB
{


/** Progress of query execution.
  * Values, transferred over network are deltas - how much was done after previously sent value.
  * The same struct is also used for summarized values.
  */
struct Progress
{
	std::atomic<size_t> rows {0};		/// Rows (source) processed.
	std::atomic<size_t> bytes {0};		/// Bytes (uncompressed, source) processed.

	/** How much rows must be processed, in total, approximately. Non-zero value is sent when there is information about some new part of job.
	  * Received values must be summed to get estimate of total rows to process.
	  * Used for rendering progress bar on client.
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

	/// Each value separately is changed atomically (but not whole object).
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
