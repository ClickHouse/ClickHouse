#include <DB/Common/Exception.h>
#include <DB/DataStreams/BlockInputStreamFromRowInputStream.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
	extern const int CANNOT_PARSE_QUOTED_STRING;
	extern const int CANNOT_PARSE_DATE;
	extern const int CANNOT_PARSE_DATETIME;
	extern const int CANNOT_READ_ARRAY_FROM_TEXT;
}


BlockInputStreamFromRowInputStream::BlockInputStreamFromRowInputStream(
	RowInputStreamPtr row_input_,
	const Block & sample_,
	size_t max_block_size_,
	UInt64 allow_errors_num_,
	Float64 allow_errors_ratio_)
	: row_input(row_input_), sample(sample_), max_block_size(max_block_size_),
	allow_errors_num(allow_errors_num_), allow_errors_ratio(allow_errors_ratio_)
{
}


Block BlockInputStreamFromRowInputStream::readImpl()
{
	Block res = sample.cloneEmpty();

	try
	{
		for (size_t rows = 0; rows < max_block_size; ++rows, ++total_rows)
		{
			try
			{
				if (!row_input->read(res))
					break;
			}
			catch (Exception & e)
			{
				/// Logic for possible skipping of errors.

				if (!(e.code() == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED
					|| e.code() == ErrorCodes::CANNOT_PARSE_QUOTED_STRING
					|| e.code() == ErrorCodes::CANNOT_PARSE_DATE
					|| e.code() == ErrorCodes::CANNOT_PARSE_DATETIME
					|| e.code() == ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT))
					throw;

				if (allow_errors_num == 0 || allow_errors_ratio == 0)
					throw;

				++num_errors;
				Float64 current_error_ratio = static_cast<Float64>(num_errors) / total_rows;

				if (num_errors > allow_errors_num
					&& current_error_ratio > allow_errors_ratio)
				{
					e.addMessage("(Already have " + toString(num_errors) + " errors"
						" out of " + toString(total_rows) + " rows"
						", which is " + toString(current_error_ratio) + " of all rows)");
					throw;
				}

				if (!row_input->allowSyncAfterError())
				{
					e.addMessage("(Input format doesn't allow to skip errors)");
					throw;
				}

				row_input->syncAfterError();
			}
		}
	}
	catch (Exception & e)
	{
		e.addMessage("(at row " + toString(total_rows + 1) + ")");
		throw;
	}

	if (res.rows() == 0)
		res.clear();
	else
		res.optimizeNestedArraysOffsets();

	return res;
}

}
