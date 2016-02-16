#include <DB/Common/Exception.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/copyData.h>


namespace DB
{


static void copyDataImpl(ReadBuffer & from, WriteBuffer & to, bool check_bytes, size_t bytes, std::atomic<bool> * is_cancelled)
{
	/// Если дочитали до конца буфера, eof() либо заполнит буфер новыми данными и переместит курсор в начало, либо вернёт false.
	while (bytes > 0 && !from.eof())
	{
		if (is_cancelled && *is_cancelled)
			return;

		/// buffer() - кусок данных, доступных для чтения; position() - курсор места, до которого уже дочитали.
		size_t count = std::min(bytes, static_cast<size_t>(from.buffer().end() - from.position()));
		to.write(from.position(), count);
		from.position() += count;
		bytes -= count;
	}

	if (check_bytes && bytes > 0)
		throw Exception("Attempt to read after EOF.", ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF);
}


void copyData(ReadBuffer & from, WriteBuffer & to)
{
	copyDataImpl(from, to, false, std::numeric_limits<size_t>::max(), nullptr);
}

void copyData(ReadBuffer & from, WriteBuffer & to, std::atomic<bool> & is_cancelled)
{
	copyDataImpl(from, to, false, std::numeric_limits<size_t>::max(), &is_cancelled);
}

void copyData(ReadBuffer & from, WriteBuffer & to, size_t bytes)
{
	copyDataImpl(from, to, true, bytes, nullptr);
}

void copyData(ReadBuffer & from, WriteBuffer & to, size_t bytes, std::atomic<bool> & is_cancelled)
{
	copyDataImpl(from, to, true, bytes, &is_cancelled);
}

}
