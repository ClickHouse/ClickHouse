#include <DB/IO/copyData.h>


namespace DB
{

void copyData(ReadBuffer & from, WriteBuffer & to)
{
	/// Если дочитали до конца буфера, eof() либо заполнит буфер новыми данными и переместит курсор в начало, либо вернёт false.
	while (!from.eof())
	{
		/// buffer() - кусок данных, доступных для чтения; position() - курсор места, до которого уже дочитали.
		to.write(from.position(), from.buffer().end() - from.position());
		from.position() = from.buffer().end();
	}
}

void copyData(ReadBuffer & from, WriteBuffer & to, size_t bytes)
{
	while (bytes > 0 && !from.eof())
	{
		size_t count = std::min(bytes, static_cast<size_t>(from.buffer().end() - from.position()));
		to.write(from.position(), count);
		from.position() += count;
		bytes -= count;
	}

	if (bytes > 0)
		throw Exception("Attempt to read after EOF.", ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF);
}
	
}
