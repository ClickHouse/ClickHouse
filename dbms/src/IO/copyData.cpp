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
	
}
