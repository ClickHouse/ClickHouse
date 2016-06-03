#pragma once

#include <DB/Core/Defines.h>
#include <algorithm>


namespace DB
{


/** Базовый класс для ReadBuffer и WriteBuffer.
  * Содержит общие типы, переменные и функции.
  *
  * ReadBuffer и WriteBuffer похожи на istream и ostream, соответственно.
  * Их приходится использовать, так как с использованием iostream-ов невозможно эффективно реализовать некоторые операции.
  * Например, используя istream, невозможно быстро читать строковые значения из tab-separated файла,
  *  чтобы после чтения, позиция осталась сразу после считанного значения.
  * (Единственный вариант - вызывать функцию std::istream::get() на каждый байт, но это тормозит из-за нескольких виртуальных вызовов.)
  *
  * Read/WriteBuffer-ы предоставляют прямой доступ к внутреннему буферу, поэтому, необходимые операции реализуются эффективнее.
  * Используется только одна виртуальная функция nextImpl(), которая вызывается редко:
  * - в случае ReadBuffer - заполнить буфер новыми данными из источика;
  * - в случае WriteBuffer - записать данные из буфера в приёмник.
  *
  * Read/WriteBuffer-ы могут владеть или не владеть своим куском памяти.
  * Во втором случае, можно эффективно читать из уже существующего куска памяти / std::string, не копируя его.
  */
class BufferBase
{
public:
	/** Курсор в буфере. Позиция записи или чтения. */
	typedef char * Position;

	/** Ссылка на диапазон памяти. */
	struct Buffer
	{
		Buffer(Position begin_pos_, Position end_pos_) : begin_pos(begin_pos_), end_pos(end_pos_) {}

		inline Position begin() const { return begin_pos; }
		inline Position end() const { return end_pos; }
		inline size_t size() const { return end_pos - begin_pos; }
		inline void resize(size_t size) { end_pos = begin_pos + size; }

		inline void swap(Buffer & other)
		{
			std::swap(begin_pos, other.begin_pos);
			std::swap(end_pos, other.end_pos);
		}

	private:
		Position begin_pos;
		Position end_pos;		/// на 1 байт после конца буфера
	};

	/** Конструктор принимает диапазон памяти, который следует использовать под буфер.
	  * offset - начальное место курсора. ReadBuffer должен установить его в конец диапазона, а WriteBuffer - в начало.
	  */
	BufferBase(Position ptr, size_t size, size_t offset)
		: internal_buffer(ptr, ptr + size), working_buffer(ptr, ptr + size), pos(ptr + offset),	bytes(0) {}

	void set(Position ptr, size_t size, size_t offset)
	{
		internal_buffer = Buffer(ptr, ptr + size);
		working_buffer = Buffer(ptr, ptr + size);
		pos = ptr + offset;
	}

	/// получить буфер
	inline Buffer & internalBuffer() { return internal_buffer; }

	/// получить часть буфера, из которого можно читать / в который можно писать данные
	inline Buffer & buffer() { return working_buffer; }

	/// получить (для чтения и изменения) позицию в буфере
	inline Position & position() { return pos; };

	/// смещение в байтах курсора от начала буфера
	inline size_t offset() const { return pos - working_buffer.begin(); }

	/** Сколько байт было прочитано/записано, считая те, что ещё в буфере. */
	size_t count() const
	{
		return bytes + offset();
	}

	/** Проверить, есть ли данные в буфере. */
	bool ALWAYS_INLINE hasPendingData() const
	{
		return pos != working_buffer.end();
	}

protected:
	/// Ссылка на кусок памяти для буфера.
	Buffer internal_buffer;

	/** Часть куска памяти, которую можно использовать.
	  * Например, если internal_buffer - 1MB, а из файла для чтения было загружено в буфер
	  *  только 10 байт, то working_buffer будет иметь размер 10 байт
	  *  (working_buffer.end() будет указывать на позицию сразу после тех 10 байт, которых можно прочитать).
	  */
	Buffer working_buffer;

	/// Позиция чтения/записи.
	Position pos;

	/** Сколько байт было прочитано/записано, не считая тех, что сейчас в буфере.
	  * (считая те, что были уже использованы и "удалены" из буфера)
	  */
	size_t bytes;
};


}
