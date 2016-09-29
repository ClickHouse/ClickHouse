#pragma once

#include <DB/IO/WriteBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>


namespace DB
{

/** Пишет данные в другой буфер, заменяя невалидные UTF-8 последовательности на указанную последовательность.
	* Если записывается уже валидный UTF-8, работает быстрее.
	* Замечение: перед использованием полученной строки, уничтожте этот объект.
	*/
class WriteBufferValidUTF8 : public BufferWithOwnMemory<WriteBuffer>
{
private:
	WriteBuffer & output_buffer;
	bool group_replacements;
	/// Последний записанный символ был replacement.
	bool just_put_replacement = false;
	std::string replacement;

	/// Таблица взята из ConvertUTF.c от Unicode, Inc. Позволяет узнать длину последовательности по первому байту.
	static const char trailingBytesForUTF8[256];

	void putReplacement();
	void putValid(char * data, size_t len);

	void nextImpl() override;
	void finish();

public:
	static const size_t DEFAULT_SIZE;

	WriteBufferValidUTF8(
		WriteBuffer & output_buffer,
		bool group_replacements = true,
		const char * replacement = "\xEF\xBF\xBD",
		size_t size = DEFAULT_SIZE);

	virtual ~WriteBufferValidUTF8() override
	{
		finish();
	}
};

}
