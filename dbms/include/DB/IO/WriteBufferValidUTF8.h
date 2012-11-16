#pragma once

#include <DB/IO/WriteBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>
#include <Poco/UTF8Encoding.h>

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
		bool just_put_replacement;
		std::string replacement;
		
		/// Таблица взята из ConvertUTF.c от Unicode, Inc. Позволяет узнать длину последовательности по первому байту.
		/**
		 * Index into the table below with the first byte of a UTF-8 sequence to
		 * get the number of trailing bytes that are supposed to follow it.
		 * Note that *legal* UTF-8 values can't have 4 or 5-bytes. The table is
		 * left as-is for anyone who may want to do such conversion, which was
		 * allowed in earlier algorithms.
		 */
		static const char trailingBytesForUTF8[256];
		
		inline void putReplacement()
		{
			if (replacement.empty() || (group_replacements && just_put_replacement))
				return;
			just_put_replacement = true;
			output_buffer.write(replacement.data(), replacement.size());
		}
		
		inline void putValid(char *data, size_t len)
		{
			if (len == 0)
				return;
			just_put_replacement = false;
			output_buffer.write(data, len);
		}
		
		void nextImpl()
		{
			char *p = &memory[0];
			char *valid_start = p;
			while (p < pos)
			{
				size_t len = 1 + static_cast<size_t>(trailingBytesForUTF8[static_cast<unsigned char>(*p)]);
				
				if (len > 4)
				{
					/// Невалидное начало последовательности. Пропустим один байт.
					putValid(valid_start, p - valid_start);
					putReplacement();
					++p;
					valid_start = p;
				}
				else if (p + len > pos)
				{
					/// Еще не вся последовательность записана.
					break;
				}
				else if (Poco::UTF8Encoding::isLegal(reinterpret_cast<unsigned char*>(p), len))
				{
					/// Валидная последовательность.
					p += len;
				}
				else
				{
					/// Невалидная последовательность. Пропустим только первый байт.
					putValid(valid_start, p - valid_start);
					putReplacement();
					++p;
					valid_start = p;
				}
			}
			putValid(valid_start, p - valid_start);
			
			size_t cnt = pos - p;
			/// Сдвинем незаконченную последовательность в начало буфера.
			for (size_t i = 0; i < cnt; ++i)
			{
				memory[i] = p[i];
			}
			working_buffer = Buffer(&memory[cnt], &memory[0] + memory.size());
		}
		
		void finish()
		{
			/// Выпишем все полные последовательности из буфера.
			nextImpl();
			/// Если осталась незаконченная последовательность, запишем replacement.
			if (working_buffer.begin() != &memory[0])
			{
				putReplacement();
			}
		}
		
	public:
		static const size_t DEFAULT_SIZE;
		
		WriteBufferValidUTF8(DB::WriteBuffer & output_buffer, bool group_replacements = true, const char * replacement = "\xEF\xBF\xBD", size_t size = DEFAULT_SIZE)
		: BufferWithOwnMemory<DB::WriteBuffer>(std::max(4LU, size)), output_buffer(output_buffer),
		  group_replacements(group_replacements), just_put_replacement(false), replacement(replacement) {}
		
		virtual ~WriteBufferValidUTF8()
		{
			finish();
		}
	};
	
}
