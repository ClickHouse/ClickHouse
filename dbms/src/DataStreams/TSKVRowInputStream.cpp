#include <DB/IO/ReadHelpers.h>
#include <DB/DataStreams/TSKVRowInputStream.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int INCORRECT_DATA;
	extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
	extern const int CANNOT_READ_ALL_DATA;
}


TSKVRowInputStream::TSKVRowInputStream(ReadBuffer & istr_, const Block & sample_, bool skip_unknown_)
	: istr(istr_), sample(sample_), skip_unknown(skip_unknown_), name_map(sample.columns())
{
	/// In this format, we assume that column name cannot contain BOM,
	///  so BOM at beginning of stream cannot be confused with name of field, and it is safe to skip it.
	skipBOMIfExists(istr);

	size_t columns = sample.columns();
	for (size_t i = 0; i < columns; ++i)
		name_map[sample.getByPosition(i).name] = i;		/// NOTE Можно было бы расположить имена более кэш-локально.
}


/** Прочитать имя поля в формате tskv.
  * Вернуть true, если после имени поля идёт знак равенства,
  *  иначе (поле без значения) вернуть false.
  * Ссылка на имя поля будет записана в ref.
  * Также может быть использован временный буфер tmp, чтобы скопировать туда имя поля.
  * При чтении, пропускает имя и знак равенства после него.
  */
static bool readName(ReadBuffer & buf, StringRef & ref, String & tmp)
{
	tmp.clear();

	while (!buf.eof())
	{
		const char * next_pos = find_first_symbols<'\t', '\n', '\\', '='>(buf.position(), buf.buffer().end());

		if (next_pos == buf.buffer().end())
		{
			tmp.append(buf.position(), next_pos - buf.position());
			buf.next();
			continue;
		}

		/// Дошли до конца имени.
		if (*next_pos != '\\')
		{
			bool have_value = *next_pos == '=';
			if (tmp.empty())
			{
				/// Данные не нужно копировать, можно ссылаться прямо на внутренность buf.
				ref = StringRef(buf.position(), next_pos - buf.position());
				buf.position() += next_pos + have_value - buf.position();
			}
			else
			{
				/// Копируем данные во временную строку и возвращаем ссылку на неё.
				tmp.append(buf.position(), next_pos - buf.position());
				buf.position() += next_pos + have_value - buf.position();
				ref = StringRef(tmp);
			}
			return have_value;
		}
		/// В имени есть эскейп-последовательность.
		else
		{
			tmp.append(buf.position(), next_pos - buf.position());
			buf.position() += next_pos + 1 - buf.position();
			if (buf.eof())
				throw Exception("Cannot parse escape sequence", ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);

			tmp.push_back(parseEscapeSequence(*buf.position()));
			++buf.position();
			continue;
		}
	}

	throw Exception("Unexpected end of stream while reading key name from TSKV format", ErrorCodes::CANNOT_READ_ALL_DATA);
}


bool TSKVRowInputStream::read(Block & block)
{
	if (istr.eof())
		return false;

	size_t columns = block.columns();

	/// Множество столбцов, для которых были считаны значения. Остальные затем заполним значениями по-умолчанию.
	/// TODO Возможность предоставить свои DEFAULT-ы.
	bool read_columns[columns];
	memset(read_columns, 0, columns);

	if (unlikely(*istr.position() == '\n'))
	{
		/// Пустая строка. Допустимо, но непонятно зачем.
		++istr.position();
	}
	else
	{
		while (true)
		{
			StringRef name_ref;
			bool has_value = readName(istr, name_ref, name_buf);

			if (has_value)
			{
				/// NOTE Возможна оптимизация путём кэширования порядка полей (который почти всегда одинаковый)
				/// и быстрой проверки на соответствие следующему ожидаемому полю, вместо поиска в хэш-таблице.

				auto it = name_map.find(name_ref);
				if (name_map.end() == it)
				{
					if (!skip_unknown)
						throw Exception("Unknown field found while parsing TSKV format: " + name_ref.toString(), ErrorCodes::INCORRECT_DATA);

					/// Если ключ не найден, то пропускаем значение.
					NullSink sink;
					readEscapedStringInto(sink, istr);
				}
				else
				{
					size_t index = it->second;

					if (read_columns[index])
						throw Exception("Duplicate field found while parsing TSKV format: " + name_ref.toString(), ErrorCodes::INCORRECT_DATA);

					read_columns[index] = true;

					auto & col = block.unsafeGetByPosition(index);
					col.type.get()->deserializeTextEscaped(*col.column.get(), istr);
				}
			}
			else
			{
				/// Единственное, что может идти без значения - это фрагмент tskv, который игнорируется.
				if (!(name_ref.size == 4 && 0 == memcmp(name_ref.data, "tskv", 4)))
					throw Exception("Found field without value while parsing TSKV format: " + name_ref.toString(), ErrorCodes::INCORRECT_DATA);
			}

			if (istr.eof())
			{
				throw Exception("Unexpected end of stream after field in TSKV format: " + name_ref.toString(), ErrorCodes::CANNOT_READ_ALL_DATA);
			}
			else if (*istr.position() == '\t')
			{
				++istr.position();
				continue;
			}
			else if (*istr.position() == '\n')
			{
				++istr.position();
				break;
			}
			else
				throw Exception("Found garbage after field in TSKV format: " + name_ref.toString(), ErrorCodes::INCORRECT_DATA);
		}
	}

	/// Заполняем не встретившиеся столбцы значениями по-умолчанию.
	for (size_t i = 0; i < columns; ++i)
		if (!read_columns[i])
			block.unsafeGetByPosition(i).column.get()->insertDefault();

	return true;
}

}
