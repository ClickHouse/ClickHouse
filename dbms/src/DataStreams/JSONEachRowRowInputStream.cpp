#include <DB/IO/ReadHelpers.h>
#include <DB/DataStreams/JSONEachRowRowInputStream.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int INCORRECT_DATA;
	extern const int CANNOT_READ_ALL_DATA;
}


JSONEachRowRowInputStream::JSONEachRowRowInputStream(ReadBuffer & istr_, const Block & sample_, bool skip_unknown_)
	: istr(istr_), sample(sample_), skip_unknown(skip_unknown_), name_map(sample.columns())
{
	/// In this format, BOM at beginning of stream cannot be confused with value, so it is safe to skip it.
	skipBOMIfExists(istr);

	size_t columns = sample.columns();
	for (size_t i = 0; i < columns; ++i)
		name_map[sample.getByPosition(i).name] = i;		/// NOTE Можно было бы расположить имена более кэш-локально.
}


/** Прочитать имя поля в формате JSON.
  * Ссылка на имя поля будет записана в ref.
  * Также может быть использован временный буфер tmp, чтобы скопировать туда имя поля.
  */
static StringRef readName(ReadBuffer & buf, String & tmp)
{
	if (buf.position() + 1 < buf.buffer().end())
	{
		const char * next_pos = find_first_symbols<'\\', '"'>(buf.position() + 1, buf.buffer().end());

		if (next_pos != buf.buffer().end() && *next_pos != '\\')
		{
			/// Наиболее вероятный вариант - в имени ключа нет эскейп-последовательностей и имя целиком поместилось в буфер.
			assertChar('"', buf);
			StringRef res(buf.position(), next_pos - buf.position());
			buf.position() += next_pos - buf.position();
			assertChar('"', buf);
			return res;
		}
	}

	readJSONString(tmp, buf);
	return tmp;
}


static void skipColonDelimeter(ReadBuffer & istr)
{
	skipWhitespaceIfAny(istr);
	assertChar(':', istr);
	skipWhitespaceIfAny(istr);
}


bool JSONEachRowRowInputStream::read(Block & block)
{
	skipWhitespaceIfAny(istr);
	if (istr.eof())
		return false;

	assertChar('{', istr);

	size_t columns = block.columns();

	/// Множество столбцов, для которых были считаны значения. Остальные затем заполним значениями по-умолчанию.
	/// TODO Возможность предоставить свои DEFAULT-ы.
	bool read_columns[columns];
	memset(read_columns, 0, columns);

	bool first = true;
	while (true)
	{
		skipWhitespaceIfAny(istr);

		if (istr.eof())
			throw Exception("Unexpected end of stream while parsing JSONEachRow format", ErrorCodes::CANNOT_READ_ALL_DATA);
		else if (*istr.position() == '}')
		{
			++istr.position();
			break;
		}

		if (first)
			first = false;
		else
		{
			assertChar(',', istr);
			skipWhitespaceIfAny(istr);
		}

		StringRef name_ref = readName(istr, name_buf);

		/// NOTE Возможна оптимизация путём кэширования порядка полей (который почти всегда одинаковый)
		/// и быстрой проверки на соответствие следующему ожидаемому полю, вместо поиска в хэш-таблице.

		auto it = name_map.find(name_ref);
		if (name_map.end() == it)
		{
			if (!skip_unknown)
				throw Exception("Unknown field found while parsing JSONEachRow format: " + name_ref.toString(), ErrorCodes::INCORRECT_DATA);

			skipColonDelimeter(istr);
			skipJSONFieldPlain(istr, name_ref);
			continue;
		}

		size_t index = it->second;

		if (read_columns[index])
			throw Exception("Duplicate field found while parsing JSONEachRow format: " + name_ref.toString(), ErrorCodes::INCORRECT_DATA);

		skipColonDelimeter(istr);

		read_columns[index] = true;

		auto & col = block.unsafeGetByPosition(index);
		col.type.get()->deserializeTextJSON(*col.column.get(), istr);
	}

	skipWhitespaceIfAny(istr);
	if (!istr.eof() && *istr.position() == ',')
		++istr.position();

	/// Заполняем не встретившиеся столбцы значениями по-умолчанию.
	for (size_t i = 0; i < columns; ++i)
		if (!read_columns[i])
			block.unsafeGetByPosition(i).column.get()->insertDefault();

	return true;
}

}
