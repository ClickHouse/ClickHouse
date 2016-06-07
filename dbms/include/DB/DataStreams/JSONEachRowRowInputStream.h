#pragma once

#include <DB/Core/Block.h>
#include <DB/DataStreams/IRowInputStream.h>
#include <DB/Common/HashTable/HashMap.h>


namespace DB
{

class ReadBuffer;


/** Поток для чтения данных в формате JSON, где каждая строчка представлена отдельным JSON объектом.
  * Объекты могут быть разделены переводом строки, другими пробельными символами в любом количестве и, возможно, запятой.
  * Поля могут быть перечислены в произвольном порядке (в том числе, в разных строках может быть разный порядок),
  *  и часть полей может отсутствовать.
  */
class JSONEachRowRowInputStream : public IRowInputStream
{
public:
	JSONEachRowRowInputStream(ReadBuffer & istr_, const Block & sample_);

	bool read(Block & block) override;

private:
	ReadBuffer & istr;
	const Block sample;

	/// Буфер для прочитанного из потока имени поля. Используется, если его потребовалось скопировать.
	String name_buf;

	/// Хэш-таблица соответствия имя поля -> позиция в блоке. NOTE Можно использовать perfect hash map.
	using NameMap = HashMap<StringRef, size_t, StringRefHash>;
	NameMap name_map;
};

}
