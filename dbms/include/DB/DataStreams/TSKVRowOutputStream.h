#pragma once

#include <DB/DataStreams/TabSeparatedRowOutputStream.h>


namespace DB
{

/** Поток для вывода данных в формате TSKV.
  * TSKV похож на TabSeparated, но перед каждым значением указывается его имя и знак равенства: name=value.
  * Этот формат весьма неэффективен.
  */
class TSKVRowOutputStream : public TabSeparatedRowOutputStream
{
public:
	TSKVRowOutputStream(WriteBuffer & ostr_, const Block & sample_);
	void writeField(const Field & field) override;

protected:
	NamesAndTypes fields;
};

}

