#pragma once

#include <DB/DataStreams/IBlockOutputStream.h>


namespace DB
{

/** Сериализует поток блоков в родном бинарном формате (с именами и типами столбцов).
  * Предназначено для взаимодействия между серверами.
  */
class NativeBlockOutputStream : public IBlockOutputStream
{
public:
	NativeBlockOutputStream(WriteBuffer & ostr_) : ostr(ostr_) {}
	void write(const Block & block);

	void flush() { ostr.next(); }

private:
	WriteBuffer & ostr;
};

}
