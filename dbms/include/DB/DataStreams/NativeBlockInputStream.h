#pragma once

#include <DB/DataTypes/DataTypeFactory.h>
#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{

/** Десериализует поток блоков из родного бинарного формата (с именами и типами столбцов).
  * Предназначено для взаимодействия между серверами.
  */
class NativeBlockInputStream : public IBlockInputStream
{
public:
	NativeBlockInputStream(ReadBuffer & istr_, DataTypeFactory & data_type_factory_)
		: istr(istr_), data_type_factory(data_type_factory_) {}
	
	/** Прочитать следующий блок.
	  * Если блоков больше нет - вернуть пустой блок (для которого operator bool возвращает false).
	  */
	Block read();

private:
	ReadBuffer & istr;
	DataTypeFactory & data_type_factory;
};

}
