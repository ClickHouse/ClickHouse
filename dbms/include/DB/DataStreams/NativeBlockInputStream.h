#pragma once

#include <DB/DataTypes/DataTypeFactory.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

/** Десериализует поток блоков из родного бинарного формата (с именами и типами столбцов).
  * Предназначено для взаимодействия между серверами.
  */
class NativeBlockInputStream : public IProfilingBlockInputStream
{
public:
	NativeBlockInputStream(ReadBuffer & istr_, const DataTypeFactory & data_type_factory_)
		: istr(istr_), data_type_factory(data_type_factory_) {}
	
	String getName() const { return "NativeBlockInputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << this;
		return res.str();
	}

protected:
	Block readImpl();

private:
	ReadBuffer & istr;
	const DataTypeFactory & data_type_factory;
};

}
