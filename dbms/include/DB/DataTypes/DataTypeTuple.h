#pragma once

#include <DB/DataTypes/IDataTypeDummy.h>


namespace DB
{

/** Тип данных - кортеж.
  * Используется только как промежуточный вариант при вычислении выражений.
  */
class DataTypeTuple : public IDataTypeDummy
{
private:
	DataTypes elems;
public:
	DataTypeTuple(DataTypes elems_) : elems(elems_) {}
	
	std::string getName() const
	{
		std::stringstream s;

		s << "Tuple(";
		for (DataTypes::const_iterator it = elems.begin(); it != elems.end(); ++it)
			s << (it == elems.begin() ? "" : ", ") << (*it)->getName();
		s << ")";
		
		return s.str();
	}
	
	SharedPtr<IDataType> clone() const { return new DataTypeTuple(elems); }
};

}

