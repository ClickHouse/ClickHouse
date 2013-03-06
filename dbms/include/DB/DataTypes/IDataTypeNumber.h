#pragma once

#include <DB/DataTypes/IDataType.h>

#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>


namespace DB
{


/** Реализует часть интерфейса IDataType, общую для всяких чисел
  * - ввод и вывод в текстовом виде.
  */
template <typename FType>
class IDataTypeNumber : public IDataType
{
public:
	typedef FType FieldType;

	bool isNumeric() const { return true; }
		
	void serializeText(const Field & field, WriteBuffer & ostr) const
	{
		writeText(get<typename NearestFieldType<FieldType>::Type>(field), ostr);
	}
	
	inline void deserializeText(Field & field, ReadBuffer & istr) const;

	void serializeTextEscaped(const Field & field, WriteBuffer & ostr) const
	{
		serializeText(field, ostr);
	}
	
	void deserializeTextEscaped(Field & field, ReadBuffer & istr) const
	{
		deserializeText(field, istr);
	}
	
	void serializeTextQuoted(const Field & field, WriteBuffer & ostr) const
	{
		serializeText(field, ostr);
	}
	
	void deserializeTextQuoted(Field & field, ReadBuffer & istr) const
	{
		deserializeText(field, istr);
	}

	size_t getSizeOfField() const { return sizeof(FieldType); }

	Field getDefault() const
	{
		return typename NearestFieldType<FieldType>::Type();
	}
};

template <typename FType> inline void IDataTypeNumber<FType>::deserializeText(Field & field, ReadBuffer & istr) const
{
	typename NearestFieldType<FieldType>::Type x;
	readIntTextUnsafe(x, istr);
	field = x;
}

template <> inline void IDataTypeNumber<Float64>::deserializeText(Field & field, ReadBuffer & istr) const
{
	Float64 x;
	readText(x, istr);
	field = x;
}
template <> inline void IDataTypeNumber<Float32>::deserializeText(Field & field, ReadBuffer & istr) const
{
	Float64 x;
	readText(x, istr);
	field = x;
}


}
