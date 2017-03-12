#pragma once

#include <DB/Core/FieldVisitors.h>


namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<IDataType>;


/** Для заданного значения Field возвращает минимальный тип данных, позволяющий хранить значение этого типа.
  * В случае, если Field - массив, конвертирует все элементы к общему типу.
  */
class FieldToDataType : public StaticVisitor<DataTypePtr>
{
public:
	DataTypePtr operator() (Null & x) const;
	DataTypePtr operator() (UInt64 & x) const;
	DataTypePtr operator() (Int64 & x) const;
	DataTypePtr operator() (Float64 & x) const;
	DataTypePtr operator() (String & x) const;
	DataTypePtr operator() (Array & x) const;
	DataTypePtr operator() (Tuple & x) const;
};

}

