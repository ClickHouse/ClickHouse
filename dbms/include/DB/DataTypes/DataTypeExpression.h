#pragma once

#include <DB/DataTypes/IDataTypeDummy.h>


namespace DB
{
	
/**
  * Лямбда-выражение.
  */
class DataTypeExpression : public IDataTypeDummy
{
private:
	DataTypes argument_types;
	DataTypePtr return_type;
	
public:
	/// Некоторые типы могут быть еще не известны.
	DataTypeExpression(DataTypes argument_types_ = DataTypes(), DataTypePtr return_type_ = nullptr)
		: argument_types(argument_types_), return_type(return_type_) {}
	
	std::string getName() const
	{
		std::string res = "Expression(";
		if (argument_types.size() > 1)
			res += "(";
		for (size_t i = 0; i < argument_types.size(); ++i)
		{
			if (i > 0)
				res += ", ";
			const DataTypePtr & type = argument_types[i];
			res += type ? type->getName() : "?";
		}
		if (argument_types.size() > 1)
			res += ")";
		res += " -> ";
		res += return_type ? return_type->getName() : "?";
		res += ")";
		return res;
	}
	
	DataTypePtr clone() const
	{
		return new DataTypeExpression(argument_types, return_type);
	}
	
	const DataTypes & getArgumentTypes() const
	{
		return argument_types;
	}
	
	const DataTypePtr & getReturnType() const
	{
		return return_type;
	}
};
	
}
