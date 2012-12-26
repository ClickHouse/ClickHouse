#pragma once

#include <DB/IO/WriteBufferFromVector.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Functions/IFunction.h>


namespace DB
{
	
/** Функции преобразования чисел и дат в строки, содержащие тот же набор байт в машинном представлении, и обратно.
	*/


template<typename Name>
class FunctionReinterpretAsStringImpl : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return Name::get();
	}
	
	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ Poco::NumberFormatter::format(arguments.size()) + ", should be 1.",
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		
		const IDataType * type = &*arguments[0];
		if (!type->isNumeric() &&
			!dynamic_cast<const DataTypeDate *>(type) &&
			!dynamic_cast<const DataTypeDateTime *>(type))
			throw Exception("Cannot reinterpret " + type->getName() + " as String", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		
		return new DataTypeString;
	}
	
	template<typename T>
	bool executeType(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (const ColumnVector<T> * col_from = dynamic_cast<const ColumnVector<T> *>(&*block.getByPosition(arguments[0]).column))
		{
			ColumnString * col_to = new ColumnString;
			block.getByPosition(result).column = col_to;
			
			const typename ColumnVector<T>::Container_t & vec_from = col_from->getData();
			ColumnUInt8::Container_t & data_to = dynamic_cast<ColumnUInt8 &>(col_to->getData()).getData();
			ColumnString::Offsets_t & offsets_to = col_to->getOffsets();
			size_t size = vec_from.size();
			data_to.resize(size * (sizeof(T) + 1));
			offsets_to.resize(size);
			int pos = 0;
			
			for (size_t i = 0; i < size; ++i)
			{
				memcpy(&data_to[pos], &vec_from[i], sizeof(T));
				
				int len = sizeof(T);
				while (len > 0 && data_to[pos + len - 1] == '\0')
					--len;
				
				pos += len;
				data_to[pos++] = '\0';
				
				offsets_to[i] = pos;
			}
			data_to.resize(pos);
		}
		else if (const ColumnConst<T> * col_from = dynamic_cast<const ColumnConst<T> *>(&*block.getByPosition(arguments[0]).column))
		{
			std::string res(reinterpret_cast<const char *>(&col_from->getData()), sizeof(T));
			while (!res.empty() && res[res.length() - 1] == '\0')
				res.erase(res.end() - 1);
			
			block.getByPosition(result).column = new ColumnConstString(col_from->size(), res);
		}
		else
		{
			return false;
		}
		
		return true;
	}
	
	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (!(	executeType<UInt8>(block, arguments, result)
			||	executeType<UInt16>(block, arguments, result)
			||	executeType<UInt32>(block, arguments, result)
			||	executeType<UInt64>(block, arguments, result)
			||	executeType<Int8>(block, arguments, result)
			||	executeType<Int16>(block, arguments, result)
			||	executeType<Int32>(block, arguments, result)
			||	executeType<Int64>(block, arguments, result)
			||	executeType<Float32>(block, arguments, result)
			||	executeType<Float64>(block, arguments, result)))
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
			+ " of argument of function " + getName(),
							ErrorCodes::ILLEGAL_COLUMN);
	}
};

template<typename ToDataType, typename Name>
class FunctionReinterpretStringAs : public IFunction
{
public:
	typedef typename ToDataType::FieldType ToFieldType;
	
	/// Получить имя функции.
	String getName() const
	{
		return Name::get();
	}
	
	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ Poco::NumberFormatter::format(arguments.size()) + ", should be 1.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		
		const IDataType * type = &*arguments[0];
		if (!dynamic_cast<const DataTypeString *>(type) &&
			!dynamic_cast<const DataTypeFixedString *>(type))
			throw Exception("Cannot reinterpret " + type->getName() + " as " + ToDataType().getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		
		return new ToDataType;
	}
	
	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (ColumnString * col_from = dynamic_cast<ColumnString *>(&*block.getByPosition(arguments[0]).column))
		{
			ColumnVector<ToFieldType> * col_res = new ColumnVector<ToFieldType>;
			block.getByPosition(result).column = col_res;
			
			ColumnUInt8::Container_t & data_from = dynamic_cast<ColumnUInt8 &>(col_from->getData()).getData();
			ColumnString::Offsets_t & offsets_from = col_from->getOffsets();
			size_t size = offsets_from.size();
			typename ColumnVector<ToFieldType>::Container_t & vec_res = col_res->getData();
			vec_res.resize(size);
			
			size_t offset = 0;
			for (size_t i = 0; i < size; ++i)
			{
				ToFieldType value = 0;
				memcpy(&value, &data_from[offset], std::min(sizeof(ToFieldType), offsets_from[i] - offset - 1));
				vec_res[i] = value;
				offset = offsets_from[i];
			}
		}
		else if (ColumnFixedString * col_from = dynamic_cast<ColumnFixedString *>(&*block.getByPosition(arguments[0]).column))
		{
			ColumnVector<ToFieldType> * col_res = new ColumnVector<ToFieldType>;
			block.getByPosition(result).column = col_res;
			
			ColumnUInt8::Container_t & data_from = dynamic_cast<ColumnUInt8 &>(col_from->getData()).getData();
			size_t step = col_from->getN();
			size_t size = data_from.size() / step;
			typename ColumnVector<ToFieldType>::Container_t & vec_res = col_res->getData();
			vec_res.resize(size);
			
			size_t offset = 0;
			size_t copy_size = std::min(step, sizeof(ToFieldType));
			for (size_t i = 0; i < size; ++i)
			{
				ToFieldType value = 0;
				memcpy(&value, &data_from[offset], copy_size);
				vec_res[i] = value;
				offset += step;
			}
		}
		else if (ColumnConst<String> * col = dynamic_cast<ColumnConst<String> *>(&*block.getByPosition(arguments[0]).column))
		{
			ToFieldType value = 0;
			const String & str = col->getData();
			memcpy(&value, str.data(), std::min(sizeof(ToFieldType), str.length()));
			ColumnConst<ToFieldType> * col_res = new ColumnConst<ToFieldType>(col->size(), value);
			block.getByPosition(result).column = col_res;
		}
		else
		{
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
			+ " of argument of function " + getName(),
							ErrorCodes::ILLEGAL_COLUMN);
		}
	}
};


struct NameReinterpretAsUInt8 		{ static const char * get() { return "reinterpretAsUInt8"; } };
struct NameReinterpretAsUInt16		{ static const char * get() { return "reinterpretAsUInt16"; } };
struct NameReinterpretAsUInt32		{ static const char * get() { return "reinterpretAsUInt32"; } };
struct NameReinterpretAsUInt64		{ static const char * get() { return "reinterpretAsUInt64"; } };
struct NameReinterpretAsInt8 		{ static const char * get() { return "reinterpretAsInt8"; } };
struct NameReinterpretAsInt16 		{ static const char * get() { return "reinterpretAsInt16"; } };
struct NameReinterpretAsInt32		{ static const char * get() { return "reinterpretAsInt32"; } };
struct NameReinterpretAsInt64		{ static const char * get() { return "reinterpretAsInt64"; } };
struct NameReinterpretAsFloat32	{ static const char * get() { return "reinterpretAsFloat32"; } };
struct NameReinterpretAsFloat64	{ static const char * get() { return "reinterpretAsFloat64"; } };
struct NameReinterpretAsDate		{ static const char * get() { return "reinterpretAsDate"; } };
struct NameReinterpretAsDateTime	{ static const char * get() { return "reinterpretAsDateTime"; } };
struct NameReinterpretAsString		{ static const char * get() { return "reinterpretAsString"; } };

typedef FunctionReinterpretStringAs<DataTypeUInt8,		NameReinterpretAsUInt8>		FunctionReinterpretAsUInt8;
typedef FunctionReinterpretStringAs<DataTypeUInt16,	NameReinterpretAsUInt16>	FunctionReinterpretAsUInt16;
typedef FunctionReinterpretStringAs<DataTypeUInt32,	NameReinterpretAsUInt32>	FunctionReinterpretAsUInt32;
typedef FunctionReinterpretStringAs<DataTypeUInt64,	NameReinterpretAsUInt64>	FunctionReinterpretAsUInt64;
typedef FunctionReinterpretStringAs<DataTypeInt8,		NameReinterpretAsInt8>		FunctionReinterpretAsInt8;
typedef FunctionReinterpretStringAs<DataTypeInt16,		NameReinterpretAsInt16>		FunctionReinterpretAsInt16;
typedef FunctionReinterpretStringAs<DataTypeInt32,		NameReinterpretAsInt32>		FunctionReinterpretAsInt32;
typedef FunctionReinterpretStringAs<DataTypeInt64,		NameReinterpretAsInt64>		FunctionReinterpretAsInt64;
typedef FunctionReinterpretStringAs<DataTypeFloat32,	NameReinterpretAsFloat32>	FunctionReinterpretAsFloat32;
typedef FunctionReinterpretStringAs<DataTypeFloat64,	NameReinterpretAsFloat64>	FunctionReinterpretAsFloat64;
typedef FunctionReinterpretStringAs<DataTypeDate,		NameReinterpretAsDate>		FunctionReinterpretAsDate;
typedef FunctionReinterpretStringAs<DataTypeDateTime,	NameReinterpretAsDateTime>	FunctionReinterpretAsDateTime;

typedef FunctionReinterpretAsStringImpl<NameReinterpretAsString>	FunctionReinterpretAsString;
	
	
}
