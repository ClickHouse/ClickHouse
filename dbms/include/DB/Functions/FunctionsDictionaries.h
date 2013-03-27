#pragma once

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeArray.h>

#include <DB/Columns/ColumnVector.h>
#include <DB/Columns/ColumnArray.h>

#include <DB/Interpreters/Context.h>

#include <DB/Functions/IFunction.h>


namespace DB
{

/** Функции, использующие словари Яндекс.Метрики
  * - словари регионов, операционных систем, поисковых систем.
  *
  * Подняться по дереву до определенного уровня.
  *  regionToCity, regionToArea, regionToCountry,
  *  OSToRoot,
  *  SEToRoot,
  *
  * Является ли первый идентификатор потомком второго.
  *  regionIn, SEIn, OSIn.
  * 
  * Получить массив идентификаторов регионов, состоящий из исходного и цепочки родителей. Порядок implementation defined.
  *  regionHierarchy, OSHierarchy, SEHierarchy.
  */


struct RegionToCityImpl
{
	static UInt32 apply(UInt32 x, const RegionsHierarchy & hierarchy) { return hierarchy.toCity(x); }
};

struct RegionToAreaImpl
{
	static UInt32 apply(UInt32 x, const RegionsHierarchy & hierarchy) { return hierarchy.toArea(x); }
};

struct RegionToCountryImpl
{
	static UInt32 apply(UInt32 x, const RegionsHierarchy & hierarchy) { return hierarchy.toCountry(x); }
};

struct OSToRootImpl
{
	static UInt8 apply(UInt8 x, const TechDataHierarchy & hierarchy) { return hierarchy.OSToMostAncestor(x); }
};

struct SEToRootImpl
{
	static UInt8 apply(UInt8 x, const TechDataHierarchy & hierarchy) { return hierarchy.SEToMostAncestor(x); }
};

struct RegionInImpl
{
	static bool apply(UInt32 x, UInt32 y, const RegionsHierarchy & hierarchy) { return hierarchy.in(x, y); }
};

struct OSInImpl
{
	static bool apply(UInt32 x, UInt32 y, const TechDataHierarchy & hierarchy) { return hierarchy.isOSIn(x, y); }
};

struct SEInImpl
{
	static bool apply(UInt32 x, UInt32 y, const TechDataHierarchy & hierarchy) { return hierarchy.isSEIn(x, y); }
};

struct RegionHierarchyImpl
{
	static UInt32 toParent(UInt32 x, const RegionsHierarchy & hierarchy) { return hierarchy.toParent(x); }
};

struct OSHierarchyImpl
{
	static UInt8 toParent(UInt8 x, const TechDataHierarchy & hierarchy) { return hierarchy.OSToParent(x); }
};

struct SEHierarchyImpl
{
	static UInt8 toParent(UInt8 x, const TechDataHierarchy & hierarchy) { return hierarchy.SEToParent(x); }
};


/// Преобразует идентификатор, используя словарь.
template <typename T, typename Transform, typename Dict, typename Name>
class FunctionTransformWithDictionary : public IFunction
{
private:
	const SharedPtr<Dict> owned_dict;
	
public:
	FunctionTransformWithDictionary(const SharedPtr<Dict> & owned_dict_)
		: owned_dict(owned_dict_)
	{
		if (!owned_dict)
			throw Exception("Dictionaries was not loaded. You need to check configuration file.", ErrorCodes::DICTIONARIES_WAS_NOT_LOADED);
	}
	
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

		if (arguments[0]->getName() != TypeName<T>::get())
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName()
				+ " (must be " + TypeName<T>::get() + ")",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return arguments[0];
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (const ColumnVector<T> * col_from = dynamic_cast<const ColumnVector<T> *>(&*block.getByPosition(arguments[0]).column))
		{
			ColumnVector<T> * col_to = new ColumnVector<T>;
			block.getByPosition(result).column = col_to;

			const typename ColumnVector<T>::Container_t & vec_from = col_from->getData();
			typename ColumnVector<T>::Container_t & vec_to = col_to->getData();
			size_t size = vec_from.size();
			vec_to.resize(size);

			const Dict & dict = *owned_dict;
			for (size_t i = 0; i < size; ++i)
				vec_to[i] = Transform::apply(vec_from[i], dict);
		}
		else if (const ColumnConst<T> * col_from = dynamic_cast<const ColumnConst<T> *>(&*block.getByPosition(arguments[0]).column))
		{
			block.getByPosition(result).column = new ColumnConst<T>(col_from->size(), Transform::apply(col_from->getData(), *owned_dict));
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of first argument of function " + Name::get(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


/// Проверяет принадлежность, используя словарь.
template <typename T, typename Transform, typename Dict, typename Name>
class FunctionIsInWithDictionary : public IFunction
{
private:
	const SharedPtr<Dict> owned_dict;

public:
	FunctionIsInWithDictionary(const SharedPtr<Dict> & owned_dict_)
		: owned_dict(owned_dict_)
	{
		if (!owned_dict)
			throw Exception("Dictionaries was not loaded. You need to check configuration file.", ErrorCodes::DICTIONARIES_WAS_NOT_LOADED);
	}

	/// Получить имя функции.
	String getName() const
	{
		return Name::get();
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ Poco::NumberFormatter::format(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (arguments[0]->getName() != TypeName<T>::get())
			throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName()
				+ " (must be " + TypeName<T>::get() + ")",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (arguments[1]->getName() != TypeName<T>::get())
			throw Exception("Illegal type " + arguments[1]->getName() + " of second argument of function " + getName()
				+ " (must be " + TypeName<T>::get() + ")",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeUInt8;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const ColumnVector<T> * col_vec1 = dynamic_cast<const ColumnVector<T> *>(&*block.getByPosition(arguments[0]).column);
		const ColumnVector<T> * col_vec2 = dynamic_cast<const ColumnVector<T> *>(&*block.getByPosition(arguments[1]).column);
		const ColumnConst<T> * col_const1 = dynamic_cast<const ColumnConst<T> *>(&*block.getByPosition(arguments[0]).column);
		const ColumnConst<T> * col_const2 = dynamic_cast<const ColumnConst<T> *>(&*block.getByPosition(arguments[1]).column);
		
		if (col_vec1 && col_vec2)
		{
			ColumnVector<UInt8> * col_to = new ColumnVector<UInt8>;
			block.getByPosition(result).column = col_to;

			const typename ColumnVector<T>::Container_t & vec_from1 = col_vec1->getData();
			const typename ColumnVector<T>::Container_t & vec_from2 = col_vec2->getData();
			typename ColumnVector<UInt8>::Container_t & vec_to = col_to->getData();
			size_t size = vec_from1.size();
			vec_to.resize(size);

			const Dict & dict = *owned_dict;
			for (size_t i = 0; i < size; ++i)
				vec_to[i] = Transform::apply(vec_from1[i], vec_from2[i], dict);
		}
		else if (col_vec1 && col_const2)
		{
			ColumnVector<UInt8> * col_to = new ColumnVector<UInt8>;
			block.getByPosition(result).column = col_to;

			const typename ColumnVector<T>::Container_t & vec_from1 = col_vec1->getData();
			const T const_from2 = col_const2->getData();
			typename ColumnVector<UInt8>::Container_t & vec_to = col_to->getData();
			size_t size = vec_from1.size();
			vec_to.resize(size);

			const Dict & dict = *owned_dict;
			for (size_t i = 0; i < size; ++i)
				vec_to[i] = Transform::apply(vec_from1[i], const_from2, dict);
		}
		else if (col_const1 && col_vec2)
		{
			ColumnVector<UInt8> * col_to = new ColumnVector<UInt8>;
			block.getByPosition(result).column = col_to;

			const T const_from1 = col_const1->getData();
			const typename ColumnVector<T>::Container_t & vec_from2 = col_vec2->getData();
			typename ColumnVector<UInt8>::Container_t & vec_to = col_to->getData();
			size_t size = vec_from2.size();
			vec_to.resize(size);

			const Dict & dict = *owned_dict;
			for (size_t i = 0; i < size; ++i)
				vec_to[i] = Transform::apply(const_from1, vec_from2[i], dict);
		}
		else if (col_const1 && col_const2)
		{
			block.getByPosition(result).column = new ColumnConst<T>(col_const1->size(),
				Transform::apply(col_const1->getData(), col_const2->getData(), *owned_dict));
		}
		else
			throw Exception("Illegal columns " + block.getByPosition(arguments[0]).column->getName()
					+ " and " + block.getByPosition(arguments[1]).column->getName()
					+ " of arguments of function " + Name::get(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


/// Получает массив идентификаторов, состоящий из исходного и цепочки родителей.
template <typename T, typename Transform, typename Dict, typename Name>
class FunctionHierarchyWithDictionary : public IFunction
{
private:
	const SharedPtr<Dict> owned_dict;
	
public:
	FunctionHierarchyWithDictionary(const SharedPtr<Dict> & owned_dict_)
	: owned_dict(owned_dict_)
	{
		if (!owned_dict)
			throw Exception("Dictionaries was not loaded. You need to check configuration file.", ErrorCodes::DICTIONARIES_WAS_NOT_LOADED);
	}
	
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
		
		if (arguments[0]->getName() != TypeName<T>::get())
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName()
			+ " (must be " + TypeName<T>::get() + ")",
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		
		return new DataTypeArray(arguments[0]);
	}
	
	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		if (const ColumnVector<T> * col_from = dynamic_cast<const ColumnVector<T> *>(&*block.getByPosition(arguments[0]).column))
		{
			ColumnVector<T> * col_values = new ColumnVector<T>;
			ColumnArray * col_array = new ColumnArray(col_values);
			block.getByPosition(result).column = col_array;
			
			ColumnArray::Offsets_t & res_offsets = col_array->getOffsets();
			typename ColumnVector<T>::Container_t & res_values = col_values->getData();
			
			const typename ColumnVector<T>::Container_t & vec_from = col_from->getData();
			size_t size = vec_from.size();
			res_offsets.resize(size);
			res_values.reserve(size * 4);
			
			const Dict & dict = *owned_dict;
			for (size_t i = 0; i < size; ++i)
			{
				T cur = vec_from[i];
				while (cur)
				{
					res_values.push_back(cur);
					cur = Transform::toParent(cur, dict);
				}
				res_offsets[i] = res_values.size();
			}
		}
		else if (const ColumnConst<T> * col_from = dynamic_cast<const ColumnConst<T> *>(&*block.getByPosition(arguments[0]).column))
		{
			Array res;
			
			const Dict & dict = *owned_dict;
			T cur = col_from->getData();
			while (cur)
			{
				res.push_back(static_cast<typename NearestFieldType<T>::Type>(cur));
				cur = Transform::toParent(cur, dict);
			}
			
			block.getByPosition(result).column = new ColumnConstArray(col_from->size(), res, new DataTypeArray(new typename DataTypeFromFieldType<T>::Type));
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
			+ " of first argument of function " + Name::get(),
							ErrorCodes::ILLEGAL_COLUMN);
	}
};


struct NameRegionToCity 	{ static const char * get() { return "regionToCity"; } };
struct NameRegionToArea 	{ static const char * get() { return "regionToArea"; } };
struct NameRegionToCountry 	{ static const char * get() { return "regionToCountry"; } };
struct NameOSToRoot 		{ static const char * get() { return "OSToRoot"; } };
struct NameSEToRoot 		{ static const char * get() { return "SEToRoot"; } };

struct NameRegionIn 		{ static const char * get() { return "regionIn"; } };
struct NameOSIn 			{ static const char * get() { return "OSIn"; } };
struct NameSEIn 			{ static const char * get() { return "SEIn"; } };

struct NameRegionHierarchy	{ static const char * get() { return "regionHierarchy"; } };
struct NameOSHierarchy	{ static const char * get() { return "OSHierarchy"; } };
struct NameSEHierarchy	{ static const char * get() { return "SEHierarchy"; } };


typedef FunctionTransformWithDictionary<UInt32,	RegionToCityImpl, RegionsHierarchy, NameRegionToCity> 		FunctionRegionToCity;
typedef FunctionTransformWithDictionary<UInt32,	RegionToAreaImpl, RegionsHierarchy, NameRegionToArea> 		FunctionRegionToArea;
typedef FunctionTransformWithDictionary<UInt32,	RegionToCountryImpl, RegionsHierarchy, NameRegionToCountry> FunctionRegionToCountry;
typedef FunctionTransformWithDictionary<UInt8,	OSToRootImpl, TechDataHierarchy, NameOSToRoot> 				FunctionOSToRoot;
typedef FunctionTransformWithDictionary<UInt8,	SEToRootImpl, TechDataHierarchy, NameSEToRoot> 				FunctionSEToRoot;

typedef FunctionIsInWithDictionary<UInt32,	RegionInImpl, RegionsHierarchy, NameRegionIn> 						FunctionRegionIn;
typedef FunctionIsInWithDictionary<UInt8,	OSInImpl, TechDataHierarchy, NameOSIn> 							FunctionOSIn;
typedef FunctionIsInWithDictionary<UInt8,	SEInImpl, TechDataHierarchy, NameSEIn> 							FunctionSEIn;

typedef FunctionHierarchyWithDictionary<UInt32, RegionHierarchyImpl, RegionsHierarchy, NameRegionHierarchy>	FunctionRegionHierarchy;
typedef FunctionHierarchyWithDictionary<UInt8, OSHierarchyImpl, TechDataHierarchy, NameOSHierarchy>			FunctionOSHierarchy;
typedef FunctionHierarchyWithDictionary<UInt8, SEHierarchyImpl, TechDataHierarchy, NameSEHierarchy>			FunctionSEHierarchy;

}
