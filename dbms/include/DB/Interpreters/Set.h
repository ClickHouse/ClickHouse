#pragma once

#include <set>
#include <boost/concept_check.hpp>

#include <statdaemons/Stopwatch.h>

#include <Yandex/logger_useful.h>

#include <DB/Core/ColumnNumbers.h>
#include <DB/Common/Arena.h>

#include <DB/DataStreams/IBlockInputStream.h>

#include <DB/Parsers/IAST.h>

#include <DB/Interpreters/HashSet.h>
#include <DB/Interpreters/AggregationCommon.h>
#include <DB/Interpreters/Limits.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnArray.h>

#include <DB/Storages/MergeTree/BoolMask.h>
#include <DB/Storages/MergeTree/PKCondition.h>

namespace DB
{


/** Структура данных для реализации выражения IN.
  */
class Set
{
public:
	Set(const Limits & limits)
		: type(EMPTY), log(&Logger::get("Set")),
		max_rows(limits.max_rows_in_set),
		max_bytes(limits.max_bytes_in_set),
		overflow_mode(limits.set_overflow_mode)
	{
	}
	
	bool empty() { return type == EMPTY; }

	/** Создать множество по выражению (для перечисления в самом запросе).
	  * types - типы того, что стоит слева от IN.
	  * node - это список значений: 1, 2, 3 или список tuple-ов: (1, 2), (3, 4), (5, 6).
	  */
	void createFromAST(DataTypes & types, ASTPtr node);

	/** Запомнить поток блоков (для подзапросов), чтобы потом его можно было прочитать и создать множество.
	  */
	void setSource(BlockInputStreamPtr stream) { source = stream; }

	BlockInputStreamPtr getSource() { return source; }

	// Возвращает false, если превышено какое-нибудь ограничение, и больше не нужно вставлять.
	bool insertFromBlock(Block & block);

	size_t size() const { return getTotalRowCount(); }

	/** Для указанных столбцов блока проверить принадлежность их значений множеству.
	  * Записать результат в столбец в позиции result.
	  */
	void execute(Block & block, const ColumnNumbers & arguments, size_t result, bool negative) const;

	std::string descibe()
	{
		if (type == KEY_64)
			return setToString(key64);
		else if (type == KEY_STRING)
			return setToString(key_string);
		else if (type == HASHED)
			return "{hashed values}";
		else if (type == EMPTY)
			return "{}";
		else
			throw DB::Exception("Unknown type");
	}
	
	void createOrderedSet()
	{
		for (auto & key : key64)
			ordered_key64.push_back(key);
		std::sort(ordered_key64.begin(), ordered_key64.end());

		for (auto & key : key_string)
			ordered_string.push_back(key);
		std::sort(ordered_string.begin(), ordered_string.end());
	}

	BoolMask mayBeTrueInRange(const Field & left, const Field & right)
	{
		if (type == KEY_64)
			return mayBeTrueInRangeImpl(left, right, ordered_key64);
		else if (type == KEY_STRING)
			return mayBeTrueInRangeImpl(left, right, ordered_string);
		else{
			std::stringstream ss;
			ss << "Unsupported set of type " << type;
			throw DB::Exception(ss.str());
		}
	}
private:
	/** Разные структуры данных, которые могут использоваться для проверки принадлежности
	  *  одного или нескольких столбцов значений множеству.
	  */
	typedef HashSet<UInt64> SetUInt64;
	typedef HashSet<StringRef, StringRefHash, StringRefZeroTraits> SetString;
	typedef HashSet<UInt128, UInt128Hash, UInt128ZeroTraits> SetHashed;

	BlockInputStreamPtr source;

	/// Специализация для случая, когда есть один числовой ключ.
	SetUInt64 key64;

	/// Специализация для случая, когда есть один строковый ключ.
	SetString key_string;
	Arena string_pool;

	/** Сравнивает 128 битные хэши.
	  * Если все ключи фиксированной длины, влезающие целиком в 128 бит, то укладывает их без изменений в 128 бит.
	  * Иначе - вычисляет SipHash от набора из всех ключей.
	  * (При этом, строки, содержащие нули посередине, могут склеиться.)
	  */
	SetHashed hashed;

	enum Type
	{
		EMPTY 		= 0,
		KEY_64		= 1,
		KEY_STRING	= 2,
		HASHED		= 3,
	};
	Type type;
	
	bool keys_fit_128_bits;
	Sizes key_sizes;

	/** Типы данных, из которых было создано множество.
	  * При проверке на принадлежность множеству, типы проверяемых столбцов должны с ними совпадать.
	  */
	DataTypes data_types;
	
	Logger * log;
	
	/// Ограничения на максимальный размер множества
	size_t max_rows;
	size_t max_bytes;
	OverflowMode overflow_mode;
	
	static Type chooseMethod(const ConstColumnPlainPtrs & key_columns, bool & keys_fit_128_bits, Sizes & key_sizes);

	/// Если в левой части IN стоит массив. Проверяем, что хоть один элемент массива лежит в множестве.
	void executeConstArray(const ColumnConstArray * key_column, ColumnUInt8::Container_t & vec_res, bool negative) const;
	void executeArray(const ColumnArray * key_column, ColumnUInt8::Container_t & vec_res, bool negative) const;
	
	/// Если в левой части набор столбцов тех же типов, что элементы множества.
	void executeOrdinary(const ConstColumnPlainPtrs & key_columns, ColumnUInt8::Container_t & vec_res, bool negative) const;
	
	/// Проверить не превышены ли допустимые размеры множества ключей
	bool checkSetSizeLimits() const;
	
	/// Считает суммарное число ключей во всех Set'ах
	size_t getTotalRowCount() const;
	/// Считает суммарный размер в байтах буфферов всех Set'ов + размер string_pool'а
	size_t getTotalByteCount() const;

	template <typename T>
	std::string setToString(const T & set)
	{
		std::stringstream ss;
		bool first = false;

		for (auto it = set.begin(); it != set.end(); ++it)
		{
			if (first)
			{
				ss << *it;
				first = false;
			}
			else
			{
				ss << ", " << *it;
			}
		}

		ss << "}";
		return ss.str();
	}

	/// несколько столбцов пока не поддерживаем
	std::vector<UInt64> ordered_key64;
	std::vector<StringRef> ordered_string;

	template <class T>
	BoolMask mayBeTrueInRangeImpl(const Field & field_left, const Field & field_right, const std::vector<T> & v)
	{
		T left = field_left.get<T>();
		T right = field_right.get<T>();

		bool can_be_true;
		bool can_be_false = true;

		/// Если во всем диапазоне одинаковый ключ и он есть в Set, то выбираем блок для in и не выбираем для notIn
		if (left == right)
		{
			if (std::find(v.begin(), v.end(), left) != v.end())
			{
				can_be_false = false;
				can_be_true = true;
			}
			else
			{
				can_be_true = false;
				can_be_false = true;
			}
		}
		else
		{
			auto left_it = std::lower_bound(v.begin(), v.end(), left);
			/// если весь диапазон, правее in
			if (left_it == v.end())
			{
				can_be_true = false;
			}
			else
			{
				auto right_it = std::upper_bound(v.begin(), v.end(), right);
				/// весь диапазон, левее in
				if (right_it == v.begin())
				{
					can_be_true = false;
				}
				else
				{
					--right_it;
					/// в диапазон не попадает ни одного ключа из in
					if (*right_it < *left_it)
					{
						can_be_true = false;
					}
					else
					{
						can_be_true = true;
					}
				}
			}
		}

		return BoolMask(can_be_true, can_be_false);
	}

};

typedef Poco::SharedPtr<Set> SetPtr;
typedef std::vector<SetPtr> Sets;


}
