#include <DB/Core/Field.h>

#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnsNumber.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/OneBlockInputStream.h>

#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTLiteral.h>

#include <DB/Interpreters/Set.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>


namespace DB
{

void SetVariants::init(Type type_)
{
	type = type_;

	switch (type)
	{
		case Type::EMPTY: break;

	#define M(NAME) \
		case Type::NAME: NAME.reset(new decltype(NAME)::element_type); break;
		APPLY_FOR_SET_VARIANTS(M)
	#undef M

		default:
			throw Exception("Unknown Set variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
	}
}


size_t SetVariants::getTotalRowCount() const
{
	switch (type)
	{
		case Type::EMPTY: return 0;

	#define M(NAME) \
		case Type::NAME: return NAME->data.size();
		APPLY_FOR_SET_VARIANTS(M)
	#undef M

		default:
			throw Exception("Unknown Set variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
	}
}


size_t SetVariants::getTotalByteCount() const
{
	switch (type)
	{
		case Type::EMPTY: return 0;

	#define M(NAME) \
		case Type::NAME: return NAME->data.getBufferSizeInBytes();
		APPLY_FOR_SET_VARIANTS(M)
	#undef M

		default:
			throw Exception("Unknown Set variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
	}
}


bool Set::checkSetSizeLimits() const
{
	if (max_rows && data.getTotalRowCount() > max_rows)
		return false;
	if (max_bytes && data.getTotalByteCount() > max_bytes)
		return false;
	return true;
}


SetVariants::Type SetVariants::chooseMethod(const ConstColumnPlainPtrs & key_columns, Sizes & key_sizes)
{
	size_t keys_size = key_columns.size();

	bool all_fixed = true;
	size_t keys_bytes = 0;
	key_sizes.resize(keys_size);
	for (size_t j = 0; j < keys_size; ++j)
	{
		if (!key_columns[j]->isFixed())
		{
			all_fixed = false;
			break;
		}
		key_sizes[j] = key_columns[j]->sizeOfField();
		keys_bytes += key_sizes[j];
	}

	/// Если есть один числовой ключ, который помещается в 64 бита
	if (keys_size == 1 && key_columns[0]->isNumeric())
	{
		size_t size_of_field = key_columns[0]->sizeOfField();
		if (size_of_field == 1)
			return SetVariants::Type::key8;
		if (size_of_field == 2)
			return SetVariants::Type::key16;
		if (size_of_field == 4)
			return SetVariants::Type::key32;
		if (size_of_field == 8)
			return SetVariants::Type::key64;
		throw Exception("Logical error: numeric column has sizeOfField not in 1, 2, 4, 8.", ErrorCodes::LOGICAL_ERROR);
	}

	/// Если ключи помещаются в N бит, будем использовать хэш-таблицу по упакованным в N-бит ключам
	if (all_fixed && keys_bytes <= 16)
		return SetVariants::Type::keys128;
	if (all_fixed && keys_bytes <= 32)
		return SetVariants::Type::keys256;

	/// Если есть один строковый ключ, то используем хэш-таблицу с ним
	if (keys_size == 1 && (typeid_cast<const ColumnString *>(key_columns[0]) || typeid_cast<const ColumnConstString *>(key_columns[0])))
		return SetVariants::Type::key_string;

	if (keys_size == 1 && typeid_cast<const ColumnFixedString *>(key_columns[0]))
		return SetVariants::Type::key_fixed_string;

	/// Иначе будем агрегировать по конкатенации ключей.
	return SetVariants::Type::hashed;
}


template <typename Method>
void NO_INLINE Set::insertFromBlockImpl(
	Method & method,
	const ConstColumnPlainPtrs & key_columns,
	size_t rows,
	SetVariants & variants)
{
	typename Method::State state;
	state.init(key_columns);
	size_t keys_size = key_columns.size();

	/// Для всех строчек
	for (size_t i = 0; i < rows; ++i)
	{
		/// Строим ключ
		typename Method::Key key = state.getKey(key_columns, keys_size, i, key_sizes);

		typename Method::Data::iterator it = method.data.find(key);
		bool inserted;
		method.data.emplace(key, it, inserted);

		if (inserted)
			method.onNewKey(*it, keys_size, i, variants.string_pool);
	}
}


bool Set::insertFromBlock(const Block & block, bool create_ordered_set)
{
	Poco::ScopedWriteRWLock lock(rwlock);

	size_t keys_size = block.columns();
	ConstColumnPlainPtrs key_columns(keys_size);
	data_types.resize(keys_size);

	/// Константные столбцы справа от IN поддерживается не напрямую. Для этого, они сначала материализуется.
	Columns materialized_columns;

	/// Запоминаем столбцы, с которыми будем работать
	for (size_t i = 0; i < keys_size; ++i)
	{
		key_columns[i] = block.getByPosition(i).column;
		data_types[i] = block.getByPosition(i).type;

		if (key_columns[i]->isConst())
		{
			materialized_columns.emplace_back(static_cast<const IColumnConst *>(key_columns[i])->convertToFullColumn());
			key_columns[i] = materialized_columns.back().get();
		}
	}

	size_t rows = block.rows();

	/// Какую структуру данных для множества использовать?
	if (empty())
		data.init(data.chooseMethod(key_columns, key_sizes));

	if (false) {}
#define M(NAME) \
	else if (data.type == SetVariants::Type::NAME) \
		insertFromBlockImpl(*data.NAME, key_columns, rows, data);
		APPLY_FOR_SET_VARIANTS(M)
#undef M
	else
		throw Exception("Unknown set variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);

	if (create_ordered_set)
		for (size_t i = 0; i < rows; ++i)
			ordered_set_elements->push_back((*key_columns[0])[i]); /// ordered_set для индекса работает только если IN одному ключу.

	if (!checkSetSizeLimits())
	{
		if (overflow_mode == OverflowMode::THROW)
			throw Exception("IN-set size exceeded."
				" Rows: " + toString(data.getTotalRowCount()) +
				", limit: " + toString(max_rows) +
				". Bytes: " + toString(data.getTotalByteCount()) +
				", limit: " + toString(max_bytes) + ".",
				ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);

		if (overflow_mode == OverflowMode::BREAK)
			return false;

		throw Exception("Logical error: unknown overflow mode", ErrorCodes::LOGICAL_ERROR);
	}

	return true;
}


/** Чтобы корректно работали выражения вида 1.0 IN (1).
  * Проверяет совместимость типов, проверяет попадание значений в диапазон допустимых значений типа, делает преобразование типа.
  * Код слегка дурацкий.
  */
static Field convertToType(const Field & src, const IDataType & type)
{
	if (type.behavesAsNumber())
	{
		bool is_uint8 	= false;
		bool is_uint16 	= false;
		bool is_uint32 	= false;
		bool is_uint64 	= false;
		bool is_int8 	= false;
		bool is_int16 	= false;
		bool is_int32 	= false;
		bool is_int64 	= false;
		bool is_float32 = false;
		bool is_float64 = false;

		false
			|| (is_uint8 	= typeid_cast<const DataTypeUInt8 *		>(&type))
			|| (is_uint16 	= typeid_cast<const DataTypeUInt16 *	>(&type))
			|| (is_uint32 	= typeid_cast<const DataTypeUInt32 *	>(&type))
			|| (is_uint64 	= typeid_cast<const DataTypeUInt64 *	>(&type))
			|| (is_int8 	= typeid_cast<const DataTypeInt8 *		>(&type))
			|| (is_int16 	= typeid_cast<const DataTypeInt16 *		>(&type))
			|| (is_int32 	= typeid_cast<const DataTypeInt32 *		>(&type))
			|| (is_int64 	= typeid_cast<const DataTypeInt64 *		>(&type))
			|| (is_float32 	= typeid_cast<const DataTypeFloat32 *	>(&type))
			|| (is_float64 	= typeid_cast<const DataTypeFloat64 *	>(&type));

		if (is_uint8 || is_uint16 || is_uint32 || is_uint64)
		{
			if (src.getType() == Field::Types::Int64)
				throw Exception("Type mismatch in IN section: " + type.getName() + " at left, signed literal at right");

			if (src.getType() == Field::Types::Float64)
				throw Exception("Type mismatch in IN section: " + type.getName() + " at left, floating point literal at right");

			if (src.getType() == Field::Types::UInt64)
			{
				UInt64 value = src.get<const UInt64 &>();
				if ((is_uint8 && value > std::numeric_limits<uint8_t>::max())
					|| (is_uint16 && value > std::numeric_limits<uint16_t>::max())
					|| (is_uint32 && value > std::numeric_limits<uint32_t>::max()))
					throw Exception("Value (" + toString(value) + ") in IN section is out of range of type " + type.getName() + " at left");

				return src;
			}

			throw Exception("Type mismatch in IN section: " + type.getName() + " at left, "
				+ Field::Types::toString(src.getType()) + " literal at right");
		}
		else if (is_int8 || is_int16 || is_int32 || is_int64)
		{
			if (src.getType() == Field::Types::Float64)
				throw Exception("Type mismatch in IN section: " + type.getName() + " at left, floating point literal at right");

			if (src.getType() == Field::Types::UInt64)
			{
				UInt64 value = src.get<const UInt64 &>();

				if ((is_int8 && value > uint8_t(std::numeric_limits<int8_t>::max()))
					|| (is_int16 && value > uint16_t(std::numeric_limits<int16_t>::max()))
					|| (is_int32 && value > uint32_t(std::numeric_limits<int32_t>::max()))
					|| (is_int64 && value > uint64_t(std::numeric_limits<int64_t>::max())))
					throw Exception("Value (" + toString(value) + ") in IN section is out of range of type " + type.getName() + " at left");

				return Field(Int64(value));
			}

			if (src.getType() == Field::Types::Int64)
			{
				Int64 value = src.get<const Int64 &>();
				if ((is_int8 && (value < std::numeric_limits<int8_t>::min() || value > std::numeric_limits<int8_t>::max()))
					|| (is_int16 && (value < std::numeric_limits<int16_t>::min() || value > std::numeric_limits<int16_t>::max()))
					|| (is_int32 && (value < std::numeric_limits<int32_t>::min() || value > std::numeric_limits<int32_t>::max())))
					throw Exception("Value (" + toString(value) + ") in IN section is out of range of type " + type.getName() + " at left");

				return src;
			}

			throw Exception("Type mismatch in IN section: " + type.getName() + " at left, "
				+ Field::Types::toString(src.getType()) + " literal at right");
		}
		else if (is_float32 || is_float64)
		{
			if (src.getType() == Field::Types::UInt64)
				return Field(Float64(src.get<UInt64>()));

			if (src.getType() == Field::Types::Int64)
				return Field(Float64(src.get<Int64>()));

			if (src.getType() == Field::Types::Float64)
				return src;

			throw Exception("Type mismatch in IN section: " + type.getName() + " at left, "
				+ Field::Types::toString(src.getType()) + " literal at right");
		}
	}

	/// В остальных случаях, приведение типа не осуществляется.
	return src;
}


void Set::createFromAST(DataTypes & types, ASTPtr node, bool create_ordered_set)
{
	/** NOTE:
	  * На данный момент в секции IN не поддерживаются выражения (вызовы функций), кроме кортежей.
	  * То есть, например, не поддерживаются массивы. А по хорошему, хотелось бы поддерживать.
	  * Для этого можно сделать constant folding с помощью ExpressionAnalyzer/ExpressionActions.
	  * Но при этом, конечно же, не забыть про производительность работы с крупными множествами.
	  */

	data_types = types;

	/// Засунем множество в блок.
	Block block;
	for (size_t i = 0, size = data_types.size(); i < size; ++i)
	{
		ColumnWithNameAndType col;
		col.type = data_types[i];
		col.column = data_types[i]->createColumn();
		col.name = "_" + toString(i);

		block.insert(col);
	}

	ASTExpressionList & list = typeid_cast<ASTExpressionList &>(*node);
	for (ASTs::iterator it = list.children.begin(); it != list.children.end(); ++it)
	{
		if (data_types.size() == 1)
		{
			if (ASTLiteral * lit = typeid_cast<ASTLiteral *>(&**it))
				block.getByPosition(0).column->insert(convertToType(lit->value, *data_types[0]));
			else
				throw Exception("Incorrect element of set. Must be literal.", ErrorCodes::INCORRECT_ELEMENT_OF_SET);
		}
		else if (ASTFunction * func = typeid_cast<ASTFunction *>(&**it))
		{
			if (func->name != "tuple")
				throw Exception("Incorrect element of set. Must be tuple.", ErrorCodes::INCORRECT_ELEMENT_OF_SET);

			size_t tuple_size = func->arguments->children.size();
			if (tuple_size != data_types.size())
				throw Exception("Incorrect size of tuple in set.", ErrorCodes::INCORRECT_ELEMENT_OF_SET);

			for (size_t j = 0; j < tuple_size; ++j)
			{
				if (ASTLiteral * lit = typeid_cast<ASTLiteral *>(&*func->arguments->children[j]))
					block.getByPosition(j).column->insert(convertToType(lit->value, *data_types[j]));
				else
					throw Exception("Incorrect element of tuple in set. Must be literal.", ErrorCodes::INCORRECT_ELEMENT_OF_SET);
			}
		}
		else
			throw Exception("Incorrect element of set", ErrorCodes::INCORRECT_ELEMENT_OF_SET);

		/// NOTE: Потом можно реализовать возможность задавать константные выражения в множествах.
	}

	if (create_ordered_set)
		ordered_set_elements = OrderedSetElementsPtr(new OrderedSetElements());

	insertFromBlock(block, create_ordered_set);

	if (create_ordered_set)
		std::sort(ordered_set_elements->begin(), ordered_set_elements->end());
}


void Set::execute(Block & block, const ColumnNumbers & arguments, size_t result, bool negative) const
{
	ColumnUInt8 * c_res = new ColumnUInt8;
	block.getByPosition(result).column = c_res;
	ColumnUInt8::Container_t & vec_res = c_res->getData();
	vec_res.resize(block.getByPosition(arguments[0]).column->size());

	Poco::ScopedReadRWLock lock(rwlock);

	/// Если множество пусто
	if (data_types.empty())
	{
		if (negative)
			memset(&vec_res[0], 1, vec_res.size());
		else
			memset(&vec_res[0], 0, vec_res.size());
		return;
	}

	DataTypeArray * array_type = typeid_cast<DataTypeArray *>(&*block.getByPosition(arguments[0]).type);

	if (array_type)
	{
		if (data_types.size() != 1 || arguments.size() != 1)
			throw Exception("Number of columns in section IN doesn't match.", ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);
		if (array_type->getNestedType()->getName() != data_types[0]->getName())
			throw Exception(std::string() + "Types in section IN don't match: " + data_types[0]->getName() + " on the right, " + array_type->getNestedType()->getName() + " on the left.", ErrorCodes::TYPE_MISMATCH);

		IColumn * in_column = &*block.getByPosition(arguments[0]).column;

		/// Константный столбец слева от IN поддерживается не напрямую. Для этого, он сначала материализуется.
		ColumnPtr materialized_column;
		if (in_column->isConst())
		{
			materialized_column = static_cast<const IColumnConst *>(in_column)->convertToFullColumn();
			in_column = materialized_column.get();
		}

		if (ColumnArray * col = typeid_cast<ColumnArray *>(in_column))
			executeArray(col, vec_res, negative);
		else
			throw Exception("Unexpected array column type: " + in_column->getName(), ErrorCodes::ILLEGAL_COLUMN);
	}
	else
	{
		if (data_types.size() != arguments.size())
			throw Exception("Number of columns in section IN doesn't match.", ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);

		/// Запоминаем столбцы, с которыми будем работать. Также проверим, что типы данных правильные.
		ConstColumnPlainPtrs key_columns(arguments.size());
		for (size_t i = 0; i < arguments.size(); ++i)
		{
			key_columns[i] = block.getByPosition(arguments[i]).column;

			if (data_types[i]->getName() != block.getByPosition(arguments[i]).type->getName())
				throw Exception("Types of column " + toString(i + 1) + " in section IN don't match: " + data_types[i]->getName() + " on the right, " + block.getByPosition(arguments[i]).type->getName() + " on the left.", ErrorCodes::TYPE_MISMATCH);
		}

		/// Константные столбцы слева от IN поддерживается не напрямую. Для этого, они сначала материализуется.
		Columns materialized_columns;
		for (auto & column_ptr : key_columns)
		{
			if (column_ptr->isConst())
			{
				materialized_columns.emplace_back(static_cast<const IColumnConst *>(column_ptr)->convertToFullColumn());
				column_ptr = materialized_columns.back().get();
			}
		}

		executeOrdinary(key_columns, vec_res, negative);
	}
}


template <typename Method>
void NO_INLINE Set::executeImpl(
	Method & method,
	const ConstColumnPlainPtrs & key_columns,
	ColumnUInt8::Container_t & vec_res,
	bool negative,
	size_t rows) const
{
	typename Method::State state;
	state.init(key_columns);
	size_t keys_size = key_columns.size();

	/// NOTE Не используется оптимизация для подряд идущих одинаковых значений.

	/// Для всех строчек
	for (size_t i = 0; i < rows; ++i)
	{
		/// Строим ключ
		typename Method::Key key = state.getKey(key_columns, keys_size, i, key_sizes);
		vec_res[i] = negative ^ (method.data.end() != method.data.find(key));
	}
}

template <typename Method>
void NO_INLINE Set::executeArrayImpl(
	Method & method,
	const ConstColumnPlainPtrs & key_columns,
	const ColumnArray::Offsets_t & offsets,
	ColumnUInt8::Container_t & vec_res,
	bool negative,
	size_t rows) const
{
	typename Method::State state;
	state.init(key_columns);
	size_t keys_size = key_columns.size();

	size_t prev_offset = 0;
	/// Для всех строчек
	for (size_t i = 0; i < rows; ++i)
	{
		UInt8 res = 0;
		/// Для всех элементов
		for (size_t j = prev_offset; j < offsets[i]; ++j)
		{
			/// Строим ключ
			typename Method::Key key = state.getKey(key_columns, keys_size, j, key_sizes);
			res |= negative ^ (method.data.end() != method.data.find(key));
			if (res)
				break;
		}
		vec_res[i] = res;
		prev_offset = offsets[i];
	}
}


void Set::executeOrdinary(const ConstColumnPlainPtrs & key_columns, ColumnUInt8::Container_t & vec_res, bool negative) const
{
	size_t rows = key_columns[0]->size();

	if (false) {}
#define M(NAME) \
	else if (data.type == SetVariants::Type::NAME) \
		executeImpl(*data.NAME, key_columns, vec_res, negative, rows);
	APPLY_FOR_SET_VARIANTS(M)
#undef M
	else
		throw Exception("Unknown set variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
}

void Set::executeArray(const ColumnArray * key_column, ColumnUInt8::Container_t & vec_res, bool negative) const
{
	size_t rows = key_column->size();
	const ColumnArray::Offsets_t & offsets = key_column->getOffsets();
	const IColumn & nested_column = key_column->getData();

	if (false) {}
#define M(NAME) \
	else if (data.type == SetVariants::Type::NAME) \
		executeArrayImpl(*data.NAME, ConstColumnPlainPtrs{&nested_column}, offsets, vec_res, negative, rows);
	APPLY_FOR_SET_VARIANTS(M)
#undef M
	else
		throw Exception("Unknown set variant.", ErrorCodes::UNKNOWN_SET_DATA_VARIANT);
}


BoolMask Set::mayBeTrueInRange(const Range & range) const
{
	if (!ordered_set_elements)
		throw DB::Exception("Ordered set in not created.");

	if (ordered_set_elements->empty())
		return BoolMask(false, true);

	const Field & left = range.left;
	const Field & right = range.right;

	bool can_be_true;
	bool can_be_false = true;

	/// Если во всем диапазоне одинаковый ключ и он есть в Set, то выбираем блок для in и не выбираем для notIn
	if (range.left_bounded && range.right_bounded && range.right_included && range.left_included && left == right)
	{
		if (std::binary_search(ordered_set_elements->begin(), ordered_set_elements->end(), left))
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
		auto left_it = range.left_bounded
			? std::lower_bound(ordered_set_elements->begin(), ordered_set_elements->end(), left)
			: ordered_set_elements->begin();

		if (range.left_bounded && !range.left_included && left_it != ordered_set_elements->end() && *left_it == left)
			++left_it;

		/// если весь диапазон, правее in
		if (left_it == ordered_set_elements->end())
		{
			can_be_true = false;
		}
		else
		{
			auto right_it = range.right_bounded
				? std::upper_bound(ordered_set_elements->begin(), ordered_set_elements->end(), right)
				: ordered_set_elements->end();

			if (range.right_bounded && !range.right_included && right_it != ordered_set_elements->begin() && *(right_it--) == right)
				--right_it;

			/// весь диапазон, левее in
			if (right_it == ordered_set_elements->begin())
			{
				can_be_true = false;
			}
			else
			{
				--right_it;
				/// в диапазон не попадает ни одного ключа из in
				if (*right_it < *left_it)
					can_be_true = false;
				else
					can_be_true = true;
			}
		}
	}

	return BoolMask(can_be_true, can_be_false);
}

}
