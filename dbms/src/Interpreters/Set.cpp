#include <DB/Core/Field.h>
#include <DB/Core/FieldVisitors.h>

#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnsNumber.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/OneBlockInputStream.h>

#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTLiteral.h>

#include <DB/Interpreters/Set.h>
#include <DB/Interpreters/ExpressionAnalyzer.h>
#include <DB/Interpreters/ExpressionActions.h>

#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataTypes/DataTypeEnum.h>


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

		if (auto converted = key_columns[i]->convertToFullColumnIfConst())
		{
			materialized_columns.emplace_back(converted);
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


/** Проверка попадания Field from, имеющим тип From в диапазон значений типа To.
  * From и To - числовые типы. Могут быть типами с плавающей запятой.
  * From - это одно из UInt64, Int64, Float64,
  *  тогда как To может быть также 8, 16, 32 битным.
  *
  * Если попадает в диапазон, то from конвертируется в Field ближайшего к To типа.
  * Если не попадает - возвращается Field(Null).
  */

template <typename From, typename To>
static Field convertNumericTypeImpl(const Field & from)
{
	From value = from.get<From>();

	if (static_cast<long double>(value) != static_cast<long double>(To(value)))
		return {};

	return Field(typename NearestFieldType<To>::Type(value));
}

template <typename To>
static Field convertNumericType(const Field & from, const IDataType & type)
{
	if (from.getType() == Field::Types::UInt64)
		return convertNumericTypeImpl<UInt64, To>(from);
	if (from.getType() == Field::Types::Int64)
		return convertNumericTypeImpl<Int64, To>(from);
	if (from.getType() == Field::Types::Float64)
		return convertNumericTypeImpl<Float64, To>(from);

	throw Exception("Type mismatch in IN section: " + type.getName() + " at left, "
		+ Field::Types::toString(from.getType()) + " at right", ErrorCodes::TYPE_MISMATCH);
}


/** Чтобы корректно работали выражения вида 1.0 IN (1) или чтобы 1 IN (1, 2.0, 2.5, -1) работало так же, как 1 IN (1, 2).
  * Проверяет совместимость типов, проверяет попадание значений в диапазон допустимых значений типа, делает преобразование типа.
  * Если значение не попадает в диапазон - возвращает Null.
  */
static Field convertToType(const Field & src, const IDataType & type)
{
	if (type.isNumeric())
	{
		if (typeid_cast<const DataTypeUInt8 *>(&type))		return convertNumericType<UInt8>(src, type);
		if (typeid_cast<const DataTypeUInt16 *>(&type))		return convertNumericType<UInt16>(src, type);
		if (typeid_cast<const DataTypeUInt32 *>(&type))		return convertNumericType<UInt32>(src, type);
		if (typeid_cast<const DataTypeUInt64 *>(&type))		return convertNumericType<UInt64>(src, type);
		if (typeid_cast<const DataTypeInt8 *>(&type))		return convertNumericType<Int8>(src, type);
		if (typeid_cast<const DataTypeInt16 *>(&type))		return convertNumericType<Int16>(src, type);
		if (typeid_cast<const DataTypeInt32 *>(&type))		return convertNumericType<Int32>(src, type);
		if (typeid_cast<const DataTypeInt64 *>(&type))		return convertNumericType<Int64>(src, type);
		if (typeid_cast<const DataTypeFloat32 *>(&type))	return convertNumericType<Float32>(src, type);
		if (typeid_cast<const DataTypeFloat64 *>(&type))	return convertNumericType<Float64>(src, type);

		const bool is_date = typeid_cast<const DataTypeDate *>(&type);
		bool is_datetime = false;
		bool is_enum8 = false;
		bool is_enum16 = false;

		if (!is_date)
			if (!(is_datetime = typeid_cast<const DataTypeDateTime *>(&type)))
				if (!(is_enum8 = typeid_cast<const DataTypeEnum8 *>(&type)))
					if (!(is_enum16 = typeid_cast<const DataTypeEnum16 *>(&type)))
						throw Exception{
							"Logical error: unknown numeric type " + type.getName(),
							ErrorCodes::LOGICAL_ERROR
						};

		const auto is_enum = is_enum8 || is_enum16;

		/// Numeric values for Enums should not be used directly in IN section
		if (src.getType() == Field::Types::UInt64 && !is_enum)
			return src;

		if (src.getType() == Field::Types::String)
		{
			/// Возможность сравнивать даты и даты-с-временем со строкой.
			const String & str = src.get<const String &>();
			ReadBufferFromString in(str);

			if (is_date)
			{
				DayNum_t date{};
				readDateText(date, in);
				if (!in.eof())
					throw Exception("String is too long for Date: " + str);

				return Field(UInt64(date));
			}
			else if (is_datetime)
			{
				time_t date_time{};
				readDateTimeText(date_time, in);
				if (!in.eof())
					throw Exception("String is too long for DateTime: " + str);

				return Field(UInt64(date_time));
			}
			else if (is_enum8)
				return Field(UInt64(static_cast<const DataTypeEnum8 &>(type).getValue(str)));
			else if (is_enum16)
				return Field(UInt64(static_cast<const DataTypeEnum16 &>(type).getValue(str)));
		}

		throw Exception("Type mismatch in IN section: " + type.getName() + " at left, "
			+ Field::Types::toString(src.getType()) + " at right", ErrorCodes::TYPE_MISMATCH);
	}
	else
	{
		if (src.getType() == Field::Types::UInt64
			|| src.getType() == Field::Types::Int64
			|| src.getType() == Field::Types::Float64
			|| src.getType() == Field::Types::Null
			|| (src.getType() == Field::Types::String
				&& !typeid_cast<const DataTypeString *>(&type)
				&& !typeid_cast<const DataTypeFixedString *>(&type))
			|| (src.getType() == Field::Types::Array
				&& !typeid_cast<const DataTypeArray *>(&type)))
			throw Exception("Type mismatch in IN section: " + type.getName() + " at left, "
				+ Field::Types::toString(src.getType()) + " at right", ErrorCodes::TYPE_MISMATCH);
	}

	return src;
}


/** Выполнить константное выражение (для элемента множества в IN). Весьма неоптимально. */
static Field evaluateConstantExpression(ASTPtr & node, const Context & context)
{
	ExpressionActionsPtr expr_for_constant_folding = ExpressionAnalyzer(
		node, context, nullptr, NamesAndTypesList{{ "_dummy", new DataTypeUInt8 }}).getConstActions();

	/// В блоке должен быть хотя бы один столбец, чтобы у него было известно число строк.
	Block block_with_constants{{ new ColumnConstUInt8(1, 0), new DataTypeUInt8, "_dummy" }};

	expr_for_constant_folding->execute(block_with_constants);

	if (!block_with_constants || block_with_constants.rows() == 0)
		throw Exception("Logical error: empty block after evaluation constant expression for IN", ErrorCodes::LOGICAL_ERROR);

	String name = node->getColumnName();

	if (!block_with_constants.has(name))
		throw Exception("Element of set in IN is not a constant expression: " + name, ErrorCodes::BAD_ARGUMENTS);

	const IColumn & result_column = *block_with_constants.getByName(name).column;

	if (!result_column.isConst())
		throw Exception("Element of set in IN is not a constant expression: " + name, ErrorCodes::BAD_ARGUMENTS);

	return result_column[0];
}


static Field extractValueFromNode(ASTPtr & node, const IDataType & type, const Context & context)
{
	if (ASTLiteral * lit = typeid_cast<ASTLiteral *>(node.get()))
		return convertToType(lit->value, type);
	else if (typeid_cast<ASTFunction *>(node.get()))
		return convertToType(evaluateConstantExpression(node, context), type);
	else
		throw Exception("Incorrect element of set. Must be literal or constant expression.", ErrorCodes::INCORRECT_ELEMENT_OF_SET);
}


void Set::createFromAST(DataTypes & types, ASTPtr node, const Context & context, bool create_ordered_set)
{
	data_types = types;

	/// Засунем множество в блок.
	Block block;
	for (size_t i = 0, size = data_types.size(); i < size; ++i)
	{
		ColumnWithTypeAndName col;
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
			Field value = extractValueFromNode(*it, *data_types[0], context);

			if (!value.isNull())
				block.getByPosition(0).column->insert(value);
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
				Field value = extractValueFromNode(func->arguments->children[j], *data_types[j], context);

				if (!value.isNull())
					block.getByPosition(j).column->insert(value);
			}
		}
		else
			throw Exception("Incorrect element of set", ErrorCodes::INCORRECT_ELEMENT_OF_SET);
	}

	if (create_ordered_set)
		ordered_set_elements = OrderedSetElementsPtr(new OrderedSetElements());

	insertFromBlock(block, create_ordered_set);

	if (create_ordered_set)
		std::sort(ordered_set_elements->begin(), ordered_set_elements->end());
}


ColumnPtr Set::execute(const Block & block, bool negative) const
{
	size_t num_key_columns = block.columns();

	if (0 == num_key_columns)
		throw Exception("Logical error: no columns passed to Set::execute method.", ErrorCodes::LOGICAL_ERROR);

	ColumnUInt8 * p_res = new ColumnUInt8;
	ColumnPtr res = p_res;
	ColumnUInt8::Container_t & vec_res = p_res->getData();
	vec_res.resize(block.getByPosition(0).column->size());

	Poco::ScopedReadRWLock lock(rwlock);

	/// Если множество пусто
	if (data_types.empty())
	{
		if (negative)
			memset(&vec_res[0], 1, vec_res.size());
		else
			memset(&vec_res[0], 0, vec_res.size());
		return res;
	}

	const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(&*block.getByPosition(0).type);

	if (array_type)
	{
		if (data_types.size() != 1 || num_key_columns != 1)
			throw Exception("Number of columns in section IN doesn't match.", ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);
		if (array_type->getNestedType()->getName() != data_types[0]->getName())
			throw Exception(std::string() + "Types in section IN don't match: " + data_types[0]->getName() + " on the right, " + array_type->getNestedType()->getName() + " on the left.", ErrorCodes::TYPE_MISMATCH);

		const IColumn * in_column = &*block.getByPosition(0).column;

		/// Константный столбец слева от IN поддерживается не напрямую. Для этого, он сначала материализуется.
		ColumnPtr materialized_column;
		if (materialized_column = in_column->convertToFullColumnIfConst())
			in_column = materialized_column.get();

		if (const ColumnArray * col = typeid_cast<const ColumnArray *>(in_column))
			executeArray(col, vec_res, negative);
		else
			throw Exception("Unexpected array column type: " + in_column->getName(), ErrorCodes::ILLEGAL_COLUMN);
	}
	else
	{
		if (data_types.size() != num_key_columns)
			throw Exception("Number of columns in section IN doesn't match.", ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);

		/// Запоминаем столбцы, с которыми будем работать. Также проверим, что типы данных правильные.
		ConstColumnPlainPtrs key_columns(num_key_columns);
		for (size_t i = 0; i < num_key_columns; ++i)
		{
			key_columns[i] = block.getByPosition(i).column;

			if (data_types[i]->getName() != block.getByPosition(i).type->getName())
				throw Exception("Types of column " + toString(i + 1) + " in section IN don't match: "
					+ data_types[i]->getName() + " on the right, " + block.getByPosition(i).type->getName() + " on the left.",
					ErrorCodes::TYPE_MISMATCH);
		}

		/// Константные столбцы слева от IN поддерживается не напрямую. Для этого, они сначала материализуется.
		Columns materialized_columns;
		for (auto & column_ptr : key_columns)
		{
			if (auto converted = column_ptr->convertToFullColumnIfConst())
			{
				materialized_columns.emplace_back(converted);
				column_ptr = materialized_columns.back().get();
			}
		}

		executeOrdinary(key_columns, vec_res, negative);
	}

	return res;
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


std::string Set::describe() const
{
	if (!ordered_set_elements)
		return "{}";

	bool first = true;
	std::stringstream ss;

	ss << "{";
	for (const Field & f : *ordered_set_elements)
	{
		ss << (first ? "" : ", ") << apply_visitor(FieldVisitorToString(), f);
		first = false;
	}
	ss << "}";
	return ss.str();
}

}
