#include <DB/Core/Field.h>
#include <DB/Core/FieldVisitors.h>
#include <DB/Core/Row.h>

#include <DB/Columns/ColumnsNumber.h>
#include <DB/Columns/ColumnTuple.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/DataStreams/OneBlockInputStream.h>

#include <DB/DataTypes/DataTypeArray.h>
#include <DB/Functions/DataTypeTraits.h>

#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTLiteral.h>

#include <DB/Interpreters/Set.h>
#include <DB/Interpreters/convertFieldToType.h>
#include <DB/Interpreters/evaluateConstantExpression.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
	extern const int SET_SIZE_LIMIT_EXCEEDED;
	extern const int TYPE_MISMATCH;
	extern const int INCORRECT_ELEMENT_OF_SET;
	extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
}

bool Set::checkSetSizeLimits() const
{
	if (max_rows && data.getTotalRowCount() > max_rows)
		return false;
	if (max_bytes && data.getTotalByteCount() > max_bytes)
		return false;
	return true;
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
	ConstColumnPlainPtrs key_columns;
	key_columns.reserve(keys_size);

	if (empty())
	{
		data_types.clear();
		data_types.reserve(keys_size);
	}

	/// Константные столбцы справа от IN поддерживается не напрямую. Для этого, они сначала материализуется.
	Columns materialized_columns;

	/// Запоминаем столбцы, с которыми будем работать
	for (size_t i = 0; i < keys_size; ++i)
	{
		key_columns.emplace_back(block.getByPosition(i).column.get());

		if (empty())
			data_types.emplace_back(block.getByPosition(i).type);

		if (auto converted = key_columns.back()->convertToFullColumnIfConst())
		{
			materialized_columns.emplace_back(converted);
			key_columns.back() = materialized_columns.back().get();
		}

		/** Flatten tuples. For case when written
		  *  (a, b) IN (SELECT (a, b) FROM table)
		  * instead of more typical
		  *  (a, b) IN (SELECT a, b FROM table)
		  */
		if (const ColumnTuple * tuple = typeid_cast<const ColumnTuple *>(key_columns.back()))
		{
			key_columns.pop_back();
			const Columns & tuple_elements = tuple->getColumns();
			for (const auto & elem : tuple_elements)
				key_columns.push_back(elem.get());

			if (empty())
			{
				data_types.pop_back();
				const Block & tuple_block = tuple->getData();
				for (size_t i = 0, size = tuple_block.columns(); i < size; ++i)
					data_types.push_back(tuple_block.unsafeGetByPosition(i).type);
			}
		}
	}

	size_t rows = block.rows();

	/// Какую структуру данных для множества использовать?
	if (empty())
		data.init(data.chooseMethod(key_columns, key_sizes));

	switch (data.type)
	{
		case SetVariants::Type::EMPTY:
			break;
#define M(NAME) \
		case SetVariants::Type::NAME: \
			insertFromBlockImpl(*data.NAME, key_columns, rows, data); \
			break;
		APPLY_FOR_SET_VARIANTS(M)
#undef M
	}

	if (create_ordered_set)
		for (size_t i = 0; i < rows; ++i)
			ordered_set_elements->push_back((*key_columns[0])[i]); /// ordered_set для индекса работает только если IN по одному ключу, а не кортажам

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


static Field extractValueFromNode(ASTPtr & node, const IDataType & type, const Context & context)
{
	if (ASTLiteral * lit = typeid_cast<ASTLiteral *>(node.get()))
		return convertFieldToType(lit->value, type);
	else if (typeid_cast<ASTFunction *>(node.get()))
		return convertFieldToType(evaluateConstantExpression(node, context), type);
	else
		throw Exception("Incorrect element of set. Must be literal or constant expression.", ErrorCodes::INCORRECT_ELEMENT_OF_SET);
}


void Set::createFromAST(const DataTypes & types, ASTPtr node, const Context & context, bool create_ordered_set)
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

		block.insert(std::move(col));
	}

	Row tuple_values;
	ASTExpressionList & list = typeid_cast<ASTExpressionList &>(*node);
	for (ASTs::iterator it = list.children.begin(); it != list.children.end(); ++it)
	{
		if (data_types.size() == 1)
		{
			Field value = extractValueFromNode(*it, *data_types[0], context);

			if (!value.isNull())
				block.getByPosition(0).column->insert(value);
		}
		else if (ASTFunction * func = typeid_cast<ASTFunction *>(it->get()))
		{
			if (func->name != "tuple")
				throw Exception("Incorrect element of set. Must be tuple.", ErrorCodes::INCORRECT_ELEMENT_OF_SET);

			size_t tuple_size = func->arguments->children.size();
			if (tuple_size != data_types.size())
				throw Exception("Incorrect size of tuple in set.", ErrorCodes::INCORRECT_ELEMENT_OF_SET);

			if (tuple_values.empty())
				tuple_values.resize(tuple_size);

			size_t j = 0;
			for (; j < tuple_size; ++j)
			{
				Field value = extractValueFromNode(func->arguments->children[j], *data_types[j], context);

				/// Если хотя бы один из элементов кортежа имеет невозможное (вне диапазона типа) значение, то и весь кортеж тоже.
				if (value.isNull())
					break;

				tuple_values[j] = value;	/// TODO Сделать move семантику для Field.
			}

			if (j == tuple_size)
				for (j = 0; j < tuple_size; ++j)
					block.getByPosition(j).column->insert(tuple_values[j]);
		}
		else
			throw Exception("Incorrect element of set", ErrorCodes::INCORRECT_ELEMENT_OF_SET);
	}

	if (create_ordered_set)
		ordered_set_elements = OrderedSetElementsPtr(new OrderedSetElements());

	insertFromBlock(block, create_ordered_set);

	if (create_ordered_set)
	{
		std::sort(ordered_set_elements->begin(), ordered_set_elements->end());
		ordered_set_elements->erase(std::unique(ordered_set_elements->begin(), ordered_set_elements->end()), ordered_set_elements->end());
	}
}


ColumnPtr Set::execute(const Block & block, bool negative) const
{
	size_t num_key_columns = block.columns();

	if (0 == num_key_columns)
		throw Exception("Logical error: no columns passed to Set::execute method.", ErrorCodes::LOGICAL_ERROR);

	auto res = std::make_shared<ColumnUInt8>();
	ColumnUInt8::Container_t & vec_res = res->getData();
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

	const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(block.getByPosition(0).type.get());

	if (array_type)
	{
		if (data_types.size() != 1 || num_key_columns != 1)
			throw Exception("Number of columns in section IN doesn't match.", ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);
		if (DataTypeTraits::removeNullable(array_type->getNestedType())->getName() !=
			DataTypeTraits::removeNullable(data_types[0])->getName())
			throw Exception(std::string() + "Types in section IN don't match: " + data_types[0]->getName() +
				" on the right, " + array_type->getNestedType()->getName() + " on the left.",
				ErrorCodes::TYPE_MISMATCH);

		const IColumn * in_column = block.getByPosition(0).column.get();

		/// Константный столбец слева от IN поддерживается не напрямую. Для этого, он сначала материализуется.
		ColumnPtr materialized_column = in_column->convertToFullColumnIfConst();
		if (materialized_column)
			in_column = materialized_column.get();

		if (const ColumnArray * col = typeid_cast<const ColumnArray *>(in_column))
			executeArray(col, vec_res, negative);
		else
			throw Exception("Unexpected array column type: " + in_column->getName(), ErrorCodes::ILLEGAL_COLUMN);
	}
	else
	{
		if (data_types.size() != num_key_columns)
		{
			std::stringstream message;
			message << "Number of columns in section IN doesn't match. "
				<< num_key_columns << " at left, " << data_types.size() << " at right.";
			throw Exception(message.str(), ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH);
		}

		/// Запоминаем столбцы, с которыми будем работать. Также проверим, что типы данных правильные.
		ConstColumnPlainPtrs key_columns;
		key_columns.reserve(num_key_columns);

		/// Константные столбцы слева от IN поддерживается не напрямую. Для этого, они сначала материализуется.
		Columns materialized_columns;

		for (size_t i = 0; i < num_key_columns; ++i)
		{
			key_columns.push_back(block.getByPosition(i).column.get());

			if (DataTypeTraits::removeNullable(data_types[i])->getName() !=
				DataTypeTraits::removeNullable(block.getByPosition(i).type)->getName())
				throw Exception("Types of column " + toString(i + 1) + " in section IN don't match: "
					+ data_types[i]->getName() + " on the right, " + block.getByPosition(i).type->getName() +
					" on the left.", ErrorCodes::TYPE_MISMATCH);

			if (auto converted = key_columns.back()->convertToFullColumnIfConst())
			{
				materialized_columns.emplace_back(converted);
				key_columns.back() = materialized_columns.back().get();
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

	switch (data.type)
	{
		case SetVariants::Type::EMPTY:
			break;
#define M(NAME) \
		case SetVariants::Type::NAME: \
			executeImpl(*data.NAME, key_columns, vec_res, negative, rows); \
			break;
	APPLY_FOR_SET_VARIANTS(M)
#undef M
	}
}

void Set::executeArray(const ColumnArray * key_column, ColumnUInt8::Container_t & vec_res, bool negative) const
{
	size_t rows = key_column->size();
	const ColumnArray::Offsets_t & offsets = key_column->getOffsets();
	const IColumn & nested_column = key_column->getData();

	switch (data.type)
	{
		case SetVariants::Type::EMPTY:
			break;
#define M(NAME) \
		case SetVariants::Type::NAME: \
			executeArrayImpl(*data.NAME, ConstColumnPlainPtrs{&nested_column}, offsets, vec_res, negative, rows); \
			break;
	APPLY_FOR_SET_VARIANTS(M)
#undef M
	}
}


/// Возвращаем BoolMask.
/// Первый элемент - может ли в диапазоне range быть элемент множества.
/// Второй элемент - может ли в диапазоне range быть элемент не из множества.
BoolMask Set::mayBeTrueInRange(const Range & range) const
{
	if (!ordered_set_elements)
		throw Exception("Ordered set in not created.");

	if (ordered_set_elements->empty())
		return {false, true};

	/// Диапазон (-inf; +inf)
	if (!range.left_bounded && !range.right_bounded)
		return {true, true};

	const Field & left = range.left;
	const Field & right = range.right;

	/// Диапазон (-inf; right|
	if (!range.left_bounded)
	{
		if (range.right_included)
			return {ordered_set_elements->front() <= right, true};
		else
			return {ordered_set_elements->front() < right, true};
	}

	/// Диапазон |left; +inf)
	if (!range.right_bounded)
	{
		if (range.left_included)
			return {ordered_set_elements->back() >= left, true};
		else
			return {ordered_set_elements->back() > left, true};
	}

	/// Диапазон из одного значения [left].
	if (range.left_included && range.right_included && left == right)
	{
		if (std::binary_search(ordered_set_elements->begin(), ordered_set_elements->end(), left))
			return {true, false};
		else
			return {false, true};
	}

	/// Первый элемент множества, который больше или равен left.
	auto left_it = std::lower_bound(ordered_set_elements->begin(), ordered_set_elements->end(), left);

	/// Если left не входит в диапазон (открытый диапазон), то возьмём следующий по порядку элемент множества.
	if (!range.left_included && left_it != ordered_set_elements->end() && *left_it == left)
		++left_it;

	/// если весь диапазон правее множества: { set } | range |
	if (left_it == ordered_set_elements->end())
		return {false, true};

	/// Первый элемент множества, который строго больше right.
	auto right_it = std::upper_bound(ordered_set_elements->begin(), ordered_set_elements->end(), right);

	/// весь диапазон левее множества: | range | { set }
	if (right_it == ordered_set_elements->begin())
		return {false, true};

	/// Последний элемент множества, который меньше или равен right.
	--right_it;

	/// Если right не входит в диапазон (открытый диапазон), то возьмём предыдущий по порядку элемент множества.
	if (!range.right_included && *right_it == right)
	{
		/// весь диапазон левее множества, хотя открытый диапазон касается множества: | range ){ set }
		if (right_it == ordered_set_elements->begin())
			return {false, true};

		--right_it;
	}

	/// В диапазон не попадает ни одного ключа из множества, хотя он расположен где-то посередине относительно его элементов: * * * * [ ] * * * *
	if (right_it < left_it)
		return {false, true};

	return {true, true};
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
