#ifndef DBMS_CORE_COLUMN_H
#define DBMS_CORE_COLUMN_H

#include <vector>

#include <boost/variant.hpp>
#include <boost/variant/recursive_variant.hpp>
#include <boost/variant/static_visitor.hpp>

#include <DB/Core/Types.h>
#include <DB/Core/Field.h>

namespace DB
{

/** Типы данных для представления столбцов значений в оперативке.
  */

typedef std::vector<UInt8> UInt8Column;
typedef std::vector<UInt16> UInt16Column;
typedef std::vector<UInt32> UInt32Column;
typedef std::vector<UInt64> UInt64Column;

typedef std::vector<Int8> Int8Column;
typedef std::vector<Int16> Int16Column;
typedef std::vector<Int32> Int32Column;
typedef std::vector<Int64> Int64Column;

typedef std::vector<Float32> Float32Column;
typedef std::vector<Float64> Float64Column;

typedef std::vector<String> StringColumn;

typedef std::vector<Field> VariantColumn;	/// Столбец произвольных значений, а также nullable значений


typedef boost::make_recursive_variant<
	UInt8Column, UInt16Column, UInt32Column, UInt64Column,
	Int8Column, Int16Column, Int32Column, Int64Column,
	Float32Column, Float64Column,
	StringColumn,
	VariantColumn,							/// Variant, Nullable
	std::vector<boost::recursive_variant_>	/// Tuple, Array
	>::type Column;

typedef std::vector<Column> TupleColumn;	/// Столбец значений типа "кортеж" - несколько столбцов произвольного типа
typedef std::vector<Column> ArrayColumn;	/// Столбец значений типа "массив" - столбец, значения в котором - массивы

}

#endif
