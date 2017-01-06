#include <DB/Common/SipHash.h>
#include <DB/Core/FieldVisitors.h>
#include <DB/Parsers/ASTLiteral.h>


namespace DB
{


String ASTLiteral::getColumnName() const
{
	/// Отдельный случай для очень больших массивов. Вместо указания всех элементов, будем использовать хэш от содержимого.
	if (value.getType() == Field::Types::Array
		&& value.get<const Array &>().size() > 100)		/// 100 - наугад.
	{
		SipHash hash;
		apply_visitor(FieldVisitorHash(hash), value);
		UInt64 low, high;
		hash.get128(low, high);
		return "__array_" + toString(low) + "_" + toString(high);
	}

	return apply_visitor(FieldVisitorToString(), value);
}

}
