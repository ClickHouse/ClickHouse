#include <DB/Common/SipHash.h>
#include <DB/Core/FieldVisitors.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/IO/WriteHelpers.h>


namespace DB
{


String ASTLiteral::getColumnName() const
{
	/// Special case for very large arrays. Instead of listing all elements, will use hash of them.
	/// (Otherwise column name will be too long, that will lead to significant slowdown of expression analysis.)
	if (value.getType() == Field::Types::Array
		&& value.get<const Array &>().size() > 100)		/// 100 - just arbitary value.
	{
		SipHash hash;
		applyVisitor(FieldVisitorHash(hash), value);
		UInt64 low, high;
		hash.get128(low, high);
		return "__array_" + toString(low) + "_" + toString(high);
	}

	return applyVisitor(FieldVisitorToString(), value);
}

}
