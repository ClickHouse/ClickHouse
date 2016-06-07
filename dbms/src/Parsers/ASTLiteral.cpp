#include <DB/Common/SipHash.h>
#include <DB/Core/FieldVisitors.h>
#include <DB/Parsers/ASTLiteral.h>


namespace DB
{


/** Обновляет SipHash по данным Field */
class FieldVisitorHash : public StaticVisitor<>
{
private:
	SipHash & hash;
public:
	FieldVisitorHash(SipHash & hash) : hash(hash) {}

	void operator() (const Null 	& x) const
	{
		UInt8 type = Field::Types::Null;
		hash.update(reinterpret_cast<const char *>(&type), sizeof(type));
	}

	void operator() (const UInt64 	& x) const
	{
		UInt8 type = Field::Types::UInt64;
		hash.update(reinterpret_cast<const char *>(&type), sizeof(type));
		hash.update(reinterpret_cast<const char *>(&x), sizeof(x));
	}

	void operator() (const Int64 	& x) const
	{
		UInt8 type = Field::Types::Int64;
		hash.update(reinterpret_cast<const char *>(&type), sizeof(type));
		hash.update(reinterpret_cast<const char *>(&x), sizeof(x));
	}

	void operator() (const Float64 	& x) const
	{
		UInt8 type = Field::Types::Float64;
		hash.update(reinterpret_cast<const char *>(&type), sizeof(type));
		hash.update(reinterpret_cast<const char *>(&x), sizeof(x));
	}

	void operator() (const String 	& x) const
	{
		UInt8 type = Field::Types::String;
		hash.update(reinterpret_cast<const char *>(&type), sizeof(type));
		size_t size = x.size();
		hash.update(reinterpret_cast<const char *>(&size), sizeof(size));
		hash.update(x.data(), x.size());
	}

	void operator() (const Array 	& x) const
	{
		UInt8 type = Field::Types::Array;
		hash.update(reinterpret_cast<const char *>(&type), sizeof(type));
		size_t size = x.size();
		hash.update(reinterpret_cast<const char *>(&size), sizeof(size));

		for (const auto & elem : x)
			apply_visitor(*this, elem);
	}
};


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
