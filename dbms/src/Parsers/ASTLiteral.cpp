#include <Common/SipHash.h>
#include <Common/FieldVisitors.h>
#include <Parsers/ASTLiteral.h>
#include <IO/WriteHelpers.h>


namespace DB
{

void ASTLiteral::updateTreeHashImpl(SipHash & hash_state) const
{
    const char * prefix = "Literal_";
    hash_state.update(prefix, strlen(prefix));
    applyVisitor(FieldVisitorHash(hash_state), value);
}

void ASTLiteral::appendColumnNameImpl(WriteBuffer & ostr) const
{
    /// 100 - just arbitrary value.
    constexpr auto MIN_ELEMENTS_FOR_HASHING = 100;

    /// Special case for very large arrays and tuples. Instead of listing all elements, will use hash of them.
    /// (Otherwise column name will be too long, that will lead to significant slowdown of expression analysis.)
    auto type = value.getType();
    if ((type == Field::Types::Array && value.get<const Array &>().size() > MIN_ELEMENTS_FOR_HASHING)
        || (type == Field::Types::Tuple && value.get<const Tuple &>().size() > MIN_ELEMENTS_FOR_HASHING))
    {
        SipHash hash;
        applyVisitor(FieldVisitorHash(hash), value);
        UInt64 low, high;
        hash.get128(low, high);

        writeCString(type == Field::Types::Array ? "__array_" : "__tuple_", ostr);
        writeText(low, ostr);
        ostr.write('_');
        writeText(high, ostr);
    }
    else
    {
        String column_name = applyVisitor(FieldVisitorToString(), value);
        writeString(column_name, ostr);
    }
}

}
