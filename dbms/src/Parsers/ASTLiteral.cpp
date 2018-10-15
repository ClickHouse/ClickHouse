#include <Common/SipHash.h>
#include <Common/FieldVisitors.h>
#include <Parsers/ASTLiteral.h>
#include <IO/WriteHelpers.h>


namespace DB
{

void ASTLiteral::appendColumnNameImpl(WriteBuffer & ostr) const
{
    /// Special case for very large arrays. Instead of listing all elements, will use hash of them.
    /// (Otherwise column name will be too long, that will lead to significant slowdown of expression analysis.)
    if (value.getType() == Field::Types::Array
        && value.get<const Array &>().size() > 100)        /// 100 - just arbitrary value.
    {
        SipHash hash;
        applyVisitor(FieldVisitorHash(hash), value);
        UInt64 low, high;
        hash.get128(low, high);

        writeCString("__array_", ostr);
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
