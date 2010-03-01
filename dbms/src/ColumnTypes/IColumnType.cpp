#include <boost/variant/apply_visitor.hpp>

#include <DB/Common/EscapeManipulators.h>
#include <DB/Common/QuoteManipulators.h>

#include <DB/ColumnTypes/IColumnType.h>


namespace DB
{

virtual void serializeTextEscaped(const Field & field, std::ostream & ostr) const
{
	FieldVisitorIsNull visitor;
	if (boost::apply_visitor(visitor, field))
		ostr << "\\N";
	else
		ostr << 
}

virtual void deserializeTextEscaped(Field & field, std::istream & istr) const;

virtual void serializeTextQuoted(const Field & field, std::ostream & ostr, bool compatible = false) const;
virtual void deserializeTextQuoted(Field & field, std::istream & istr, bool compatible = false) const;

};

}

#endif
