#pragma once

#include <mysqlxx/Row.h>
#include <mysqlxx/Null.h>
#include <mysqlxx/Manip.h>
#include <common/MetrikaTypes.h>
#include <Core/Field.h>
#include <Core/FieldVisitors.h>
#include <IO/WriteHelpers.h>

/// This is for Yandex.Metrica code.

namespace mysqlxx
{
    inline std::ostream & operator<< (mysqlxx::EscapeManipResult res, const DB::Array & value)
    {
        return res.ostr << DB::applyVisitor(DB::FieldVisitorToString(), DB::Field(value));
    }

    inline std::ostream & operator<< (mysqlxx::QuoteManipResult res, const DB::Array & value)
    {
        throw Poco::Exception("Cannot quote Array with mysqlxx::quote.");
    }

    inline std::istream & operator>> (mysqlxx::UnEscapeManipResult res, DB::Array & value)
    {
        throw Poco::Exception("Cannot unescape Array with mysqlxx::unescape.");
    }

    inline std::istream & operator>> (mysqlxx::UnQuoteManipResult res, DB::Array & value)
    {
        throw Poco::Exception("Cannot unquote Array with mysqlxx::unquote.");
    }


    inline std::ostream & operator<< (mysqlxx::EscapeManipResult res, const DB::Tuple & value)
    {
        return res.ostr << DB::applyVisitor(DB::FieldVisitorToString(), DB::Field(value));
    }

    inline std::ostream & operator<< (mysqlxx::QuoteManipResult res, const DB::Tuple & value)
    {
        throw Poco::Exception("Cannot quote Tuple with mysqlxx::quote.");
    }

    inline std::istream & operator>> (mysqlxx::UnEscapeManipResult res, DB::Tuple & value)
    {
        throw Poco::Exception("Cannot unescape Tuple with mysqlxx::unescape.");
    }

    inline std::istream & operator>> (mysqlxx::UnQuoteManipResult res, DB::Tuple & value)
    {
        throw Poco::Exception("Cannot unquote Tuple with mysqlxx::unquote.");
    }

    template <> inline VisitID_t Value::get<VisitID_t>() const { return VisitID_t(getUInt()); }
}


namespace DB
{

/// Вывести mysqlxx::Row в tab-separated виде
inline void writeEscapedRow(const mysqlxx::Row & row, WriteBuffer & buf)
{
    for (size_t i = 0; i < row.size(); ++i)
    {
        if (i != 0)
            buf.write('\t');

        if (unlikely(row[i].isNull()))
        {
            buf.write("\\N", 2);
            continue;
        }

        writeAnyEscapedString<'\''>(row[i].data(), row[i].data() + row[i].length(), buf);
    }
}


template <typename T>
inline void writeText(const mysqlxx::Null<T> & x,    WriteBuffer & buf)
{
    if (x.isNull())
        writeCString("\\N", buf);
    else
        writeText(static_cast<const T &>(x), buf);
}


template <typename T>
inline void writeQuoted(const mysqlxx::Null<T> & x,        WriteBuffer & buf)
{
    if (x.isNull())
        writeCString("NULL", buf);
    else
        writeText(static_cast<const T &>(x), buf);
}


template <typename T>
inline Field toField(const mysqlxx::Null<T> & x)
{
    return x.isNull() ? Field(Null()) : toField(static_cast<const T &>(x));
}

}

