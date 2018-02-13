#pragma once

#include <cstring>
#include <iostream>
#include <type_traits>
#include <vector>

#include <mysqlxx/Types.h>
#include <mysqlxx/Row.h>
#include <mysqlxx/Null.h>


namespace mysqlxx
{

/** @brief Манипулятор ostream, который escape-ит строки для записи в tab delimited файл.
  * Использование: tab_separated_ostr << mysqlxx::escape << x;
  */
enum escape_enum
{
    escape
};


/** @brief Манипулятор ostream, который quote-ит строки для записи в MySQL запрос.
  * Внимание! Не использует функции MySQL API, а использует свой метод quote-инга,
  * который может быть некорректным при использовании некоторых кодировок
  * (multi-byte attack), а также может оказаться некорректным при изменении libmysqlclient.
  * Это сделано для увеличения производительности и это имеет значение.
  *
  * Использование: query << mysqlxx::quote << x;
  */
enum quote_enum
{
    quote
};


struct EscapeManipResult
{
    std::ostream & ostr;

    EscapeManipResult(std::ostream & ostr_) : ostr(ostr_) {}

    std::ostream & operator<< (bool value)                  { return ostr << static_cast<int>(value); }
    std::ostream & operator<< (char value)                  { return ostr << static_cast<int>(value); }
    std::ostream & operator<< (unsigned char value)         { return ostr << static_cast<int>(value); }
    std::ostream & operator<< (signed char value)           { return ostr << static_cast<int>(value); }

    template <typename T>
    typename std::enable_if<std::is_arithmetic<T>::value, std::ostream &>::type
    operator<< (T value) { return ostr << value; }

    std::ostream & operator<< (const LocalDate & value)      { return ostr << value; }
    std::ostream & operator<< (const LocalDateTime & value)  { return ostr << value; }

    std::ostream & operator<< (const std::string & value)
    {
        writeEscapedData(value.data(), value.length());
        return ostr;
    }


    std::ostream & operator<< (const char * value)
    {
        while (const char * it = std::strpbrk(value, "\t\n\\"))
        {
            ostr.write(value, it - value);
            switch (*it)
            {
                case '\t':
                    ostr.write("\\t", 2);
                    break;
                case '\n':
                    ostr.write("\\n", 2);
                    break;
                case '\\':
                    ostr.write("\\\\", 2);
                    break;
                default:
                    ;
            }
            value = it + 1;
        }
        return ostr << value;
    }


    std::ostream & operator<< (const Value & string)
    {
        writeEscapedData(string.data(), string.size());
        return ostr;
    }


    std::ostream & operator<< (const Row & row)
    {
        for (size_t i = 0; i < row.size(); ++i)
        {
            if (i != 0)
                ostr << '\t';

            if (row[i].isNull())
            {
                ostr << "\\N";
                continue;
            }

            (*this) << row[i];
        }

        return ostr;
    }


    template <typename T>
    std::ostream & operator<< (const Null<T> & value)
    {
        if(value.is_null)
            ostr << "\\N";
        else
            *this << value.data;

        return ostr ;
    }


    template <typename T>
    std::ostream & operator<< (const std::vector<T> & value)
    {
        throw Poco::Exception(std::string(__PRETTY_FUNCTION__) + " is not implemented");
    }

private:

    void writeEscapedData(const char * data, size_t length)
    {
        size_t i = 0;

        while (true)
        {
            size_t remaining_length = std::strlen(data);
            (*this) << data;
            if (i + remaining_length == length)
                break;

            ostr.write("\\0", 2);
            i += remaining_length + 1;
            data += remaining_length + 1;
        }
    }
};

inline EscapeManipResult operator<< (std::ostream & ostr, escape_enum manip)
{
    return EscapeManipResult(ostr);
}


struct QuoteManipResult
{
public:
    std::ostream & ostr;

    QuoteManipResult(std::ostream & ostr_) : ostr(ostr_) {}

    std::ostream & operator<< (bool value)                  { return ostr << static_cast<int>(value); }
    std::ostream & operator<< (char value)                  { return ostr << static_cast<int>(value); }
    std::ostream & operator<< (unsigned char value)         { return ostr << static_cast<int>(value); }
    std::ostream & operator<< (signed char value)           { return ostr << static_cast<int>(value); }

    template <typename T>
    typename std::enable_if<std::is_arithmetic<T>::value, std::ostream &>::type
    operator<< (T value) { return ostr << value; }

    std::ostream & operator<< (const LocalDate & value)     { return ostr << '\'' << value << '\''; }
    std::ostream & operator<< (const LocalDateTime & value) { return ostr << '\'' << value << '\''; }

    std::ostream & operator<< (const std::string & value)
    {
        ostr.put('\'');
        writeEscapedData(value.data(), value.length());
        ostr.put('\'');

        return ostr;
    }


    std::ostream & operator<< (const char * value)
    {
        ostr.put('\'');
        writeEscapedCString(value);
        ostr.put('\'');
        return ostr;
    }

    template <typename T>
    std::ostream & operator<< (const Null<T> & value)
    {
        if(value.is_null)
        {
            ostr << "\\N";
        }
        else
        {
            *this << value.data;
        }
        return ostr ;
    }

    template <typename T>
    std::ostream & operator<< (const std::vector<T> & value)
    {
        throw Poco::Exception(std::string(__PRETTY_FUNCTION__) + " is not implemented");
    }

private:

    void writeEscapedCString(const char * value)
    {
        while (const char * it = std::strpbrk(value, "'\\\""))
        {
            ostr.write(value, it - value);
            switch (*it)
            {
                case '"':
                    ostr.write("\\\"", 2);
                    break;
                case '\'':
                    ostr.write("\\'", 2);
                    break;
                case '\\':
                    ostr.write("\\\\", 2);
                    break;
                default:
                    ;
            }
            value = it + 1;
        }
        ostr << value;
    }


    void writeEscapedData(const char * data, size_t length)
    {
        size_t i = 0;

        while (true)
        {
            size_t remaining_length = std::strlen(data);
            writeEscapedCString(data);
            if (i + remaining_length == length)
                break;

            ostr.write("\\0", 2);
            i += remaining_length + 1;
            data += remaining_length + 1;
        }
    }
};

inline QuoteManipResult operator<< (std::ostream & ostr, quote_enum manip)
{
    return QuoteManipResult(ostr);
}


/** Манипулятор istream, позволяет считывать значения из tab delimited файла.
  */
enum unescape_enum
{
    unescape
};


/** Манипулятор istream, который позволяет читать значения в кавычках или без.
  */
enum unquote_enum
{
    unquote
};


inline void parseEscapeSequence(std::istream & istr, std::string & value)
{
    char c = istr.get();
    if (!istr.good())
        throw Poco::Exception("Cannot parse string: unexpected end of input.");

    switch(c)
    {
        case 'b':
            value.push_back('\b');
            break;
        case 'f':
            value.push_back('\f');
            break;
        case 'n':
            value.push_back('\n');
            break;
        case 'r':
            value.push_back('\r');
            break;
        case 't':
            value.push_back('\t');
            break;
        default:
            value.push_back(c);
        break;
    }
}


struct UnEscapeManipResult
{
    std::istream & istr;

    UnEscapeManipResult(std::istream & istr_) : istr(istr_) {}

    std::istream & operator>> (bool                 & value) { int tmp = 0; istr >> tmp; value = tmp; return istr; }
    std::istream & operator>> (char                 & value) { int tmp = 0; istr >> tmp; value = tmp; return istr; }
    std::istream & operator>> (unsigned char        & value) { int tmp = 0; istr >> tmp; value = tmp; return istr; }
    std::istream & operator>> (signed char          & value) { int tmp = 0; istr >> tmp; value = tmp; return istr; }

    template <typename T>
    typename std::enable_if<std::is_arithmetic<T>::value, std::istream &>::type
    operator>> (T & value) { return istr >> value; }

    std::istream & operator>> (std::string & value)
    {
        value.clear();

        char c;
        while (1)
        {
            istr.get(c);
            if (!istr.good())
                break;

            switch (c)
            {
                case '\\':
                    parseEscapeSequence(istr, value);
                    break;

                case '\t':
                    istr.unget();
                    return istr;
                    break;

                case '\n':
                    istr.unget();
                    return istr;
                    break;

                default:
                    value.push_back(c);
                    break;
            }
        }
        return istr;
    }

    /// Чтение NULL-able типа.
    template <typename T>
    std::istream & operator>> (Null<T> & value)
    {
        char c;
        istr.get(c);
        if (c == '\\' && istr.peek() == 'N')
        {
            value.is_null = true;
            istr.ignore();
        }
        else
        {
            istr.unget();
            value.is_null = false;
            *this >> value.data;
        }
        return istr;
    }

    std::istream & operator>> (LocalDate & value)
    {
        std::string s;
        (*this) >> s;
        value = LocalDate(s);
        return istr;
    }

    std::istream & operator>> (LocalDateTime & value)
    {
        std::string s;
        (*this) >> s;
        value = LocalDateTime(s);
        return istr;
    }

    template <typename T>
    std::istream & operator>> (std::vector<T> & value)
    {
        throw Poco::Exception(std::string(__PRETTY_FUNCTION__) + " is not implemented");
    }
};

inline UnEscapeManipResult operator>> (std::istream & istr, unescape_enum manip)
{
    return UnEscapeManipResult(istr);
}


struct UnQuoteManipResult
{
public:
    std::istream & istr;

    UnQuoteManipResult(std::istream & istr_) : istr(istr_) {}

    std::istream & operator>> (bool                 & value) { int tmp = 0; istr >> tmp; value = tmp; return istr; }
    std::istream & operator>> (char                 & value) { int tmp = 0; istr >> tmp; value = tmp; return istr; }
    std::istream & operator>> (unsigned char        & value) { int tmp = 0; istr >> tmp; value = tmp; return istr; }
    std::istream & operator>> (signed char          & value) { int tmp = 0; istr >> tmp; value = tmp; return istr; }

    template <typename T>
    typename std::enable_if<std::is_arithmetic<T>::value, std::istream &>::type
    operator>> (T & value) { return istr >> value; }

    std::istream & operator>> (std::string & value)
    {
        value.clear();
        readQuote();

        char c;
        while (1)
        {
            istr.get(c);
            if (!istr.good())
                break;

            switch (c)
            {
                case '\\':
                    parseEscapeSequence(istr, value);
                    break;

                case '\'':
                    return istr;
                    break;

                default:
                    value.push_back(c);
                    break;
            }
        }
        throw Poco::Exception("Cannot parse string: unexpected end of input.");
    }

    template <typename T>
    std::istream & operator>> (std::vector<T> & value)
    {
        throw Poco::Exception(std::string(__PRETTY_FUNCTION__) + " is not implemented");
    }

private:

    void readQuote()
    {
        char c = istr.get();
        if (!istr.good())
            throw Poco::Exception("Cannot parse string: unexpected end of input.");
        if (c != '\'')
            throw Poco::Exception("Cannot parse string: missing opening single quote.");
    }
};

inline UnQuoteManipResult operator>> (std::istream & istr, unquote_enum manip)
{
    return UnQuoteManipResult(istr);
}


}
