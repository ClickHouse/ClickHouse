#include <string>
#include <string.h>

#include <Poco/UTF8Encoding.h>
#include <Poco/NumberParser.h>
#include <common/JSON.h>
#include <common/find_symbols.h>
#include <common/preciseExp10.h>

#include <iostream>

#define JSON_MAX_DEPTH 100


POCO_IMPLEMENT_EXCEPTION(JSONException, Poco::Exception, "JSONException")


/// Прочитать беззнаковое целое в простом формате из не-0-terminated строки.
static UInt64 readUIntText(const char * buf, const char * end)
{
    UInt64 x = 0;

    if (buf == end)
        throw JSONException("JSON: cannot parse unsigned integer: unexpected end of data.");

    while (buf != end)
    {
        switch (*buf)
        {
            case '+':
                break;
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                x *= 10;
                x += *buf - '0';
                break;
            default:
                return x;
        }
        ++buf;
    }

    return x;
}


/// Прочитать знаковое целое в простом формате из не-0-terminated строки.
static Int64 readIntText(const char * buf, const char * end)
{
    bool negative = false;
    UInt64 x = 0;

    if (buf == end)
        throw JSONException("JSON: cannot parse signed integer: unexpected end of data.");

    bool run = true;
    while (buf != end && run)
    {
        switch (*buf)
        {
            case '+':
                break;
            case '-':
                negative = true;
                break;
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                x *= 10;
                x += *buf - '0';
                break;
            default:
                run = false;
                break;
        }
        ++buf;
    }

    return negative ? -x : x;
}


/// Прочитать число с плавающей запятой в простом формате, с грубым округлением, из не-0-terminated строки.
static double readFloatText(const char * buf, const char * end)
{
    bool negative = false;
    double x = 0;
    bool after_point = false;
    double power_of_ten = 1;

    if (buf == end)
        throw JSONException("JSON: cannot parse floating point number: unexpected end of data.");

    bool run = true;
    while (buf != end && run)
    {
        switch (*buf)
        {
            case '+':
                break;
            case '-':
                negative = true;
                break;
            case '.':
                after_point = true;
                break;
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                if (after_point)
                {
                    power_of_ten /= 10;
                    x += (*buf - '0') * power_of_ten;
                }
                else
                {
                    x *= 10;
                    x += *buf - '0';
                }
                break;
            case 'e':
            case 'E':
            {
                ++buf;
                Int32 exponent = readIntText(buf, end);
                x *= preciseExp10(exponent);

                run = false;
                break;
            }
            default:
                run = false;
                break;
        }
        ++buf;
    }
    if (negative)
        x = -x;

    return x;
}


void JSON::checkInit() const
{
    if (!(ptr_begin < ptr_end))
        throw JSONException("JSON: begin >= end.");

    if (level > JSON_MAX_DEPTH)
        throw JSONException("JSON: too deep.");
}


JSON::ElementType JSON::getType() const
{
    switch (*ptr_begin)
    {
        case '{':
            return TYPE_OBJECT;
        case '[':
            return TYPE_ARRAY;
        case 't':
        case 'f':
            return TYPE_BOOL;
        case 'n':
            return TYPE_NULL;
        case '-':
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
            return TYPE_NUMBER;
        case '"':
        {
            /// Проверим - это просто строка или name-value pair
            Pos after_string = skipString();
            if (after_string < ptr_end && *after_string == ':')
                return TYPE_NAME_VALUE_PAIR;
            else
                return TYPE_STRING;
        }
        default:
            throw JSONException(std::string("JSON: unexpected char ") + *ptr_begin + ", expected one of '{[tfn-0123456789\"'");
    }
}


void JSON::checkPos(Pos pos) const
{
    if (pos >= ptr_end || ptr_begin == nullptr)
        throw JSONException("JSON: unexpected end of data.");
}


JSON::Pos JSON::skipString() const
{
    //std::cerr << "skipString()\t" << data() << std::endl;

    Pos pos = ptr_begin;
    checkPos(pos);
    if (*pos != '"')
        throw JSONException(std::string("JSON: expected \", got ") + *pos);
    ++pos;

    /// fast path: находим следующую двойную кавычку. Если перед ней нет бэкслеша - значит это конец строки (при допущении корректности JSON).
    Pos closing_quote = reinterpret_cast<const char *>(memchr(reinterpret_cast<const void *>(pos), '\"', ptr_end - pos));
    if (nullptr != closing_quote && closing_quote[-1] != '\\')
        return closing_quote + 1;

    /// slow path
    while (pos < ptr_end && *pos != '"')
    {
        if (*pos == '\\')
        {
            ++pos;
            checkPos(pos);
            if (*pos == 'u')
            {
                pos += 4;
                checkPos(pos);
            }
        }
        ++pos;
    }

    checkPos(pos);
    if (*pos != '"')
        throw JSONException(std::string("JSON: expected \", got ") + *pos);
    ++pos;

    return pos;
}


JSON::Pos JSON::skipNumber() const
{
    //std::cerr << "skipNumber()\t" << data() << std::endl;

    Pos pos = ptr_begin;

    checkPos(pos);
    if (*pos == '-')
        ++pos;

    while (pos < ptr_end && *pos >= '0' && *pos <= '9')
        ++pos;
    if (pos < ptr_end && *pos == '.')
        ++pos;
    while (pos < ptr_end && *pos >= '0' && *pos <= '9')
        ++pos;
    if (pos < ptr_end && (*pos == 'e' || *pos == 'E'))
        ++pos;
    if (pos < ptr_end && *pos == '-')
        ++pos;
     while (pos < ptr_end && *pos >= '0' && *pos <= '9')
        ++pos;

    return pos;
}


JSON::Pos JSON::skipBool() const
{
    //std::cerr << "skipBool()\t" << data() << std::endl;

    Pos pos = ptr_begin;
    checkPos(pos);

    if (*ptr_begin == 't')
        pos += 4;
    else if (*ptr_begin == 'f')
        pos += 5;
    else
        throw JSONException("JSON: expected true or false.");

    return pos;
}


JSON::Pos JSON::skipNull() const
{
    //std::cerr << "skipNull()\t" << data() << std::endl;

    return ptr_begin + 4;
}


JSON::Pos JSON::skipNameValuePair() const
{
    //std::cerr << "skipNameValuePair()\t" << data() << std::endl;

    Pos pos = skipString();
    checkPos(pos);

    if (*pos != ':')
        throw JSONException("JSON: expected :.");
    ++pos;

    return JSON(pos, ptr_end, level + 1).skipElement();

}


JSON::Pos JSON::skipArray() const
{
    //std::cerr << "skipArray()\t" << data() << std::endl;

    if (!isArray())
        throw JSONException("JSON: expected [");
    Pos pos = ptr_begin;
    ++pos;
    checkPos(pos);
    if (*pos == ']')
        return ++pos;

    while (true)
    {
        pos = JSON(pos, ptr_end, level + 1).skipElement();

        checkPos(pos);

        switch (*pos)
        {
            case ',':
                ++pos;
                break;
            case ']':
                return ++pos;
            default:
                throw JSONException(std::string("JSON: expected one of ',]', got ") + *pos);
        }
    }
}


JSON::Pos JSON::skipObject() const
{
    //std::cerr << "skipObject()\t" << data() << std::endl;

    if (!isObject())
        throw JSONException("JSON: expected {");
    Pos pos = ptr_begin;
    ++pos;
    checkPos(pos);
    if (*pos == '}')
        return ++pos;

    while (true)
    {
        pos = JSON(pos, ptr_end, level + 1).skipNameValuePair();

        checkPos(pos);

        switch (*pos)
        {
            case ',':
                ++pos;
                break;
            case '}':
                return ++pos;
            default:
                throw JSONException(std::string("JSON: expected one of ',}', got ") + *pos);
        }
    }
}


JSON::Pos JSON::skipElement() const
{
    //std::cerr << "skipElement()\t" << data() << std::endl;

    ElementType type = getType();

    switch (type)
    {
        case TYPE_NULL:
            return skipNull();
        case TYPE_BOOL:
            return skipBool();
        case TYPE_NUMBER:
            return skipNumber();
        case TYPE_STRING:
            return skipString();
        case TYPE_NAME_VALUE_PAIR:
            return skipNameValuePair();
        case TYPE_ARRAY:
            return skipArray();
        case TYPE_OBJECT:
            return skipObject();
        default:
            throw JSONException("Logical error in JSON: unknown element type: " + std::to_string(type));
    }
}

size_t JSON::size() const
{
    size_t i = 0;

    for (const_iterator it = begin(); it != end(); ++it)
        ++i;

    return i;
}


bool JSON::empty() const
{
    return size() == 0;
}


JSON JSON::operator[] (size_t n) const
{
    ElementType type = getType();

    if (type != TYPE_ARRAY)
        throw JSONException("JSON: not array when calling operator[](size_t) method.");

    Pos pos = ptr_begin;
    ++pos;
    checkPos(pos);

    size_t i = 0;
    const_iterator it = begin();
    while (i < n && it != end())
    {
        ++it;
        ++i;
    }

    if (i != n)
        throw JSONException("JSON: array index " + std::to_string(n) + " out of bounds.");

    return *it;
}


JSON::Pos JSON::searchField(const char * data, size_t size) const
{
    ElementType type = getType();

    if (type != TYPE_OBJECT)
        throw JSONException("JSON: not object when calling operator[](const char *) or has(const char *) method.");

    const_iterator it = begin();
    for (; it != end(); ++it)
    {
        if (!it->hasEscapes())
        {
            if (static_cast<int>(size) + 2 > it->dataEnd() - it->data())
                continue;
            if (!strncmp(data, it->data() + 1, size))
                break;
        }
        else
        {
            std::string current_name = it->getName();
            if (current_name.size() == size && 0 == memcmp(current_name.data(), data, size))
                break;
        }
    }

    if (it == end())
        return nullptr;
    else
        return it->data();
}


bool JSON::hasEscapes() const
{
    Pos pos = ptr_begin + 1;
    while (pos < ptr_end && *pos != '"' && *pos != '\\')
        ++pos;

    if (*pos == '"')
        return false;
    else if (*pos == '\\')
        return true;
    throw JSONException("JSON: unexpected end of data.");
}


bool JSON::hasSpecialChars() const
{
    Pos pos = ptr_begin + 1;
    while (pos < ptr_end && *pos != '"'
        && *pos != '\\' && *pos != '\r' && *pos != '\n' && *pos != '\t'
        && *pos != '\f' && *pos != '\b' && *pos != '\0' && *pos != '\'')
        ++pos;

    if (*pos == '"')
        return false;
    else if (pos < ptr_end)
        return true;
    throw JSONException("JSON: unexpected end of data.");
}


JSON JSON::operator[] (const std::string & name) const
{
    Pos pos = searchField(name);
    if (!pos)
        throw JSONException("JSON: there is no element '" + std::string(name) + "' in object.");

    return JSON(pos, ptr_end, level + 1).getValue();
}


bool JSON::has(const char * data, size_t size) const
{
    return nullptr != searchField(data, size);
}


double JSON::getDouble() const
{
    return readFloatText(ptr_begin, ptr_end);
}

Int64 JSON::getInt() const
{
    return readIntText(ptr_begin, ptr_end);
}

UInt64 JSON::getUInt() const
{
    return readUIntText(ptr_begin, ptr_end);
}

bool JSON::getBool() const
{
    if (*ptr_begin == 't')
        return true;
    if (*ptr_begin == 'f')
        return false;
    throw JSONException("JSON: cannot parse boolean.");
}

std::string JSON::getString() const
{
    Pos s = ptr_begin;

    if (*s != '"')
        throw JSONException(std::string("JSON: expected \", got ") + *s);
    ++s;
    checkPos(s);

    std::string buf;
    do
    {
        Pos p = find_first_symbols<'\\','"'>(s, ptr_end);
        if (p >= ptr_end)
        {
            break;
        }
        buf.append(s, p);
        s = p;
        switch (*s)
        {
            case '\\':
                ++s;
                checkPos(s);

                switch (*s)
                {
                    case '"':
                        buf += '"';
                        break;
                    case '\\':
                        buf += '\\';
                        break;
                    case '/':
                        buf += '/';
                        break;
                    case 'b':
                        buf += '\b';
                        break;
                    case 'f':
                        buf += '\f';
                        break;
                    case 'n':
                        buf += '\n';
                        break;
                    case 'r':
                        buf += '\r';
                        break;
                    case 't':
                        buf += '\t';
                        break;
                    case 'u':
                    {
                        Poco::UTF8Encoding utf8;

                        ++s;
                        checkPos(s + 4);
                        std::string hex(s, 4);
                        s += 3;
                        int unicode;
                        try
                        {
                            unicode = Poco::NumberParser::parseHex(hex);
                        }
                        catch (const Poco::SyntaxException &)
                        {
                            throw JSONException("JSON: incorrect syntax: incorrect HEX code.");
                        }
                        buf.resize(buf.size() + 6);    /// максимальный размер UTF8 многобайтовой последовательности
                        int res = utf8.convert(unicode,
                            reinterpret_cast<unsigned char *>(const_cast<char*>(buf.data())) + buf.size() - 6, 6);
                        if (!res)
                            throw JSONException("JSON: cannot convert unicode " + std::to_string(unicode)
                                + " to UTF8.");
                        buf.resize(buf.size() - 6 + res);
                        break;
                    }
                    default:
                        buf += *s;
                        break;
                }
                ++s;
                break;
            case '"':
                return buf;
            default:
                throw JSONException("find_first_symbols<...>() failed in unexpected way");
        }
    } while (s < ptr_end);
    throw JSONException("JSON: incorrect syntax (expected end of string, found end of JSON).");
}

std::string JSON::getName() const
{
    return getString();
}

StringRef JSON::getRawString() const
{
    Pos s = ptr_begin;
    if (*s != '"')
        throw JSONException(std::string("JSON: expected \", got ") + *s);
    while (++s != ptr_end && *s != '"');
    if (s != ptr_end)
        return StringRef(ptr_begin + 1, s - ptr_begin - 1);
    throw JSONException("JSON: incorrect syntax (expected end of string, found end of JSON).");
}

StringRef JSON::getRawName() const
{
    return getRawString();
}

JSON JSON::getValue() const
{
    Pos pos = skipString();
    checkPos(pos);
    if (*pos != ':')
        throw JSONException("JSON: expected :.");
    ++pos;
    checkPos(pos);
    return JSON(pos, ptr_end, level + 1);
}


double JSON::toDouble() const
{
    ElementType type = getType();

    if (type == TYPE_NUMBER)
        return getDouble();
    else if (type == TYPE_STRING)
        return JSON(ptr_begin + 1, ptr_end, level + 1).getDouble();
    else
        throw JSONException("JSON: cannot convert value to double.");
}

Int64 JSON::toInt() const
{
    ElementType type = getType();

    if (type == TYPE_NUMBER)
        return getInt();
    else if (type == TYPE_STRING)
        return JSON(ptr_begin + 1, ptr_end, level + 1).getInt();
    else
        throw JSONException("JSON: cannot convert value to signed integer.");
}

UInt64 JSON::toUInt() const
{
    ElementType type = getType();

    if (type == TYPE_NUMBER)
        return getUInt();
    else if (type == TYPE_STRING)
        return JSON(ptr_begin + 1, ptr_end, level + 1).getUInt();
    else
        throw JSONException("JSON: cannot convert value to unsigned integer.");
}

std::string JSON::toString() const
{
    ElementType type = getType();

    if (type == TYPE_STRING)
        return getString();
    else
    {
        Pos pos = skipElement();
        return std::string(ptr_begin, pos - ptr_begin);
    }
}


JSON::iterator JSON::iterator::begin() const
{
    ElementType type = getType();

    if (type != TYPE_ARRAY && type != TYPE_OBJECT)
        throw JSONException("JSON: not array or object when calling begin() method.");

    //std::cerr << "begin()\t" << data() << std::endl;

    Pos pos = ptr_begin + 1;
    checkPos(pos);
    if (*pos == '}' || *pos == ']')
        return end();

    return JSON(pos, ptr_end, level + 1);
}

JSON::iterator JSON::iterator::end() const
{
    return JSON(nullptr, ptr_end, level + 1);
}

JSON::iterator & JSON::iterator::operator++()
{
    Pos pos = skipElement();
    checkPos(pos);

    if (*pos != ',')
        ptr_begin = nullptr;
    else
    {
        ++pos;
        checkPos(pos);
        ptr_begin = pos;
    }

    return *this;
}

JSON::iterator JSON::iterator::operator++(int) // NOLINT
{
    iterator copy(*this);
    ++*this;
    return copy;
}

template <>
double JSON::get<double>() const
{
    return getDouble();
}

template <>
std::string JSON::get<std::string>() const
{
    return getString();
}

template <>
Int64 JSON::get<Int64>() const
{
    return getInt();
}

template <>
UInt64 JSON::get<UInt64>() const
{
    return getUInt();
}

template <>
bool JSON::get<bool>() const
{
    return getBool();
}

template <>
bool JSON::isType<std::string>() const
{
    return isString();
}

template <>
bool JSON::isType<UInt64>() const
{
    return isNumber();
}

template <>
bool JSON::isType<Int64>() const
{
    return isNumber();
}

template <>
bool JSON::isType<bool>() const
{
    return isBool();
}

