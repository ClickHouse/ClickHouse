#pragma once

#include <typeinfo>
#include <Poco/Exception.h>
#include <base/StringRef.h>
#include <base/types.h>


/** A very simple class for reading JSON (or its fragments).
  * Represents a reference to a piece of memory that contains JSON (or its fragment).
  * Does not create any data structures in memory. Does not allocate memory (except for std::string).
  * Does not parse JSON to the end (parses only the part needed to execute the called method).
  * Parsing of the necessary part is started each time methods are called.
  * Can work with truncated JSON.
  * At the same time, (unlike SAX-like parsers), provides convenient methods for working.
  *
  * This data structure is more optimal if you need to extract several elements from a large number of small JSONs.
  * That is, it is suitable for processing "visit parameters" and "online store parameters" in Yandex.Metrica.
  * If you need to do a lot of work with one large JSON, then this class may be less optimal.
  *
  * The following conventions are available:
  * 1. It is assumed that there are no whitespace characters in JSON.
  * 2. It is assumed that strings in JSON are in UTF-8 encoding; \u-sequences can also be used.
  *    Strings are returned in UTF-8 encoding, \u-sequences are converted to UTF-8.
  * 3. But a surrogate pair of two \uXXXX\uYYYY is converted not to UTF-8, but to CESU-8.
  * 4. Correct JSON is parsed correctly.
  *    When working with incorrect JSON, an exception is thrown or incorrect results are returned.
  *    (example: it is considered that if the symbol 'n' is encountered, then 'ull' (null) follows it;
  *     if ',1,' follows it, then no exception is thrown, and thus an incorrect result is returned)
  * 5. The nesting depth of JSON is limited (see MAX_JSON_DEPTH in cpp file).
  *    When it is necessary to go to a greater depth, an exception is thrown.
  * 6. Unlike JSON, allows parsing values like 64-bit number, signed or unsigned.
  *    At the same time, if the number is fractional - then the fractional part is silently discarded.
  * 7. Floating point numbers are parsed with not maximum precision.
  *
  * Suitable only for reading JSON, modification is not provided.
  * All methods are immutable, except operator++.
  */


// NOLINTBEGIN(google-explicit-constructor)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-dynamic-exception-spec"
POCO_DECLARE_EXCEPTION(Foundation_API, JSONException, Poco::Exception)
#pragma clang diagnostic pop
// NOLINTEND(google-explicit-constructor)

class JSON
{
private:
    using Pos = const char *;
    Pos ptr_begin;
    Pos ptr_end;
    unsigned level;

public:
    JSON(Pos ptr_begin_, Pos ptr_end_, unsigned level_ = 0) : ptr_begin(ptr_begin_), ptr_end(ptr_end_), level(level_)
    {
        checkInit();
    }

    explicit JSON(std::string_view s) : ptr_begin(s.data()), ptr_end(s.data() + s.size()), level(0)
    {
        checkInit();
    }

    JSON(const JSON & rhs)
    {
        *this = rhs;
    }

    JSON & operator=(const JSON & rhs) = default;

    const char * data() const { return ptr_begin; }
    const char * dataEnd() const { return ptr_end; }

    enum ElementType : uint8_t
    {
        TYPE_OBJECT,
        TYPE_ARRAY,
        TYPE_NUMBER,
        TYPE_STRING,
        TYPE_BOOL,
        TYPE_NULL,
        TYPE_NAME_VALUE_PAIR,
        TYPE_NOTYPE,
    };

    ElementType getType() const;

    bool isObject() const        { return getType() == TYPE_OBJECT; }
    bool isArray() const         { return getType() == TYPE_ARRAY; }
    bool isNumber() const        { return getType() == TYPE_NUMBER; }
    bool isString() const        { return getType() == TYPE_STRING; }
    bool isBool() const          { return getType() == TYPE_BOOL; }
    bool isNull() const          { return getType() == TYPE_NULL; }
    bool isNameValuePair() const { return getType() == TYPE_NAME_VALUE_PAIR; }

    /// Number of elements in array or object; if element is not array or object, then exception.
    size_t size() const;

    /// Whether array or object is empty; if element is not array or object, then exception.
    bool empty() const;

    /// Get array element by index; if element is not array, then exception.
    JSON operator[] (size_t n) const;

    /// Get object element by name; if element is not object, then exception.
    JSON operator[] (const std::string & name) const;

    /// Whether object has element with given name; if element is not object, then exception.
    bool has(const std::string & name) const { return has(name.data(), name.size()); }
    bool has(const char * data, size_t size) const;

    /// Get element value; exception if element has wrong type.
    template <class T>
    T get() const;

    /// if value is missing or type is wrong, then returns default value
    template <class T>
    T getWithDefault(const std::string & key, const T & default_ = T()) const;

    double      getDouble() const;
    Int64       getInt() const;    /// Discard fractional part.
    UInt64      getUInt() const;    /// Discard fractional part. If number is negative - exception.
    std::string getString() const;
    bool        getBool() const;
    std::string getName() const;    /// Get name of name-value pair.
    JSON        getValue() const;    /// Get value of name-value pair.

    std::string_view getRawString() const;
    std::string_view getRawName() const;

    /// Get element value; if element is string, then parse value from string; if not string or number - then exception.
    double      toDouble() const;
    Int64       toInt() const;
    UInt64      toUInt() const;

    /** Convert any element to string.
      * For string returns its value, for all other elements - serialized representation.
      */
    std::string toString() const;

    /// JSON class is simultaneously an iterator over itself.
    using iterator = JSON;
    using const_iterator = JSON;

    iterator operator* () const { return *this; }
    const JSON * operator-> () const { return this; }
    bool operator== (const JSON & rhs) const { return ptr_begin == rhs.ptr_begin; }
    bool operator!= (const JSON & rhs) const { return ptr_begin != rhs.ptr_begin; }

    /** If element is array or object, then begin() returns iterator,
      * which points to the first element of array or first name-value pair of object.
      */
    iterator begin() const;

    /** end() - value that cannot be used; signals that elements are finished.
      */
    iterator end() const;

    /// Move to next array element or next name-value pair of object.
    iterator & operator++();
    iterator operator++(int); // NOLINT(cert-dcl21-cpp)

    /// Whether string has escape sequences
    bool hasEscapes() const;

    /// Whether string has special characters from the set \, ', \0, \b, \f, \r, \n, \t, possibly escaped.
    bool hasSpecialChars() const;

private:
    /// Check recursion depth and memory range correctness.
    void checkInit() const;
    /// Check that pos lies within memory range.
    void checkPos(Pos pos) const;

    /// Return position after given element.
    Pos skipString() const;
    Pos skipNumber() const;
    Pos skipBool() const;
    Pos skipNull() const;
    Pos skipNameValuePair() const;
    Pos skipObject() const;
    Pos skipArray() const;

    Pos skipElement() const;

    /// Find name-value pair with given name in object.
    Pos searchField(const std::string & name) const { return searchField(name.data(), name.size()); }
    Pos searchField(const char * data, size_t size) const;

    template <class T>
    bool isType() const;
};

template <class T>
T JSON::getWithDefault(const std::string & key, const T & default_) const
{
    if (has(key))
    {
        JSON key_json = (*this)[key];

        if (key_json.isType<T>())
            return key_json.get<T>();
        return default_;
    }
    return default_;
}
