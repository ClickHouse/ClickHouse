//
// Var.h
//
// $Id: //poco/svn/Foundation/include/Poco/Var.h#2 $
//
// Library: Foundation
// Package: Dynamic
// Module:  Var
//
// Definition of the Var class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Var_INCLUDED
#define Foundation_Var_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Format.h"
#include "Poco/SharedPtr.h"
#include "Poco/Dynamic/VarHolder.h"
#include "Poco/Dynamic/VarIterator.h"
#include <typeinfo>


namespace Poco {
namespace Dynamic {


template <typename T>
class Struct;


class Foundation_API Var
	/// Var allows to store data of different types and to convert between these types transparently.
	/// Var puts forth the best effort to provide intuitive and reasonable conversion semantics and prevent 
	/// unexpected data loss, particularly when performing narrowing or signedness conversions of numeric data types.
	///
	/// An attempt to convert or extract from a non-initialized ("empty") Var variable shall result
	/// in an exception being thrown.
	///
	/// Loss of signedness is not allowed for numeric values. This means that if an attempt is made to convert 
	/// the internal value which is a negative signed integer to an unsigned integer type storage, a RangeException is thrown. 
	/// Overflow is not allowed, so if the internal value is a larger number than the target numeric type size can accomodate, 
	/// a RangeException is thrown.
	///
	/// Precision loss, such as in conversion from floating-point types to integers or from double to float on platforms
	/// where they differ in size (provided internal actual value fits in float min/max range), is allowed.
	/// 
	/// String truncation is allowed -- it is possible to convert between string and character when string length is 
	/// greater than 1. An empty string gets converted to the char '\0', a non-empty string is truncated to the first character. 
	///
	/// Boolean conversion is performed as follows:
	///
	/// A string value "false" (not case sensitive), "0" or "" (empty string) can be converted to a boolean value false,
	/// any other string not being false by the above criteria evaluates to true (e.g: "hi" -> true).
	/// Integer 0 values are false, everything else is true.
	/// Floating point values equal to the minimal FP representation on a given platform are false, everything else is true.
	///
	/// Arithmetic operations with POD types as well as between Var's are supported, subject to following
	/// limitations:
	/// 
	/// 	- for std::string and const char* values, only '+' and '+=' operations are supported
	/// 
	/// 	- for integral and floating point numeric values, following operations are supported:
	/// 	  '+', '+=', '-', '-=', '*', '*=' , '/' and '/=' 
	///
	/// 	- for integral values, following operations are supported:
	///		  prefix and postfix increment (++) and decrement (--)
	/// 
	/// 	- for all other types, InvalidArgumentException is thrown upon attempt of an arithmetic operation
	/// 
	/// A Var can be created from and converted to a value of any type for which a specialization of 
	/// VarHolderImpl is available. For supported types, see VarHolder documentation.
{
public:
	typedef SharedPtr<Var>             Ptr;
	typedef Poco::Dynamic::VarIterator Iterator;
	typedef const VarIterator          ConstIterator;

	Var();
		/// Creates an empty Var.

	template <typename T> 
	Var(const T& val)
		/// Creates the Var from the given value.
#ifdef POCO_NO_SOO
		: _pHolder(new VarHolderImpl<T>(val))
	{
	}
#else
	{
		construct(val);
	}
#endif

	Var(const char* pVal);
		// Convenience constructor for const char* which gets mapped to a std::string internally, i.e. pVal is deep-copied.

	Var(const Var& other);
		/// Copy constructor.

	~Var();
		/// Destroys the Var.

	void swap(Var& other);
		/// Swaps the content of the this Var with the other Var.

	ConstIterator begin() const;
		/// Returns the const Var iterator.

	ConstIterator end() const;
		/// Returns the const Var iterator.

	Iterator begin();
		/// Returns the Var iterator.

	Iterator end();
		/// Returns the Var iterator.

	template <typename T> 
	void convert(T& val) const
		/// Invoke this method to perform a safe conversion.
		///
		/// Example usage:
		///     Var any("42");
		///     int i;
		///     any.convert(i);
		///
		/// Throws a RangeException if the value does not fit
		/// into the result variable.
		/// Throws a NotImplementedException if conversion is
		/// not available for the given type.
		/// Throws InvalidAccessException if Var is empty.
	{
		VarHolder* pHolder = content();

		if (!pHolder)
			throw InvalidAccessException("Can not convert empty value.");

		pHolder->convert(val);
	}
	
	template <typename T> 
	T convert() const
		/// Invoke this method to perform a safe conversion.
		///
		/// Example usage:
		///     Var any("42");
		///     int i = any.convert<int>();
		///
		/// Throws a RangeException if the value does not fit
		/// into the result variable.
		/// Throws a NotImplementedException if conversion is
		/// not available for the given type.
		/// Throws InvalidAccessException if Var is empty.
	{
		VarHolder* pHolder = content();

		if (!pHolder)
			throw InvalidAccessException("Can not convert empty value.");

		if (typeid(T) == pHolder->type()) return extract<T>();

		T result;
		pHolder->convert(result);
		return result;
	}
	
	template <typename T>
	operator T () const
		/// Safe conversion operator for implicit type
		/// conversions. If the requested type T is same as the 
		/// type being held, the operation performed is direct 
		/// extraction, otherwise it is the conversion of the value
		/// from type currently held to the one requested.
		///
		/// Throws a RangeException if the value does not fit
		/// into the result variable.
		/// Throws a NotImplementedException if conversion is
		/// not available for the given type.
		/// Throws InvalidAccessException if Var is empty.
	{
		VarHolder* pHolder = content();

		if (!pHolder)
				throw InvalidAccessException("Can not convert empty value.");

		if (typeid(T) == pHolder->type())
			return extract<T>();
		else
		{
			T result;
			pHolder->convert(result);
			return result;
		}
	}

	template <typename T>
	const T& extract() const
		/// Returns a const reference to the actual value.
		///
		/// Must be instantiated with the exact type of
		/// the stored value, otherwise a BadCastException
		/// is thrown.
		/// Throws InvalidAccessException if Var is empty.
	{
		VarHolder* pHolder = content();

		if (pHolder && pHolder->type() == typeid(T))
		{
			VarHolderImpl<T>* pHolderImpl = static_cast<VarHolderImpl<T>*>(pHolder);
			return pHolderImpl->value();
		}
		else if (!pHolder)
			throw InvalidAccessException("Can not extract empty value.");
		else
			throw BadCastException(format("Can not convert %s to %s.",
				std::string(pHolder->type().name()),
				std::string(typeid(T).name())));
	}

	template <typename T> 
	Var& operator = (const T& other)
		/// Assignment operator for assigning POD to Var
	{
#ifdef POCO_NO_SOO
		Var tmp(other);
		swap(tmp);
#else
		construct(other);
#endif
		return *this;
	}

	bool operator ! () const;
		/// Logical NOT operator.

	Var& operator = (const Var& other);
		/// Assignment operator specialization for Var

	template <typename T>
	const Var operator + (const T& other) const
		/// Addition operator for adding POD to Var
	{
		return convert<T>() + other;
	}

	const Var operator + (const Var& other) const;
		/// Addition operator specialization for Var

	const Var operator + (const char* other) const;
		/// Addition operator specialization for adding const char* to Var

	Var& operator ++ ();
		/// Pre-increment operator

	const Var operator ++ (int);
		/// Post-increment operator

	Var& operator -- ();
		/// Pre-decrement operator

	const Var operator -- (int);
		/// Post-decrement operator

	template <typename T> 
	Var& operator += (const T& other)
		/// Addition asignment operator for addition/assignment of POD to Var.
	{
		return *this = convert<T>() + other;
	}

	Var& operator += (const Var& other);
		/// Addition asignment operator overload for Var

	Var& operator += (const char* other);
		/// Addition asignment operator overload for const char*

	template <typename T> 
	const Var operator - (const T& other) const
		/// Subtraction operator for subtracting POD from Var
	{
		return convert<T>() - other;
	}

	const Var operator - (const Var& other) const;
		/// Subtraction operator overload for Var

	template <typename T> 
	Var& operator -= (const T& other)
		/// Subtraction asignment operator
	{
		return *this = convert<T>() - other;
	}

	Var& operator -= (const Var& other);
		/// Subtraction asignment operator overload for Var

	template <typename T> 
	const Var operator * (const T& other) const
		/// Multiplication operator for multiplying Var with POD
	{
		return convert<T>() * other;
	}

	const Var operator * (const Var& other) const;
		/// Multiplication operator overload for Var

	template <typename T> 
	Var& operator *= (const T& other)
		/// Multiplication asignment operator
	{
		return *this = convert<T>() * other;
	}

	Var& operator *= (const Var& other);
		/// Multiplication asignment operator overload for Var

	template <typename T> 
	const Var operator / (const T& other) const
		/// Division operator for dividing Var with POD
	{
		return convert<T>() / other;
	}

	const Var operator / (const Var& other) const;
		/// Division operator overload for Var

	template <typename T> 
	Var& operator /= (const T& other)
		/// Division asignment operator
	{
		return *this = convert<T>() / other;
	}

	Var& operator /= (const Var& other);
		/// Division asignment operator specialization for Var

	template <typename T> 
	bool operator == (const T& other) const
		/// Equality operator
	{
		if (isEmpty()) return false;
		return convert<T>() == other;
	}

	bool operator == (const char* other) const;
		/// Equality operator overload for const char*

	bool operator == (const Var& other) const;
		/// Equality operator overload for Var

	template <typename T> 
	bool operator != (const T& other) const
		/// Inequality operator
	{
		if (isEmpty()) return true;
		return convert<T>() != other;
	}

	bool operator != (const Var& other) const;
		/// Inequality operator overload for Var

	bool operator != (const char* other) const;
		/// Inequality operator overload for const char*

	template <typename T> 
	bool operator < (const T& other) const
		/// Less than operator
	{
		if (isEmpty()) return false;
		return convert<T>() < other;
	}

	bool operator < (const Var& other) const;
		/// Less than operator overload for Var

	template <typename T> 
	bool operator <= (const T& other) const
		/// Less than or equal operator
	{
		if (isEmpty()) return false;
		return convert<T>() <= other;
	}

	bool operator <= (const Var& other) const;
		/// Less than or equal operator overload for Var

	template <typename T> 
	bool operator > (const T& other) const
		/// Greater than operator
	{
		if (isEmpty()) return false;
		return convert<T>() > other;
	}

	bool operator > (const Var& other) const;
		/// Greater than operator overload for Var

	template <typename T> 
	bool operator >= (const T& other) const
		/// Greater than or equal operator
	{
		if (isEmpty()) return false;
		return convert<T>() >= other;
	}

	bool operator >= (const Var& other) const;
		/// Greater than or equal operator overload for Var

	template <typename T>
	bool operator || (const T& other) const
		/// Logical OR operator
	{
		if (isEmpty()) return false;
		return convert<bool>() || other;
	}

	bool operator || (const Var& other) const;
		/// Logical OR operator operator overload for Var

	template <typename T>
	bool operator && (const T& other) const
		/// Logical AND operator.
	{
		if (isEmpty()) return false;
		return convert<bool>() && other;
	}

	bool operator && (const Var& other) const;
		/// Logical AND operator operator overload for Var.

	bool isArray() const;
		/// Returns true if Var is not empty.

	bool isVector() const;
		/// Returns true if Var represents a vector.

	bool isList() const;
		/// Returns true if Var represents a list.

	bool isDeque() const;
		/// Returns true if Var represents a deque.

	bool isStruct() const;
		/// Returns true if Var represents a struct.

	char& at(std::size_t n);
		/// Returns character at position n. This function only works with
		/// Var containing a std::string.


	template <typename T>
	Var& operator [] (const T& n)
	{
		return getAt(n);
	}

	template <typename T>
	const Var& operator [] (const T& n) const
	{
		return const_cast<Var*>(this)->getAt(n);
	}

	Var& operator [] (const std::string& name);
		/// Index operator by name, only use on Vars where isStruct
		/// returns true! In all other cases InvalidAccessException is thrown.

	const Var& operator [] (const std::string& name) const;
		/// Index operator by name, only use on Vars where isStruct
		/// returns true! In all other cases InvalidAccessException is thrown.

	const std::type_info& type() const;
		/// Returns the type information of the stored content.

	void empty();
		/// Empties Var.

	bool isEmpty() const;
		/// Returns true if empty.

	bool isInteger() const;
		/// Returns true if stored value is integer.

	bool isSigned() const;
		/// Returns true if stored value is signed.

	bool isNumeric() const;
		/// Returns true if stored value is numeric.
		/// Returns false for numeric strings (e.g. "123" is string, not number)

	bool isBoolean() const;
		/// Returns true if stored value is boolean.
		/// Returns false for boolean strings (e.g. "true" is string, not number)

	bool isString() const;
		/// Returns true if stored value is std::string.

	std::size_t size() const;
		/// Returns the size of this Var.
		/// This function returns 0 when Var is empty, 1 for POD or the size (i.e. length)
		/// for held container.

	std::string toString() const
		/// Returns the stored value as string.
	{
		VarHolder* pHolder = content();

		if (!pHolder)
				throw InvalidAccessException("Can not convert empty value.");

		if (typeid(std::string) == pHolder->type())
			return extract<std::string>();
		else
		{
			std::string result;
			pHolder->convert(result);
			return result;
		}
	}

	static Var parse(const std::string& val);
		/// Parses the string which must be in JSON format

	static std::string toString(const Var& var);
		/// Converts the Var to a string in JSON format. Note that toString(const Var&) will return
		/// a different result than Var::convert<std::string>() and Var::toString()!
	
private:
	Var& getAt(std::size_t n);
	Var& getAt(const std::string& n);

	static Var parse(const std::string& val, std::string::size_type& offset);
		/// Parses the string which must be in JSON format

	static Var parseObject(const std::string& val, std::string::size_type& pos);
	static Var parseArray(const std::string& val, std::string::size_type& pos);
	static std::string parseString(const std::string& val, std::string::size_type& pos);
	static std::string parseJSONString(const std::string& val, std::string::size_type& pos);
	static void skipWhiteSpace(const std::string& val, std::string::size_type& pos);

	template <typename T>
	T add(const Var& other) const
	{
		return convert<T>() + other.convert<T>();
	}

	template <typename T>
	T subtract(const Var& other) const
	{
		return convert<T>() - other.convert<T>();
	}
	
	template <typename T>
	T multiply(const Var& other) const
	{
		return convert<T>() * other.convert<T>();
	}

	template <typename T>
	T divide(const Var& other) const
	{
		return convert<T>() / other.convert<T>();
	}

	template <typename T, typename E>
	VarHolderImpl<T>* holderImpl(const std::string errorMessage = "") const
	{
		VarHolder* pHolder = content();

		if (pHolder && pHolder->type() == typeid(T))
			return static_cast<VarHolderImpl<T>*>(pHolder);
		else if (!pHolder)
			throw InvalidAccessException("Can not access empty value.");
		else
			throw E(errorMessage);
	}

	Var& structIndexOperator(VarHolderImpl<Struct<int> >* pStr, int n) const;

#ifdef POCO_NO_SOO

	VarHolder* content() const
	{
		return _pHolder;
	}

	void destruct()
	{
		if (!isEmpty()) delete content();
	}

	VarHolder* _pHolder;

#else

	VarHolder* content() const
	{
		return _placeholder.content();
	}

	template<typename ValueType>
	void construct(const ValueType& value)
	{
		if (sizeof(VarHolderImpl<ValueType>) <= Placeholder<ValueType>::Size::value)
		{
			new (reinterpret_cast<VarHolder*>(_placeholder.holder)) VarHolderImpl<ValueType>(value);
			_placeholder.setLocal(true);
		}
		else
		{
			_placeholder.pHolder = new VarHolderImpl<ValueType>(value);
			_placeholder.setLocal(false);
		}
	}

	void construct(const char* value)
	{
		std::string val(value);
		if (sizeof(VarHolderImpl<std::string>) <= Placeholder<std::string>::Size::value)
		{
			new (reinterpret_cast<VarHolder*>(_placeholder.holder)) VarHolderImpl<std::string>(val);
			_placeholder.setLocal(true);
		}
		else
		{
			_placeholder.pHolder = new VarHolderImpl<std::string>(val);
			_placeholder.setLocal(false);
		}
	}

	void construct(const Var& other)
	{
		if (!other.isEmpty())
			other.content()->clone(&_placeholder);
		else
			_placeholder.erase();
	}

	void destruct()
	{
		if (!isEmpty())
		{
			if (_placeholder.isLocal())
				content()->~VarHolder();
			else
				delete content();
		}
	}

	Placeholder<VarHolder> _placeholder;

#endif // POCO_NO_SOO
};


///
/// inlines
///


///
/// Var members
///

inline void Var::swap(Var& other)
{
#ifdef POCO_NO_SOO

	std::swap(_pHolder, other._pHolder);

#else

	if (this == &other) return;

	if (!_placeholder.isLocal() && !other._placeholder.isLocal())
	{
		std::swap(_placeholder.pHolder, other._placeholder.pHolder);
	}
	else
	{
		Var tmp(*this);
		try
		{
			if (_placeholder.isLocal()) destruct();
			construct(other);
			other = tmp;
		}
		catch (...)
		{
			construct(tmp);
			throw;
		}
	}

#endif
}


inline const std::type_info& Var::type() const
{
	VarHolder* pHolder = content();
	return pHolder ? pHolder->type() : typeid(void);
}


inline Var::ConstIterator Var::begin() const
{
	if (isEmpty()) return ConstIterator(const_cast<Var*>(this), true);

	return ConstIterator(const_cast<Var*>(this), false);
}

inline Var::ConstIterator Var::end() const
{
	return ConstIterator(const_cast<Var*>(this), true);
}

inline Var::Iterator Var::begin()
{
	if (isEmpty()) return Iterator(const_cast<Var*>(this), true);

	return Iterator(const_cast<Var*>(this), false);
}

inline Var::Iterator Var::end()
{
	return Iterator(this, true);
}


inline Var& Var::operator [] (const std::string& name)
{
	return getAt(name);
}


inline const Var& Var::operator [] (const std::string& name) const
{
	return const_cast<Var*>(this)->getAt(name);
}


inline const Var Var::operator + (const char* other) const
{
	return convert<std::string>() + other;
}


inline Var& Var::operator += (const char*other)
{
	return *this = convert<std::string>() + other;
}


inline bool Var::operator ! () const
{
	return !convert<bool>();
}


inline bool Var::isEmpty() const
{
	return 0 == content();
}


inline bool Var::isArray() const
{
	if (isEmpty() || 
		isString()) return false;

	VarHolder* pHolder = content();
	return pHolder ? pHolder->isArray() : false;
}


inline bool Var::isVector() const
{
	VarHolder* pHolder = content();
	return pHolder ? pHolder->isVector() : false;
}


inline bool Var::isList() const
{
	VarHolder* pHolder = content();
	return pHolder ? pHolder->isList() : false;
}


inline bool Var::isDeque() const
{
	VarHolder* pHolder = content();
	return pHolder ? pHolder->isDeque() : false;
}


inline bool Var::isStruct() const
{
	VarHolder* pHolder = content();
	return pHolder ? pHolder->isStruct() : false;
}


inline bool Var::isInteger() const
{
	VarHolder* pHolder = content();
	return pHolder ? pHolder->isInteger() : false;
}


inline bool Var::isSigned() const
{
	VarHolder* pHolder = content();
	return pHolder ? pHolder->isSigned() : false;
}


inline bool Var::isNumeric() const
{
	VarHolder* pHolder = content();
	return pHolder ? pHolder->isNumeric() : false;
}


inline bool Var::isBoolean() const
{
	VarHolder* pHolder = content();
	return pHolder ? pHolder->isBoolean() : false;
}


inline bool Var::isString() const
{
	VarHolder* pHolder = content();
	return pHolder ? pHolder->isString() : false;
}


inline std::size_t Var::size() const
{
	VarHolder* pHolder = content();
	return pHolder ? pHolder->size() : 0;
}


///
/// Var non-member functions
///

inline const Var operator + (const char* other, const Var& da)
	/// Addition operator for adding Var to const char*
{
	std::string tmp = other;
	return tmp + da.convert<std::string>();
}


inline char operator + (const char& other, const Var& da)
	/// Addition operator for adding Var to char
{
	return other + da.convert<char>();
}


inline char operator - (const char& other, const Var& da)
	/// Subtraction operator for subtracting Var from char
{
	return other - da.convert<char>();
}


inline char operator * (const char& other, const Var& da)
	/// Multiplication operator for multiplying Var with char
{
	return other * da.convert<char>();
}


inline char operator / (const char& other, const Var& da)
	/// Division operator for dividing Var with char
{
	return other / da.convert<char>();
}


inline char operator += (char& other, const Var& da)
	/// Addition asignment operator for adding Var to char
{
	return other += da.convert<char>();
}


inline char operator -= (char& other, const Var& da)
	/// Subtraction asignment operator for subtracting Var from char
{
	return other -= da.convert<char>();
}


inline char operator *= (char& other, const Var& da)
	/// Multiplication asignment operator for multiplying Var with char
{
	return other *= da.convert<char>();
}


inline char operator /= (char& other, const Var& da)
	/// Division asignment operator for dividing Var with char
{
	return other /= da.convert<char>();
}


inline bool operator == (const char& other, const Var& da)
	/// Equality operator for comparing Var with char
{
	if (da.isEmpty()) return false;
	return other == da.convert<char>();
}


inline bool operator != (const char& other, const Var& da)
	/// Inequality operator for comparing Var with char
{
	if (da.isEmpty()) return true;
	return other != da.convert<char>();
}


inline bool operator < (const char& other, const Var& da)
	/// Less than operator for comparing Var with char
{
	if (da.isEmpty()) return false;
	return other < da.convert<char>();
}


inline bool operator <= (const char& other, const Var& da)
	/// Less than or equal operator for comparing Var with char
{
	if (da.isEmpty()) return false;
	return other <= da.convert<char>();
}


inline bool operator > (const char& other, const Var& da)
	/// Greater than operator for comparing Var with char
{
	if (da.isEmpty())return false;
	return other > da.convert<char>();
}


inline bool operator >= (const char& other, const Var& da)
	/// Greater than or equal operator for comparing Var with char
{
	if (da.isEmpty())return false;
	return other >= da.convert<char>();
}


inline Poco::Int8 operator + (const Poco::Int8& other, const Var& da)
	/// Addition operator for adding Var to Poco::Int8
{
	return other + da.convert<Poco::Int8>();
}


inline Poco::Int8 operator - (const Poco::Int8& other, const Var& da)
	/// Subtraction operator for subtracting Var from Poco::Int8
{
	return other - da.convert<Poco::Int8>();
}


inline Poco::Int8 operator * (const Poco::Int8& other, const Var& da)
	/// Multiplication operator for multiplying Var with Poco::Int8
{
	return other * da.convert<Poco::Int8>();
}


inline Poco::Int8 operator / (const Poco::Int8& other, const Var& da)
	/// Division operator for dividing Var with Poco::Int8
{
	return other / da.convert<Poco::Int8>();
}


inline Poco::Int8 operator += (Poco::Int8& other, const Var& da)
	/// Addition asignment operator for adding Var to Poco::Int8
{
	return other += da.convert<Poco::Int8>();
}


inline Poco::Int8 operator -= (Poco::Int8& other, const Var& da)
	/// Subtraction asignment operator for subtracting Var from Poco::Int8
{
	return other -= da.convert<Poco::Int8>();
}


inline Poco::Int8 operator *= (Poco::Int8& other, const Var& da)
	/// Multiplication asignment operator for multiplying Var with Poco::Int8
{
	return other *= da.convert<Poco::Int8>();
}


inline Poco::Int8 operator /= (Poco::Int8& other, const Var& da)
	/// Division asignment operator for dividing Var with Poco::Int8
{
	return other /= da.convert<Poco::Int8>();
}


inline bool operator == (const Poco::Int8& other, const Var& da)
	/// Equality operator for comparing Var with Poco::Int8
{
	if (da.isEmpty()) return false;
	return other == da.convert<Poco::Int8>();
}


inline bool operator != (const Poco::Int8& other, const Var& da)
	/// Inequality operator for comparing Var with Poco::Int8
{
	if (da.isEmpty()) return true;
	return other != da.convert<Poco::Int8>();
}


inline bool operator < (const Poco::Int8& other, const Var& da)
	/// Less than operator for comparing Var with Poco::Int8
{
	if (da.isEmpty()) return false;
	return other < da.convert<Poco::Int8>();
}


inline bool operator <= (const Poco::Int8& other, const Var& da)
	/// Less than or equal operator for comparing Var with Poco::Int8
{
	if (da.isEmpty()) return false;
	return other <= da.convert<Poco::Int8>();
}


inline bool operator > (const Poco::Int8& other, const Var& da)
	/// Greater than operator for comparing Var with Poco::Int8
{
	if (da.isEmpty()) return false;
	return other > da.convert<Poco::Int8>();
}


inline bool operator >= (const Poco::Int8& other, const Var& da)
	/// Greater than or equal operator for comparing Var with Poco::Int8
{
	if (da.isEmpty()) return false;
	return other >= da.convert<Poco::Int8>();
}


inline Poco::UInt8 operator + (const Poco::UInt8& other, const Var& da)
	/// Addition operator for adding Var to Poco::UInt8
{
	return other + da.convert<Poco::UInt8>();
}


inline Poco::UInt8 operator - (const Poco::UInt8& other, const Var& da)
	/// Subtraction operator for subtracting Var from Poco::UInt8
{
	return other - da.convert<Poco::UInt8>();
}


inline Poco::UInt8 operator * (const Poco::UInt8& other, const Var& da)
	/// Multiplication operator for multiplying Var with Poco::UInt8
{
	return other * da.convert<Poco::UInt8>();
}


inline Poco::UInt8 operator / (const Poco::UInt8& other, const Var& da)
	/// Division operator for dividing Var with Poco::UInt8
{
	return other / da.convert<Poco::UInt8>();
}


inline Poco::UInt8 operator += (Poco::UInt8& other, const Var& da)
	/// Addition asignment operator for adding Var to Poco::UInt8
{
	return other += da.convert<Poco::UInt8>();
}


inline Poco::UInt8 operator -= (Poco::UInt8& other, const Var& da)
	/// Subtraction asignment operator for subtracting Var from Poco::UInt8
{
	return other -= da.convert<Poco::UInt8>();
}


inline Poco::UInt8 operator *= (Poco::UInt8& other, const Var& da)
	/// Multiplication asignment operator for multiplying Var with Poco::UInt8
{
	return other *= da.convert<Poco::UInt8>();
}


inline Poco::UInt8 operator /= (Poco::UInt8& other, const Var& da)
	/// Division asignment operator for dividing Var with Poco::UInt8
{
	return other /= da.convert<Poco::UInt8>();
}


inline bool operator == (const Poco::UInt8& other, const Var& da)
	/// Equality operator for comparing Var with Poco::UInt8
{
	if (da.isEmpty()) return false;
	return other == da.convert<Poco::UInt8>();
}


inline bool operator != (const Poco::UInt8& other, const Var& da)
	/// Inequality operator for comparing Var with Poco::UInt8
{
	if (da.isEmpty()) return true;
	return other != da.convert<Poco::UInt8>();
}


inline bool operator < (const Poco::UInt8& other, const Var& da)
	/// Less than operator for comparing Var with Poco::UInt8
{
	if (da.isEmpty()) return false;
	return other < da.convert<Poco::UInt8>();
}


inline bool operator <= (const Poco::UInt8& other, const Var& da)
	/// Less than or equal operator for comparing Var with Poco::UInt8
{
	if (da.isEmpty()) return false;
	return other <= da.convert<Poco::UInt8>();
}


inline bool operator > (const Poco::UInt8& other, const Var& da)
	/// Greater than operator for comparing Var with Poco::UInt8
{
	if (da.isEmpty()) return false;
	return other > da.convert<Poco::UInt8>();
}


inline bool operator >= (const Poco::UInt8& other, const Var& da)
	/// Greater than or equal operator for comparing Var with Poco::UInt8
{
	if (da.isEmpty()) return false;
	return other >= da.convert<Poco::UInt8>();
}


inline Poco::Int16 operator + (const Poco::Int16& other, const Var& da)
	/// Addition operator for adding Var to Poco::Int16
{
	return other + da.convert<Poco::Int16>();
}


inline Poco::Int16 operator - (const Poco::Int16& other, const Var& da)
	/// Subtraction operator for subtracting Var from Poco::Int16
{
	return other - da.convert<Poco::Int16>();
}


inline Poco::Int16 operator * (const Poco::Int16& other, const Var& da)
	/// Multiplication operator for multiplying Var with Poco::Int16
{
	return other * da.convert<Poco::Int16>();
}


inline Poco::Int16 operator / (const Poco::Int16& other, const Var& da)
	/// Division operator for dividing Var with Poco::Int16
{
	return other / da.convert<Poco::Int16>();
}


inline Poco::Int16 operator += (Poco::Int16& other, const Var& da)
	/// Addition asignment operator for adding Var to Poco::Int16
{
	return other += da.convert<Poco::Int16>();
}


inline Poco::Int16 operator -= (Poco::Int16& other, const Var& da)
	/// Subtraction asignment operator for subtracting Var from Poco::Int16
{
	return other -= da.convert<Poco::Int16>();
}


inline Poco::Int16 operator *= (Poco::Int16& other, const Var& da)
	/// Multiplication asignment operator for multiplying Var with Poco::Int16
{
	return other *= da.convert<Poco::Int16>();
}


inline Poco::Int16 operator /= (Poco::Int16& other, const Var& da)
	/// Division asignment operator for dividing Var with Poco::Int16
{
	return other /= da.convert<Poco::Int16>();
}


inline bool operator == (const Poco::Int16& other, const Var& da)
	/// Equality operator for comparing Var with Poco::Int16
{
	if (da.isEmpty()) return false;
	return other == da.convert<Poco::Int16>();
}


inline bool operator != (const Poco::Int16& other, const Var& da)
	/// Inequality operator for comparing Var with Poco::Int16
{
	if (da.isEmpty()) return true;
	return other != da.convert<Poco::Int16>();
}


inline bool operator < (const Poco::Int16& other, const Var& da)
	/// Less than operator for comparing Var with Poco::Int16
{
	if (da.isEmpty()) return false;
	return other < da.convert<Poco::Int16>();
}


inline bool operator <= (const Poco::Int16& other, const Var& da)
	/// Less than or equal operator for comparing Var with Poco::Int16
{
	if (da.isEmpty()) return false;
	return other <= da.convert<Poco::Int16>();
}


inline bool operator > (const Poco::Int16& other, const Var& da)
	/// Greater than operator for comparing Var with Poco::Int16
{
	if (da.isEmpty()) return false;
	return other > da.convert<Poco::Int16>();
}


inline bool operator >= (const Poco::Int16& other, const Var& da)
	/// Greater than or equal operator for comparing Var with Poco::Int16
{
	if (da.isEmpty()) return false;
	return other >= da.convert<Poco::Int16>();
}


inline Poco::UInt16 operator + (const Poco::UInt16& other, const Var& da)
	/// Addition operator for adding Var to Poco::UInt16
{
	return other + da.convert<Poco::UInt16>();
}


inline Poco::UInt16 operator - (const Poco::UInt16& other, const Var& da)
	/// Subtraction operator for subtracting Var from Poco::UInt16
{
	return other - da.convert<Poco::UInt16>();
}


inline Poco::UInt16 operator * (const Poco::UInt16& other, const Var& da)
	/// Multiplication operator for multiplying Var with Poco::UInt16
{
	return other * da.convert<Poco::UInt16>();
}


inline Poco::UInt16 operator / (const Poco::UInt16& other, const Var& da)
	/// Division operator for dividing Var with Poco::UInt16
{
	return other / da.convert<Poco::UInt16>();
}


inline Poco::UInt16 operator += (Poco::UInt16& other, const Var& da)
	/// Addition asignment operator for adding Var to Poco::UInt16
{
	return other += da.convert<Poco::UInt16>();
}


inline Poco::UInt16 operator -= (Poco::UInt16& other, const Var& da)
	/// Subtraction asignment operator for subtracting Var from Poco::UInt16
{
	return other -= da.convert<Poco::UInt16>();
}


inline Poco::UInt16 operator *= (Poco::UInt16& other, const Var& da)
	/// Multiplication asignment operator for multiplying Var with Poco::UInt16
{
	return other *= da.convert<Poco::UInt16>();
}


inline Poco::UInt16 operator /= (Poco::UInt16& other, const Var& da)
	/// Division asignment operator for dividing Var with Poco::UInt16
{
	return other /= da.convert<Poco::UInt16>();
}


inline bool operator == (const Poco::UInt16& other, const Var& da)
	/// Equality operator for comparing Var with Poco::UInt16
{
	if (da.isEmpty()) return false;
	return other == da.convert<Poco::UInt16>();
}


inline bool operator != (const Poco::UInt16& other, const Var& da)
	/// Inequality operator for comparing Var with Poco::UInt16
{
	if (da.isEmpty()) return true;
	return other != da.convert<Poco::UInt16>();
}


inline bool operator < (const Poco::UInt16& other, const Var& da)
	/// Less than operator for comparing Var with Poco::UInt16
{
	if (da.isEmpty()) return false;
	return other < da.convert<Poco::UInt16>();
}


inline bool operator <= (const Poco::UInt16& other, const Var& da)
	/// Less than or equal operator for comparing Var with Poco::UInt16
{
	if (da.isEmpty()) return false;
	return other <= da.convert<Poco::UInt16>();
}


inline bool operator > (const Poco::UInt16& other, const Var& da)
	/// Greater than operator for comparing Var with Poco::UInt16
{
	if (da.isEmpty()) return false;
	return other > da.convert<Poco::UInt16>();
}


inline bool operator >= (const Poco::UInt16& other, const Var& da)
	/// Greater than or equal operator for comparing Var with Poco::UInt16
{
	if (da.isEmpty()) return false;
	return other >= da.convert<Poco::UInt16>();
}


inline Poco::Int32 operator + (const Poco::Int32& other, const Var& da)
	/// Addition operator for adding Var to Poco::Int32
{
	return other + da.convert<Poco::Int32>();
}


inline Poco::Int32 operator - (const Poco::Int32& other, const Var& da)
	/// Subtraction operator for subtracting Var from Poco::Int32
{
	return other - da.convert<Poco::Int32>();
}


inline Poco::Int32 operator * (const Poco::Int32& other, const Var& da)
	/// Multiplication operator for multiplying Var with Poco::Int32
{
	return other * da.convert<Poco::Int32>();
}


inline Poco::Int32 operator / (const Poco::Int32& other, const Var& da)
	/// Division operator for dividing Var with Poco::Int32
{
	return other / da.convert<Poco::Int32>();
}


inline Poco::Int32 operator += (Poco::Int32& other, const Var& da)
	/// Addition asignment operator for adding Var to Poco::Int32
{
	return other += da.convert<Poco::Int32>();
}


inline Poco::Int32 operator -= (Poco::Int32& other, const Var& da)
	/// Subtraction asignment operator for subtracting Var from Poco::Int32
{
	return other -= da.convert<Poco::Int32>();
}


inline Poco::Int32 operator *= (Poco::Int32& other, const Var& da)
	/// Multiplication asignment operator for multiplying Var with Poco::Int32
{
	return other *= da.convert<Poco::Int32>();
}


inline Poco::Int32 operator /= (Poco::Int32& other, const Var& da)
	/// Division asignment operator for dividing Var with Poco::Int32
{
	return other /= da.convert<Poco::Int32>();
}


inline bool operator == (const Poco::Int32& other, const Var& da)
	/// Equality operator for comparing Var with Poco::Int32
{
	if (da.isEmpty()) return false;
	return other == da.convert<Poco::Int32>();
}


inline bool operator != (const Poco::Int32& other, const Var& da)
	/// Inequality operator for comparing Var with Poco::Int32
{
	if (da.isEmpty()) return true;
	return other != da.convert<Poco::Int32>();
}


inline bool operator < (const Poco::Int32& other, const Var& da)
	/// Less than operator for comparing Var with Poco::Int32
{
	if (da.isEmpty()) return false;
	return other < da.convert<Poco::Int32>();
}


inline bool operator <= (const Poco::Int32& other, const Var& da)
	/// Less than or equal operator for comparing Var with Poco::Int32
{
	if (da.isEmpty()) return false;
	return other <= da.convert<Poco::Int32>();
}


inline bool operator > (const Poco::Int32& other, const Var& da)
	/// Greater than operator for comparing Var with Poco::Int32
{
	if (da.isEmpty()) return false;
	return other > da.convert<Poco::Int32>();
}


inline bool operator >= (const Poco::Int32& other, const Var& da)
	/// Greater than or equal operator for comparing Var with Poco::Int32
{
	if (da.isEmpty()) return false;
	return other >= da.convert<Poco::Int32>();
}


inline Poco::UInt32 operator + (const Poco::UInt32& other, const Var& da)
	/// Addition operator for adding Var to Poco::UInt32
{
	return other + da.convert<Poco::UInt32>();
}


inline Poco::UInt32 operator - (const Poco::UInt32& other, const Var& da)
	/// Subtraction operator for subtracting Var from Poco::UInt32
{
	return other - da.convert<Poco::UInt32>();
}


inline Poco::UInt32 operator * (const Poco::UInt32& other, const Var& da)
	/// Multiplication operator for multiplying Var with Poco::UInt32
{
	return other * da.convert<Poco::UInt32>();
}


inline Poco::UInt32 operator / (const Poco::UInt32& other, const Var& da)
	/// Division operator for dividing Var with Poco::UInt32
{
	return other / da.convert<Poco::UInt32>();
}


inline Poco::UInt32 operator += (Poco::UInt32& other, const Var& da)
	/// Addition asignment operator for adding Var to Poco::UInt32
{
	return other += da.convert<Poco::UInt32>();
}


inline Poco::UInt32 operator -= (Poco::UInt32& other, const Var& da)
	/// Subtraction asignment operator for subtracting Var from Poco::UInt32
{
	return other -= da.convert<Poco::UInt32>();
}


inline Poco::UInt32 operator *= (Poco::UInt32& other, const Var& da)
	/// Multiplication asignment operator for multiplying Var with Poco::UInt32
{
	return other *= da.convert<Poco::UInt32>();
}


inline Poco::UInt32 operator /= (Poco::UInt32& other, const Var& da)
	/// Division asignment operator for dividing Var with Poco::UInt32
{
	return other /= da.convert<Poco::UInt32>();
}


inline bool operator == (const Poco::UInt32& other, const Var& da)
	/// Equality operator for comparing Var with Poco::UInt32
{
	if (da.isEmpty()) return false;
	return other == da.convert<Poco::UInt32>();
}


inline bool operator != (const Poco::UInt32& other, const Var& da)
	/// Inequality operator for comparing Var with Poco::UInt32
{
	if (da.isEmpty()) return true;
	return other != da.convert<Poco::UInt32>();
}


inline bool operator < (const Poco::UInt32& other, const Var& da)
	/// Less than operator for comparing Var with Poco::UInt32
{
	if (da.isEmpty()) return false;
	return other < da.convert<Poco::UInt32>();
}


inline bool operator <= (const Poco::UInt32& other, const Var& da)
	/// Less than or equal operator for comparing Var with Poco::UInt32
{
	if (da.isEmpty()) return false;
	return other <= da.convert<Poco::UInt32>();
}


inline bool operator > (const Poco::UInt32& other, const Var& da)
	/// Greater than operator for comparing Var with Poco::UInt32
{
	if (da.isEmpty()) return false;
	return other > da.convert<Poco::UInt32>();
}


inline bool operator >= (const Poco::UInt32& other, const Var& da)
	/// Greater than or equal operator for comparing Var with Poco::UInt32
{
	if (da.isEmpty()) return false;
	return other >= da.convert<Poco::UInt32>();
}


inline Poco::Int64 operator + (const Poco::Int64& other, const Var& da)
	/// Addition operator for adding Var to Poco::Int64
{
	return other + da.convert<Poco::Int64>();
}


inline Poco::Int64 operator - (const Poco::Int64& other, const Var& da)
	/// Subtraction operator for subtracting Var from Poco::Int64
{
	return other - da.convert<Poco::Int64>();
}


inline Poco::Int64 operator * (const Poco::Int64& other, const Var& da)
	/// Multiplication operator for multiplying Var with Poco::Int64
{
	return other * da.convert<Poco::Int64>();
}


inline Poco::Int64 operator / (const Poco::Int64& other, const Var& da)
	/// Division operator for dividing Var with Poco::Int64
{
	return other / da.convert<Poco::Int64>();
}


inline Poco::Int64 operator += (Poco::Int64& other, const Var& da)
	/// Addition asignment operator for adding Var to Poco::Int64
{
	return other += da.convert<Poco::Int64>();
}


inline Poco::Int64 operator -= (Poco::Int64& other, const Var& da)
	/// Subtraction asignment operator for subtracting Var from Poco::Int64
{
	return other -= da.convert<Poco::Int64>();
}


inline Poco::Int64 operator *= (Poco::Int64& other, const Var& da)
	/// Multiplication asignment operator for multiplying Var with Poco::Int64
{
	return other *= da.convert<Poco::Int64>();
}


inline Poco::Int64 operator /= (Poco::Int64& other, const Var& da)
	/// Division asignment operator for dividing Var with Poco::Int64
{
	return other /= da.convert<Poco::Int64>();
}


inline bool operator == (const Poco::Int64& other, const Var& da)
	/// Equality operator for comparing Var with Poco::Int64
{
	if (da.isEmpty()) return false;
	return other == da.convert<Poco::Int64>();
}


inline bool operator != (const Poco::Int64& other, const Var& da)
	/// Inequality operator for comparing Var with Poco::Int64
{
	if (da.isEmpty()) return true;
	return other != da.convert<Poco::Int64>();
}


inline bool operator < (const Poco::Int64& other, const Var& da)
	/// Less than operator for comparing Var with Poco::Int64
{
	if (da.isEmpty()) return false;
	return other < da.convert<Poco::Int64>();
}


inline bool operator <= (const Poco::Int64& other, const Var& da)
	/// Less than or equal operator for comparing Var with Poco::Int64
{
	if (da.isEmpty()) return false;
	return other <= da.convert<Poco::Int64>();
}


inline bool operator > (const Poco::Int64& other, const Var& da)
	/// Greater than operator for comparing Var with Poco::Int64
{
	if (da.isEmpty()) return false;
	return other > da.convert<Poco::Int64>();
}


inline bool operator >= (const Poco::Int64& other, const Var& da)
	/// Greater than or equal operator for comparing Var with Poco::Int64
{
	if (da.isEmpty()) return false;
	return other >= da.convert<Poco::Int64>();
}


inline Poco::UInt64 operator + (const Poco::UInt64& other, const Var& da)
	/// Addition operator for adding Var to Poco::UInt64
{
	return other + da.convert<Poco::UInt64>();
}


inline Poco::UInt64 operator - (const Poco::UInt64& other, const Var& da)
	/// Subtraction operator for subtracting Var from Poco::UInt64
{
	return other - da.convert<Poco::UInt64>();
}


inline Poco::UInt64 operator * (const Poco::UInt64& other, const Var& da)
	/// Multiplication operator for multiplying Var with Poco::UInt64
{
	return other * da.convert<Poco::UInt64>();
}


inline Poco::UInt64 operator / (const Poco::UInt64& other, const Var& da)
	/// Division operator for dividing Var with Poco::UInt64
{
	return other / da.convert<Poco::UInt64>();
}


inline Poco::UInt64 operator += (Poco::UInt64& other, const Var& da)
	/// Addition asignment operator for adding Var to Poco::UInt64
{
	return other += da.convert<Poco::UInt64>();
}


inline Poco::UInt64 operator -= (Poco::UInt64& other, const Var& da)
	/// Subtraction asignment operator for subtracting Var from Poco::UInt64
{
	return other -= da.convert<Poco::UInt64>();
}


inline Poco::UInt64 operator *= (Poco::UInt64& other, const Var& da)
	/// Multiplication asignment operator for multiplying Var with Poco::UInt64
{
	return other *= da.convert<Poco::UInt64>();
}


inline Poco::UInt64 operator /= (Poco::UInt64& other, const Var& da)
	/// Division asignment operator for dividing Var with Poco::UInt64
{
	return other /= da.convert<Poco::UInt64>();
}


inline bool operator == (const Poco::UInt64& other, const Var& da)
	/// Equality operator for comparing Var with Poco::UInt64
{
	if (da.isEmpty()) return false;
	return other == da.convert<Poco::UInt64>();
}


inline bool operator != (const Poco::UInt64& other, const Var& da)
	/// Inequality operator for comparing Var with Poco::UInt64
{
	if (da.isEmpty()) return true;
	return other != da.convert<Poco::UInt64>();
}


inline bool operator < (const Poco::UInt64& other, const Var& da)
	/// Less than operator for comparing Var with Poco::UInt64
{
	if (da.isEmpty()) return false;
	return other < da.convert<Poco::UInt64>();
}


inline bool operator <= (const Poco::UInt64& other, const Var& da)
	/// Less than or equal operator for comparing Var with Poco::UInt64
{
	if (da.isEmpty()) return false;
	return other <= da.convert<Poco::UInt64>();
}


inline bool operator > (const Poco::UInt64& other, const Var& da)
	/// Greater than operator for comparing Var with Poco::UInt64
{
	if (da.isEmpty()) return false;
	return other > da.convert<Poco::UInt64>();
}


inline bool operator >= (const Poco::UInt64& other, const Var& da)
	/// Greater than or equal operator for comparing Var with Poco::UInt64
{
	if (da.isEmpty()) return false;
	return other >= da.convert<Poco::UInt64>();
}


inline float operator + (const float& other, const Var& da)
	/// Addition operator for adding Var to float
{
	return other + da.convert<float>();
}


inline float operator - (const float& other, const Var& da)
	/// Subtraction operator for subtracting Var from float
{
	return other - da.convert<float>();
}


inline float operator * (const float& other, const Var& da)
	/// Multiplication operator for multiplying Var with float
{
	return other * da.convert<float>();
}


inline float operator / (const float& other, const Var& da)
	/// Division operator for dividing Var with float
{
	return other / da.convert<float>();
}


inline float operator += (float& other, const Var& da)
	/// Addition asignment operator for adding Var to float
{
	return other += da.convert<float>();
}


inline float operator -= (float& other, const Var& da)
	/// Subtraction asignment operator for subtracting Var from float
{
	return other -= da.convert<float>();
}


inline float operator *= (float& other, const Var& da)
	/// Multiplication asignment operator for multiplying Var with float
{
	return other *= da.convert<float>();
}


inline float operator /= (float& other, const Var& da)
	/// Division asignment operator for dividing Var with float
{
	return other /= da.convert<float>();
}


inline bool operator == (const float& other, const Var& da)
	/// Equality operator for comparing Var with float
{
	if (da.isEmpty()) return false;
	return other == da.convert<float>();
}


inline bool operator != (const float& other, const Var& da)
	/// Inequality operator for comparing Var with float
{
	if (da.isEmpty()) return true;
	return other != da.convert<float>();
}


inline bool operator < (const float& other, const Var& da)
	/// Less than operator for comparing Var with float
{
	if (da.isEmpty()) return false;
	return other < da.convert<float>();
}


inline bool operator <= (const float& other, const Var& da)
	/// Less than or equal operator for comparing Var with float
{
	if (da.isEmpty()) return false;
	return other <= da.convert<float>();
}


inline bool operator > (const float& other, const Var& da)
	/// Greater than operator for comparing Var with float
{
	if (da.isEmpty()) return false;
	return other > da.convert<float>();
}


inline bool operator >= (const float& other, const Var& da)
	/// Greater than or equal operator for comparing Var with float
{
	if (da.isEmpty()) return false;
	return other >= da.convert<float>();
}


inline double operator + (const double& other, const Var& da)
	/// Addition operator for adding Var to double
{
	return other + da.convert<double>();
}


inline double operator - (const double& other, const Var& da)
	/// Subtraction operator for subtracting Var from double
{
	return other - da.convert<double>();
}


inline double operator * (const double& other, const Var& da)
	/// Multiplication operator for multiplying Var with double
{
	return other * da.convert<double>();
}


inline double operator / (const double& other, const Var& da)
	/// Division operator for dividing Var with double
{
	return other / da.convert<double>();
}


inline double operator += (double& other, const Var& da)
	/// Addition asignment operator for adding Var to double
{
	return other += da.convert<double>();
}


inline double operator -= (double& other, const Var& da)
	/// Subtraction asignment operator for subtracting Var from double
{
	return other -= da.convert<double>();
}


inline double operator *= (double& other, const Var& da)
	/// Multiplication asignment operator for multiplying Var with double
{
	return other *= da.convert<double>();
}


inline double operator /= (double& other, const Var& da)
	/// Division asignment operator for dividing Var with double
{
	return other /= da.convert<double>();
}


inline bool operator == (const double& other, const Var& da)
	/// Equality operator for comparing Var with double
{
	if (da.isEmpty()) return false;
	return other == da.convert<double>();
}


inline bool operator != (const double& other, const Var& da)
	/// Inequality operator for comparing Var with double
{
	if (da.isEmpty()) return true;
	return other != da.convert<double>();
}


inline bool operator < (const double& other, const Var& da)
	/// Less than operator for comparing Var with double
{
	if (da.isEmpty()) return false;
	return other < da.convert<double>();
}


inline bool operator <= (const double& other, const Var& da)
	/// Less than or equal operator for comparing Var with double
{
	if (da.isEmpty()) return false;
	return other <= da.convert<double>();
}


inline bool operator > (const double& other, const Var& da)
	/// Greater than operator for comparing Var with double
{
	if (da.isEmpty()) return false;
	return other > da.convert<double>();
}


inline bool operator >= (const double& other, const Var& da)
	/// Greater than or equal operator for comparing Var with double
{
	if (da.isEmpty()) return false;
	return other >= da.convert<double>();
}


inline bool operator == (const bool& other, const Var& da)
	/// Equality operator for comparing Var with bool
{
	if (da.isEmpty()) return false;
	return other == da.convert<bool>();
}


inline bool operator != (const bool& other, const Var& da)
	/// Inequality operator for comparing Var with bool
{
	if (da.isEmpty()) return true;
	return other != da.convert<bool>();
}


inline bool operator == (const std::string& other, const Var& da)
	/// Equality operator for comparing Var with std::string
{
	if (da.isEmpty()) return false;
	return other == da.convert<std::string>();
}


inline bool operator != (const std::string& other, const Var& da)
	/// Inequality operator for comparing Var with std::string
{
	if (da.isEmpty()) return true;
	return other != da.convert<std::string>();
}


inline bool operator == (const UTF16String& other, const Var& da)
	/// Equality operator for comparing Var with UTF16String
{
	if (da.isEmpty()) return false;
	return other == da.convert<UTF16String>();
}


inline bool operator != (const UTF16String& other, const Var& da)
	/// Inequality operator for comparing Var with UTF16String
{
	if (da.isEmpty()) return true;
	return other != da.convert<UTF16String>();
}


inline bool operator == (const char* other, const Var& da)
	/// Equality operator for comparing Var with const char*
{
	if (da.isEmpty()) return false;
	return da.convert<std::string>() == other;
}


inline bool operator != (const char* other, const Var& da)
	/// Inequality operator for comparing Var with const char*
{
	if (da.isEmpty()) return true;
	return da.convert<std::string>() != other;
}


#ifndef POCO_LONG_IS_64_BIT


inline long operator + (const long& other, const Var& da)
	/// Addition operator for adding Var to long
{
	return other + da.convert<long>();
}


inline long operator - (const long& other, const Var& da)
	/// Subtraction operator for subtracting Var from long
{
	return other - da.convert<long>();
}


inline long operator * (const long& other, const Var& da)
	/// Multiplication operator for multiplying Var with long
{
	return other * da.convert<long>();
}


inline long operator / (const long& other, const Var& da)
	/// Division operator for dividing Var with long
{
	return other / da.convert<long>();
}


inline long operator += (long& other, const Var& da)
	/// Addition asignment operator for adding Var to long
{
	return other += da.convert<long>();
}


inline long operator -= (long& other, const Var& da)
	/// Subtraction asignment operator for subtracting Var from long
{
	return other -= da.convert<long>();
}


inline long operator *= (long& other, const Var& da)
	/// Multiplication asignment operator for multiplying Var with long
{
	return other *= da.convert<long>();
}


inline long operator /= (long& other, const Var& da)
	/// Division asignment operator for dividing Var with long
{
	return other /= da.convert<long>();
}


inline bool operator == (const long& other, const Var& da)
	/// Equality operator for comparing Var with long
{
	if (da.isEmpty()) return false;
	return other == da.convert<long>();
}


inline bool operator != (const long& other, const Var& da)
	/// Inequality operator for comparing Var with long
{
	if (da.isEmpty()) return true;
	return other != da.convert<long>();
}


inline bool operator < (const long& other, const Var& da)
	/// Less than operator for comparing Var with long
{
	if (da.isEmpty()) return false;
	return other < da.convert<long>();
}


inline bool operator <= (const long& other, const Var& da)
	/// Less than or equal operator for comparing Var with long
{
	if (da.isEmpty()) return false;
	return other <= da.convert<long>();
}


inline bool operator > (const long& other, const Var& da)
	/// Greater than operator for comparing Var with long
{
	if (da.isEmpty()) return false;
	return other > da.convert<long>();
}


inline bool operator >= (const long& other, const Var& da)
	/// Greater than or equal operator for comparing Var with long
{
	if (da.isEmpty()) return false;
	return other >= da.convert<long>();
}


#endif // POCO_LONG_IS_64_BIT


} // namespace Dynamic


//@ deprecated
typedef Dynamic::Var DynamicAny;


} // namespace Poco


#endif // Foundation_Var_INCLUDED
