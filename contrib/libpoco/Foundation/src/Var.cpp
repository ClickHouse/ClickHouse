//
// Var.cpp
//
// $Id: //poco/svn/Foundation/src/Var.cpp#3 $
//
// Library: Foundation
// Package: Core
// Module:  Var
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Dynamic/Var.h"
#include "Poco/Dynamic/Struct.h"
#include <algorithm>
#include <cctype>
#include <vector>
#include <list>
#include <deque>


namespace Poco {
namespace Dynamic {


Var::Var()
#ifdef POCO_NO_SOO
	: _pHolder(0)
#endif
{
}


Var::Var(const char* pVal)
#ifdef POCO_NO_SOO 
	: _pHolder(new VarHolderImpl<std::string>(pVal))
{
}
#else
{
	construct(std::string(pVal));
}
#endif


Var::Var(const Var& other)
#ifdef POCO_NO_SOO
	: _pHolder(other._pHolder ? other._pHolder->clone() : 0)
{
}
#else
{
	if ((this != &other) && !other.isEmpty())
			construct(other);
}
#endif


Var::~Var()
{
	destruct();
}


Var& Var::operator = (const Var& rhs)
{
#ifdef POCO_NO_SOO
	Var tmp(rhs);
	swap(tmp);
#else
	if ((this != &rhs) && !rhs.isEmpty())
		construct(rhs);
	else if ((this != &rhs) && rhs.isEmpty())
		_placeholder.erase();
#endif
	return *this;
}


const Var Var::operator + (const Var& other) const
{
	if (isInteger())
	{
		if (isSigned())
			return add<Poco::Int64>(other);
		else
			return add<Poco::UInt64>(other);
	}
	else if (isNumeric())
		return add<double>(other);
	else if (isString())
		return add<std::string>(other);
	else
		throw InvalidArgumentException("Invalid operation for this data type.");
}


Var& Var::operator += (const Var& other)
{
	if (isInteger())
	{
		if (isSigned())
			return *this = add<Poco::Int64>(other);
		else
			return *this = add<Poco::UInt64>(other);
	}
	else if (isNumeric())
		return *this = add<double>(other);
	else if (isString())
		return *this = add<std::string>(other);
	else
		throw InvalidArgumentException("Invalid operation for this data type.");
}


const Var Var::operator - (const Var& other) const
{
	if (isInteger())
	{
		if (isSigned())
			return subtract<Poco::Int64>(other);
		else
			return subtract<Poco::UInt64>(other);
	}
	else if (isNumeric())
		return subtract<double>(other);
	else
		throw InvalidArgumentException("Invalid operation for this data type.");
}


Var& Var::operator -= (const Var& other)
{
	if (isInteger())
	{
		if (isSigned())
			return *this = subtract<Poco::Int64>(other);
		else
			return *this = subtract<Poco::UInt64>(other);
	}
	else if (isNumeric())
		return *this = subtract<double>(other);
	else
		throw InvalidArgumentException("Invalid operation for this data type.");
}


const Var Var::operator * (const Var& other) const
{
	if (isInteger())
	{
		if (isSigned())
			return multiply<Poco::Int64>(other);
		else
			return multiply<Poco::UInt64>(other);
	}
	else if (isNumeric())
		return multiply<double>(other);
	else
		throw InvalidArgumentException("Invalid operation for this data type.");
}


Var& Var::operator *= (const Var& other)
{
	if (isInteger())
	{
		if (isSigned())
			return *this = multiply<Poco::Int64>(other);
		else
			return *this = multiply<Poco::UInt64>(other);
	}
	else if (isNumeric())
		return *this = multiply<double>(other);
	else
		throw InvalidArgumentException("Invalid operation for this data type.");
}


const Var Var::operator / (const Var& other) const
{
	if (isInteger())
	{
		if (isSigned())
			return divide<Poco::Int64>(other);
		else
			return divide<Poco::UInt64>(other);
	}
	else if (isNumeric())
		return divide<double>(other);
	else
		throw InvalidArgumentException("Invalid operation for this data type.");
}


Var& Var::operator /= (const Var& other)
{
	if (isInteger())
	{
		if (isSigned())
			return *this = divide<Poco::Int64>(other);
		else
			return *this = divide<Poco::UInt64>(other);
	}
	else if (isNumeric())
		return *this = divide<double>(other);
	else
		throw InvalidArgumentException("Invalid operation for this data type.");
}


Var& Var::operator ++ ()
{
	if (!isInteger())
		throw InvalidArgumentException("Invalid operation for this data type.");

	return *this = *this + 1;
}


const Var Var::operator ++ (int)
{
	if (!isInteger())
		throw InvalidArgumentException("Invalid operation for this data type.");

	Var tmp(*this);
	*this += 1;
	return tmp;
}


Var& Var::operator -- ()
{
	if (!isInteger())
		throw InvalidArgumentException("Invalid operation for this data type.");

	return *this = *this - 1;
}


const Var Var::operator -- (int)
{
	if (!isInteger())
		throw InvalidArgumentException("Invalid operation for this data type.");

	Var tmp(*this);
	*this -= 1;
	return tmp;
}


bool Var::operator == (const Var& other) const
{
	if (isEmpty() != other.isEmpty()) return false;
	if (isEmpty() && other.isEmpty()) return true;
	return convert<std::string>() == other.convert<std::string>();
}


bool Var::operator == (const char* other) const
{
	if (isEmpty()) return false;
	return convert<std::string>() == other;
}


bool Var::operator != (const Var& other) const
{
	if (isEmpty() && other.isEmpty()) return false;
	else if (isEmpty() || other.isEmpty()) return true;

	return convert<std::string>() != other.convert<std::string>();
}


bool Var::operator != (const char* other) const
{
	if (isEmpty()) return true;
	return convert<std::string>() != other;
}


bool Var::operator < (const Var& other) const
{
	if (isEmpty() || other.isEmpty()) return false;
	return convert<std::string>() < other.convert<std::string>();
}


bool Var::operator <= (const Var& other) const
{
	if (isEmpty() || other.isEmpty()) return false;
	return convert<std::string>() <= other.convert<std::string>();
}


bool Var::operator > (const Var& other) const
{
	if (isEmpty() || other.isEmpty()) return false;
	return convert<std::string>() > other.convert<std::string>();
}


bool Var::operator >= (const Var& other) const
{
	if (isEmpty() || other.isEmpty()) return false;
	return convert<std::string>() >= other.convert<std::string>();
}


bool Var::operator || (const Var& other) const
{
	if (isEmpty() || other.isEmpty()) return false;
	return convert<bool>() || other.convert<bool>();
}


bool Var::operator && (const Var& other) const
{
	if (isEmpty() || other.isEmpty()) return false;
	return convert<bool>() && other.convert<bool>();
}


void Var::empty()
{
#ifdef POCO_NO_SOO
	delete _pHolder;
	_pHolder = 0;
#else
	if (_placeholder.isLocal()) this->~Var();
	else delete content();
	_placeholder.erase();
#endif
}


Var& Var::getAt(std::size_t n)
{
	if (isVector())
		return holderImpl<std::vector<Var>,
			InvalidAccessException>("Not a vector.")->operator[](n);
	else if (isList())
		return holderImpl<std::list<Var>,
			InvalidAccessException>("Not a list.")->operator[](n);
	else if (isDeque())
		return holderImpl<std::deque<Var>,
			InvalidAccessException>("Not a deque.")->operator[](n);
	else if (isStruct())
		return structIndexOperator(holderImpl<Struct<int>,
			InvalidAccessException>("Not a struct."), static_cast<int>(n));
	else if (!isString() && !isEmpty() && (n == 0))
		return *this;
	
	throw RangeException("Index out of bounds.");
}


char& Var::at(std::size_t n)
{
	if (isString())
	{
		return holderImpl<std::string,
			InvalidAccessException>("Not a string.")->operator[](n);
	}

	throw InvalidAccessException("Not a string.");
}


Var& Var::getAt(const std::string& name)
{
	return holderImpl<DynamicStruct,
		InvalidAccessException>("Not a struct.")->operator[](name);
}


Var Var::parse(const std::string& val)
{
	std::string::size_type t = 0;
	return parse(val, t);
}


Var Var::parse(const std::string& val, std::string::size_type& pos)
{
	// { -> an Object==DynamicStruct
	// [ -> an array
	// '/" -> a string (strip '/")
	// other: also treat as string
	skipWhiteSpace(val, pos);
	if (pos < val.size())
	{
		switch (val[pos])
		{
		case '{':
			return parseObject(val, pos);
		case '[':
			return parseArray(val, pos);
		case '"':
			return parseJSONString(val, pos);
		default:
			return parseString(val, pos);
		}
	}
	std::string empty;
	return empty;
}


Var Var::parseObject(const std::string& val, std::string::size_type& pos)
{
	poco_assert_dbg (val[pos] == '{');
	++pos;
	skipWhiteSpace(val, pos);
	DynamicStruct aStruct;
	while (val[pos] != '}' && pos < val.size())
	{
		std::string key = parseString(val, pos);
		skipWhiteSpace(val, pos);
		if (val[pos] != ':')
			throw DataFormatException("Incorrect object, must contain: key : value pairs"); 
		++pos; // skip past :
		Var value = parse(val, pos);
		aStruct.insert(key, value);
		skipWhiteSpace(val, pos);
		if (val[pos] == ',')
		{
			++pos;
			skipWhiteSpace(val, pos);
		}
	}
	if (val[pos] != '}')
		throw DataFormatException("Unterminated object"); 
	++pos;
	return aStruct;
}


Var Var::parseArray(const std::string& val, std::string::size_type& pos)
{
	poco_assert_dbg (val[pos] == '[');
	++pos;
	skipWhiteSpace(val, pos);
	std::vector<Var> result;
	while (val[pos] != ']' && pos < val.size())
	{
		result.push_back(parse(val, pos));
		skipWhiteSpace(val, pos);
		if (val[pos] == ',')
		{
			++pos;
			skipWhiteSpace(val, pos);
		}
	}
	if (val[pos] != ']')
		throw DataFormatException("Unterminated array"); 
	++pos;
	return result;
}


std::string Var::parseString(const std::string& val, std::string::size_type& pos)
{
	if (val[pos] == '"')
	{
		return parseJSONString(val, pos);
	}
	else
	{
		std::string result;
		while (pos < val.size() 
			&& !Poco::Ascii::isSpace(val[pos]) 
			&& val[pos] != ','
			&& val[pos] != ']'
			&& val[pos] != '}')
		{
			result += val[pos++];
		}
		return result;
	}
}


std::string Var::parseJSONString(const std::string& val, std::string::size_type& pos)
{
	poco_assert_dbg (val[pos] == '"');
	++pos;
	std::string result;
	bool done = false;
	while (pos < val.size() && !done)
	{
		switch (val[pos])
		{
		case '"':
			done = true;
			++pos;
			break;
		case '\\':
			if (pos < val.size())
			{
				++pos;
				switch (val[pos])
				{
				case 'b':
					result += '\b';
					break; 
				case 'f':
					result += '\f';
					break; 
				case 'n':
					result += '\n';
					break; 
				case 'r':
					result += '\r';
					break; 
				case 't':
					result += '\t';
					break; 
				default:
					result += val[pos];
					break;
				}
				break;
			}
			else
			{
				result += val[pos];
			}
			++pos;
			break;
		default:
			result += val[pos++];
			break;
		}
	}
	if (!done) throw Poco::DataFormatException("unterminated JSON string");
	return result;
}


void Var::skipWhiteSpace(const std::string& val, std::string::size_type& pos)
{
	while (std::isspace(val[pos]))
		++pos;
}


std::string Var::toString(const Var& any)
{
	std::string res;
	Impl::appendJSONValue(res, any);
	return res;
}


Var& Var::structIndexOperator(VarHolderImpl<Struct<int> >* pStr, int n) const
{
	return pStr->operator[](n);
}


} } // namespace Poco::Dynamic
