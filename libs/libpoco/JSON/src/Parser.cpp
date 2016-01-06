//
// Parser.cpp
//
// $Id$
//
// Library: JSON
// Package: JSON
// Module:  Parser
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/JSON/Parser.h"
#include "Poco/JSON/JSONException.h"
#include "Poco/Ascii.h"
#include "Poco/Token.h"
#include "Poco/UTF8Encoding.h"
#undef min
#undef max
#include <limits>
#include <clocale>
#include <istream>


namespace Poco {
namespace JSON {


static const unsigned char UTF8_LEAD_BITS[4] = { 0x00, 0xC0, 0xE0, 0xF0 };


const int Parser::_asciiClass[] = {
    xx,      xx,      xx,      xx,      xx,      xx,      xx,      xx,
    xx,      C_WHITE, C_WHITE, xx,      xx,      C_WHITE, xx,      xx,
    xx,      xx,      xx,      xx,      xx,      xx,      xx,      xx,
    xx,      xx,      xx,      xx,      xx,      xx,      xx,      xx,

    C_SPACE, C_ETC,   C_QUOTE, C_ETC,   C_ETC,   C_ETC,   C_ETC,   C_ETC,
    C_ETC,   C_ETC,   C_STAR,  C_PLUS,  C_COMMA, C_MINUS, C_POINT, C_SLASH,
    C_ZERO,  C_DIGIT, C_DIGIT, C_DIGIT, C_DIGIT, C_DIGIT, C_DIGIT, C_DIGIT,
    C_DIGIT, C_DIGIT, C_COLON, C_ETC,   C_ETC,   C_ETC,   C_ETC,   C_ETC,

    C_ETC,   C_ABCDF, C_ABCDF, C_ABCDF, C_ABCDF, C_E,     C_ABCDF, C_ETC,
    C_ETC,   C_ETC,   C_ETC,   C_ETC,   C_ETC,   C_ETC,   C_ETC,   C_ETC,
    C_ETC,   C_ETC,   C_ETC,   C_ETC,   C_ETC,   C_ETC,   C_ETC,   C_ETC,
    C_ETC,   C_ETC,   C_ETC,   C_LSQRB, C_BACKS, C_RSQRB, C_ETC,   C_ETC,

    C_ETC,   C_LOW_A, C_LOW_B, C_LOW_C, C_LOW_D, C_LOW_E, C_LOW_F, C_ETC,
    C_ETC,   C_ETC,   C_ETC,   C_ETC,   C_LOW_L, C_ETC,   C_LOW_N, C_ETC,
    C_ETC,   C_ETC,   C_LOW_R, C_LOW_S, C_LOW_T, C_LOW_U, C_ETC,   C_ETC,
    C_ETC,   C_ETC,   C_ETC,   C_LCURB, C_ETC,   C_RCURB, C_ETC,   C_ETC
};


const int Parser::_stateTransitionTable[NR_STATES][NR_CLASSES] = {
/*
                 white                                      1-9                                   ABCDF  etc
             space |  {  }  [  ]  :  ,  "  \  /  +  -  .  0  |  a  b  c  d  e  f  l  n  r  s  t  u  |  E  |  * */
/*start  GO*/ {GO,GO,-6,xx,-5,xx,xx,xx,xx,xx,CB,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx},
/*ok     OK*/ {OK,OK,xx,-8,xx,-7,xx,-3,xx,xx,CB,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx},
/*object OB*/ {OB,OB,xx,-9,xx,xx,xx,xx,SB,xx,CB,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx},
/*key    KE*/ {KE,KE,xx,xx,xx,xx,xx,xx,SB,xx,CB,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx},
/*colon  CO*/ {CO,CO,xx,xx,xx,xx,-2,xx,xx,xx,CB,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx},
/*value  VA*/ {VA,VA,-6,xx,-5,xx,xx,xx,SB,xx,CB,xx,MX,xx,ZX,IX,xx,xx,xx,xx,xx,FA,xx,NU,xx,xx,TR,xx,xx,xx,xx,xx},
/*array  AR*/ {AR,AR,-6,xx,-5,-7,xx,xx,SB,xx,CB,xx,MX,xx,ZX,IX,xx,xx,xx,xx,xx,FA,xx,NU,xx,xx,TR,xx,xx,xx,xx,xx},
/*string ST*/ {ST,xx,ST,ST,ST,ST,ST,ST,-4,EX,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST},
/*escape EC*/ {xx,xx,xx,xx,xx,xx,xx,xx,ST,ST,ST,xx,xx,xx,xx,xx,xx,ST,xx,xx,xx,ST,xx,ST,ST,xx,ST,U1,xx,xx,xx,xx},
/*u1     U1*/ {xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,U2,U2,U2,U2,U2,U2,U2,U2,xx,xx,xx,xx,xx,xx,U2,U2,xx,xx},
/*u2     U2*/ {xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,U3,U3,U3,U3,U3,U3,U3,U3,xx,xx,xx,xx,xx,xx,U3,U3,xx,xx},
/*u3     U3*/ {xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,U4,U4,U4,U4,U4,U4,U4,U4,xx,xx,xx,xx,xx,xx,U4,U4,xx,xx},
/*u4     U4*/ {xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,UC,UC,UC,UC,UC,UC,UC,UC,xx,xx,xx,xx,xx,xx,UC,UC,xx,xx},
/*minus  MI*/ {xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,ZE,IT,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx},
/*zero   ZE*/ {OK,OK,xx,-8,xx,-7,xx,-3,xx,xx,CB,xx,xx,DF,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx},
/*int    IT*/ {OK,OK,xx,-8,xx,-7,xx,-3,xx,xx,CB,xx,xx,DF,IT,IT,xx,xx,xx,xx,DE,xx,xx,xx,xx,xx,xx,xx,xx,DE,xx,xx},
/*frac   FR*/ {OK,OK,xx,-8,xx,-7,xx,-3,xx,xx,CB,xx,xx,xx,FR,FR,xx,xx,xx,xx,E1,xx,xx,xx,xx,xx,xx,xx,xx,E1,xx,xx},
/*e      E1*/ {xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,E2,E2,xx,E3,E3,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx},
/*ex     E2*/ {xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,E3,E3,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx},
/*exp    E3*/ {OK,OK,xx,-8,xx,-7,xx,-3,xx,xx,xx,xx,xx,xx,E3,E3,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx},
/*tr     T1*/ {xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,T2,xx,xx,xx,xx,xx,xx,xx},
/*tru    T2*/ {xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,T3,xx,xx,xx,xx},
/*1      T3*/ {xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,CB,xx,xx,xx,xx,xx,xx,xx,xx,xx,OK,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx},
/*fa     F1*/ {xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,F2,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx},
/*fal    F2*/ {xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,F3,xx,xx,xx,xx,xx,xx,xx,xx,xx},
/*fals   F3*/ {xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,F4,xx,xx,xx,xx,xx,xx},
/*0      F4*/ {xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,CB,xx,xx,xx,xx,xx,xx,xx,xx,xx,OK,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx},
/*nu     N1*/ {xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,N2,xx,xx,xx,xx},
/*nul    N2*/ {xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,N3,xx,xx,xx,xx,xx,xx,xx,xx,xx},
/*null   N3*/ {xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,CB,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,OK,xx,xx,xx,xx,xx,xx,xx,xx,xx},
/*/      C1*/ {xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,C2},
/*/*     C2*/ {C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C3},
/**      C3*/ {C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,CE,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C2,C3},
/*_.     FX*/ {OK,OK,xx,-8,xx,-7,xx,-3,xx,xx,xx,xx,xx,xx,FR,FR,xx,xx,xx,xx,E1,xx,xx,xx,xx,xx,xx,xx,xx,E1,xx,xx},
/*\      D1*/ {xx,xx,xx,xx,xx,xx,xx,xx,xx,D2,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx},
/*\      D2*/ {xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,xx,U1,xx,xx,xx,xx},
};


Parser::Parser(const Handler::Ptr& pHandler, std::size_t bufSize) :
	_pHandler(pHandler),
	_state(GO),
	_beforeCommentState(0),
	_type(JSON_T_NONE),
	_escaped(0),
	_comment(0),
	_utf16HighSurrogate(0),
	_depth(JSON_UNLIMITED_DEPTH),
	_top(-1),
	_stack(JSON_PARSER_STACK_SIZE),
	_parseBuffer(bufSize),
	_decimalPoint('.'),
	_allowNullByte(true),
	_allowComments(false)
{
	_parseBuffer.resize(0);
	push(MODE_DONE);
}


Parser::~Parser()
{
}


void Parser::reset()
{
	_state = GO;
	_beforeCommentState = 0;
	_type = JSON_T_NONE;
	_escaped = 0;
	_utf16HighSurrogate = 0;
	_top = -1;

	_stack.clear();
	_parseBuffer.resize(0);
	push(MODE_DONE);
	if (_pHandler) _pHandler->reset();
}


Dynamic::Var Parser::parse(const std::string& json)
{
	std::string::const_iterator it = json.begin();
	std::string::const_iterator end = json.end();
	Source<std::string::const_iterator> source(it, end);

	int c = 0;
	while(source.nextChar(c))
	{
		if (0 == parseChar(c, source))
			throw SyntaxException("JSON syntax error");
	}

	if (!done())
		throw JSONException("JSON syntax error");

	return asVar();
}


Dynamic::Var Parser::parse(std::istream& in)
{
	std::istreambuf_iterator<char> it(in.rdbuf());
	std::istreambuf_iterator<char> end;
	Source<std::istreambuf_iterator<char> > source(it, end);

	int c = 0;
	while(source.nextChar(c))
	{
		if (0 == parseChar(c, source)) throw JSONException("JSON syntax error");
	}

	if (!done())
		throw JSONException("JSON syntax error");

	return asVar();
}


bool Parser::push(int mode)
{
	_top += 1;
	if (_depth < 0)
	{
		if (_top >= _stack.size())
			_stack.resize(_stack.size() * 2, true);
	}
	else
	{
		if (_top >= _depth) return false;
	}

	_stack[_top] = mode;
	return true;
}


bool Parser::pop(int mode)
{
	if (_top < 0 || _stack[_top] != mode)
		return false;

	_top -= 1;
	return true;
}


void Parser::clearBuffer()
{
	_parseBuffer.resize(0);
}


void Parser::parseBufferPopBackChar()
{
	poco_assert(_parseBuffer.size() >= 1);
	_parseBuffer.resize(_parseBuffer.size() - 1);
}


void Parser::parseBufferPushBackChar(char c)
{
	if (_parseBuffer.size() + 1 >= _parseBuffer.capacity())
		_parseBuffer.setCapacity(_parseBuffer.capacity() * 2);

	_parseBuffer.append(c);
}


void Parser::addEscapedCharToParseBuffer(CharIntType nextChar)
{
	_escaped = 0;
	// remove the backslash
	parseBufferPopBackChar();

	switch(nextChar)
	{
	case 'b':
		parseBufferPushBackChar('\b');
		break;
	case 'f':
		parseBufferPushBackChar('\f');
		break;
	case 'n':
		parseBufferPushBackChar('\n');
		break;
	case 'r':
		parseBufferPushBackChar('\r');
		break;
	case 't':
		parseBufferPushBackChar('\t');
		break;
	case '"':
		parseBufferPushBackChar('"');
		break;
	case '\\':
		parseBufferPushBackChar('\\');
		break;
	case '/':
		parseBufferPushBackChar('/');
		break;
	case 'u':
		parseBufferPushBackChar('\\');
		parseBufferPushBackChar('u');
		break;
	default:
		break;
	}
}


void Parser::addCharToParseBuffer(CharIntType nextChar, int nextClass)
{
	if (_escaped)
	{
		addEscapedCharToParseBuffer(nextChar);
		return;
	}
	else if (!_comment)
	{
		if ((_type != JSON_T_NONE) ||
			!((nextClass == C_SPACE) || (nextClass == C_WHITE)))
		{
			parseBufferPushBackChar((char) nextChar);
		}
	}
}


Parser::CharIntType Parser::decodeUnicodeChar()
{
	int i;
	unsigned uc = 0;
	char* p;
	int trailBytes;

	poco_assert(_parseBuffer.size() >= 6);
	p = &_parseBuffer[_parseBuffer.size() - 4];

	for (i = 12; i >= 0; i -= 4, ++p)
	{
		unsigned x = *p;

		if (x >= 'a')      x -= ('a' - 10);
		else if (x >= 'A') x -= ('A' - 10);
		else               x &= ~0x30u;

		poco_assert(x < 16);
		uc |= x << i;
	}

	if ( !_allowNullByte && uc == 0 ) return 0; 

	// clear UTF-16 char from buffer
	_parseBuffer.resize(_parseBuffer.size() - 6);

	if (_utf16HighSurrogate)
	{
		if (isLowSurrogate(uc))
		{
			uc = decodeSurrogatePair(_utf16HighSurrogate, uc);
			trailBytes = 3;
			_utf16HighSurrogate = 0;
		}
		else // high surrogate without a following low surrogate
		{
			return 0;
		}
	}
	else
	{
		if (uc < 0x80)
		{
			trailBytes = 0;
		}
		else if (uc < 0x800)
		{
			trailBytes = 1;
		}
		else if (isHighSurrogate(uc))
		{
			// save the high surrogate and wait for the low surrogate
			_utf16HighSurrogate = uc;
			return 1;
		}
		else if (isLowSurrogate(uc))
		{
			// low surrogate without a preceding high surrogate 
			return 0;
		}
		else
		{
			trailBytes = 2;
		}
	}

	_parseBuffer.append((char) ((uc >> (trailBytes * 6)) | UTF8_LEAD_BITS[trailBytes]));

	for (i = trailBytes * 6 - 6; i >= 0; i -= 6)
	{
		_parseBuffer.append((char) (((uc >> i) & 0x3F) | 0x80));
	}

	return 1;
}


void Parser::parseBuffer()
{
	if (_pHandler)
	{
		int type = _type; // just to silence g++

		if (type != JSON_T_NONE)
		{
			assertNonContainer();

			switch(type)
			{
			case JSON_T_TRUE:
				{
					_pHandler->value(true);
					break;
				}
			case JSON_T_FALSE:
				{
					_pHandler->value(false);
					break;
				}
			case JSON_T_NULL:
				{
					_pHandler->null();
					break;
				}
			case JSON_T_FLOAT:
				{ 
					// Float can't end with a dot
					if (_parseBuffer[_parseBuffer.size() - 1] == '.' ) throw SyntaxException("JSON syntax error");

					double float_value = NumberParser::parseFloat(std::string(_parseBuffer.begin(), _parseBuffer.size()));
					_pHandler->value(float_value);
					break;
				}
			case JSON_T_INTEGER:
				{
#if defined(POCO_HAVE_INT64)
					try
					{
						Int64 value = NumberParser::parse64(std::string(_parseBuffer.begin(), _parseBuffer.size()));
						// if number is 32-bit, then handle as such
						if (value > std::numeric_limits<int>::max()
						 || value < std::numeric_limits<int>::min() )
						{
							_pHandler->value(value);
						}
						else
						{
							_pHandler->value(static_cast<int>(value));
						}
					}
					// try to handle error as unsigned in case of overflow
					catch ( const SyntaxException& )
					{
						UInt64 value = NumberParser::parseUnsigned64(std::string(_parseBuffer.begin(), _parseBuffer.size()));
						// if number is 32-bit, then handle as such
						if ( value > std::numeric_limits<unsigned>::max() )
						{
							_pHandler->value(value);
						}
						else
						{
							_pHandler->value(static_cast<unsigned>(value));
						}
					}
#else
					try
					{
						int value = NumberParser::parse(std::string(_parseBuffer.begin(), _parseBuffer.size()));
						_pHandler->value(value);
					}
					// try to handle error as unsigned in case of overflow
					catch ( const SyntaxException& )
					{
						unsigned value = NumberParser::parseUnsigned(std::string(_parseBuffer.begin(), _parseBuffer.size()));
						_pHandler->value(value);
					}
#endif
				}
				break;
			case JSON_T_STRING:
				{
					_pHandler->value(std::string(_parseBuffer.begin(), _parseBuffer.size()));
					break;
				}
			}
		}
	}

	clearBuffer();
}

int Parser::utf8CheckFirst(char byte)
{
	unsigned char u = (unsigned char) byte;

	if(u < 0x80)
		return 1;

	if (0x80 <= u && u <= 0xBF) 
	{
		// second, third or fourth byte of a multi-byte
		// sequence, i.e. a "continuation byte"
		return 0;
	}
	else if(u == 0xC0 || u == 0xC1) 
	{
		// overlong encoding of an ASCII byte
		return 0;
	}
	else if(0xC2 <= u && u <= 0xDF) 
	{
		// 2-byte sequence
		return 2;
	}
	else if(0xE0 <= u && u <= 0xEF) 
	{
		// 3-byte sequence
		return 3;
	}
	else if(0xF0 <= u && u <= 0xF4) 
	{
		// 4-byte sequence
		return 4;
	}
	else 
	{ 
		// u >= 0xF5
		// Restricted (start of 4-, 5- or 6-byte sequence) or invalid UTF-8
		return 0;
	}
}

} } // namespace Poco::JSON
