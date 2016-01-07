//
// Parser.h
//
// $Id$
//
// Library: JSON
// Package: JSON
// Module:  Parser
//
// Definition of the Parser class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//

/*
Copyright (c) 2005 JSON.org

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

The Software shall be used for Good, not Evil.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef JSON_JSONParser_INCLUDED
#define JSON_JSONParser_INCLUDED


#include "Poco/JSON/JSON.h"
#include "Poco/JSON/Object.h"
#include "Poco/JSON/Array.h"
#include "Poco/JSON/ParseHandler.h"
#include "Poco/JSON/JSONException.h"
#include "Poco/UTF8Encoding.h"
#include "Poco/Dynamic/Var.h"
#include <string>


namespace Poco {
namespace JSON {


class JSON_API Parser
	/// A RFC 4627 compatible class for parsing JSON strings or streams.
	/// 
	/// See http://www.ietf.org/rfc/rfc4627.txt for specification.
	/// 
	/// Usage example:
	/// 
	///    std::string json = "{ \"name\" : \"Franky\", \"children\" : [ \"Jonas\", \"Ellen\" ] }";
	///    Parser parser;
	///    Var result = parser.parse(json);
	///    // ... use result
	///    parser.reset();
	///    std::ostringstream ostr;
	///    PrintHandler::Ptr pHandler = new PrintHandler(ostr);
	///    parser.setHandler(pHandler);
	///    parser.parse(json); // ostr.str() == json
	/// 
{
public:
	typedef std::char_traits<char> CharTraits;
	typedef CharTraits::int_type   CharIntType;

	enum Classes
	{
		C_SPACE,  /* space */
		C_WHITE,  /* other whitespace */
		C_LCURB,  /* {  */
		C_RCURB,  /* } */
		C_LSQRB,  /* [ */
		C_RSQRB,  /* ] */
		C_COLON,  /* : */
		C_COMMA,  /* , */
		C_QUOTE,  /* " */
		C_BACKS,  /* \ */
		C_SLASH,  /* / */
		C_PLUS,   /* + */
		C_MINUS,  /* - */
		C_POINT,  /* . */
		C_ZERO ,  /* 0 */
		C_DIGIT,  /* 123456789 */
		C_LOW_A,  /* a */
		C_LOW_B,  /* b */
		C_LOW_C,  /* c */
		C_LOW_D,  /* d */
		C_LOW_E,  /* e */
		C_LOW_F,  /* f */
		C_LOW_L,  /* l */
		C_LOW_N,  /* n */
		C_LOW_R,  /* r */
		C_LOW_S,  /* s */
		C_LOW_T,  /* t */
		C_LOW_U,  /* u */
		C_ABCDF,  /* ABCDF */
		C_E,      /* E */
		C_ETC,    /* everything else */
		C_STAR,   /* * */   
		NR_CLASSES
	};
	
	enum States
		/// State codes
	{
		GO,  /* start    */
		OK,  /* ok       */
		OB,  /* object   */
		KE,  /* key      */
		CO,  /* colon    */
		VA,  /* value    */
		AR,  /* array    */
		ST,  /* string   */
		EC,  /* escape   */
		U1,  /* u1       */
		U2,  /* u2       */
		U3,  /* u3       */
		U4,  /* u4       */
		MI,  /* minus    */
		ZE,  /* zero     */
		IT,  /* integer  */
		FR,  /* fraction */
		E1,  /* e        */
		E2,  /* ex       */
		E3,  /* exp      */
		T1,  /* tr       */
		T2,  /* tru      */
		T3,  /* true     */
		F1,  /* fa       */
		F2,  /* fal      */
		F3,  /* fals     */
		F4,  /* false    */
		N1,  /* nu       */
		N2,  /* nul      */
		N3,  /* null     */
		C1,  /* /        */
		C2,  /* / *     */
		C3,  /* *        */
		FX,  /* *.* *eE* */
		D1,  /* second UTF-16 character decoding started by \ */
		D2,  /* second UTF-16 character proceeded by u */
		NR_STATES
	};

	enum Modes
		/// Modes that can be pushed on the _pStack.
	{
		MODE_ARRAY = 1,
		MODE_DONE = 2,
		MODE_KEY = 3,
		MODE_OBJECT = 4
	};

	enum Actions
	{
		CB = -10, /* _comment begin */
		CE = -11, /* _comment end */
		FA = -12, /* 0 */
		TR = -13, /* 0 */
		NU = -14, /* null */
		DE = -15, /* double detected by exponent e E */
		DF = -16, /* double detected by fraction . */
		SB = -17, /* string begin */
		MX = -18, /* integer detected by minus */
		ZX = -19, /* integer detected by zero */
		IX = -20, /* integer detected by 1-9 */
		EX = -21, /* next char is _escaped */
		UC = -22  /* Unicode character read */
	};
	
	enum JSONType
	{
		JSON_T_NONE = 0,
		JSON_T_INTEGER,
		JSON_T_FLOAT,
		JSON_T_NULL,
		JSON_T_TRUE,
		JSON_T_FALSE,
		JSON_T_STRING,
		JSON_T_MAX
	};
	
	static const std::size_t JSON_PARSE_BUFFER_SIZE = 4096;
	static const std::size_t JSON_PARSER_STACK_SIZE = 128;
	static const int         JSON_UNLIMITED_DEPTH = -1;

	Parser(const Handler::Ptr& pHandler = new ParseHandler, std::size_t bufSize = JSON_PARSE_BUFFER_SIZE);
		/// Creates JSON Parser.

	virtual ~Parser();
		/// Destroys JSON Parser.

	void reset();
		/// Resets the parser.

	void setAllowComments(bool comments);
		/// Allow comments. By default, comments are not allowed.

	bool getAllowComments() const;
		/// Returns true if comments are allowed, false otherwise.
		/// By default, comments are not allowed.
		
	void setAllowNullByte(bool nullByte);
		/// Allow null byte in strings. By default, null byte is allowed.

	bool getAllowNullByte() const;
		/// Returns true if null byte is allowed, false otherwise.
		/// By default, null bytes are allowed.

	void setDepth(std::size_t depth);
		/// Sets the allowed JSON depth.

	std::size_t getDepth() const;
		/// Returns the allowed JSON depth.

	Dynamic::Var parse(const std::string& json);
		/// Parses a string.

	Dynamic::Var parse(std::istream& in);
		/// Parses a JSON from the input stream.

	void setHandler(const Handler::Ptr& pHandler);
		/// Set the handler.

	const Handler::Ptr& getHandler();
		/// Returns the handler.

	Dynamic::Var asVar() const;
		/// Returns the result of parsing;

	Dynamic::Var result() const;
		/// Returns the result of parsing as Dynamic::Var;

private:
	Parser(const Parser&);
	Parser& operator = (const Parser&);

	typedef Poco::Buffer<char> BufType;

	bool push(int mode);
		/// Push a mode onto the _pStack. Return false if there is overflow.

	bool pop(int mode);
		/// Pops the stack, assuring that the current mode matches the expectation.
		/// Returns false if there is underflow or if the modes mismatch.

	void growBuffer();

	void clearBuffer();

	void parseBufferPushBackChar(char c);

	void parseBufferPopBackChar();

	void addCharToParseBuffer(CharIntType nextChar, int nextClass);

	void addEscapedCharToParseBuffer(CharIntType nextChar);

	CharIntType decodeUnicodeChar();

	void assertNotStringNullBool();

	void assertNonContainer();

	void parseBuffer();

	template <typename IT>
	class Source
	{
	public:
		Source(const IT& it, const IT& end) : _it(it), _end(end)
		{
		}
	
		~Source()
		{
		}

		bool nextChar(CharIntType& c)
		{
			if (_it == _end) return false;
			c = *_it;
			++_it;
			return true;
		}

	private:
		IT _it;
		IT _end;
	};

	template <typename S>
	bool parseChar(CharIntType nextChar, S& source)
		/// Called for each character (or partial character) in JSON string.
		/// It accepts UTF-8, UTF-16, or UTF-32. If the character is accepted,
		/// it returns true, otherwise false.
	{
		CharIntType nextClass, nextState;
		unsigned char ch = static_cast<unsigned char>(CharTraits::to_char_type(nextChar));

		// Determine the character's class.
		if ((!_allowNullByte && ch == 0)) return false;
		if (ch >= 0x80)
		{
			nextClass = C_ETC;
			CharIntType count = utf8CheckFirst(nextChar);
			if (!count)
			{
				throw Poco::JSON::JSONException("Bad character.");
			}

			char buffer[4];
			buffer[0] = nextChar;
			for(int i = 1; i < count; ++i)
			{
				int c = 0;
				if (!source.nextChar(c)) throw Poco::JSON::JSONException("Invalid UTF8 sequence found");
				buffer[i] = c;
			}
		
			if (!Poco::UTF8Encoding::isLegal((unsigned char*) buffer, count))
			{
				throw Poco::JSON::JSONException("No legal UTF8 found");
			}

			for(int i = 0; i < count; ++i)
			{
				parseBufferPushBackChar(buffer[i]);
			}
			return true;
		}
		else
		{
			nextClass = _asciiClass[nextChar];
			if (nextClass <= xx) return false; 
		}

		addCharToParseBuffer(nextChar, nextClass);

		// Get the next _state from the _state transition table.
		nextState = _stateTransitionTable[_state][nextClass];
		if (nextState >= 0)
		{
			_state = nextState;
		}
		else 
		{
			// Or perform one of the actions.
			switch (nextState)
			{
			// Unicode character 
			case UC:
				if(!decodeUnicodeChar()) return false;
				// check if we need to read a second UTF-16 char
				if (_utf16HighSurrogate) _state = D1;
				else _state = ST;
				break;
			// _escaped char 
			case EX:
				_escaped = 1;
				_state = EC;
				break;
			// integer detected by minus
			case MX:
				_type = JSON_T_INTEGER;
				_state = MI;
				break;
			// integer detected by zero
			case ZX:
				_type = JSON_T_INTEGER;
				_state = ZE;
				break;
				// integer detected by 1-9 
			case IX:
				_type = JSON_T_INTEGER;
				_state = IT;
				break;
			// floating point number detected by exponent
			case DE:
				assertNotStringNullBool();
				_type = JSON_T_FLOAT;
				_state = E1;
				break;
			// floating point number detected by fraction
			case DF:
				assertNotStringNullBool();
				_type = JSON_T_FLOAT;
				_state = FX;
				break;
			// string begin "
			case SB:
				clearBuffer();
				poco_assert(_type == JSON_T_NONE);
				_type = JSON_T_STRING;
				_state = ST;
				break;

			// n
			case NU:
				poco_assert(_type == JSON_T_NONE);
				_type = JSON_T_NULL;
				_state = N1;
				break;
			// f
			case FA:
				poco_assert(_type == JSON_T_NONE);
				_type = JSON_T_FALSE;
				_state = F1;
				break;
			// t
			case TR:
				poco_assert(_type == JSON_T_NONE);
				_type = JSON_T_TRUE;
				_state = T1;
				break;

			// closing comment
			case CE:
				_comment = 0;
				poco_assert(_parseBuffer.size() == 0);
				poco_assert(_type == JSON_T_NONE);
				_state = _beforeCommentState;
				break;

			// opening comment
			case CB:
				if (!_allowComments) return false;
				parseBufferPopBackChar();
				parseBuffer();
				poco_assert(_parseBuffer.size() == 0);
				poco_assert(_type != JSON_T_STRING);
				switch (_stack[_top])
				{
				case MODE_ARRAY:
					case MODE_OBJECT:
						switch(_state)
						{
						case VA:
						case AR:
							_beforeCommentState = _state;
							break;
						default:
							_beforeCommentState = OK;
							break;
						}
						break;
						default:
							_beforeCommentState = _state;
							break;
						}
					_type = JSON_T_NONE;
					_state = C1;
					_comment = 1;
					break;
				// empty }
				case -9:
				{
					clearBuffer();
					if (_pHandler) _pHandler->endObject();

					if (!pop(MODE_KEY)) return false;
					_state = OK;
					break;
				}
				// }
				case -8:
				{
					parseBufferPopBackChar();
					parseBuffer();
					if (_pHandler) _pHandler->endObject();
					if (!pop(MODE_OBJECT)) return false;
					_type = JSON_T_NONE;
					_state = OK;
					break;
				}
				// ]
				case -7:
				{
					parseBufferPopBackChar();
					parseBuffer();
					if (_pHandler) _pHandler->endArray();
					if (!pop(MODE_ARRAY)) return false;
					_type = JSON_T_NONE;
					_state = OK;
					break;
				}
				// {
				case -6:
				{
					parseBufferPopBackChar();
					if (_pHandler) _pHandler->startObject();
					if (!push(MODE_KEY)) return false;
					poco_assert(_type == JSON_T_NONE);
					_state = OB;
					break;
				}
				// [
				case -5:
				{
					parseBufferPopBackChar();
					if (_pHandler) _pHandler->startArray();
					if (!push(MODE_ARRAY)) return false;
					poco_assert(_type == JSON_T_NONE);
					_state = AR;
					break;
				}
				// string end "
				case -4:
					parseBufferPopBackChar();
					switch (_stack[_top])
					{
					case MODE_KEY:
						{
						poco_assert(_type == JSON_T_STRING);
						_type = JSON_T_NONE;
						_state = CO;

						if (_pHandler) 
						{
							_pHandler->key(std::string(_parseBuffer.begin(), _parseBuffer.size()));
						}
						clearBuffer();
						break;
						}
					case MODE_ARRAY:
					case MODE_OBJECT:
						poco_assert(_type == JSON_T_STRING);
						parseBuffer();
						_type = JSON_T_NONE;
						_state = OK;
						break;
					default:
						return false;
					}
					break;

				// ,
				case -3:
					{
					parseBufferPopBackChar();
					parseBuffer();
					switch (_stack[_top])
					{
					case MODE_OBJECT:
						//A comma causes a flip from object mode to key mode.
						if (!pop(MODE_OBJECT) || !push(MODE_KEY)) return false;
						poco_assert(_type != JSON_T_STRING);
						_type = JSON_T_NONE;
						_state = KE;
						break;
					case MODE_ARRAY:
						poco_assert(_type != JSON_T_STRING);
						_type = JSON_T_NONE;
						_state = VA;
						break;
					default:
						return false;
					}
					break;
					}
				// :
				case -2:
					// A colon causes a flip from key mode to object mode.
					parseBufferPopBackChar();
					if (!pop(MODE_KEY) || !push(MODE_OBJECT)) return false;
					poco_assert(_type == JSON_T_NONE);
					_state = VA;
					break;
				//Bad action.
				default:
					return false;
			}
		}
		return true;
	}

	bool done();

	static CharIntType utf8CheckFirst(char byte);

	static const int _asciiClass[128];
		/// This array maps the 128 ASCII characters into character classes.
		/// The remaining Unicode characters should be mapped to C_ETC.
		/// Non-whitespace control characters are errors.

	static const int _stateTransitionTable[NR_STATES][NR_CLASSES];
	static const int xx = -1;

	bool isHighSurrogate(unsigned uc);
	bool isLowSurrogate(unsigned uc);
	unsigned decodeSurrogatePair(unsigned hi, unsigned lo);

	Handler::Ptr   _pHandler;
	signed char    _state;
	signed char    _beforeCommentState;
	JSONType       _type;
	signed char    _escaped;
	signed char    _comment;
	unsigned short _utf16HighSurrogate;
	int            _depth;
	int            _top;
	BufType        _stack;
	BufType        _parseBuffer;
	char           _decimalPoint;
	bool           _allowNullByte;
	bool           _allowComments;
};


inline void Parser::setAllowComments(bool comments)
{
	_allowComments = comments;
}


inline bool Parser::getAllowComments() const
{
	return _allowComments;
}


inline void Parser::setAllowNullByte(bool nullByte)
{
	_allowNullByte = nullByte;
}


inline bool Parser::getAllowNullByte() const
{
	return _allowNullByte;
}


inline void Parser::setDepth(std::size_t depth)
{
	_depth = static_cast<int>(depth);
}


inline std::size_t Parser::getDepth() const
{
	return static_cast<int>(_depth);
}


inline void Parser::setHandler(const Handler::Ptr& pHandler)
{
	_pHandler = pHandler;
}


inline const Handler::Ptr& Parser::getHandler()
{
	return _pHandler;
}


inline Dynamic::Var Parser::result() const
{
	return _pHandler->asVar();
}


inline Dynamic::Var Parser::asVar() const
{
	if (_pHandler) return _pHandler->asVar();

	return Dynamic::Var();
}


inline bool Parser::done()
{
	return _state == OK && pop(MODE_DONE);
}


inline void Parser::assertNotStringNullBool()
{
	poco_assert(_type != JSON_T_FALSE &&
		_type != JSON_T_TRUE &&
		_type != JSON_T_NULL &&
		_type != JSON_T_STRING);
}


inline void Parser::assertNonContainer()
{
	poco_assert(_type == JSON_T_NULL ||
		_type == JSON_T_FALSE ||
		_type == JSON_T_TRUE ||
		_type == JSON_T_FLOAT ||
		_type == JSON_T_INTEGER ||
		_type == JSON_T_STRING);
}


inline void Parser::growBuffer()
{
	_parseBuffer.setCapacity(_parseBuffer.size() * 2, true);
}


inline bool Parser::isHighSurrogate(unsigned uc)
{
	return (uc & 0xFC00) == 0xD800;
}


inline bool Parser::isLowSurrogate(unsigned uc)
{
	return (uc & 0xFC00) == 0xDC00;
}


inline unsigned Parser::decodeSurrogatePair(unsigned hi, unsigned lo)
{
	return ((hi & 0x3FF) << 10) + (lo & 0x3FF) + 0x10000;
}


}} // namespace Poco::JSON


#endif // JSON_JSONParser_INCLUDED
