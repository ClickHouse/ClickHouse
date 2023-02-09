//
// XMLStreamParser.cpp
//
// Library: XML
// Package: XML
// Module:  XMLStreamParser
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// Based on libstudxml (http://www.codesynthesis.com/projects/libstudxml/).
// Copyright (c) 2009-2013 Code Synthesis Tools CC.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/XML/XMLStreamParser.h"
#include <new>
#include <cstring>
#include <istream>
#include <ostream>
#include <sstream>


namespace Poco {
namespace XML {


struct StreamExceptionController
{
	StreamExceptionController(std::istream& is):
		_istr(is),
		_oldState(_istr.exceptions())
	{
		_istr.exceptions(_oldState & ~std::istream::failbit);
	}

	~StreamExceptionController()
	{
		std::istream::iostate s = _istr.rdstate();
		s &= ~std::istream::failbit;

		// If our error state (sans failbit) intersects with the
		// exception state then that means we have an active
		// exception and changing error/exception state will
		// cause another to be thrown.
		if (!(_oldState & s))
		{
			// Clear failbit if it was caused by eof.
			//
			if (_istr.fail() && _istr.eof())
				_istr.clear(s);

			_istr.exceptions(_oldState);
		}
	}

private:
	StreamExceptionController(const StreamExceptionController&);
	StreamExceptionController& operator = (const StreamExceptionController&);

private:
	std::istream& _istr;
	std::istream::iostate _oldState;
};


static const char* parserEventStrings[] =
{
	"start element",
	"end element",
	"start attribute",
	"end attribute",
	"characters",
	"start namespace declaration",
	"end namespace declaration",
	"end of file"
};


std::ostream& operator << (std::ostream& os, XMLStreamParser::EventType e)
{
	return os << parserEventStrings[e];
}


XMLStreamParser::XMLStreamParser(std::istream& is, const std::string& iname, FeatureType f):
	_size(0),
	_inputName(iname),
	_feature(f)
{
	_data.is = &is;
	init();
}


XMLStreamParser::XMLStreamParser(const void* data, std::size_t size, const std::string& iname, FeatureType f):
	_size(size),
	_inputName(iname),
	_feature(f)
{
	poco_assert(data != 0 && size != 0);

	_data.buf = data;
	init();
}


XMLStreamParser::~XMLStreamParser()
{
	if (_parser) XML_ParserFree(_parser);
}


void XMLStreamParser::init()
{
	_depth = 0;
	_parserState = state_next;
	_currentEvent = EV_EOF;
	_queue = EV_EOF;

	_qualifiedName = &_qname;
	_pvalue = &_value;

	_line = 0;
	_column = 0;

	_currentAttributeIndex = 0;
	_startNamespaceIndex = 0;
	_endNamespaceIndex = 0;

	if ((_feature & RECEIVE_ATTRIBUTE_MAP) != 0 && (_feature & RECEIVE_ATTRIBUTES_EVENT) != 0)
		_feature &= ~RECEIVE_ATTRIBUTE_MAP;

	// Allocate the XMLStreamParser. Make sure nothing else can throw after
	// this call since otherwise we will leak it.
	//
	_parser = XML_ParserCreateNS(0, XML_Char(' '));

	if (_parser == 0)
		throw std::bad_alloc();

	// Get prefixes in addition to namespaces and local names.
	//
	XML_SetReturnNSTriplet(_parser, true);

	// Set handlers.
	//
	XML_SetUserData(_parser, this);

	if ((_feature & RECEIVE_ELEMENTS) != 0)
	{
		XML_SetStartElementHandler(_parser, &handleStartElement);
		XML_SetEndElementHandler(_parser, &handleEndElement);
	}

	if ((_feature & RECEIVE_CHARACTERS) != 0)
		XML_SetCharacterDataHandler(_parser, &handleCharacters);

	if ((_feature & RECEIVE_NAMESPACE_DECLS) != 0)
		XML_SetNamespaceDeclHandler(_parser, &handleStartNamespaceDecl, &handleEndNamespaceDecl);
}


void XMLStreamParser::handleError()
{
	XML_Error e(XML_GetErrorCode(_parser));

	if (e == XML_ERROR_ABORTED)
	{
		// For now we only abort the XMLStreamParser in the handleCharacters() and
		// handleStartElement() handlers.
		//
		switch (content())
		{
		case Content::Empty:
			throw XMLStreamParserException(*this, "characters in empty content");
		case Content::Simple:
			throw XMLStreamParserException(*this, "element in simple content");
		case Content::Complex:
			throw XMLStreamParserException(*this, "characters in complex content");
		default:
			poco_assert(false);
		}
	}
	else
		throw XMLStreamParserException(_inputName, XML_GetCurrentLineNumber(_parser), XML_GetCurrentColumnNumber(_parser), XML_ErrorString(e));
}


XMLStreamParser::EventType XMLStreamParser::next()
{
	if (_parserState == state_next)
		return nextImpl(false);
	else
	{
		// If we previously peeked at start/end_element, then adjust
		// state accordingly.
		//
		switch (_currentEvent)
		{
		case EV_END_ELEMENT:
		{
			if (!_elementState.empty() && _elementState.back().depth == _depth)
				popElement();

			_depth--;
			break;
		}
		case EV_START_ELEMENT:
		{
			_depth++;
			break;
		}
		default:
			break;
		}

		_parserState = state_next;
		return _currentEvent;
	}
}


const std::string& XMLStreamParser::attribute(const QName& qn) const
{
	if (const ElementEntry* e = getElement())
	{
		AttributeMapType::const_iterator i(e->attributeMap.find(qn));

		if (i != e->attributeMap.end())
		{
			if (!i->second.handled)
			{
				i->second.handled = true;
				e->attributesUnhandled--;
			}
			return i->second.value;
		}
	}

	throw XMLStreamParserException(*this, "attribute '" + qn.toString() + "' expected");
}


std::string XMLStreamParser::attribute(const QName& qn, const std::string& dv) const
{
	if (const ElementEntry* e = getElement())
	{
		AttributeMapType::const_iterator i(e->attributeMap.find(qn));

		if (i != e->attributeMap.end())
		{
			if (!i->second.handled)
			{
				i->second.handled = true;
				e->attributesUnhandled--;
			}
			return i->second.value;
		}
	}

	return dv;
}


bool XMLStreamParser::attributePresent(const QName& qn) const
{
	if (const ElementEntry* e = getElement())
	{
		AttributeMapType::const_iterator i(e->attributeMap.find(qn));

		if (i != e->attributeMap.end())
		{
			if (!i->second.handled)
			{
				i->second.handled = true;
				e->attributesUnhandled--;
			}
			return true;
		}
	}

	return false;
}


void XMLStreamParser::nextExpect(EventType e)
{
	if (next() != e)
		throw XMLStreamParserException(*this, std::string(parserEventStrings[e]) + " expected");
}


void XMLStreamParser::nextExpect(EventType e, const std::string& ns, const std::string& n)
{
	if (next() != e || namespaceURI() != ns || localName() != n)
		throw XMLStreamParserException(*this, std::string(parserEventStrings[e]) + " '" + QName(ns, n).toString() + "' expected");
}


std::string XMLStreamParser::element()
{
	content(Content::Simple);
	std::string r;

	// The content of the element can be empty in which case there
	// will be no characters event.
	//
	EventType e(next());
	if (e == EV_CHARACTERS)
	{
		r.swap(value());
		e = next();
	}

	// We cannot really get anything other than end_element since
	// the simple content validation won't allow it.
	//
	poco_assert(e == EV_END_ELEMENT);

	return r;
}


std::string XMLStreamParser::element(const QName& qn, const std::string& dv)
{
	if (peek() == EV_START_ELEMENT && getQName() == qn)
	{
		next();
		return element();
	}

	return dv;
}


const XMLStreamParser::ElementEntry* XMLStreamParser::getElementImpl() const
{
	// The handleStartElement() Expat handler may have already provisioned
	// an entry in the element stack. In this case, we need to get the
	// one before it, if any.
	//
	const ElementEntry* r(0);
	ElementState::size_type n(_elementState.size() - 1);

	if (_elementState[n].depth == _depth)
		r = &_elementState[n];
	else if (n != 0 && _elementState[n].depth > _depth)
	{
		n--;
		if (_elementState[n].depth == _depth)
			r = &_elementState[n];
	}

	return r;
}


void XMLStreamParser::popElement()
{
	// Make sure there are no unhandled attributes left.
	//
	const ElementEntry& e(_elementState.back());
	if (e.attributesUnhandled != 0)
	{
		// Find the first unhandled attribute and report it.
		//
		for (const auto& p: e.attributeMap)
		{
			if (!p.second.handled)
				throw XMLStreamParserException(*this, "unexpected attribute '" + p.first.toString() + "'");
		}
		poco_assert(false);
	}

	_elementState.pop_back();
}


XMLStreamParser::EventType XMLStreamParser::nextImpl(bool peek)
{
	EventType e(nextBody());

	// Content-specific processing. Note that we handle characters in the
	// handleCharacters() Expat handler for two reasons. Firstly, it is faster
	// to ignore the whitespaces at the source. Secondly, this allows us
	// to distinguish between element and attribute characters. We can
	// move this processing to the handler because the characters event
	// is never queued.
	//
	switch (e)
	{
	case EV_END_ELEMENT:
	{
		// If this is a peek, then avoid popping the stack just yet.
		// This way, the attribute map will still be valid until we
		// call next().
		//
		if (!peek)
		{
			if (!_elementState.empty() && _elementState.back().depth == _depth)
				popElement();

			_depth--;
		}
		break;
	}
	case EV_START_ELEMENT:
	{
		if (const ElementEntry* pEntry = getElement())
		{
			switch (pEntry->content)
			{
			case Content::Empty:
				throw XMLStreamParserException(*this, "element in empty content");
			case Content::Simple:
				throw XMLStreamParserException(*this, "element in simple content");
			default:
				break;
			}
		}

		// If this is a peek, then delay adjusting the depth.
		//
		if (!peek)
			_depth++;

		break;
	}
	default:
		break;
	}

	return e;
}


XMLStreamParser::EventType XMLStreamParser::nextBody()
{
	// See if we have any start namespace declarations we need to return.
	//
	if (_startNamespaceIndex < _startNamespace.size())
	{
		// Based on the previous event determine what's the next one must be.
		//
		switch (_currentEvent)
		{
		case EV_START_NAMESPACE_DECL:
		{
			if (++_startNamespaceIndex == _startNamespace.size())
			{
				_startNamespaceIndex = 0;
				_startNamespace.clear();
				_qualifiedName = &_qname;
				break; // No more declarations.
			}
			// Fall through.
		}
		case EV_START_ELEMENT:
		{
			_currentEvent = EV_START_NAMESPACE_DECL;
			_qualifiedName = &_startNamespace[_startNamespaceIndex];
			return _currentEvent;
		}
		default:
		{
			poco_assert(false);
			return _currentEvent = EV_EOF;
		}
		}
	}

	// See if we have any attributes we need to return as events.
	//
	if (_currentAttributeIndex < _attributes.size())
	{
		// Based on the previous event determine what's the next one must be.
		//
		switch (_currentEvent)
		{
		case EV_START_ATTRIBUTE:
		{
			_currentEvent = EV_CHARACTERS;
			_pvalue = &_attributes[_currentAttributeIndex].value;
			return _currentEvent;
		}
		case EV_CHARACTERS:
		{
			_currentEvent = EV_END_ATTRIBUTE; // Name is already set.
			return _currentEvent;
		}
		case EV_END_ATTRIBUTE:
		{
			if (++_currentAttributeIndex == _attributes.size())
			{
				_currentAttributeIndex = 0;
				_attributes.clear();
				_qualifiedName = &_qname;
				_pvalue = &_value;
				break; // No more attributes.
			}
			// Fall through.
		}
		case EV_START_ELEMENT:
		case EV_START_NAMESPACE_DECL:
		{
			_currentEvent = EV_START_ATTRIBUTE;
			_qualifiedName = &_attributes[_currentAttributeIndex].qname;
			return _currentEvent;
		}
		default:
		{
			poco_assert(false);
			return _currentEvent = EV_EOF;
		}
		}
	}

	// See if we have any end namespace declarations we need to return.
	//
	if (_endNamespaceIndex < _endNamespace.size())
	{
		// Based on the previous event determine what's the next one must be.
		//
		switch (_currentEvent)
		{
		case EV_END_NAMESPACE_DECL:
		{
			if (++_endNamespaceIndex == _endNamespace.size())
			{
				_endNamespaceIndex = 0;
				_endNamespace.clear();
				_qualifiedName = &_qname;
				break; // No more declarations.
			}
			// Fall through.
		}
			// The end namespace declaration comes before the end element
			// which means it can follow pretty much any other event.
			//
		default:
		{
			_currentEvent = EV_END_NAMESPACE_DECL;
			_qualifiedName = &_endNamespace[_endNamespaceIndex];
			return _currentEvent;
		}
		}
	}

	// Check the queue.
	//
	if (_queue != EV_EOF)
	{
		_currentEvent = _queue;
		_queue = EV_EOF;

		_line = XML_GetCurrentLineNumber(_parser);
		_column = XML_GetCurrentColumnNumber(_parser);

		return _currentEvent;
	}

	// Reset the character accumulation flag.
	//
	_accumulateContent = false;

	XML_ParsingStatus ps;
	XML_GetParsingStatus(_parser, &ps);

	switch (ps.parsing)
	{
	case XML_INITIALIZED:
	{
		// As if we finished the previous chunk.
		break;
	}
	case XML_PARSING:
	{
		poco_assert(false);
		return _currentEvent = EV_EOF;
	}
	case XML_FINISHED:
	{
		return _currentEvent = EV_EOF;
	}
	case XML_SUSPENDED:
	{
		switch (XML_ResumeParser(_parser))
		{
		case XML_STATUS_SUSPENDED:
		{
			// If the XMLStreamParser is again in the suspended state, then
			// that means we have the next event.
			//
			return _currentEvent;
		}
		case XML_STATUS_OK:
		{
			// Otherwise, we need to get and parse the next chunk of data
			// unless this was the last chunk, in which case this is eof.
			//
			if (ps.finalBuffer)
				return _currentEvent = EV_EOF;

			break;
		}
		case XML_STATUS_ERROR:
			handleError();
		}
		break;
	}
	}

	// Get and parse the next chunk of data until we get the next event
	// or reach eof.
	//
	if (!_accumulateContent)
		_currentEvent = EV_EOF;

	XML_Status s;
	do
	{
		if (_size != 0)
		{
			s = XML_Parse(_parser, static_cast<const char*>(_data.buf), static_cast<int>(_size), true);

			if (s == XML_STATUS_ERROR)
				handleError();

			break;
		}
		else
		{
			const size_t cap(4096);

			char* b(static_cast<char*>(XML_GetBuffer(_parser, cap)));
			if (b == 0)
				throw std::bad_alloc();

			// Temporarily unset the exception failbit. Also clear the fail bit
			// when we reset the old state if it was caused by eof.
			//
			std::istream& is(*_data.is);
			{
				StreamExceptionController sec(is);
				is.read(b, static_cast<std::streamsize>(cap));
			}

			// If the caller hasn't configured the stream to use exceptions,
			// then use the parsing exception to report an error.
			//
			if (is.bad() || (is.fail() && !is.eof()))
				throw XMLStreamParserException(*this, "io failure");

			bool eof(is.eof());

			s = XML_ParseBuffer(_parser, static_cast<int>(is.gcount()), eof);

			if (s == XML_STATUS_ERROR)
				handleError();

			if (eof)
				break;
		}
	} while (s != XML_STATUS_SUSPENDED);

	return _currentEvent;
}


static void splitName(const XML_Char* s, QName& qn)
{
	std::string& ns(qn.namespaceURI());
	std::string& name(qn.localName());
	std::string& prefix(qn.prefix());

	const char* p(strchr(s, ' '));

	if (p == 0)
	{
		ns.clear();
		name = s;
		prefix.clear();
	}
	else
	{
		ns.assign(s, 0, p - s);

		s = p + 1;
		p = strchr(s, ' ');

		if (p == 0)
		{
			name = s;
			prefix.clear();
		}
		else
		{
			name.assign(s, 0, p - s);
			prefix = p + 1;
		}
	}
}


void XMLCALL XMLStreamParser::handleStartElement(void* v, const XML_Char* name, const XML_Char** atts)
{
	XMLStreamParser& p(*static_cast<XMLStreamParser*>(v));

	XML_ParsingStatus ps;
	XML_GetParsingStatus(p._parser, &ps);

	// Expat has a (mis)-feature of a possibily calling handlers even
	// after the non-resumable XML_StopParser call.
	//
	if (ps.parsing == XML_FINISHED)
		return;

	// Cannot be a followup event.
	//
	poco_assert(ps.parsing == XML_PARSING);

	// When accumulating characters in simple content, we expect to
	// see more characters or end element. Seeing start element is
	// possible but means violation of the content model.
	//
	if (p._accumulateContent)
	{
		// It would have been easier to throw the exception directly,
		// however, the Expat code is most likely not exception safe.
		//
		p._line = XML_GetCurrentLineNumber(p._parser);
		p._column = XML_GetCurrentColumnNumber(p._parser);
		XML_StopParser(p._parser, false);
		return;
	}

	p._currentEvent = EV_START_ELEMENT;
	splitName(name, p._qname);

	p._line = XML_GetCurrentLineNumber(p._parser);
	p._column = XML_GetCurrentColumnNumber(p._parser);

	// Handle attributes.
	//
	if (*atts != 0)
	{
		bool am((p._feature & RECEIVE_ATTRIBUTE_MAP) != 0);
		bool ae((p._feature & RECEIVE_ATTRIBUTES_EVENT) != 0);

		// Provision an entry for this element.
		//
		ElementEntry* pe(0);
		if (am)
		{
			p._elementState.emplace_back(p._depth + 1);
			pe = &p._elementState.back();
		}

		if (am || ae)
		{
			for (; *atts != 0; atts += 2)
			{
				if (am)
				{
					QName qn;
					splitName(*atts, qn);
					AttributeMapType::value_type v(qn, AttributeValueType());
					v.second.value = *(atts + 1);
					v.second.handled = false;
					pe->attributeMap.insert(v);
				}
				else
				{
					p._attributes.emplace_back();
					splitName(*atts, p._attributes.back().qname);
					p._attributes.back().value = *(atts + 1);
				}
			}

			if (am)
				pe->attributesUnhandled = pe->attributeMap.size();
		}
	}

	XML_StopParser(p._parser, true);
}


void XMLCALL XMLStreamParser::handleEndElement(void* v, const XML_Char* name)
{
	XMLStreamParser& p(*static_cast<XMLStreamParser*>(v));

	XML_ParsingStatus ps;
	XML_GetParsingStatus(p._parser, &ps);

	// Expat has a (mis)-feature of a possibily calling handlers even
	// after the non-resumable XML_StopParser call.
	//
	if (ps.parsing == XML_FINISHED)
		return;

	// This can be a followup event for empty elements (<foo/>). In this
	// case the element name is already set.
	//
	if (ps.parsing != XML_PARSING)
		p._queue = EV_END_ELEMENT;
	else
	{
		splitName(name, p._qname);

		// If we are accumulating characters, then queue this event.
		//
		if (p._accumulateContent)
			p._queue = EV_END_ELEMENT;
		else
		{
			p._currentEvent = EV_END_ELEMENT;

			p._line = XML_GetCurrentLineNumber(p._parser);
			p._column = XML_GetCurrentColumnNumber(p._parser);
		}

		XML_StopParser(p._parser, true);
	}
}


void XMLCALL XMLStreamParser::handleCharacters(void* v, const XML_Char* s, int n)
{
	XMLStreamParser& p(*static_cast<XMLStreamParser*>(v));

	XML_ParsingStatus ps;
	XML_GetParsingStatus(p._parser, &ps);

	// Expat has a (mis)-feature of a possibily calling handlers even
	// after the non-resumable XML_StopParser call.
	//
	if (ps.parsing == XML_FINISHED)
		return;

	Content cont(p.content());

	// If this is empty or complex content, see if these are whitespaces.
	//
	switch (cont)
	{
	case Content::Empty:
	case Content::Complex:
	{
		for (int i(0); i != n; ++i)
		{
			char c(s[i]);
			if (c == 0x20 || c == 0x0A || c == 0x0D || c == 0x09)
				continue;

			// It would have been easier to throw the exception directly,
			// however, the Expat code is most likely not exception safe.
			//
			p._line = XML_GetCurrentLineNumber(p._parser);
			p._column = XML_GetCurrentColumnNumber(p._parser);
			XML_StopParser(p._parser, false);
			break;
		}
		return;
	}
	default:
		break;
	}

	// Append the characters if we are accumulating. This can also be a
	// followup event for another character event. In this case also
	// append the data.
	//
	if (p._accumulateContent || ps.parsing != XML_PARSING)
	{
		poco_assert(p._currentEvent == EV_CHARACTERS);
		p._value.append(s, n);
	}
	else
	{
		p._currentEvent = EV_CHARACTERS;
		p._value.assign(s, n);

		p._line = XML_GetCurrentLineNumber(p._parser);
		p._column = XML_GetCurrentColumnNumber(p._parser);

		// In simple content we need to accumulate all the characters
		// into a single event. To do this we will let the XMLStreamParser run
		// until we reach the end of the element.
		//
		if (cont == Content::Simple)
			p._accumulateContent = true;
		else
			XML_StopParser(p._parser, true);
	}
}


void XMLCALL XMLStreamParser::handleStartNamespaceDecl(void* v, const XML_Char* prefix, const XML_Char* ns)
{
	XMLStreamParser& p(*static_cast<XMLStreamParser*>(v));

	XML_ParsingStatus ps;
	XML_GetParsingStatus(p._parser, &ps);

	// Expat has a (mis)-feature of a possibily calling handlers even
	// after the non-resumable XML_StopParser call.
	//
	if (ps.parsing == XML_FINISHED)
		return;

	p._startNamespace.emplace_back();
	p._startNamespace.back().prefix() = (prefix != 0 ? prefix : "");
	p._startNamespace.back().namespaceURI() = (ns != 0 ? ns : "");
}


void XMLCALL XMLStreamParser::handleEndNamespaceDecl(void* v, const XML_Char* prefix)
{
	XMLStreamParser& p(*static_cast<XMLStreamParser*>(v));

	XML_ParsingStatus ps;
	XML_GetParsingStatus(p._parser, &ps);

	// Expat has a (mis)-feature of a possibily calling handlers even
	// after the non-resumable XML_StopParser call.
	//
	if (ps.parsing == XML_FINISHED)
		return;

	p._endNamespace.emplace_back();
	p._endNamespace.back().prefix() = (prefix != 0 ? prefix : "");
}


} } // namespace Poco::XML
