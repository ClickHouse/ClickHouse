//
// XMLStreamParser.h
//
// Library: XML
// Package: XML
// Module:  XMLStreamParser
//
// Definition of the XMLStreamParser class.
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// Based on libstudxml (http://www.codesynthesis.com/projects/libstudxml/).
// Copyright (c) 2009-2013 Code Synthesis Tools CC.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef XML_XMLStreamParser_INCLUDED
#define XML_XMLStreamParser_INCLUDED


// We only support UTF-8 expat.
#ifdef XML_UNICODE
#    error UTF-16 expat (XML_UNICODE defined) is not supported
#endif


#include "Poco/XML/Content.h"
#include "Poco/XML/QName.h"
#include "Poco/XML/ValueTraits.h"
#if defined(POCO_UNBUNDLED_EXPAT)
#    include <expat.h>
#else
#    include "Poco/XML/expat.h"
#endif
#include <cstddef>
#include <iosfwd>
#include <map>
#include <string>
#include <vector>


namespace Poco
{
namespace XML
{


    class XML_API XMLStreamParser
    /// The streaming XML pull parser and streaming XML serializer. The parser
    /// is a conforming, non-validating XML 1.0 implementation (see Implementation Notes
    /// for details). The application character encoding (that is, the encoding used
    /// in the application's memory) for both parser and serializer is UTF-8.
    /// The output encoding of the serializer is UTF-8 as well. The parser supports
    /// UTF-8, UTF-16, ISO-8859-1, and US-ASCII input encodings.
    ///
    /// Attribute map:
    ///
    /// Attribute map lookup. If attribute is not found, then the version
    /// without the default value throws an appropriate parsing exception
    /// while the version with the default value returns that value.
    ///
    /// Note also that there is no attribute(ns, name) version since it
    /// would conflict with attribute(name, dv) (qualified attributes
    /// are not very common).
    ///
    /// Attribute map is valid throughout at the "element level" until
    /// end_element and not just during EV_START_ELEMENT. As a special case,
    /// the map is still valid after peek() that returned end_element until
    /// this end_element event is retrieved with next().
    ///
    /// Using parser:
    ///
    ///     XMLStreamParser p(ifs, argv[1]);
    ///     for (XMLStreamParser::EventType e: p)
    ///     {
    ///         switch (e)
    ///         {
    ///         case XMLStreamParser::EV_START_ELEMENT:
    ///             cerr << p.line () << ':' << p.column () << ": start " << p.name () << endl;
    ///             break;
    ///         case XMLStreamParser::EV_END_ELEMENT:
    ///             cerr << p.line () << ':' << p.column () << ": end " << p.name () << endl;
    ///             break;
    ///         case XMLStreamParser::EV_START_ATTRIBUTE:
    ///             ...
    ///         case XMLStreamParser::EV_END_ATTRIBUTE:
    ///             ...
    ///         case XMLStreamParser::EV_CHARACTERS:
    ///             ...
    ///         }
    ///     }
    {
    public:
        enum EventType
        /// Parsing events.
        {
            EV_START_ELEMENT,
            EV_END_ELEMENT,
            EV_START_ATTRIBUTE,
            EV_END_ATTRIBUTE,
            EV_CHARACTERS,
            EV_START_NAMESPACE_DECL,
            EV_END_NAMESPACE_DECL,
            EV_EOF
        };

        using FeatureType = unsigned short;
        /// If both receive_attributes_event and RECEIVE_ATTRIBUTE_MAP are
        /// specified, then RECEIVE_ATTRIBUTES_EVENT is assumed.

        static const FeatureType RECEIVE_ELEMENTS = 0x0001;
        static const FeatureType RECEIVE_CHARACTERS = 0x0002;
        static const FeatureType RECEIVE_ATTRIBUTE_MAP = 0x0004;
        static const FeatureType RECEIVE_ATTRIBUTES_EVENT = 0x0008;
        static const FeatureType RECEIVE_NAMESPACE_DECLS = 0x0010;
        static const FeatureType RECEIVE_DEFAULT = RECEIVE_ELEMENTS | RECEIVE_CHARACTERS | RECEIVE_ATTRIBUTE_MAP;

        struct AttributeValueType
        {
            std::string value;
            mutable bool handled;
        };

        using AttributeMapType = std::map<QName, AttributeValueType>;

        struct XML_API Iterator
        // C++11 range-based for support. Generally, the iterator interface
        // doesn't make much sense for the XMLStreamParser so for now we have an
        // implementation that is just enough to the range-based for.
        {
            using value_type = EventType;

            Iterator(XMLStreamParser * p = 0, EventType e = EV_EOF) : _parser(p), _e(e) { }

            value_type operator*() const { return _e; }

            Iterator & operator++()
            {
                _e = _parser->next();
                return *this;
            }

            bool operator==(Iterator y) const
            /// Comparison only makes sense when comparing to end (eof).
            {
                return _e == EV_EOF && y._e == EV_EOF;
            }

            bool operator!=(Iterator y) const
            /// Comparison only makes sense when comparing to end (eof).
            {
                return !(*this == y);
            }

        private:
            XMLStreamParser * _parser;
            EventType _e;
        };

        Iterator begin() { return Iterator(this, next()); }

        Iterator end() { return Iterator(this, EV_EOF); }

        XMLStreamParser(std::istream &, const std::string & inputName, FeatureType = RECEIVE_DEFAULT);
        /// The parser constructor takes three arguments: the stream to parse,
        /// input name that is used in diagnostics to identify the document being
        /// parsed, and the list of events we want the parser to report.
        ///
        /// Parse std::istream. Input name is used in diagnostics to identify
        /// the document being parsed.
        ///
        /// If stream exceptions are enabled then std::ios_base::failure
        /// exception is used to report io errors (badbit and failbit).
        /// Otherwise, those are reported as the parsing exception.

        XMLStreamParser(const void * data, std::size_t size, const std::string & inputName, FeatureType = RECEIVE_DEFAULT);
        /// Parse memory buffer that contains the whole document. Input name
        /// is used in diagnostics to identify the document being parsed.

        ~XMLStreamParser();
        /// Destroys the XMLStreamParser.

        EventType next();
        /// Call the next() function when we are ready to handle the next piece of XML.

        void nextExpect(EventType);
        /// Get the next event and make sure that it's what's expected. If it
        /// is not, then throw an appropriate parsing exception.

        void nextExpect(EventType, const std::string & name);
        void nextExpect(EventType, const QName & qname);
        void nextExpect(EventType, const std::string & ns, const std::string & name);

        EventType peek();
        EventType event();
        /// Return the event that was last returned by the call to next() or peek().

        const std::string & inputName() const;
        const QName & getQName() const;
        const std::string & namespaceURI() const;
        const std::string & localName() const;
        const std::string & prefix() const;
        std::string & value();
        const std::string & value() const;
        template <typename T>
        T value() const;
        Poco::UInt64 line() const;
        Poco::UInt64 column() const;
        const std::string & attribute(const std::string & name) const;
        template <typename T>
        T attribute(const std::string & name) const;
        std::string attribute(const std::string & name, const std::string & deflt) const;
        template <typename T>
        T attribute(const std::string & name, const T & deflt) const;
        const std::string & attribute(const QName & qname) const;
        template <typename T>
        T attribute(const QName & qname) const;
        std::string attribute(const QName & qname, const std::string & deflt) const;
        template <typename T>
        T attribute(const QName & qname, const T & deflt) const;
        bool attributePresent(const std::string & name) const;
        bool attributePresent(const QName & qname) const;
        const AttributeMapType & attributeMap() const;

        void content(Content);
        Content content() const;

        void nextExpect(EventType, const std::string & name, Content);
        void nextExpect(EventType, const QName & qname, Content);
        void nextExpect(EventType, const std::string & ns, const std::string & name, Content);

        // Helpers for parsing elements with simple content. The first two
        // functions assume that EV_START_ELEMENT has already been parsed. The
        // rest parse the complete element, from start to end.
        //
        // Note also that as with attribute(), there is no (namespace,name)
        // overload since it would conflicts with (namespace,deflt).
        std::string element();

        template <typename T>
        T element();
        std::string element(const std::string & name);
        std::string element(const QName & qname);
        template <typename T>
        T element(const std::string & name);
        template <typename T>
        T element(const QName & qname);
        std::string element(const std::string & name, const std::string & deflt);
        std::string element(const QName & qname, const std::string & deflt);
        template <typename T>
        T element(const std::string & name, const T & deflt);
        template <typename T>
        T element(const QName & qname, const T & deflt);

    private:
        XMLStreamParser(const XMLStreamParser &);
        XMLStreamParser & operator=(const XMLStreamParser &);

        static void XMLCALL handleStartElement(void *, const XML_Char *, const XML_Char **);
        static void XMLCALL handleEndElement(void *, const XML_Char *);
        static void XMLCALL handleCharacters(void *, const XML_Char *, int);
        static void XMLCALL handleStartNamespaceDecl(void *, const XML_Char *, const XML_Char *);
        static void XMLCALL handleEndNamespaceDecl(void *, const XML_Char *);

        void init();
        EventType nextImpl(bool peek);
        EventType nextBody();
        void handleError();

        // If _size is 0, then data is std::istream. Otherwise, it is a buffer.
        union
        {
            std::istream * is;
            const void * buf;
        } _data;

        std::size_t _size;
        const std::string _inputName;
        FeatureType _feature;
        XML_Parser _parser;
        std::size_t _depth;
        bool _accumulateContent; // Whether we are accumulating character content.
        enum
        {
            state_next,
            state_peek
        } _parserState;
        EventType _currentEvent;
        EventType _queue;
        QName _qname;
        std::string _value;
        const QName * _qualifiedName;
        std::string * _pvalue;
        Poco::UInt64 _line;
        Poco::UInt64 _column;

        struct AttributeType
        {
            QName qname;
            std::string value;
        };

        typedef std::vector<AttributeType> attributes;
        attributes _attributes;
        attributes::size_type _currentAttributeIndex; // Index of the current attribute.

        typedef std::vector<QName> NamespaceDecls;
        NamespaceDecls _startNamespace;
        NamespaceDecls::size_type _startNamespaceIndex; // Index of the current decl.
        NamespaceDecls _endNamespace;
        NamespaceDecls::size_type _endNamespaceIndex; // Index of the current decl.

        struct ElementEntry
        {
            ElementEntry(std::size_t d, Content c = Content::Mixed) : depth(d), content(c), attributesUnhandled(0) { }

            std::size_t depth;
            Content content;
            AttributeMapType attributeMap;
            mutable AttributeMapType::size_type attributesUnhandled;
        };

        typedef std::vector<ElementEntry> ElementState;
        std::vector<ElementEntry> _elementState;

        const AttributeMapType _emptyAttrMap;

        const ElementEntry * getElement() const;
        const ElementEntry * getElementImpl() const;
        void popElement();
    };


    XML_API std::ostream & operator<<(std::ostream &, XMLStreamParser::EventType);


    //
    // inlines
    //
    inline XMLStreamParser::EventType XMLStreamParser::event()
    // Return the even that was last returned by the call to next() or peek().
    {
        return _currentEvent;
    }


    inline const std::string & XMLStreamParser::inputName() const
    {
        return _inputName;
    }


    inline const QName & XMLStreamParser::getQName() const
    {
        return *_qualifiedName;
    }


    inline const std::string & XMLStreamParser::namespaceURI() const
    {
        return _qualifiedName->namespaceURI();
    }


    inline const std::string & XMLStreamParser::localName() const
    {
        return _qualifiedName->localName();
    }


    inline const std::string & XMLStreamParser::prefix() const
    {
        return _qualifiedName->prefix();
    }


    inline std::string & XMLStreamParser::value()
    {
        return *_pvalue;
    }


    inline const std::string & XMLStreamParser::value() const
    {
        return *_pvalue;
    }


    inline Poco::UInt64 XMLStreamParser::line() const
    {
        return _line;
    }


    inline Poco::UInt64 XMLStreamParser::column() const
    {
        return _column;
    }


    inline XMLStreamParser::EventType XMLStreamParser::peek()
    {
        if (_parserState == state_peek)
            return _currentEvent;
        else
        {
            EventType e(nextImpl(true));
            _parserState = state_peek; // Set it after the call to nextImpl().
            return e;
        }
    }


    template <typename T>
    inline T XMLStreamParser::value() const
    {
        return ValueTraits<T>::parse(value(), *this);
    }


    inline const std::string & XMLStreamParser::attribute(const std::string & n) const
    {
        return attribute(QName(n));
    }


    template <typename T>
    inline T XMLStreamParser::attribute(const std::string & n) const
    {
        return attribute<T>(QName(n));
    }


    inline std::string XMLStreamParser::attribute(const std::string & n, const std::string & dv) const
    {
        return attribute(QName(n), dv);
    }


    template <typename T>
    inline T XMLStreamParser::attribute(const std::string & n, const T & dv) const
    {
        return attribute<T>(QName(n), dv);
    }


    template <typename T>
    inline T XMLStreamParser::attribute(const QName & qn) const
    {
        return ValueTraits<T>::parse(attribute(qn), *this);
    }


    inline bool XMLStreamParser::attributePresent(const std::string & n) const
    {
        return attributePresent(QName(n));
    }


    inline const XMLStreamParser::AttributeMapType & XMLStreamParser::attributeMap() const
    {
        if (const ElementEntry * e = getElement())
        {
            e->attributesUnhandled = 0; // Assume all handled.
            return e->attributeMap;
        }

        return _emptyAttrMap;
    }


    inline void XMLStreamParser::nextExpect(EventType e, const QName & qn)
    {
        nextExpect(e, qn.namespaceURI(), qn.localName());
    }


    inline void XMLStreamParser::nextExpect(EventType e, const std::string & n)
    {
        nextExpect(e, std::string(), n);
    }


    inline void XMLStreamParser::nextExpect(EventType e, const QName & qn, Content c)
    {
        nextExpect(e, qn);
        poco_assert(e == EV_START_ELEMENT);
        content(c);
    }


    inline void XMLStreamParser::nextExpect(EventType e, const std::string & n, Content c)
    {
        nextExpect(e, std::string(), n);
        poco_assert(e == EV_START_ELEMENT);
        content(c);
    }


    inline void XMLStreamParser::nextExpect(EventType e, const std::string & ns, const std::string & n, Content c)
    {
        nextExpect(e, ns, n);
        poco_assert(e == EV_START_ELEMENT);
        content(c);
    }


    template <typename T>
    inline T XMLStreamParser::element()
    {
        return ValueTraits<T>::parse(element(), *this);
    }


    inline std::string XMLStreamParser::element(const std::string & n)
    {
        nextExpect(EV_START_ELEMENT, n);
        return element();
    }


    inline std::string XMLStreamParser::element(const QName & qn)
    {
        nextExpect(EV_START_ELEMENT, qn);
        return element();
    }


    template <typename T>
    inline T XMLStreamParser::element(const std::string & n)
    {
        return ValueTraits<T>::parse(element(n), *this);
    }


    template <typename T>
    inline T XMLStreamParser::element(const QName & qn)
    {
        return ValueTraits<T>::parse(element(qn), *this);
    }


    inline std::string XMLStreamParser::element(const std::string & n, const std::string & dv)
    {
        return element(QName(n), dv);
    }


    template <typename T>
    inline T XMLStreamParser::element(const std::string & n, const T & dv)
    {
        return element<T>(QName(n), dv);
    }


    inline void XMLStreamParser::content(Content c)
    {
        poco_assert(_parserState == state_next);

        if (!_elementState.empty() && _elementState.back().depth == _depth)
            _elementState.back().content = c;
        else
            _elementState.emplace_back(_depth, c);
    }


    inline Content XMLStreamParser::content() const
    {
        poco_assert(_parserState == state_next);

        return !_elementState.empty() && _elementState.back().depth == _depth ? _elementState.back().content : Content(Content::Mixed);
    }


    inline const XMLStreamParser::ElementEntry * XMLStreamParser::getElement() const
    {
        return _elementState.empty() ? 0 : getElementImpl();
    }


    template <typename T>
    T XMLStreamParser::attribute(const QName & qn, const T & dv) const
    {
        if (const ElementEntry * e = getElement())
        {
            AttributeMapType::const_iterator i(e->attributeMap.find(qn));

            if (i != e->attributeMap.end())
            {
                if (!i->second.handled)
                {
                    i->second.handled = true;
                    e->attributesUnhandled--;
                }
                return ValueTraits<T>::parse(i->second.value, *this);
            }
        }

        return dv;
    }


    template <typename T>
    T XMLStreamParser::element(const QName & qn, const T & dv)
    {
        if (peek() == EV_START_ELEMENT && getQName() == qn)
        {
            next();
            return element<T>();
        }

        return dv;
    }


}
} // namespace Poco::XML


#endif // XML_XMLStreamParser_INCLUDED
