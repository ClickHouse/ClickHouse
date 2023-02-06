//
// XMLStreamParserTest.cpp
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//

#include "XMLStreamParserTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/XML/XMLStreamParser.h"
#include "Poco/Exception.h"
#include <sstream>
#include <string>
#include <vector>
#include <iostream>


using namespace Poco::XML;


XMLStreamParserTest::XMLStreamParserTest(const std::string& name):
	CppUnit::TestCase(name)
{
}


XMLStreamParserTest::~XMLStreamParserTest()
{
}


void XMLStreamParserTest::testParser()
{
	// Test error handling.
	//
	try
	{
		std::istringstream is("<root><nested>X</nasted></root>");
		XMLStreamParser p(is, "test");

		assertTrue (p.next() == XMLStreamParser::EV_START_ELEMENT);
		assertTrue (p.next() == XMLStreamParser::EV_START_ELEMENT);
		assertTrue (p.next() == XMLStreamParser::EV_CHARACTERS && p.value() == "X");
		p.next();
		assertTrue (false);
	}
	catch (const Poco::Exception&)
	{
		// cerr << e.what () << endl;
	}

	try
	{
		std::istringstream is("<root/>");
		is.exceptions(std::ios_base::badbit | std::ios_base::failbit);
		XMLStreamParser p(is, "test");

		is.setstate(std::ios_base::badbit);
		p.next();
		assertTrue (false);
	}
	catch (const std::ios_base::failure&)
	{
	}

	// Test the nextExpect() functionality.
	//
	{
		std::istringstream is("<root/>");
		XMLStreamParser p(is, "test");
		p.nextExpect(XMLStreamParser::EV_START_ELEMENT, "root");
		p.nextExpect(XMLStreamParser::EV_END_ELEMENT);
	}

	try
	{
		std::istringstream is("<root/>");
		XMLStreamParser p(is, "test");
		p.nextExpect(XMLStreamParser::EV_END_ELEMENT);
		assertTrue (false);
	}
	catch (const Poco::Exception&)
	{
		// cerr << e.what () << endl;
	}

	try
	{
		std::istringstream is("<root/>");
		XMLStreamParser p(is, "test");
		p.nextExpect(XMLStreamParser::EV_START_ELEMENT, "root1");
		assertTrue (false);
	}
	catch (const Poco::Exception&)
	{
		// cerr << e.what () << endl;
	}

	// Test nextExpect() with content setting.
	//
	{
		std::istringstream is("<root>  </root>");
		XMLStreamParser p(is, "empty");

		p.nextExpect(XMLStreamParser::EV_START_ELEMENT, "root", Content::Empty);
		p.nextExpect(XMLStreamParser::EV_END_ELEMENT);
		p.nextExpect(XMLStreamParser::EV_EOF);
	}

	// Test namespace declarations.
	//
	{
		// Followup end element event that should be precedeeded by end
		// namespace declaration.
		//
		std::istringstream is("<root xmlns:a='a'/>");
		XMLStreamParser p(is, "test", XMLStreamParser::RECEIVE_DEFAULT | XMLStreamParser::RECEIVE_NAMESPACE_DECLS);

		p.nextExpect(XMLStreamParser::EV_START_ELEMENT, "root");
		p.nextExpect(XMLStreamParser::EV_START_NAMESPACE_DECL);
		p.nextExpect(XMLStreamParser::EV_END_NAMESPACE_DECL);
		p.nextExpect(XMLStreamParser::EV_END_ELEMENT);
	}

	// Test value extraction.
	//
	{
		std::istringstream is("<root>123</root>");
		XMLStreamParser p(is, "test");
		p.nextExpect(XMLStreamParser::EV_START_ELEMENT, "root");
		p.nextExpect(XMLStreamParser::EV_CHARACTERS);
		assertTrue (p.value<int>() == 123);
		p.nextExpect(XMLStreamParser::EV_END_ELEMENT);
	}

	// Test attribute maps.
	//
	{
		std::istringstream is("<root a='a' b='b' d='123' t='true'/>");
		XMLStreamParser p(is, "test");
		p.nextExpect(XMLStreamParser::EV_START_ELEMENT, "root");

		assertTrue (p.attribute("a") == "a");
		assertTrue (p.attribute("b", "B") == "b");
		assertTrue (p.attribute("c", "C") == "C");
		assertTrue (p.attribute<int>("d") == 123);
		assertTrue (p.attribute<bool>("t") == true);
		assertTrue (p.attribute("f", false) == false);

		p.nextExpect(XMLStreamParser::EV_END_ELEMENT);
	}

	{
		std::istringstream is("<root a='a'><nested a='A'><inner/></nested></root>");
		XMLStreamParser p(is, "test");
		p.nextExpect(XMLStreamParser::EV_START_ELEMENT, "root");
		assertTrue (p.attribute("a") == "a");
		assertTrue (p.peek() == XMLStreamParser::EV_START_ELEMENT && p.localName() == "nested");
		assertTrue (p.attribute("a") == "a");
		p.nextExpect(XMLStreamParser::EV_START_ELEMENT, "nested");
		assertTrue (p.attribute("a") == "A");
		p.nextExpect(XMLStreamParser::EV_START_ELEMENT, "inner");
		assertTrue (p.attribute("a", "") == "");
		p.nextExpect(XMLStreamParser::EV_END_ELEMENT);
		assertTrue (p.attribute("a") == "A");
		assertTrue (p.peek() == XMLStreamParser::EV_END_ELEMENT);
		assertTrue (p.attribute("a") == "A"); // Still valid.
		p.nextExpect(XMLStreamParser::EV_END_ELEMENT);
		assertTrue (p.attribute("a") == "a");
		p.nextExpect(XMLStreamParser::EV_END_ELEMENT);
		assertTrue (p.attribute("a", "") == "");
	}

	try
	{
		std::istringstream is("<root a='a' b='b'/>");
		XMLStreamParser p(is, "test");
		p.nextExpect(XMLStreamParser::EV_START_ELEMENT, "root");
		assertTrue (p.attribute("a") == "a");
		p.nextExpect(XMLStreamParser::EV_END_ELEMENT);
		assertTrue (false);
	}
	catch (const Poco::Exception&)
	{
		// cerr << e.what () << endl;
	}

	try
	{
		std::istringstream is("<root a='abc'/>");
		XMLStreamParser p(is, "test");
		p.nextExpect(XMLStreamParser::EV_START_ELEMENT, "root");
		p.attribute<int>("a");
		assertTrue (false);
	}
	catch (const Poco::Exception&)
	{
		// cerr << e.what () << endl;
	}

	// Test peeking and getting the current event.
	//
	{
		std::istringstream is("<root x='x'>x<nested/></root>");
		XMLStreamParser p(is, "peek", XMLStreamParser::RECEIVE_DEFAULT | XMLStreamParser::RECEIVE_ATTRIBUTES_EVENT);

		assertTrue (p.event() == XMLStreamParser::EV_EOF);

		assertTrue (p.peek() == XMLStreamParser::EV_START_ELEMENT);
		assertTrue (p.next() == XMLStreamParser::EV_START_ELEMENT);
		assertTrue (p.event() == XMLStreamParser::EV_START_ELEMENT);

		assertTrue (p.peek() == XMLStreamParser::EV_START_ATTRIBUTE);
		assertTrue (p.event() == XMLStreamParser::EV_START_ATTRIBUTE);
		assertTrue (p.next() == XMLStreamParser::EV_START_ATTRIBUTE);

		assertTrue (p.peek() == XMLStreamParser::EV_CHARACTERS && p.value() == "x");
		assertTrue (p.next() == XMLStreamParser::EV_CHARACTERS && p.value() == "x");
		assertTrue (p.event() == XMLStreamParser::EV_CHARACTERS && p.value() == "x");

		assertTrue (p.peek() == XMLStreamParser::EV_END_ATTRIBUTE);
		assertTrue (p.event() == XMLStreamParser::EV_END_ATTRIBUTE);
		assertTrue (p.next() == XMLStreamParser::EV_END_ATTRIBUTE);

		assertTrue (p.peek() == XMLStreamParser::EV_CHARACTERS && p.value() == "x");
		assertTrue (p.next() == XMLStreamParser::EV_CHARACTERS && p.value() == "x");
		assertTrue (p.event() == XMLStreamParser::EV_CHARACTERS && p.value() == "x");

		assertTrue (p.peek() == XMLStreamParser::EV_START_ELEMENT);
		assertTrue (p.next() == XMLStreamParser::EV_START_ELEMENT);
		assertTrue (p.event() == XMLStreamParser::EV_START_ELEMENT);

		assertTrue (p.peek() == XMLStreamParser::EV_END_ELEMENT);
		assertTrue (p.next() == XMLStreamParser::EV_END_ELEMENT);
		assertTrue (p.event() == XMLStreamParser::EV_END_ELEMENT);

		assertTrue (p.peek() == XMLStreamParser::EV_END_ELEMENT);
		assertTrue (p.next() == XMLStreamParser::EV_END_ELEMENT);
		assertTrue (p.event() == XMLStreamParser::EV_END_ELEMENT);

		assertTrue (p.peek() == XMLStreamParser::EV_EOF);
		assertTrue (p.next() == XMLStreamParser::EV_EOF);
		assertTrue (p.event() == XMLStreamParser::EV_EOF);
	}

	// Test content processing.
	//

	// empty
	//
	{
		std::istringstream is("<root x=' x '>  \n\t </root>");
		XMLStreamParser p(is, "empty", XMLStreamParser::RECEIVE_DEFAULT | XMLStreamParser::RECEIVE_ATTRIBUTES_EVENT);

		assertTrue (p.next() == XMLStreamParser::EV_START_ELEMENT);
		p.content(Content::Empty);
		assertTrue (p.next() == XMLStreamParser::EV_START_ATTRIBUTE);
		assertTrue (p.next() == XMLStreamParser::EV_CHARACTERS && p.value() == " x ");
		assertTrue (p.next() == XMLStreamParser::EV_END_ATTRIBUTE);
		assertTrue (p.next() == XMLStreamParser::EV_END_ELEMENT);
		assertTrue (p.next() == XMLStreamParser::EV_EOF);
	}

	try
	{
		std::istringstream is("<root>  \n &amp; X \t </root>");
		XMLStreamParser p(is, "empty");

		assertTrue (p.next() == XMLStreamParser::EV_START_ELEMENT);
		p.content(Content::Empty);
		p.next();
		assertTrue (false);
	}
	catch (const Poco::Exception&)
	{
		// cerr << e.what () << endl;
	}

	// simple
	//
	{
		std::istringstream is("<root> X </root>");
		XMLStreamParser p(is, "simple");

		assertTrue (p.next() == XMLStreamParser::EV_START_ELEMENT);
		p.content(Content::Simple);
		assertTrue (p.next() == XMLStreamParser::EV_CHARACTERS && p.value() == " X ");
		assertTrue (p.next() == XMLStreamParser::EV_END_ELEMENT);
		assertTrue (p.next() == XMLStreamParser::EV_EOF);
	}

	try
	{
		std::istringstream is("<root> ? <nested/></root>");
		XMLStreamParser p(is, "simple");

		assertTrue (p.next() == XMLStreamParser::EV_START_ELEMENT);
		p.content(Content::Simple);
		assertTrue (p.next() == XMLStreamParser::EV_CHARACTERS && p.value() == " ? ");
		p.next();
		assertTrue (false);
	}
	catch (const Poco::Exception&)
	{
		// cerr << e.what () << endl;
	}

	{
		// Test content accumulation in simple content.
		//
		std::istringstream is("<root xmlns:a='a'>1&#x32;3</root>");
		XMLStreamParser p(is, "simple", XMLStreamParser::RECEIVE_DEFAULT | XMLStreamParser::RECEIVE_NAMESPACE_DECLS);

		assertTrue (p.next() == XMLStreamParser::EV_START_ELEMENT);
		p.nextExpect(XMLStreamParser::EV_START_NAMESPACE_DECL);
		p.content(Content::Simple);
		assertTrue (p.next() == XMLStreamParser::EV_CHARACTERS && p.value() == "123");
		p.nextExpect(XMLStreamParser::EV_END_NAMESPACE_DECL);
		assertTrue (p.next() == XMLStreamParser::EV_END_ELEMENT);
		assertTrue (p.next() == XMLStreamParser::EV_EOF);
	}

	try
	{
		// Test error handling in accumulation in simple content.
		//
		std::istringstream is("<root xmlns:a='a'>1&#x32;<nested/>3</root>");
		XMLStreamParser p(is, "simple", XMLStreamParser::RECEIVE_DEFAULT | XMLStreamParser::RECEIVE_NAMESPACE_DECLS);

		assertTrue (p.next() == XMLStreamParser::EV_START_ELEMENT);
		p.nextExpect(XMLStreamParser::EV_START_NAMESPACE_DECL);
		p.content(Content::Simple);
		p.next();
		assertTrue (false);
	}
	catch (const Poco::Exception&)
	{
		// cerr << e.what () << endl;
	}

	// complex
	//
	{
		std::istringstream is("<root x=' x '>\n"
				"  <nested>\n"
				"    <inner/>\n"
				"    <inner> X </inner>\n"
				"  </nested>\n"
				"</root>\n");
		XMLStreamParser p(is, "complex", XMLStreamParser::RECEIVE_DEFAULT | XMLStreamParser::RECEIVE_ATTRIBUTES_EVENT);

		assertTrue (p.next() == XMLStreamParser::EV_START_ELEMENT); // root
		p.content(Content::Complex);

		assertTrue (p.next() == XMLStreamParser::EV_START_ATTRIBUTE);
		assertTrue (p.next() == XMLStreamParser::EV_CHARACTERS && p.value() == " x ");
		assertTrue (p.next() == XMLStreamParser::EV_END_ATTRIBUTE);

		assertTrue (p.next() == XMLStreamParser::EV_START_ELEMENT); // nested
		p.content(Content::Complex);

		assertTrue (p.next() == XMLStreamParser::EV_START_ELEMENT); // inner
		p.content(Content::Empty);
		assertTrue (p.next() == XMLStreamParser::EV_END_ELEMENT);   // inner

		assertTrue (p.next() == XMLStreamParser::EV_START_ELEMENT); // inner
		p.content(Content::Simple);
		assertTrue (p.next() == XMLStreamParser::EV_CHARACTERS && p.value() == " X ");
		assertTrue (p.next() == XMLStreamParser::EV_END_ELEMENT);   // inner

		assertTrue (p.next() == XMLStreamParser::EV_END_ELEMENT);   // nested
		assertTrue (p.next() == XMLStreamParser::EV_END_ELEMENT);   // root
		assertTrue (p.next() == XMLStreamParser::EV_EOF);
	}

	try
	{
		std::istringstream is("<root> \n<n/> X <n> X </n>  </root>");
		XMLStreamParser p(is, "complex");

		assertTrue (p.next() == XMLStreamParser::EV_START_ELEMENT);
		p.content(Content::Complex);
		assertTrue (p.next() == XMLStreamParser::EV_START_ELEMENT);
		assertTrue (p.next() == XMLStreamParser::EV_END_ELEMENT);
		p.next();
		assertTrue (false);
	}
	catch (const Poco::Exception&)
	{
		// cerr << e.what () << endl;
	}

	// Test element with simple content helpers.
	//
	{
		std::istringstream is("<root>"
				"  <nested>X</nested>"
				"  <nested/>"
				"  <nested>123</nested>"
				"  <nested>Y</nested>"
				"  <t:nested xmlns:t='test'>Z</t:nested>"
				"  <nested>234</nested>"
				"  <t:nested xmlns:t='test'>345</t:nested>"
				"  <nested>A</nested>"
				"  <t:nested xmlns:t='test'>B</t:nested>"
				"  <nested1>A</nested1>"
				"  <t:nested1 xmlns:t='test'>B</t:nested1>"
				"  <nested>1</nested>"
				"  <t:nested xmlns:t='test'>2</t:nested>"
				"  <nested1>1</nested1>"
				"  <t:nested1 xmlns:t='test'>2</t:nested1>"
				"</root>");
		XMLStreamParser p(is, "element");

		p.nextExpect(XMLStreamParser::EV_START_ELEMENT, "root", Content::Complex);

		p.nextExpect(XMLStreamParser::EV_START_ELEMENT, "nested");
		assertTrue (p.element() == "X");

		p.nextExpect(XMLStreamParser::EV_START_ELEMENT, "nested");
		assertTrue (p.element() == "");

		p.nextExpect(XMLStreamParser::EV_START_ELEMENT, "nested");
		assertTrue (p.element<unsigned int>() == 123);

		assertTrue (p.element("nested") == "Y");
		assertTrue (p.element(QName("test", "nested")) == "Z");

		assertTrue (p.element<unsigned int>("nested") == 234);
		assertTrue (p.element<unsigned int>(QName("test", "nested")) == 345);

		assertTrue (p.element("nested", "a") == "A");
		assertTrue (p.element(QName("test", "nested"), "b") == "B");

		assertTrue (p.element("nested", "a") == "a" && p.element("nested1") == "A");
		assertTrue (p.element(QName("test", "nested"), "b") == "b" && p.element(QName("test", "nested1")) == "B");

		assertTrue (p.element<unsigned int>("nested", 10) == 1);
		assertTrue (p.element<unsigned int>(QName("test", "nested"), 20) == 2);

		assertTrue (p.element<unsigned int>("nested", 10) == 10 && p.element<unsigned int>("nested1") == 1);
		assertTrue (p.element<unsigned int>(QName("test", "nested"), 20) == 20 && p.element<unsigned int>(QName("test", "nested1")) == 2);

		p.nextExpect(XMLStreamParser::EV_END_ELEMENT);
	}

	// Test the iterator interface.
	//
	{
		std::istringstream is("<root><nested>X</nested></root>");
		XMLStreamParser p(is, "iterator");

		std::vector<XMLStreamParser::EventType> v;

		for (XMLStreamParser::Iterator i(p.begin()); i != p.end(); ++i)
			v.push_back(*i);

		//for (XMLStreamParser::EventType e: p)
		//  v.push_back (e);

		assertTrue (v.size() == 5);
		assertTrue (v[0] == XMLStreamParser::EV_START_ELEMENT);
		assertTrue (v[1] == XMLStreamParser::EV_START_ELEMENT);
		assertTrue (v[2] == XMLStreamParser::EV_CHARACTERS);
		assertTrue (v[3] == XMLStreamParser::EV_END_ELEMENT);
		assertTrue (v[4] == XMLStreamParser::EV_END_ELEMENT);
	}

	// Test space extraction into the std::string value.
	//
	{
		std::istringstream is("<root a=' a '> b </root>");
		XMLStreamParser p(is, "test");
		p.nextExpect(XMLStreamParser::EV_START_ELEMENT, "root");
		assertTrue (p.attribute<std::string>("a") == " a ");
		p.nextExpect(XMLStreamParser::EV_CHARACTERS);
		assertTrue (p.value<std::string>() == " b ");
		p.nextExpect(XMLStreamParser::EV_END_ELEMENT);
	}
}


void XMLStreamParserTest::setUp()
{
}


void XMLStreamParserTest::tearDown()
{
}


CppUnit::Test* XMLStreamParserTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("XMLStreamParserTest");

	CppUnit_addTest(pSuite, XMLStreamParserTest, testParser);

	return pSuite;
}
