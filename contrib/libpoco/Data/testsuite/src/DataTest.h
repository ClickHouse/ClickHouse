//
// DataTest.h
//
// $Id: //poco/Main/Data/testsuite/src/DataTest.h#6 $
//
// Definition of the DataTest class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DataTest_INCLUDED
#define DataTest_INCLUDED


#include "Poco/Data/Data.h"
#include "Poco/BinaryReader.h"
#include "Poco/BinaryWriter.h"
#include "Poco/Data/Row.h"
#include "CppUnit/TestCase.h"


class DataTest: public CppUnit::TestCase
{
public:
	DataTest(const std::string& name);
	~DataTest();

	void testSession();
	void testStatementFormatting();
	void testFeatures();
	void testProperties();
	void testLOB();
	void testCLOB();
	void testCLOBStreams();
	void testColumnVector();
	void testColumnVectorBool();
	void testColumnDeque();
	void testColumnList();
	void testRow();
	void testRowSort();
	void testRowFormat();
	void testDateAndTime();
	void testExternalBindingAndExtraction();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
	void testRowStrictWeak(const Poco::Data::Row& row1, 
		const Poco::Data::Row& row2, 
		const Poco::Data::Row& row3);
		/// Strict weak ordering requirement for sorted containers
		/// as described in Josuttis "The Standard C++ Library"
		/// chapter 6.5. pg. 176.
		/// For this to pass, the following condition must be satisifed: 
		/// row1 < row2 < row3

	void writeToCLOB(Poco::BinaryWriter& writer);
	void readFromCLOB(Poco::BinaryReader& reader);
};


#endif // DataTest_INCLUDED
