//
// TestCase.h
//
// $Id: //poco/1.4/CppUnit/include/CppUnit/TestCase.h#1 $
//


#ifndef CppUnit_TestCase_INCLUDED
#define CppUnit_TestCase_INCLUDED


#include "CppUnit/CppUnit.h"
#include "CppUnit/Guards.h"
#include "CppUnit/Test.h"
#include "CppUnit/CppUnitException.h"
#include <string>
#include <typeinfo>


namespace CppUnit {


class TestResult;


/*
 * A test case defines the fixture to run multiple tests. To define a test case
 * 1) implement a subclass of TestCase
 * 2) define instance variables that store the state of the fixture
 * 3) initialize the fixture state by overriding setUp
 * 4) clean-up after a test by overriding tearDown.
 *
 * Each test runs in its own fixture so there
 * can be no side effects among test runs.
 * Here is an example:
 *
 * class MathTest : public TestCase {
 *     protected: int m_value1;
 *     protected: int m_value2;
 *
 *     public: MathTest (std::string name)
 *                 : TestCase (name) {
 *     }
 *
 *     protected: void setUp () {
 *         m_value1 = 2;
 *         m_value2 = 3;
 *     }
 * }
 *
 *
 * For each test implement a method which interacts
 * with the fixture. Verify the expected results with assertions specified
 * by calling assert on the expression you want to test:
 *
 *    protected: void testAdd () {
 *        int result = value1 + value2;
 *        assert (result == 5);
 *    }
 *
 * Once the methods are defined you can run them. To do this, use
 * a TestCaller.
 *
 * Test *test = new TestCaller<MathTest>("testAdd", MathTest::testAdd);
 * test->run ();
 *
 *
 * The tests to be run can be collected into a TestSuite. CppUnit provides
 * different test runners which can run a test suite and collect the results.
 * The test runners expect a static method suite as the entry
 * point to get a test to run.
 *
 * public: static MathTest::suite () {
 *      TestSuite *suiteOfTests = new TestSuite;
 *      suiteOfTests->addTest(new TestCaller<MathTest>("testAdd", testAdd));
 *      suiteOfTests->addTest(new TestCaller<MathTest>("testDivideByZero", testDivideByZero));
 *      return suiteOfTests;
 *  }
 *
 * Note that the caller of suite assumes lifetime control
 * for the returned suite.
 *
 * see TestResult, TestSuite and TestCaller
 *
 */
class CppUnit_API TestCase: public Test
{
    REFERENCEOBJECT (TestCase)

public:
	TestCase(const std::string& Name);
	~TestCase();

	virtual void run(TestResult* result);
	virtual TestResult* run();
	virtual int countTestCases();
	const std::string& name() const;
	std::string toString();

	virtual void setUp();
	virtual void tearDown();

protected:
	virtual void runTest();
	TestResult* defaultResult();

	void assertImplementation(bool condition,
	                          const std::string& conditionExpression = "",
	                          long lineNumber = CppUnitException::CPPUNIT_UNKNOWNLINENUMBER,
	                          const std::string& fileName = CppUnitException::CPPUNIT_UNKNOWNFILENAME);

	void loop1assertImplementation(bool condition,
	                               const std::string& conditionExpression = "",
	                               long lineNumber = CppUnitException::CPPUNIT_UNKNOWNLINENUMBER,
                                   long dataLineNumber = CppUnitException::CPPUNIT_UNKNOWNLINENUMBER,
	                               const std::string& fileName = CppUnitException::CPPUNIT_UNKNOWNFILENAME);

	void loop2assertImplementation(bool condition,
	                               const std::string& conditionExpression = "",
	                               long lineNumber = CppUnitException::CPPUNIT_UNKNOWNLINENUMBER,
                                   long data1LineNumber = CppUnitException::CPPUNIT_UNKNOWNLINENUMBER,
                                   long data2LineNumber = CppUnitException::CPPUNIT_UNKNOWNLINENUMBER,
	                               const std::string& fileName = CppUnitException::CPPUNIT_UNKNOWNFILENAME);

	void assertEquals(long expected,
	                  long actual,
	                  long lineNumber = CppUnitException::CPPUNIT_UNKNOWNLINENUMBER,
	                  const std::string& fileName = CppUnitException::CPPUNIT_UNKNOWNFILENAME);

	void assertEquals(double expected,
	                  double actual,
                      double delta,
                      long lineNumber = CppUnitException::CPPUNIT_UNKNOWNLINENUMBER,
                      const std::string& fileName = CppUnitException::CPPUNIT_UNKNOWNFILENAME);

	void assertEquals(const std::string& expected, 
	                  const std::string& actual,
	                  long lineNumber = CppUnitException::CPPUNIT_UNKNOWNLINENUMBER,
	                  const std::string& fileName = CppUnitException::CPPUNIT_UNKNOWNFILENAME);

	void assertEquals(const void* expected,
	                  const void* actual,
	                  long lineNumber = CppUnitException::CPPUNIT_UNKNOWNLINENUMBER,
	                  const std::string& fileName = CppUnitException::CPPUNIT_UNKNOWNFILENAME);

	std::string notEqualsMessage(long expected, long actual);
	std::string notEqualsMessage(double expected, double actual);
	std::string notEqualsMessage(const void* expected, const void* actual);
	std::string notEqualsMessage(const std::string& expected, const std::string& actual);

	void assertNotNull(const void* pointer,
	                   const std::string& pointerExpression = "",
	                   long lineNumber = CppUnitException::CPPUNIT_UNKNOWNLINENUMBER,
	                   const std::string& fileName = CppUnitException::CPPUNIT_UNKNOWNFILENAME);

	void assertNull(const void* pointer,  
	                const std::string& pointerExpression = "",
	                long lineNumber = CppUnitException::CPPUNIT_UNKNOWNLINENUMBER,
	                const std::string& fileName = CppUnitException::CPPUNIT_UNKNOWNFILENAME);

	void fail(const std::string& message = "",
	          long lineNumber = CppUnitException::CPPUNIT_UNKNOWNLINENUMBER,
	          const std::string& fileName = CppUnitException::CPPUNIT_UNKNOWNFILENAME);

	void warn(const std::string& message = "",
              long lineNumber = CppUnitException::CPPUNIT_UNKNOWNLINENUMBER,
              const std::string& fileName = CppUnitException::CPPUNIT_UNKNOWNFILENAME);


private:
	const std::string _name;
};


// Constructs a test case
inline TestCase::TestCase(const std::string& name): _name (name)
{
}


// Destructs a test case
inline TestCase::~TestCase()
{
}


// Returns a count of all the tests executed
inline int TestCase::countTestCases()
{
	return 1; 
}


// Returns the name of the test case
inline const std::string& TestCase::name() const
{
	return _name; 
}


// A hook for fixture set up
inline void TestCase::setUp()
{
}


// A hook for fixture tear down
inline void TestCase::tearDown()
{
}


// Returns the name of the test case instance
inline std::string TestCase::toString()
{
	const std::type_info& thisClass = typeid(*this); 
	return std::string(thisClass.name()) + "." + name(); 
}


// A set of macros which allow us to get the line number
// and file name at the point of an error.
// Just goes to show that preprocessors do have some
// redeeming qualities.
#undef assert
#define assert(condition) \
	(this->assertImplementation((condition), (#condition), __LINE__, __FILE__))

#define loop_1_assert(data1line, condition) \
	(this->loop1assertImplementation((condition), (#condition), __LINE__, data1line, __FILE__))

#define loop_2_assert(data1line, data2line, condition) \
	(this->loop2assertImplementation((condition), (#condition), __LINE__, data1line, data2line, __FILE__))

#define assertEqualDelta(expected, actual, delta) \
	(this->assertEquals((expected), (actual), (delta), __LINE__, __FILE__))

#define assertEqual(expected, actual) \
	(this->assertEquals((expected), (actual), __LINE__, __FILE__))

#define assertNullPtr(ptr) \
	(this->assertNull((ptr), #ptr, __LINE__, __FILE__))
	
#define assertNotNullPtr(ptr) \
	(this->assertNotNull((ptr), #ptr, __LINE__, __FILE__))

#define failmsg(msg) \
	(this->fail(msg, __LINE__, __FILE__))

#define warnmsg(msg) \
	(this->fail(msg, __LINE__, __FILE__))


} // namespace CppUnit


#endif // CppUnit_TestCase_INCLUDED
