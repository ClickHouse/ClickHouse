//
// Orthodox.h
//
// $Id: //poco/1.4/CppUnit/include/CppUnit/Orthodox.h#1 $
//


#ifndef CppUnit_Orthodox_INCLUDED
#define CppUnit_Orthodox_INCLUDED


#include "CppUnit/CppUnit.h"
#include "CppUnit/TestCase.h"


namespace CppUnit {


/*
 * Orthodox performs a simple set of tests on an arbitary
 * class to make sure that it supports at least the
 * following operations:
 *
 *      default construction    - constructor
 *      equality/inequality     - operator== && operator!=
 *      assignment              - operator=
 *      negation                - operator!
 *      safe passage            - copy construction
 *
 * If operations for each of these are not declared
 * the template will not instantiate.  If it does
 * instantiate, tests are performed to make sure
 * that the operations have correct semantics.
 *
 * Adding an orthodox test to a suite is very
 * easy:
 *
 * public: Test *suite ()  {
 *     TestSuite *suiteOfTests = new TestSuite;
 *     suiteOfTests->addTest (new ComplexNumberTest ("testAdd");
 *     suiteOfTests->addTest (new TestCaller<Orthodox<Complex> > ());
 *     return suiteOfTests;
 *  }
 *
 * Templated test cases be very useful when you are want to
 * make sure that a group of classes have the same form.
 *
 * see TestSuite
 */
template <class ClassUnderTest> 
class Orthodox: public TestCase
{
public:
	Orthodox(): TestCase("Orthodox") 
	{
	}

protected:
    ClassUnderTest call(ClassUnderTest object);
    void runTest ();
};


// Run an orthodoxy test
template <class ClassUnderTest> 
void Orthodox<ClassUnderTest>::runTest()
{
    // make sure we have a default constructor
    ClassUnderTest   a, b, c;

    // make sure we have an equality operator
    assert (a == b);

    // check the inverse
    b.operator= (a.operator! ());
    assert (a != b);

    // double inversion
    b = !!a;
    assert (a == b);

    // invert again
    b = !a;

    // check calls
    c = a;
    assert (c == call (a));

    c = b;
    assert (c == call (b));
}


// Exercise a call
template <class ClassUnderTest> 
ClassUnderTest Orthodox<ClassUnderTest>::call(ClassUnderTest object)
{
    return object;
}


} // namespace CppUnit


#endif // CppUnit_Orthodox_INCLUDED
