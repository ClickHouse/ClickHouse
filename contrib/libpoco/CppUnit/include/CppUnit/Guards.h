//
// Guards.h
//
// $Id: //poco/1.4/CppUnit/include/CppUnit/Guards.h#1 $
//


#ifndef CppUnit_Guards_INCLUDED
#define CppUnit_Guards_INCLUDED


// Prevent copy construction and assignment for a class
#define REFERENCEOBJECT(className) \
private: \
	className(const className& other); \
	className& operator = (const className& other);


#endif // CppUnit_Guards_INCLUDED
