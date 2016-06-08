//
// Units.cpp
//
// $Id: //poco/svn/Util/samples/Units/src/Units.cpp#1 $
//
// This sample demonstrates the Units.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Util/Units.h"
#include <iostream>


using namespace Poco::Util::Units::Values;
using namespace Poco::Util::Units::Constants;


using Poco::Util::Units::square;
using Poco::Util::Units::cube;


namespace Poco {
namespace Util {
namespace Units {
namespace Units {
typedef Compose<Scale<N, 1, 1000>, Power<m, -3> > SpecificWeight;// [kN/m^3]
} // namespace Units
namespace Values {
typedef Value<double, Units::SpecificWeight> SpecificWeight;
} } } } // namespace Poco::Util::Units


int main()
{
	std::cout << "One mile is " << km(mile(1)) << std::endl;
	// Output: One mile is 1.60934 km

	std::cout << "Flow rate is " << m3(mile(1)*inch(80)*foot(9))/s(minute(5)) << std::endl;
	// Output: Flow rate is 29.9026 (m)^3.(s)^-1

	hour h;
	//h = cm(3); // Compile-time error: incompatible units
	//h = 4;     // Compile-time error: 4 of what?
	h = day(4);  // Ok: h is 96 hours

	m l = cm(42);
	std::cout << cm(42) << " == " << l << " == " << milli(l) << std::endl;
	std::cout << "Area of circle with radius " << l << 
		" is " << mm2(square(l) * pi) << std::endl;

	SpecificWeight sw(9.81); // water

	std::cout << "Volume of a water cube with side size " << m(0.1) << 
		" is " << liter(cube(m(0.1))) << 
		" and weighs " << N(sw * cube(m(.1))) << std::endl;

	m radius_equator(6378135);
	m radius_pole(6356750);
	m3 vol((pi * square(radius_equator) * radius_pole) * 4/3);

	std::cout << "Volume of Earth is " << vol 
		<< " (" << yotta(liter(vol)) << ")" << std::endl;
	std::cout << "It takes " << minute(AU/c) << " for a Sun beam to reach Earth." << std::endl;


	std::cout << std::endl << m(1) << " is:" << std::endl; 
	std::cout << "-------------" << std::endl; 

	std::cout << deca(m(1)) << std::endl; 
	std::cout << hecto(m(1)) << std::endl;
	std::cout << kilo(m(1)) << std::endl; 
	std::cout << mega(m(1)) << std::endl; 
	std::cout << giga(m(1)) << std::endl; 
	std::cout << tera(m(1)) << std::endl; 
	std::cout << peta(m(1)) << std::endl; 
	std::cout << exa(m(1)) << std::endl; 
	std::cout << zetta(m(1)) << std::endl;
	std::cout << yotta(m(1)) << std::endl;
				 
	std::cout << deci(m(1)) << std::endl; 
	std::cout << centi(m(1)) << std::endl;
	std::cout << milli(m(1)) << std::endl;
	std::cout << micro(m(1)) << std::endl;
	std::cout << nano(m(1)) << std::endl; 
	std::cout << pico(m(1)) << std::endl; 
	std::cout << femto(m(1)) << std::endl;
	std::cout << atto(m(1)) << std::endl; 
	std::cout << zepto(m(1)) << std::endl;
	std::cout << yocto(m(1)) << std::endl;

	return 0;
}
