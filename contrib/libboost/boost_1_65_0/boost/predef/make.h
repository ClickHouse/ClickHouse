/*
Copyright Rene Rivera 2008-2015
Distributed under the Boost Software License, Version 1.0.
(See accompanying file LICENSE_1_0.txt or copy at
http://www.boost.org/LICENSE_1_0.txt)
*/
#include <boost/predef/detail/test.h>

#ifndef BOOST_PREDEF_MAKE_H
#define BOOST_PREDEF_MAKE_H

/*
Shorthands for the common version number formats used by vendors...
*/

/*`
[heading `BOOST_PREDEF_MAKE_..` macros]

These set of macros decompose common vendor version number
macros which are composed version, revision, and patch digits.
The naming convention indicates:

* The base of the specified version number. "`BOOST_PREDEF_MAKE_0X`" for
  hexadecimal digits, and "`BOOST_PREDEF_MAKE_10`" for decimal digits.
* The format of the vendor version number. Where "`V`" indicates the version digits,
  "`R`" indicates the revision digits, "`P`" indicates the patch digits, and "`0`"
  indicates an ignored digit.

Macros are:
*/
/*` `BOOST_PREDEF_MAKE_0X_VRP(V)` */
#define BOOST_PREDEF_MAKE_0X_VRP(V) BOOST_VERSION_NUMBER((V&0xF00)>>8,(V&0xF0)>>4,(V&0xF))
/*` `BOOST_PREDEF_MAKE_0X_VVRP(V)` */
#define BOOST_PREDEF_MAKE_0X_VVRP(V) BOOST_VERSION_NUMBER((V&0xFF00)>>8,(V&0xF0)>>4,(V&0xF))
/*` `BOOST_PREDEF_MAKE_0X_VRPP(V)` */
#define BOOST_PREDEF_MAKE_0X_VRPP(V) BOOST_VERSION_NUMBER((V&0xF000)>>12,(V&0xF00)>>8,(V&0xFF))
/*` `BOOST_PREDEF_MAKE_0X_VVRR(V)` */
#define BOOST_PREDEF_MAKE_0X_VVRR(V) BOOST_VERSION_NUMBER((V&0xFF00)>>8,(V&0xFF),0)
/*` `BOOST_PREDEF_MAKE_0X_VRRPPPP(V)` */
#define BOOST_PREDEF_MAKE_0X_VRRPPPP(V) BOOST_VERSION_NUMBER((V&0xF000000)>>24,(V&0xFF0000)>>16,(V&0xFFFF))
/*` `BOOST_PREDEF_MAKE_0X_VVRRP(V)` */
#define BOOST_PREDEF_MAKE_0X_VVRRP(V) BOOST_VERSION_NUMBER((V&0xFF000)>>12,(V&0xFF0)>>4,(V&0xF))
/*` `BOOST_PREDEF_MAKE_0X_VRRPP000(V)` */
#define BOOST_PREDEF_MAKE_0X_VRRPP000(V) BOOST_VERSION_NUMBER((V&0xF0000000)>>28,(V&0xFF00000)>>20,(V&0xFF000)>>12)
/*` `BOOST_PREDEF_MAKE_0X_VVRRPP(V)` */
#define BOOST_PREDEF_MAKE_0X_VVRRPP(V) BOOST_VERSION_NUMBER((V&0xFF0000)>>16,(V&0xFF00)>>8,(V&0xFF))
/*` `BOOST_PREDEF_MAKE_10_VPPP(V)` */
#define BOOST_PREDEF_MAKE_10_VPPP(V) BOOST_VERSION_NUMBER(((V)/1000)%10,0,(V)%1000)
/*` `BOOST_PREDEF_MAKE_10_VRP(V)` */
#define BOOST_PREDEF_MAKE_10_VRP(V) BOOST_VERSION_NUMBER(((V)/100)%10,((V)/10)%10,(V)%10)
/*` `BOOST_PREDEF_MAKE_10_VRP000(V)` */
#define BOOST_PREDEF_MAKE_10_VRP000(V) BOOST_VERSION_NUMBER(((V)/100000)%10,((V)/10000)%10,((V)/1000)%10)
/*` `BOOST_PREDEF_MAKE_10_VRPP(V)` */
#define BOOST_PREDEF_MAKE_10_VRPP(V) BOOST_VERSION_NUMBER(((V)/1000)%10,((V)/100)%10,(V)%100)
/*` `BOOST_PREDEF_MAKE_10_VRR(V)` */
#define BOOST_PREDEF_MAKE_10_VRR(V) BOOST_VERSION_NUMBER(((V)/100)%10,(V)%100,0)
/*` `BOOST_PREDEF_MAKE_10_VRRPP(V)` */
#define BOOST_PREDEF_MAKE_10_VRRPP(V) BOOST_VERSION_NUMBER(((V)/10000)%10,((V)/100)%100,(V)%100)
/*` `BOOST_PREDEF_MAKE_10_VRR000(V)` */
#define BOOST_PREDEF_MAKE_10_VRR000(V) BOOST_VERSION_NUMBER(((V)/100000)%10,((V)/1000)%100,0)
/*` `BOOST_PREDEF_MAKE_10_VV00(V)` */
#define BOOST_PREDEF_MAKE_10_VV00(V) BOOST_VERSION_NUMBER(((V)/100)%100,0,0)
/*` `BOOST_PREDEF_MAKE_10_VVRR(V)` */
#define BOOST_PREDEF_MAKE_10_VVRR(V) BOOST_VERSION_NUMBER(((V)/100)%100,(V)%100,0)
/*` `BOOST_PREDEF_MAKE_10_VVRRPP(V)` */
#define BOOST_PREDEF_MAKE_10_VVRRPP(V) BOOST_VERSION_NUMBER(((V)/10000)%100,((V)/100)%100,(V)%100)
/*` `BOOST_PREDEF_MAKE_10_VVRR0PP00(V)` */
#define BOOST_PREDEF_MAKE_10_VVRR0PP00(V) BOOST_VERSION_NUMBER(((V)/10000000)%100,((V)/100000)%100,((V)/100)%100)
/*` `BOOST_PREDEF_MAKE_10_VVRR0PPPP(V)` */
#define BOOST_PREDEF_MAKE_10_VVRR0PPPP(V) BOOST_VERSION_NUMBER(((V)/10000000)%100,((V)/100000)%100,(V)%10000)
/*` `BOOST_PREDEF_MAKE_10_VVRR00PP00(V)` */
#define BOOST_PREDEF_MAKE_10_VVRR00PP00(V) BOOST_VERSION_NUMBER(((V)/100000000)%100,((V)/1000000)%100,((V)/100)%100)
/*`
[heading `BOOST_PREDEF_MAKE_*..` date macros]

Date decomposition macros return a date in the relative to the 1970
Epoch date. If the month is not available, January 1st is used as the month and day.
If the day is not available, but the month is, the 1st of the month is used as the day.
*/
/*` `BOOST_PREDEF_MAKE_DATE(Y,M,D)` */
#define BOOST_PREDEF_MAKE_DATE(Y,M,D) BOOST_VERSION_NUMBER((Y)%10000-1970,(M)%100,(D)%100)
/*` `BOOST_PREDEF_MAKE_YYYYMMDD(V)` */
#define BOOST_PREDEF_MAKE_YYYYMMDD(V) BOOST_PREDEF_MAKE_DATE(((V)/10000)%10000,((V)/100)%100,(V)%100)
/*` `BOOST_PREDEF_MAKE_YYYY(V)` */
#define BOOST_PREDEF_MAKE_YYYY(V) BOOST_PREDEF_MAKE_DATE(V,1,1)
/*` `BOOST_PREDEF_MAKE_YYYYMM(V)` */
#define BOOST_PREDEF_MAKE_YYYYMM(V) BOOST_PREDEF_MAKE_DATE((V)/100,(V)%100,1)

#endif
