// Boost.Geometry

// Copyright (c) 2015-2016 Oracle and/or its affiliates.

// Contributed and/or modified by Vissarion Fysikopoulos, on behalf of Oracle

// Use, modification and distribution is subject to the Boost Software License,
// Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_GEOMETRY_FORMULAS_AREA_FORMULAS_HPP
#define BOOST_GEOMETRY_FORMULAS_AREA_FORMULAS_HPP

#include <boost/geometry/formulas/flattening.hpp>
#include <boost/math/special_functions/hypot.hpp>

namespace boost { namespace geometry { namespace formula
{

/*!
\brief Formulas for computing spherical and ellipsoidal polygon area.
 The current class computes the area of the trapezoid defined by a segment
 the two meridians passing by the endpoints and the equator.
\author See
- Danielsen JS, The area under the geodesic. Surv Rev 30(232):
61–66, 1989
- Charles F.F Karney, Algorithms for geodesics, 2011
https://arxiv.org/pdf/1109.4448.pdf
*/

template <
        typename CT,
        std::size_t SeriesOrder = 2,
        bool ExpandEpsN = true
>
class area_formulas
{

public:

    //TODO: move the following to a more general space to be used by other
    //      classes as well
    /*
        Evaluate the polynomial in x using Horner's method.
    */
    template <typename NT, typename IteratorType>
    static inline NT horner_evaluate(NT x,
                                     IteratorType begin,
                                     IteratorType end)
    {
        NT result(0);
        IteratorType it = end;
        do
        {
            result = result * x + *--it;
        }
        while (it != begin);
        return result;
    }

    /*
        Clenshaw algorithm for summing trigonometric series
        https://en.wikipedia.org/wiki/Clenshaw_algorithm
    */
    template <typename NT, typename IteratorType>
    static inline NT clenshaw_sum(NT cosx,
                                  IteratorType begin,
                                  IteratorType end)
    {
        IteratorType it = end;
        bool odd = true;
        CT b_k, b_k1(0), b_k2(0);
        do
        {
            CT c_k = odd ? *--it : NT(0);
            b_k = c_k + NT(2) * cosx * b_k1 - b_k2;
            b_k2 = b_k1;
            b_k1 = b_k;
            odd = !odd;
        }
        while (it != begin);

        return *begin + b_k1 * cosx - b_k2;
    }

    template<typename T>
    static inline void normalize(T& x, T& y)
    {
        T h = boost::math::hypot(x, y);
        x /= h;
        y /= h;
    }

    /*
     Generate and evaluate the series expansion of the following integral

        I4 = -integrate( (t(ep2) - t(k2*sin(sigma1)^2)) / (ep2 - k2*sin(sigma1)^2)
           * sin(sigma1)/2, sigma1, pi/2, sigma )
     where

        t(x) = sqrt(1+1/x)*asinh(sqrt(x)) + x

     valid for ep2 and k2 small.  We substitute k2 = 4 * eps / (1 - eps)^2
     and ep2 = 4 * n / (1 - n)^2 and expand in eps and n.

     The resulting sum of the series is of the form

        sum(C4[l] * cos((2*l+1)*sigma), l, 0, maxpow-1) )

     The above expansion is performed in Computer Algebra System Maxima.
     The C++ code (that yields the function evaluate_coeffs_n below) is generated
     by the following Maxima script and is based on script:
     http://geographiclib.sourceforge.net/html/geod.mac

        // Maxima script begin
        taylordepth:5$
        ataylor(expr,var,ord):=expand(ratdisrep(taylor(expr,var,0,ord)))$
        jtaylor(expr,var1,var2,ord):=block([zz],expand(subst([zz=1],
        ratdisrep(taylor(subst([var1=zz*var1,var2=zz*var2],expr),zz,0,ord)))))$

        compute(maxpow):=block([int,t,intexp,area, x,ep2,k2],
        maxpow:maxpow-1,
        t : sqrt(1+1/x) * asinh(sqrt(x)) + x,
        int:-(tf(ep2) - tf(k2*sin(sigma)^2)) / (ep2 - k2*sin(sigma)^2)
        * sin(sigma)/2,
        int:subst([tf(ep2)=subst([x=ep2],t),
        tf(k2*sin(sigma)^2)=subst([x=k2*sin(sigma)^2],t)],
        int),
        int:subst([abs(sin(sigma))=sin(sigma)],int),
        int:subst([k2=4*eps/(1-eps)^2,ep2=4*n/(1-n)^2],int),
        intexp:jtaylor(int,n,eps,maxpow),
        area:trigreduce(integrate(intexp,sigma)),
        area:expand(area-subst(sigma=%pi/2,area)),
        for i:0 thru maxpow do C4[i]:coeff(area,cos((2*i+1)*sigma)),
        if expand(area-sum(C4[i]*cos((2*i+1)*sigma),i,0,maxpow)) # 0
        then error("left over terms in I4"),
        'done)$

        printcode(maxpow):=
        block([tab2:"    ",tab3:"      "],
        print(" switch (SeriesOrder) {"),
        for nn:1 thru maxpow do block([c],
        print(concat(tab2,"case ",string(nn-1),":")),
        c:0,
        for m:0 thru nn-1 do block(
          [q:jtaylor(subst([n=n],C4[m]),n,eps,nn-1),
          linel:1200],
          for j:m thru nn-1 do (
            print(concat(tab3,"coeffs_n[",c,"] = ",
                string(horner(coeff(q,eps,j))),";")),
            c:c+1)
        ),
        print(concat(tab3,"break;"))),
        print("    }"),
        'done)$

        maxpow:6$
        compute(maxpow)$
        printcode(maxpow)$
        // Maxima script end

     In the resulting code we should replace each number x by CT(x)
     e.g. using the following scirpt:
       sed -e 's/[0-9]\+/CT(&)/g; s/\[CT(/\[/g; s/)\]/\]/g;
               s/case\sCT(/case /g; s/):/:/g'
    */

    static inline void evaluate_coeffs_n(CT n, CT coeffs_n[])
    {

        switch (SeriesOrder) {
        case 0:
            coeffs_n[0] = CT(2)/CT(3);
            break;
        case 1:
            coeffs_n[0] = (CT(10)-CT(4)*n)/CT(15);
            coeffs_n[1] = -CT(1)/CT(5);
            coeffs_n[2] = CT(1)/CT(45);
            break;
        case 2:
            coeffs_n[0] = (n*(CT(8)*n-CT(28))+CT(70))/CT(105);
            coeffs_n[1] = (CT(16)*n-CT(7))/CT(35);
            coeffs_n[2] = -CT(2)/CT(105);
            coeffs_n[3] = (CT(7)-CT(16)*n)/CT(315);
            coeffs_n[4] = -CT(2)/CT(105);
            coeffs_n[5] = CT(4)/CT(525);
            break;
        case 3:
            coeffs_n[0] = (n*(n*(CT(4)*n+CT(24))-CT(84))+CT(210))/CT(315);
            coeffs_n[1] = ((CT(48)-CT(32)*n)*n-CT(21))/CT(105);
            coeffs_n[2] = (-CT(32)*n-CT(6))/CT(315);
            coeffs_n[3] = CT(11)/CT(315);
            coeffs_n[4] = (n*(CT(32)*n-CT(48))+CT(21))/CT(945);
            coeffs_n[5] = (CT(64)*n-CT(18))/CT(945);
            coeffs_n[6] = -CT(1)/CT(105);
            coeffs_n[7] = (CT(12)-CT(32)*n)/CT(1575);
            coeffs_n[8] = -CT(8)/CT(1575);
            coeffs_n[9] = CT(8)/CT(2205);
            break;
        case 4:
            coeffs_n[0] = (n*(n*(n*(CT(16)*n+CT(44))+CT(264))-CT(924))+CT(2310))/CT(3465);
            coeffs_n[1] = (n*(n*(CT(48)*n-CT(352))+CT(528))-CT(231))/CT(1155);
            coeffs_n[2] = (n*(CT(1088)*n-CT(352))-CT(66))/CT(3465);
            coeffs_n[3] = (CT(121)-CT(368)*n)/CT(3465);
            coeffs_n[4] = CT(4)/CT(1155);
            coeffs_n[5] = (n*((CT(352)-CT(48)*n)*n-CT(528))+CT(231))/CT(10395);
            coeffs_n[6] = ((CT(704)-CT(896)*n)*n-CT(198))/CT(10395);
            coeffs_n[7] = (CT(80)*n-CT(99))/CT(10395);
            coeffs_n[8] = CT(4)/CT(1155);
            coeffs_n[9] = (n*(CT(320)*n-CT(352))+CT(132))/CT(17325);
            coeffs_n[10] = (CT(384)*n-CT(88))/CT(17325);
            coeffs_n[11] = -CT(8)/CT(1925);
            coeffs_n[12] = (CT(88)-CT(256)*n)/CT(24255);
            coeffs_n[13] = -CT(16)/CT(8085);
            coeffs_n[14] = CT(64)/CT(31185);
            break;
        case 5:
            coeffs_n[0] = (n*(n*(n*(n*(CT(100)*n+CT(208))+CT(572))+CT(3432))-CT(12012))+CT(30030))
                          /CT(45045);
            coeffs_n[1] = (n*(n*(n*(CT(64)*n+CT(624))-CT(4576))+CT(6864))-CT(3003))/CT(15015);
            coeffs_n[2] = (n*((CT(14144)-CT(10656)*n)*n-CT(4576))-CT(858))/CT(45045);
            coeffs_n[3] = ((-CT(224)*n-CT(4784))*n+CT(1573))/CT(45045);
            coeffs_n[4] = (CT(1088)*n+CT(156))/CT(45045);
            coeffs_n[5] = CT(97)/CT(15015);
            coeffs_n[6] = (n*(n*((-CT(64)*n-CT(624))*n+CT(4576))-CT(6864))+CT(3003))/CT(135135);
            coeffs_n[7] = (n*(n*(CT(5952)*n-CT(11648))+CT(9152))-CT(2574))/CT(135135);
            coeffs_n[8] = (n*(CT(5792)*n+CT(1040))-CT(1287))/CT(135135);
            coeffs_n[9] = (CT(468)-CT(2944)*n)/CT(135135);
            coeffs_n[10] = CT(1)/CT(9009);
            coeffs_n[11] = (n*((CT(4160)-CT(1440)*n)*n-CT(4576))+CT(1716))/CT(225225);
            coeffs_n[12] = ((CT(4992)-CT(8448)*n)*n-CT(1144))/CT(225225);
            coeffs_n[13] = (CT(1856)*n-CT(936))/CT(225225);
            coeffs_n[14] = CT(8)/CT(10725);
            coeffs_n[15] = (n*(CT(3584)*n-CT(3328))+CT(1144))/CT(315315);
            coeffs_n[16] = (CT(1024)*n-CT(208))/CT(105105);
            coeffs_n[17] = -CT(136)/CT(63063);
            coeffs_n[18] = (CT(832)-CT(2560)*n)/CT(405405);
            coeffs_n[19] = -CT(128)/CT(135135);
            coeffs_n[20] = CT(128)/CT(99099);
            break;
        }
    }

    /*
       Expand in k2 and ep2.
    */
    static inline void evaluate_coeffs_ep(CT ep, CT coeffs_n[])
    {
        switch (SeriesOrder) {
        case 0:
            coeffs_n[0] = CT(2)/CT(3);
            break;
        case 1:
            coeffs_n[0] = (CT(10)-ep)/CT(15);
            coeffs_n[1] = -CT(1)/CT(20);
            coeffs_n[2] = CT(1)/CT(180);
            break;
        case 2:
            coeffs_n[0] = (ep*(CT(4)*ep-CT(7))+CT(70))/CT(105);
            coeffs_n[1] = (CT(4)*ep-CT(7))/CT(140);
            coeffs_n[2] = CT(1)/CT(42);
            coeffs_n[3] = (CT(7)-CT(4)*ep)/CT(1260);
            coeffs_n[4] = -CT(1)/CT(252);
            coeffs_n[5] = CT(1)/CT(2100);
            break;
        case 3:
            coeffs_n[0] = (ep*((CT(12)-CT(8)*ep)*ep-CT(21))+CT(210))/CT(315);
            coeffs_n[1] = ((CT(12)-CT(8)*ep)*ep-CT(21))/CT(420);
            coeffs_n[2] = (CT(3)-CT(2)*ep)/CT(126);
            coeffs_n[3] = -CT(1)/CT(72);
            coeffs_n[4] = (ep*(CT(8)*ep-CT(12))+CT(21))/CT(3780);
            coeffs_n[5] = (CT(2)*ep-CT(3))/CT(756);
            coeffs_n[6] = CT(1)/CT(360);
            coeffs_n[7] = (CT(3)-CT(2)*ep)/CT(6300);
            coeffs_n[8] = -CT(1)/CT(1800);
            coeffs_n[9] = CT(1)/CT(17640);
            break;
        case 4:
            coeffs_n[0] = (ep*(ep*(ep*(CT(64)*ep-CT(88))+CT(132))-CT(231))+CT(2310))/CT(3465);
            coeffs_n[1] = (ep*(ep*(CT(64)*ep-CT(88))+CT(132))-CT(231))/CT(4620);
            coeffs_n[2] = (ep*(CT(16)*ep-CT(22))+CT(33))/CT(1386);
            coeffs_n[3] = (CT(8)*ep-CT(11))/CT(792);
            coeffs_n[4] = CT(1)/CT(110);
            coeffs_n[5] = (ep*((CT(88)-CT(64)*ep)*ep-CT(132))+CT(231))/CT(41580);
            coeffs_n[6] = ((CT(22)-CT(16)*ep)*ep-CT(33))/CT(8316);
            coeffs_n[7] = (CT(11)-CT(8)*ep)/CT(3960);
            coeffs_n[8] = -CT(1)/CT(495);
            coeffs_n[9] = (ep*(CT(16)*ep-CT(22))+CT(33))/CT(69300);
            coeffs_n[10] = (CT(8)*ep-CT(11))/CT(19800);
            coeffs_n[11] = CT(1)/CT(1925);
            coeffs_n[12] = (CT(11)-CT(8)*ep)/CT(194040);
            coeffs_n[13] = -CT(1)/CT(10780);
            coeffs_n[14] = CT(1)/CT(124740);
            break;
        case 5:
            coeffs_n[0] = (ep*(ep*(ep*((CT(832)-CT(640)*ep)*ep-CT(1144))+CT(1716))-CT(3003))+CT(30030))/CT(45045);
            coeffs_n[1] = (ep*(ep*((CT(832)-CT(640)*ep)*ep-CT(1144))+CT(1716))-CT(3003))/CT(60060);
            coeffs_n[2] = (ep*((CT(208)-CT(160)*ep)*ep-CT(286))+CT(429))/CT(18018);
            coeffs_n[3] = ((CT(104)-CT(80)*ep)*ep-CT(143))/CT(10296);
            coeffs_n[4] = (CT(13)-CT(10)*ep)/CT(1430);
            coeffs_n[5] = -CT(1)/CT(156);
            coeffs_n[6] = (ep*(ep*(ep*(CT(640)*ep-CT(832))+CT(1144))-CT(1716))+CT(3003))/CT(540540);
            coeffs_n[7] = (ep*(ep*(CT(160)*ep-CT(208))+CT(286))-CT(429))/CT(108108);
            coeffs_n[8] = (ep*(CT(80)*ep-CT(104))+CT(143))/CT(51480);
            coeffs_n[9] = (CT(10)*ep-CT(13))/CT(6435);
            coeffs_n[10] = CT(5)/CT(3276);
            coeffs_n[11] = (ep*((CT(208)-CT(160)*ep)*ep-CT(286))+CT(429))/CT(900900);
            coeffs_n[12] = ((CT(104)-CT(80)*ep)*ep-CT(143))/CT(257400);
            coeffs_n[13] = (CT(13)-CT(10)*ep)/CT(25025);
            coeffs_n[14] = -CT(1)/CT(2184);
            coeffs_n[15] = (ep*(CT(80)*ep-CT(104))+CT(143))/CT(2522520);
            coeffs_n[16] = (CT(10)*ep-CT(13))/CT(140140);
            coeffs_n[17] = CT(5)/CT(45864);
            coeffs_n[18] = (CT(13)-CT(10)*ep)/CT(1621620);
            coeffs_n[19] = -CT(1)/CT(58968);
            coeffs_n[20] = CT(1)/CT(792792);
            break;
        }
    }

    /*
        Given the set of coefficients coeffs1[] evaluate on var2 and return
        the set of coefficients coeffs2[]
    */
    static inline void evaluate_coeffs_var2(CT var2,
                                            CT coeffs1[],
                                            CT coeffs2[]){
        std::size_t begin(0), end(0);
        for(std::size_t i = 0; i <= SeriesOrder; i++){
            end = begin + SeriesOrder + 1 - i;
            coeffs2[i] = ((i==0) ? CT(1) : pow(var2,int(i)))
                        * horner_evaluate(var2, coeffs1 + begin, coeffs1 + end);
            begin = end;
        }
    }

    /*
        Compute the spherical excess of a geodesic (or shperical) segment
    */
    template <
                bool LongSegment,
                typename PointOfSegment
             >
    static inline CT spherical(PointOfSegment const& p1,
                               PointOfSegment const& p2)
    {
        CT excess;

        if(LongSegment) // not for segments parallel to equator
        {
            CT cbet1 = cos(geometry::get_as_radian<1>(p1));
            CT sbet1 = sin(geometry::get_as_radian<1>(p1));
            CT cbet2 = cos(geometry::get_as_radian<1>(p2));
            CT sbet2 = sin(geometry::get_as_radian<1>(p2));

            CT omg12 = geometry::get_as_radian<0>(p1)
                     - geometry::get_as_radian<0>(p2);
            CT comg12 = cos(omg12);
            CT somg12 = sin(omg12);

            CT alp1 = atan2(cbet1 * sbet2
                            - sbet1 * cbet2 * comg12,
                            cbet2 * somg12);

            CT alp2 = atan2(cbet1 * sbet2 * comg12
                            - sbet1 * cbet2,
                            cbet1 * somg12);

            excess = alp2 - alp1;

        } else {

            // Trapezoidal formula

            CT tan_lat1 =
                    tan(geometry::get_as_radian<1>(p1) / 2.0);
            CT tan_lat2 =
                    tan(geometry::get_as_radian<1>(p2) / 2.0);

            excess = CT(2.0)
                    * atan(((tan_lat1 + tan_lat2) / (CT(1) + tan_lat1 * tan_lat2))
                           * tan((geometry::get_as_radian<0>(p2)
                                - geometry::get_as_radian<0>(p1)) / 2));
        }

        return excess;
    }

    struct return_type_ellipsoidal
    {
        return_type_ellipsoidal()
            :   spherical_term(0),
                ellipsoidal_term(0)
        {}

        CT spherical_term;
        CT ellipsoidal_term;
    };

    /*
        Compute the ellipsoidal correction of a geodesic (or shperical) segment
    */
    template <
                template <typename, bool, bool, bool, bool, bool> class Inverse,
                //typename AzimuthStrategy,
                typename PointOfSegment,
                typename SpheroidConst
             >
    static inline return_type_ellipsoidal ellipsoidal(PointOfSegment const& p1,
                                                      PointOfSegment const& p2,
                                                      SpheroidConst spheroid_const)
    {
        return_type_ellipsoidal result;

        // Azimuth Approximation

        typedef Inverse<CT, false, true, true, false, false> inverse_type;
        typedef typename inverse_type::result_type inverse_result;

        inverse_result i_res = inverse_type::apply(get_as_radian<0>(p1),
                                                   get_as_radian<1>(p1),
                                                   get_as_radian<0>(p2),
                                                   get_as_radian<1>(p2),
                                                   spheroid_const.m_spheroid);

        CT alp1 = i_res.azimuth;
        CT alp2 = i_res.reverse_azimuth;

        // Constants

        CT const ep = spheroid_const.m_ep;
        CT const f = formula::flattening<CT>(spheroid_const.m_spheroid);
        CT const one_minus_f = CT(1) - f;
        std::size_t const series_order_plus_one = SeriesOrder + 1;
        std::size_t const series_order_plus_two = SeriesOrder + 2;

        // Basic trigonometric computations

        CT tan_bet1 = tan(get_as_radian<1>(p1)) * one_minus_f;
        CT tan_bet2 = tan(get_as_radian<1>(p2)) * one_minus_f;
        CT cos_bet1 = cos(atan(tan_bet1));
        CT cos_bet2 = cos(atan(tan_bet2));
        CT sin_bet1 = tan_bet1 * cos_bet1;
        CT sin_bet2 = tan_bet2 * cos_bet2;
        CT sin_alp1 = sin(alp1);
        CT cos_alp1 = cos(alp1);
        CT cos_alp2 = cos(alp2);
        CT sin_alp0 = sin_alp1 * cos_bet1;

        // Spherical term computation

        CT sin_omg1 = sin_alp0 * sin_bet1;
        CT cos_omg1 = cos_alp1 * cos_bet1;
        CT sin_omg2 = sin_alp0 * sin_bet2;
        CT cos_omg2 = cos_alp2 * cos_bet2;
        CT cos_omg12 =  cos_omg1 * cos_omg2 + sin_omg1 * sin_omg2;
        CT excess;

        bool meridian = get<0>(p2) - get<0>(p1) == CT(0)
              || get<1>(p1) == CT(90) || get<1>(p1) == -CT(90)
              || get<1>(p2) == CT(90) || get<1>(p2) == -CT(90);

        if (!meridian && cos_omg12 > -CT(0.7)
                      && sin_bet2 - sin_bet1 < CT(1.75)) // short segment
        {
            CT sin_omg12 =  cos_omg1 * sin_omg2 - sin_omg1 * cos_omg2;
            normalize(sin_omg12, cos_omg12);

            CT cos_omg12p1 = CT(1) + cos_omg12;
            CT cos_bet1p1 = CT(1) + cos_bet1;
            CT cos_bet2p1 = CT(1) + cos_bet2;
            excess = CT(2) * atan2(sin_omg12 * (sin_bet1 * cos_bet2p1 + sin_bet2 * cos_bet1p1),
                                cos_omg12p1 * (sin_bet1 * sin_bet2 + cos_bet1p1 * cos_bet2p1));
        }
        else
        {
            /*
                    CT sin_alp2 = sin(alp2);
                    CT sin_alp12 = sin_alp2 * cos_alp1 - cos_alp2 * sin_alp1;
                    CT cos_alp12 = cos_alp2 * cos_alp1 + sin_alp2 * sin_alp1;
                    excess = atan2(sin_alp12, cos_alp12);
            */
                    excess = alp2 - alp1;
        }

        result.spherical_term = excess;

        // Ellipsoidal term computation (uses integral approximation)

        CT cos_alp0 = math::sqrt(CT(1) - math::sqr(sin_alp0));
        CT cos_sig1 = cos_alp1 * cos_bet1;
        CT cos_sig2 = cos_alp2 * cos_bet2;
        CT sin_sig1 = sin_bet1;
        CT sin_sig2 = sin_bet2;

        normalize(sin_sig1, cos_sig1);
        normalize(sin_sig2, cos_sig2);

        CT coeffs[SeriesOrder + 1];
        const std::size_t coeffs_var_size = (series_order_plus_two
                                            * series_order_plus_one) / 2;
        CT coeffs_var[coeffs_var_size];

        if(ExpandEpsN){ // expand by eps and n

            CT k2 = math::sqr(ep * cos_alp0);
            CT sqrt_k2_plus_one = math::sqrt(CT(1) + k2);
            CT eps = (sqrt_k2_plus_one - CT(1)) / (sqrt_k2_plus_one + CT(1));
            CT n = f / (CT(2) - f);

            // Generate and evaluate the polynomials on n
            // to get the series coefficients (that depend on eps)
            evaluate_coeffs_n(n, coeffs_var);

            // Generate and evaluate the polynomials on eps (i.e. var2 = eps)
            // to get the final series coefficients
            evaluate_coeffs_var2(eps, coeffs_var, coeffs);

        }else{ // expand by k2 and ep

            CT k2 = math::sqr(ep * cos_alp0);
            CT ep2 = math::sqr(ep);

            // Generate and evaluate the polynomials on ep2
            evaluate_coeffs_ep(ep2, coeffs_var);

            // Generate and evaluate the polynomials on k2 (i.e. var2 = k2)
            evaluate_coeffs_var2(k2, coeffs_var, coeffs);

        }

        // Evaluate the trigonometric sum
        CT I12 = clenshaw_sum(cos_sig2, coeffs, coeffs + series_order_plus_one)
               - clenshaw_sum(cos_sig1, coeffs, coeffs + series_order_plus_one);

        // The part of the ellipsodal correction that depends on
        // point coordinates
        result.ellipsoidal_term = cos_alp0 * sin_alp0 * I12;

        return result;
    }

    // Keep track whenever a segment crosses the prime meridian
    // First normalize to [0,360)
    template <typename PointOfSegment, typename StateType>
    static inline int crosses_prime_meridian(PointOfSegment const& p1,
                                             PointOfSegment const& p2,
                                             StateType& state)
    {
        CT const pi
            = geometry::math::pi<CT>();
        CT const two_pi
            = geometry::math::two_pi<CT>();

        CT p1_lon = get_as_radian<0>(p1)
                                - ( floor( get_as_radian<0>(p1) / two_pi )
                                  * two_pi );
        CT p2_lon = get_as_radian<0>(p2)
                                - ( floor( get_as_radian<0>(p2) / two_pi )
                                  * two_pi );

        CT max_lon = (std::max)(p1_lon, p2_lon);
        CT min_lon = (std::min)(p1_lon, p2_lon);

        if(max_lon > pi && min_lon < pi && max_lon - min_lon > pi)
        {
            state.m_crosses_prime_meridian++;
        }

        return state.m_crosses_prime_meridian;
    }

};

}}} // namespace boost::geometry::formula


#endif // BOOST_GEOMETRY_FORMULAS_AREA_FORMULAS_HPP
