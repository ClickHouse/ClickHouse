//Copyright (c) 2011-2012 Mail.RU
//Copyright (c) 2011-2012 Maksim Kalinchenko
//Copyright (c) 2012 Sokolov Yura aka funny-falcon
//
//MIT License
//
//Permission is hereby granted, free of charge, to any person obtaining
//a copy of this software and associated documentation files (the
//"Software"), to deal in the Software without restriction, including
//without limitation the rights to use, copy, modify, merge, publish,
//distribute, sublicense, and/or sell copies of the Software, and to
//permit persons to whom the Software is furnished to do so, subject to
//the following conditions:
//
//The above copyright notice and this permission notice shall be
//included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
//EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
//MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
//NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
//LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
//OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
//WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#include <stdexcept>


#define L 0xFFFFFFFF

static unsigned int L27_38[] = {L / 27, L / 28, L / 29, L / 30, L / 31, L / 32,
                               L / 33, L / 34, L / 35, L / 36, L / 37, L / 38,
                               L / 39, L / 40, L / 41, L / 42, L / 43, L / 44,
                               L / 45, L / 46, L / 47, L / 48, L / 49, L / 50,
                               L / 51, L / 52, L / 53, L / 54, L / 55, L / 56,
                               L / 57, L / 58, L / 59, L / 60, L / 61, L / 62
                               };
static unsigned int LL27_38[] = {L/(26*27), L/(27*28), L/(28*29), L/(29*30), L/(30*31), L/(31*32),
                                 L/(32*33), L/(33*34), L/(34*35), L/(35*36), L/(36*37), L/(37*38),
                                 L/(38*39), L/(39*40), L/(40*41), L/(41*42), L/(42*43), L/(43*44),
                                 L/(44*45), L/(45*46), L/(46*47), L/(47*48), L/(48*49), L/(49*50),
                                 L/(50*51), L/(51*52), L/(52*53), L/(53*54), L/(54*55), L/(55*56),
                                 L/(56*57), L/(57*58), L/(58*59), L/(59*60), L/(60*61), L/(61*62)
                               };

unsigned int sumburConsistentHash(unsigned int hashed_int, unsigned int capacity)
{
    unsigned int h = hashed_int;
    unsigned int capa = capacity;
    unsigned int part, n, i, c;

    if (capa == 0)
        throw std::runtime_error("Sumbur is not applicable to empty cluster");

    part = L / capa;

    if (L - h < part) return 0;

    n = 1;

    do {
        if (h >= L / 2) h -= L / 2;
        else {
            n = 2;
            if (L / 2 - h < part) return 1;
        }
        if (capa == 2) return 1;

#define curslice(i) (L / (i * (i - 1)))
#define unroll(i) \
        if (curslice(i) <= h) h -= curslice(i); \
        else {                                  \
            h += curslice(i) * (i - n - 1);     \
            n = i;                              \
            if (L / i - h < part) return n-1;   \
        }                                       \
        if (capa == i) return (n-1)

        unroll(3); unroll(4); unroll(5);
        unroll(6); unroll(7); unroll(8);
        unroll(9); unroll(10); unroll(11);
        unroll(12); unroll(13); unroll(14);
        unroll(15); unroll(16); unroll(17);
        unroll(18); unroll(19); unroll(20);
        unroll(21); unroll(22); unroll(23);
        unroll(24); unroll(25); unroll(26);

        for (i = 27; i <= capa && i <= 62; i++) {
            c = LL27_38[i-27];
            if (c <= h) {
                h -= c;
            }
            else {
                h += c * (i - n - 1);
                n = i;
                if (L27_38[i-27] - h < part) return n-1;
            }
        }

        for(i = 63; i <= capa; i++) {
            c = L / (i * (i - 1));
            if (c <= h) {
                h -= c;
            }
            else {
                h += c * (i - n - 1);
                n = i;
                if (L / i - h < part) return n - 1;
            }
        }
    } while(0);
    return n - 1;
}
