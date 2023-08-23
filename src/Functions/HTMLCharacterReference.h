/* C++ code produced by gperf version 3.1 */
/* Command-line: gperf -t --output-file=HTMLCharacterReference.h HTMLCharacterReference.gperf  */
/* Computed positions: -k'2-9,13,15' */

#if !((' ' == 32) && ('!' == 33) && ('"' == 34) && ('#' == 35) \
      && ('%' == 37) && ('&' == 38) && ('\'' == 39) && ('(' == 40) \
      && (')' == 41) && ('*' == 42) && ('+' == 43) && (',' == 44) \
      && ('-' == 45) && ('.' == 46) && ('/' == 47) && ('0' == 48) \
      && ('1' == 49) && ('2' == 50) && ('3' == 51) && ('4' == 52) \
      && ('5' == 53) && ('6' == 54) && ('7' == 55) && ('8' == 56) \
      && ('9' == 57) && (':' == 58) && (';' == 59) && ('<' == 60) \
      && ('=' == 61) && ('>' == 62) && ('?' == 63) && ('A' == 65) \
      && ('B' == 66) && ('C' == 67) && ('D' == 68) && ('E' == 69) \
      && ('F' == 70) && ('G' == 71) && ('H' == 72) && ('I' == 73) \
      && ('J' == 74) && ('K' == 75) && ('L' == 76) && ('M' == 77) \
      && ('N' == 78) && ('O' == 79) && ('P' == 80) && ('Q' == 81) \
      && ('R' == 82) && ('S' == 83) && ('T' == 84) && ('U' == 85) \
      && ('V' == 86) && ('W' == 87) && ('X' == 88) && ('Y' == 89) \
      && ('Z' == 90) && ('[' == 91) && ('\\' == 92) && (']' == 93) \
      && ('^' == 94) && ('_' == 95) && ('a' == 97) && ('b' == 98) \
      && ('c' == 99) && ('d' == 100) && ('e' == 101) && ('f' == 102) \
      && ('g' == 103) && ('h' == 104) && ('i' == 105) && ('j' == 106) \
      && ('k' == 107) && ('l' == 108) && ('m' == 109) && ('n' == 110) \
      && ('o' == 111) && ('p' == 112) && ('q' == 113) && ('r' == 114) \
      && ('s' == 115) && ('t' == 116) && ('u' == 117) && ('v' == 118) \
      && ('w' == 119) && ('x' == 120) && ('y' == 121) && ('z' == 122) \
      && ('{' == 123) && ('|' == 124) && ('}' == 125) && ('~' == 126))
/* The character set is not based on ISO-646.  */
#error "gperf generated tables don't work with this execution character set. Please report a bug to <bug-gperf@gnu.org>."
#endif

#line 7 "HTMLCharacterReference.gperf"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wimplicit-fallthrough"
#pragma GCC diagnostic ignored "-Wzero-as-null-pointer-constant"
#pragma GCC diagnostic ignored "-Wunused-macros"
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#pragma GCC diagnostic ignored "-Wshorten-64-to-32"
#line 15 "HTMLCharacterReference.gperf"
struct NameAndGlyph {
const char *name;
const char *glyph;
};
#include <string.h>

#define TOTAL_KEYWORDS 2125
#define MIN_WORD_LENGTH 4
#define MAX_WORD_LENGTH 33
#define MIN_HASH_VALUE 4
#define MAX_HASH_VALUE 17865
/* maximum key range = 17862, duplicates = 0 */

class HTMLCharacterHash
{
private:
  static inline unsigned int hash (const char *str, size_t len);
public:
  static const struct NameAndGlyph *Lookup (const char *str, size_t len);
};

inline unsigned int
HTMLCharacterHash::hash (const char *str, size_t len)
{
  static const unsigned short asso_values[] =
    {
      17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866,
      17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866,
      17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866,
      17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866,
      17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866,
      17866, 17866,    20,    45,     0,    15,    55, 17866,    80,   180,
          0,     0,     0, 17866,    45,   240,  2335,     0,   310,  1635,
        390,   135,   465,  1100,  1480,    30,   900,  1185,   455,   650,
        970,    75,  1820,   305,   240,   460,   555,    25,  2185,   155,
        390,   385,   280,   530,   115,   210,   250,  3440,   130,  1370,
        770,   565,   160,   595,   260,  1695,   995,  5681,  1250,  3865,
         30,    45,    80,    15,    10,    40,     0,    60,   200,   135,
       2065,     5,  2765,   340,  3506,  3449,   355,  3895,  1620,  3195,
       2754,  1270,   295,  1075,   180,    55, 17866, 17866, 17866, 17866,
      17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866,
      17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866,
      17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866,
      17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866,
      17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866,
      17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866,
      17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866,
      17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866,
      17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866,
      17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866,
      17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866,
      17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866,
      17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866, 17866
    };
  unsigned int hval = len;

  switch (hval)
    {
      default:
        hval += asso_values[static_cast<unsigned char>(str[14])];
      /*FALLTHROUGH*/
      case 14:
      case 13:
        hval += asso_values[static_cast<unsigned char>(str[12])];
      /*FALLTHROUGH*/
      case 12:
      case 11:
      case 10:
      case 9:
        hval += asso_values[static_cast<unsigned char>(str[8])];
      /*FALLTHROUGH*/
      case 8:
        hval += asso_values[static_cast<unsigned char>(str[7]+1)];
      /*FALLTHROUGH*/
      case 7:
        hval += asso_values[static_cast<unsigned char>(str[6]+2)];
      /*FALLTHROUGH*/
      case 6:
        hval += asso_values[static_cast<unsigned char>(str[5]+3)];
      /*FALLTHROUGH*/
      case 5:
        hval += asso_values[static_cast<unsigned char>(str[4]+5)];
      /*FALLTHROUGH*/
      case 4:
        hval += asso_values[static_cast<unsigned char>(str[3]+1)];
      /*FALLTHROUGH*/
      case 3:
        hval += asso_values[static_cast<unsigned char>(str[2])];
      /*FALLTHROUGH*/
      case 2:
        hval += asso_values[static_cast<unsigned char>(str[1]+13)];
        break;
    }
  return hval;
}

const struct NameAndGlyph *
HTMLCharacterHash::Lookup (const char *str, size_t len)
{
  static const struct NameAndGlyph wordlist[] =
    {
      {""}, {""}, {""}, {""},
#line 668 "HTMLCharacterReference.gperf"
      {"&gt;", ">"},
      {""}, {""}, {""}, {""},
#line 1026 "HTMLCharacterReference.gperf"
      {"&lt;", "<"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1034 "HTMLCharacterReference.gperf"
      {"&ltri;", "◃"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 464 "HTMLCharacterReference.gperf"
      {"&dtri;", "▿"},
      {""}, {""}, {""},
#line 1009 "HTMLCharacterReference.gperf"
      {"&lrm;", "‎"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 514 "HTMLCharacterReference.gperf"
      {"&ensp;", " "},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 76 "HTMLCharacterReference.gperf"
      {"&ap;", "≈"},
      {""},
#line 552 "HTMLCharacterReference.gperf"
      {"&euro;", "€"},
      {""}, {""}, {""}, {""},
#line 551 "HTMLCharacterReference.gperf"
      {"&euml;", "ë"},
      {""}, {""}, {""}, {""},
#line 522 "HTMLCharacterReference.gperf"
      {"&epsi;", "ε"},
      {""}, {""}, {""}, {""}, {""},
#line 525 "HTMLCharacterReference.gperf"
      {"&epsiv;", "ϵ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 96 "HTMLCharacterReference.gperf"
      {"&auml;", "ä"},
#line 528 "HTMLCharacterReference.gperf"
      {"&eqsim;", "≂"},
      {""}, {""}, {""}, {""},
#line 650 "HTMLCharacterReference.gperf"
      {"&gnsim;", "⋧"},
      {""}, {""}, {""}, {""},
#line 975 "HTMLCharacterReference.gperf"
      {"&lnsim;", "⋦"},
      {""}, {""}, {""},
#line 460 "HTMLCharacterReference.gperf"
      {"&dsol;", "⧶"},
      {""}, {""},
#line 1025 "HTMLCharacterReference.gperf"
      {"&Lt;", "≪"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 157 "HTMLCharacterReference.gperf"
      {"&bnot;", "⌐"},
      {""}, {""}, {""},
#line 412 "HTMLCharacterReference.gperf"
      {"&dot;", "˙"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 90 "HTMLCharacterReference.gperf"
      {"&ast;", "*"},
      {""}, {""}, {""}, {""}, {""},
#line 216 "HTMLCharacterReference.gperf"
      {"&bsol;", "\\"},
      {""}, {""}, {""},
#line 547 "HTMLCharacterReference.gperf"
      {"&eta;", "η"},
      {""}, {""}, {""},
#line 34 "HTMLCharacterReference.gperf"
      {"&af;", "⁡"},
      {""}, {""}, {""}, {""}, {""},
#line 160 "HTMLCharacterReference.gperf"
      {"&bot;", "⊥"},
#line 221 "HTMLCharacterReference.gperf"
      {"&bump;", "≎"},
      {""}, {""},
#line 791 "HTMLCharacterReference.gperf"
      {"&it;", "⁢"},
#line 332 "HTMLCharacterReference.gperf"
      {"&cup;", "∪"},
      {""}, {""}, {""}, {""}, {""},
#line 580 "HTMLCharacterReference.gperf"
      {"&fork;", "⋔"},
      {""}, {""}, {""}, {""}, {""},
#line 581 "HTMLCharacterReference.gperf"
      {"&forkv;", "⫙"},
#line 161 "HTMLCharacterReference.gperf"
      {"&bottom;", "⊥"},
      {""}, {""},
#line 297 "HTMLCharacterReference.gperf"
      {"&comp;", "∁"},
#line 338 "HTMLCharacterReference.gperf"
      {"&cupor;", "⩅"},
      {""}, {""}, {""},
#line 644 "HTMLCharacterReference.gperf"
      {"&gnap;", "⪊"},
      {""}, {""}, {""},
#line 411 "HTMLCharacterReference.gperf"
      {"&Dot;", "¨"},
#line 969 "HTMLCharacterReference.gperf"
      {"&lnap;", "⪉"},
      {""}, {""},
#line 757 "HTMLCharacterReference.gperf"
      {"&in;", "∈"},
      {""}, {""}, {""}, {""}, {""},
#line 647 "HTMLCharacterReference.gperf"
      {"&gne;", "⪈"},
#line 1995 "HTMLCharacterReference.gperf"
      {"&Uuml;", "Ü"},
      {""}, {""},
#line 667 "HTMLCharacterReference.gperf"
      {"&Gt;", "≫"},
#line 972 "HTMLCharacterReference.gperf"
      {"&lne;", "⪇"},
#line 1973 "HTMLCharacterReference.gperf"
      {"&Upsi;", "ϒ"},
      {""}, {""}, {""},
#line 633 "HTMLCharacterReference.gperf"
      {"&gfr;", "𝔤"},
      {""}, {""}, {""}, {""},
#line 948 "HTMLCharacterReference.gperf"
      {"&lfr;", "𝔩"},
      {""}, {""}, {""}, {""},
#line 490 "HTMLCharacterReference.gperf"
      {"&efr;", "𝔢"},
      {""}, {""}, {""}, {""},
#line 382 "HTMLCharacterReference.gperf"
      {"&dfr;", "𝔡"},
      {""},
#line 536 "HTMLCharacterReference.gperf"
      {"&equiv;", "≡"},
#line 675 "HTMLCharacterReference.gperf"
      {"&gtrarr;", "⥸"},
      {""}, {""},
#line 2120 "HTMLCharacterReference.gperf"
      {"&Yuml;", "Ÿ"},
      {""}, {""}, {""},
#line 802 "HTMLCharacterReference.gperf"
      {"&Jfr;", "𝔍"},
      {""}, {""}, {""}, {""},
#line 36 "HTMLCharacterReference.gperf"
      {"&afr;", "𝔞"},
      {""}, {""}, {""}, {""},
#line 154 "HTMLCharacterReference.gperf"
      {"&bne;", "=⃥"},
#line 2144 "HTMLCharacterReference.gperf"
      {"&zwnj;", "‌"},
      {""}, {""}, {""},
#line 567 "HTMLCharacterReference.gperf"
      {"&ffr;", "𝔣"},
#line 2121 "HTMLCharacterReference.gperf"
      {"&yuml;", "ÿ"},
#line 991 "HTMLCharacterReference.gperf"
      {"&lopar;", "⦅"},
      {""},
#line 1370 "HTMLCharacterReference.gperf"
      {"&Or;", "⩔"},
#line 128 "HTMLCharacterReference.gperf"
      {"&bfr;", "𝔟"},
      {""}, {""}, {""}, {""}, {""},
#line 110 "HTMLCharacterReference.gperf"
      {"&bbrk;", "⎵"},
      {""}, {""}, {""},
#line 2135 "HTMLCharacterReference.gperf"
      {"&zfr;", "𝔷"},
      {""}, {""}, {""},
#line 2069 "HTMLCharacterReference.gperf"
      {"&wr;", "≀"},
#line 702 "HTMLCharacterReference.gperf"
      {"&hfr;", "𝔥"},
#line 797 "HTMLCharacterReference.gperf"
      {"&iuml;", "ï"},
      {""}, {""}, {""}, {""},
#line 1734 "HTMLCharacterReference.gperf"
      {"&Sqrt;", "√"},
      {""}, {""}, {""},
#line 79 "HTMLCharacterReference.gperf"
      {"&ape;", "≊"},
      {""}, {""}, {""}, {""},
#line 381 "HTMLCharacterReference.gperf"
      {"&Dfr;", "𝔇"},
      {""}, {""}, {""}, {""},
#line 259 "HTMLCharacterReference.gperf"
      {"&cfr;", "𝔠"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 516 "HTMLCharacterReference.gperf"
      {"&eogon;", "ę"},
      {""}, {""},
#line 763 "HTMLCharacterReference.gperf"
      {"&int;", "∫"},
      {""},
#line 1035 "HTMLCharacterReference.gperf"
      {"&ltrie;", "⊴"},
      {""}, {""}, {""},
#line 674 "HTMLCharacterReference.gperf"
      {"&gtrapprox;", "⪆"},
#line 1005 "HTMLCharacterReference.gperf"
      {"&lrarr;", "⇆"},
      {""}, {""},
#line 513 "HTMLCharacterReference.gperf"
      {"&eng;", "ŋ"},
      {""},
#line 539 "HTMLCharacterReference.gperf"
      {"&erarr;", "⥱"},
      {""}, {""}, {""},
#line 1003 "HTMLCharacterReference.gperf"
      {"&lpar;", "("},
#line 73 "HTMLCharacterReference.gperf"
      {"&aogon;", "ą"},
      {""}, {""}, {""},
#line 519 "HTMLCharacterReference.gperf"
      {"&epar;", "⋕"},
      {""}, {""}, {""},
#line 1485 "HTMLCharacterReference.gperf"
      {"&Qfr;", "𝔔"},
      {""}, {""},
#line 579 "HTMLCharacterReference.gperf"
      {"&forall;", "∀"},
      {""},
#line 54 "HTMLCharacterReference.gperf"
      {"&ang;", "∠"},
      {""}, {""}, {""}, {""},
#line 1803 "HTMLCharacterReference.gperf"
      {"&Sup;", "⋑"},
      {""}, {""}, {""},
#line 2068 "HTMLCharacterReference.gperf"
      {"&wp;", "℘"},
#line 1934 "HTMLCharacterReference.gperf"
      {"&Ufr;", "𝔘"},
      {""},
#line 977 "HTMLCharacterReference.gperf"
      {"&loarr;", "⇽"},
      {""}, {""},
#line 803 "HTMLCharacterReference.gperf"
      {"&jfr;", "𝔧"},
#line 322 "HTMLCharacterReference.gperf"
      {"&csup;", "⫐"},
      {""}, {""},
#line 369 "HTMLCharacterReference.gperf"
      {"&DD;", "ⅅ"},
#line 1800 "HTMLCharacterReference.gperf"
      {"&Sum;", "∑"},
#line 1393 "HTMLCharacterReference.gperf"
      {"&Ouml;", "Ö"},
      {""}, {""}, {""}, {""},
#line 648 "HTMLCharacterReference.gperf"
      {"&gneq;", "⪈"},
#line 649 "HTMLCharacterReference.gperf"
      {"&gneqq;", "≩"},
      {""},
#line 1024 "HTMLCharacterReference.gperf"
      {"&LT;", "<"},
      {""},
#line 973 "HTMLCharacterReference.gperf"
      {"&lneq;", "⪇"},
#line 974 "HTMLCharacterReference.gperf"
      {"&lneqq;", "≨"},
      {""},
#line 1299 "HTMLCharacterReference.gperf"
      {"&nu;", "ν"},
#line 947 "HTMLCharacterReference.gperf"
      {"&Lfr;", "𝔏"},
      {""},
#line 466 "HTMLCharacterReference.gperf"
      {"&duarr;", "⇵"},
#line 340 "HTMLCharacterReference.gperf"
      {"&curarr;", "↷"},
      {""},
#line 2110 "HTMLCharacterReference.gperf"
      {"&Yfr;", "𝔜"},
#line 485 "HTMLCharacterReference.gperf"
      {"&eDot;", "≑"},
      {""}, {""},
#line 524 "HTMLCharacterReference.gperf"
      {"&epsilon;", "ε"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 315 "HTMLCharacterReference.gperf"
      {"&crarr;", "↵"},
#line 305 "HTMLCharacterReference.gperf"
      {"&conint;", "∮"},
      {""}, {""},
#line 846 "HTMLCharacterReference.gperf"
      {"&lArr;", "⇐"},
#line 327 "HTMLCharacterReference.gperf"
      {"&cuepr;", "⋞"},
      {""}, {""},
#line 2111 "HTMLCharacterReference.gperf"
      {"&yfr;", "𝔶"},
      {""}, {""}, {""}, {""}, {""},
#line 358 "HTMLCharacterReference.gperf"
      {"&dArr;", "⇓"},
#line 706 "HTMLCharacterReference.gperf"
      {"&hoarr;", "⇿"},
      {""}, {""},
#line 1000 "HTMLCharacterReference.gperf"
      {"&loz;", "◊"},
#line 194 "HTMLCharacterReference.gperf"
      {"&boxv;", "│"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 592 "HTMLCharacterReference.gperf"
      {"&frac34;", "¾"},
#line 1298 "HTMLCharacterReference.gperf"
      {"&Nu;", "Ν"},
#line 736 "HTMLCharacterReference.gperf"
      {"&ifr;", "𝔦"},
      {""}, {""},
#line 309 "HTMLCharacterReference.gperf"
      {"&coprod;", "∐"},
#line 1084 "HTMLCharacterReference.gperf"
      {"&Mu;", "Μ"},
      {""}, {""},
#line 1955 "HTMLCharacterReference.gperf"
      {"&Uogon;", "Ų"},
      {""}, {""},
#line 1565 "HTMLCharacterReference.gperf"
      {"&Rfr;", "ℜ"},
#line 694 "HTMLCharacterReference.gperf"
      {"&hbar;", "ℏ"},
#line 223 "HTMLCharacterReference.gperf"
      {"&bumpe;", "≏"},
#line 593 "HTMLCharacterReference.gperf"
      {"&frac35;", "⅗"},
      {""}, {""}, {""},
#line 862 "HTMLCharacterReference.gperf"
      {"&lbarr;", "⤌"},
#line 586 "HTMLCharacterReference.gperf"
      {"&frac14;", "¼"},
      {""},
#line 646 "HTMLCharacterReference.gperf"
      {"&gnE;", "≩"},
#line 780 "HTMLCharacterReference.gperf"
      {"&iota;", "ι"},
#line 197 "HTMLCharacterReference.gperf"
      {"&boxvH;", "╪"},
      {""}, {""},
#line 971 "HTMLCharacterReference.gperf"
      {"&lnE;", "≨"},
      {""}, {""},
#line 595 "HTMLCharacterReference.gperf"
      {"&frac45;", "⅘"},
      {""},
#line 1300 "HTMLCharacterReference.gperf"
      {"&num;", "#"},
#line 689 "HTMLCharacterReference.gperf"
      {"&hArr;", "⇔"},
      {""},
#line 587 "HTMLCharacterReference.gperf"
      {"&frac15;", "⅕"},
#line 666 "HTMLCharacterReference.gperf"
      {"&GT;", ">"},
      {""},
#line 193 "HTMLCharacterReference.gperf"
      {"&boxV;", "║"},
#line 202 "HTMLCharacterReference.gperf"
      {"&boxvl;", "┤"},
#line 584 "HTMLCharacterReference.gperf"
      {"&frac12;", "½"},
      {""},
#line 632 "HTMLCharacterReference.gperf"
      {"&Gfr;", "𝔊"},
      {""}, {""}, {""}, {""},
#line 1182 "HTMLCharacterReference.gperf"
      {"&not;", "¬"},
      {""},
#line 1199 "HTMLCharacterReference.gperf"
      {"&notin;", "∉"},
#line 532 "HTMLCharacterReference.gperf"
      {"&equals;", "="},
      {""},
#line 1680 "HTMLCharacterReference.gperf"
      {"&Sfr;", "𝔖"},
#line 312 "HTMLCharacterReference.gperf"
      {"&copy;", "©"},
#line 206 "HTMLCharacterReference.gperf"
      {"&boxvr;", "├"},
#line 225 "HTMLCharacterReference.gperf"
      {"&bumpeq;", "≏"},
      {""}, {""},
#line 1498 "HTMLCharacterReference.gperf"
      {"&quot;", "\""},
      {""},
#line 591 "HTMLCharacterReference.gperf"
      {"&frac25;", "⅖"},
      {""},
#line 1249 "HTMLCharacterReference.gperf"
      {"&npr;", "⊀"},
      {""}, {""},
#line 585 "HTMLCharacterReference.gperf"
      {"&frac13;", "⅓"},
      {""}, {""}, {""},
#line 195 "HTMLCharacterReference.gperf"
      {"&boxVH;", "╬"},
      {""}, {""}, {""},
#line 1760 "HTMLCharacterReference.gperf"
      {"&Star;", "⋆"},
#line 866 "HTMLCharacterReference.gperf"
      {"&lbrke;", "⦋"},
#line 588 "HTMLCharacterReference.gperf"
      {"&frac16;", "⅙"},
#line 183 "HTMLCharacterReference.gperf"
      {"&boxplus;", "⊞"},
      {""}, {""},
#line 776 "HTMLCharacterReference.gperf"
      {"&iogon;", "į"},
#line 594 "HTMLCharacterReference.gperf"
      {"&frac38;", "⅜"},
      {""},
#line 1341 "HTMLCharacterReference.gperf"
      {"&Ofr;", "𝔒"},
      {""},
#line 200 "HTMLCharacterReference.gperf"
      {"&boxVl;", "╢"},
      {""},
#line 1976 "HTMLCharacterReference.gperf"
      {"&Upsilon;", "Υ"},
      {""},
#line 1100 "HTMLCharacterReference.gperf"
      {"&nbsp;", " "},
#line 186 "HTMLCharacterReference.gperf"
      {"&boxUl;", "╜"},
#line 590 "HTMLCharacterReference.gperf"
      {"&frac23;", "⅔"},
      {""},
#line 1181 "HTMLCharacterReference.gperf"
      {"&Not;", "⫬"},
      {""}, {""}, {""},
#line 1445 "HTMLCharacterReference.gperf"
      {"&Pr;", "⪻"},
#line 2065 "HTMLCharacterReference.gperf"
      {"&wfr;", "𝔴"},
      {""},
#line 204 "HTMLCharacterReference.gperf"
      {"&boxVr;", "╟"},
#line 589 "HTMLCharacterReference.gperf"
      {"&frac18;", "⅛"},
#line 418 "HTMLCharacterReference.gperf"
      {"&dotplus;", "∔"},
#line 78 "HTMLCharacterReference.gperf"
      {"&apE;", "⩰"},
      {""},
#line 190 "HTMLCharacterReference.gperf"
      {"&boxUr;", "╙"},
      {""}, {""},
#line 566 "HTMLCharacterReference.gperf"
      {"&Ffr;", "𝔉"},
      {""}, {""},
#line 596 "HTMLCharacterReference.gperf"
      {"&frac56;", "⅚"},
      {""}, {""}, {""}, {""}, {""},
#line 679 "HTMLCharacterReference.gperf"
      {"&gtrless;", "≷"},
      {""},
#line 95 "HTMLCharacterReference.gperf"
      {"&Auml;", "Ä"},
      {""}, {""},
#line 619 "HTMLCharacterReference.gperf"
      {"&ge;", "≥"},
      {""}, {""}, {""}, {""},
#line 884 "HTMLCharacterReference.gperf"
      {"&le;", "≤"},
      {""}, {""},
#line 831 "HTMLCharacterReference.gperf"
      {"&lAarr;", "⇚"},
      {""},
#line 487 "HTMLCharacterReference.gperf"
      {"&ee;", "ⅇ"},
      {""}, {""}, {""},
#line 597 "HTMLCharacterReference.gperf"
      {"&frac58;", "⅝"},
      {""},
#line 1142 "HTMLCharacterReference.gperf"
      {"&nGt;", "≫⃒"},
#line 630 "HTMLCharacterReference.gperf"
      {"&gesl;", "⋛︀"},
      {""}, {""}, {""},
#line 1133 "HTMLCharacterReference.gperf"
      {"&nfr;", "𝔫"},
#line 156 "HTMLCharacterReference.gperf"
      {"&bNot;", "⫭"},
#line 384 "HTMLCharacterReference.gperf"
      {"&dharl;", "⇃"},
#line 520 "HTMLCharacterReference.gperf"
      {"&eparsl;", "⧣"},
      {""}, {""}, {""},
#line 66 "HTMLCharacterReference.gperf"
      {"&angrt;", "∟"},
      {""},
#line 635 "HTMLCharacterReference.gperf"
      {"&gg;", "≫"},
#line 1571 "HTMLCharacterReference.gperf"
      {"&Rho;", "Ρ"},
      {""},
#line 56 "HTMLCharacterReference.gperf"
      {"&angle;", "∠"},
#line 1004 "HTMLCharacterReference.gperf"
      {"&lparlt;", "⦓"},
#line 949 "HTMLCharacterReference.gperf"
      {"&lg;", "≶"},
#line 1486 "HTMLCharacterReference.gperf"
      {"&qfr;", "𝔮"},
      {""},
#line 385 "HTMLCharacterReference.gperf"
      {"&dharr;", "⇂"},
#line 598 "HTMLCharacterReference.gperf"
      {"&frac78;", "⅞"},
#line 491 "HTMLCharacterReference.gperf"
      {"&eg;", "⪚"},
      {""}, {""}, {""}, {""}, {""},
#line 625 "HTMLCharacterReference.gperf"
      {"&ges;", "⩾"},
      {""}, {""}, {""},
#line 341 "HTMLCharacterReference.gperf"
      {"&curarrm;", "⤼"},
#line 926 "HTMLCharacterReference.gperf"
      {"&les;", "⩽"},
#line 1290 "HTMLCharacterReference.gperf"
      {"&ntgl;", "≹"},
#line 1273 "HTMLCharacterReference.gperf"
      {"&nspar;", "∦"},
      {""}, {""},
#line 622 "HTMLCharacterReference.gperf"
      {"&geq;", "≥"},
      {""}, {""}, {""}, {""},
#line 923 "HTMLCharacterReference.gperf"
      {"&leq;", "≤"},
      {""}, {""},
#line 1201 "HTMLCharacterReference.gperf"
      {"&notinE;", "⋹̸"},
      {""},
#line 1132 "HTMLCharacterReference.gperf"
      {"&Nfr;", "𝔑"},
      {""}, {""}, {""}, {""},
#line 1061 "HTMLCharacterReference.gperf"
      {"&Mfr;", "𝔐"},
      {""}, {""}, {""}, {""}, {""},
#line 1284 "HTMLCharacterReference.gperf"
      {"&nsup;", "⊅"},
#line 323 "HTMLCharacterReference.gperf"
      {"&csupe;", "⫒"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 494 "HTMLCharacterReference.gperf"
      {"&egs;", "⪖"},
#line 1019 "HTMLCharacterReference.gperf"
      {"&lsqb;", "["},
#line 1318 "HTMLCharacterReference.gperf"
      {"&nvsim;", "∼⃒"},
      {""}, {""}, {""}, {""}, {""},
#line 346 "HTMLCharacterReference.gperf"
      {"&curren;", "¤"},
      {""}, {""},
#line 796 "HTMLCharacterReference.gperf"
      {"&Iuml;", "Ï"},
#line 222 "HTMLCharacterReference.gperf"
      {"&bumpE;", "⪮"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 49 "HTMLCharacterReference.gperf"
      {"&and;", "∧"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1255 "HTMLCharacterReference.gperf"
      {"&nrarr;", "↛"},
      {""}, {""}, {""}, {""}, {""},
#line 1257 "HTMLCharacterReference.gperf"
      {"&nrarrw;", "↝̸"},
      {""}, {""},
#line 1244 "HTMLCharacterReference.gperf"
      {"&npar;", "∦"},
      {""}, {""}, {""},
#line 762 "HTMLCharacterReference.gperf"
      {"&Int;", "∬"},
      {""}, {""},
#line 57 "HTMLCharacterReference.gperf"
      {"&angmsd;", "∡"},
      {""}, {""},
#line 255 "HTMLCharacterReference.gperf"
      {"&cent;", "¢"},
      {""},
#line 313 "HTMLCharacterReference.gperf"
      {"&copysr;", "℗"},
      {""},
#line 35 "HTMLCharacterReference.gperf"
      {"&Afr;", "𝔄"},
      {""},
#line 671 "HTMLCharacterReference.gperf"
      {"&gtdot;", "⋗"},
      {""}, {""},
#line 701 "HTMLCharacterReference.gperf"
      {"&Hfr;", "ℌ"},
      {""},
#line 1029 "HTMLCharacterReference.gperf"
      {"&ltdot;", "⋖"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 463 "HTMLCharacterReference.gperf"
      {"&dtdot;", "⋱"},
      {""}, {""}, {""}, {""},
#line 1101 "HTMLCharacterReference.gperf"
      {"&nbump;", "≎̸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1303 "HTMLCharacterReference.gperf"
      {"&nvap;", "≍⃒"},
      {""}, {""}, {""},
#line 1200 "HTMLCharacterReference.gperf"
      {"&notindot;", "⋵̸"},
#line 951 "HTMLCharacterReference.gperf"
      {"&lHar;", "⥢"},
      {""}, {""}, {""}, {""}, {""},
#line 653 "HTMLCharacterReference.gperf"
      {"&grave;", "`"},
      {""}, {""}, {""},
#line 383 "HTMLCharacterReference.gperf"
      {"&dHar;", "⥥"},
#line 217 "HTMLCharacterReference.gperf"
      {"&bsolb;", "⧅"},
      {""}, {""}, {""}, {""},
#line 543 "HTMLCharacterReference.gperf"
      {"&esdot;", "≐"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1552 "HTMLCharacterReference.gperf"
      {"&Re;", "ℜ"},
#line 1414 "HTMLCharacterReference.gperf"
      {"&Pfr;", "𝔓"},
#line 623 "HTMLCharacterReference.gperf"
      {"&geqq;", "≧"},
#line 72 "HTMLCharacterReference.gperf"
      {"&Aogon;", "Ą"},
      {""}, {""},
#line 1140 "HTMLCharacterReference.gperf"
      {"&nGg;", "⋙̸"},
#line 924 "HTMLCharacterReference.gperf"
      {"&leqq;", "≦"},
#line 324 "HTMLCharacterReference.gperf"
      {"&ctdot;", "⋯"},
      {""},
#line 370 "HTMLCharacterReference.gperf"
      {"&dd;", "ⅆ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 262 "HTMLCharacterReference.gperf"
      {"&check;", "✓"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 735 "HTMLCharacterReference.gperf"
      {"&Ifr;", "ℑ"},
      {""},
#line 1321 "HTMLCharacterReference.gperf"
      {"&nwarr;", "↖"},
      {""}, {""}, {""},
#line 1309 "HTMLCharacterReference.gperf"
      {"&nvgt;", ">⃒"},
      {""}, {""}, {""},
#line 2077 "HTMLCharacterReference.gperf"
      {"&Xfr;", "𝔛"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 209 "HTMLCharacterReference.gperf"
      {"&breve;", "˘"},
      {""}, {""}, {""},
#line 779 "HTMLCharacterReference.gperf"
      {"&Iota;", "Ι"},
#line 1247 "HTMLCharacterReference.gperf"
      {"&npart;", "∂̸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 124 "HTMLCharacterReference.gperf"
      {"&beta;", "β"},
      {""}, {""},
#line 634 "HTMLCharacterReference.gperf"
      {"&Gg;", "⋙"},
#line 2109 "HTMLCharacterReference.gperf"
      {"&yen;", "¥"},
      {""},
#line 1148 "HTMLCharacterReference.gperf"
      {"&nhpar;", "⫲"},
#line 121 "HTMLCharacterReference.gperf"
      {"&bernou;", "ℬ"},
      {""},
#line 2134 "HTMLCharacterReference.gperf"
      {"&Zfr;", "ℨ"},
#line 2133 "HTMLCharacterReference.gperf"
      {"&zeta;", "ζ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 617 "HTMLCharacterReference.gperf"
      {"&gdot;", "ġ"},
      {""},
#line 578 "HTMLCharacterReference.gperf"
      {"&ForAll;", "∀"},
      {""}, {""}, {""},
#line 670 "HTMLCharacterReference.gperf"
      {"&gtcir;", "⩺"},
#line 809 "HTMLCharacterReference.gperf"
      {"&Jsercy;", "Ј"},
      {""}, {""},
#line 486 "HTMLCharacterReference.gperf"
      {"&edot;", "ė"},
#line 1028 "HTMLCharacterReference.gperf"
      {"&ltcir;", "⩹"},
      {""},
#line 868 "HTMLCharacterReference.gperf"
      {"&lbrkslu;", "⦍"},
      {""}, {""},
#line 759 "HTMLCharacterReference.gperf"
      {"&infin;", "∞"},
#line 207 "HTMLCharacterReference.gperf"
      {"&bprime;", "‵"},
      {""},
#line 922 "HTMLCharacterReference.gperf"
      {"&leg;", "⋚"},
#line 1145 "HTMLCharacterReference.gperf"
      {"&nGtv;", "≫̸"},
#line 775 "HTMLCharacterReference.gperf"
      {"&Iogon;", "Į"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 375 "HTMLCharacterReference.gperf"
      {"&deg;", "°"},
      {""}, {""}, {""}, {""}, {""},
#line 681 "HTMLCharacterReference.gperf"
      {"&gvertneqq;", "≩︀"},
      {""}, {""}, {""},
#line 127 "HTMLCharacterReference.gperf"
      {"&Bfr;", "𝔅"},
#line 1040 "HTMLCharacterReference.gperf"
      {"&lvertneqq;", "≨︀"},
#line 205 "HTMLCharacterReference.gperf"
      {"&boxvR;", "╞"},
      {""}, {""},
#line 636 "HTMLCharacterReference.gperf"
      {"&ggg;", "⋙"},
      {""},
#line 1286 "HTMLCharacterReference.gperf"
      {"&nsupe;", "⊉"},
#line 631 "HTMLCharacterReference.gperf"
      {"&gesles;", "⪔"},
#line 1114 "HTMLCharacterReference.gperf"
      {"&ne;", "≠"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 2129 "HTMLCharacterReference.gperf"
      {"&zdot;", "ż"},
      {""}, {""}, {""}, {""},
#line 320 "HTMLCharacterReference.gperf"
      {"&csub;", "⫏"},
#line 781 "HTMLCharacterReference.gperf"
      {"&iprod;", "⨼"},
#line 1246 "HTMLCharacterReference.gperf"
      {"&nparsl;", "⫽⃥"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1147 "HTMLCharacterReference.gperf"
      {"&nharr;", "↮"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 251 "HTMLCharacterReference.gperf"
      {"&cdot;", "ċ"},
#line 295 "HTMLCharacterReference.gperf"
      {"&comma;", ","},
#line 454 "HTMLCharacterReference.gperf"
      {"&drcorn;", "⌟"},
      {""}, {""}, {""},
#line 203 "HTMLCharacterReference.gperf"
      {"&boxVR;", "╠"},
      {""}, {""}, {""},
#line 662 "HTMLCharacterReference.gperf"
      {"&gscr;", "ℊ"},
#line 189 "HTMLCharacterReference.gperf"
      {"&boxUR;", "╚"},
      {""}, {""}, {""},
#line 1013 "HTMLCharacterReference.gperf"
      {"&lscr;", "𝓁"},
      {""},
#line 455 "HTMLCharacterReference.gperf"
      {"&drcrop;", "⌌"},
      {""},
#line 59 "HTMLCharacterReference.gperf"
      {"&angmsdab;", "⦩"},
#line 542 "HTMLCharacterReference.gperf"
      {"&escr;", "ℯ"},
      {""}, {""}, {""}, {""},
#line 457 "HTMLCharacterReference.gperf"
      {"&dscr;", "𝒹"},
      {""},
#line 50 "HTMLCharacterReference.gperf"
      {"&andand;", "⩕"},
      {""}, {""},
#line 53 "HTMLCharacterReference.gperf"
      {"&andv;", "⩚"},
      {""}, {""}, {""}, {""},
#line 807 "HTMLCharacterReference.gperf"
      {"&Jscr;", "𝒥"},
      {""},
#line 810 "HTMLCharacterReference.gperf"
      {"&jsercy;", "ј"},
      {""}, {""},
#line 88 "HTMLCharacterReference.gperf"
      {"&ascr;", "𝒶"},
      {""}, {""}, {""}, {""},
#line 263 "HTMLCharacterReference.gperf"
      {"&checkmark;", "✓"},
      {""}, {""}, {""},
#line 63 "HTMLCharacterReference.gperf"
      {"&angmsdaf;", "⦭"},
#line 602 "HTMLCharacterReference.gperf"
      {"&fscr;", "𝒻"},
#line 626 "HTMLCharacterReference.gperf"
      {"&gescc;", "⪩"},
      {""}, {""}, {""},
#line 212 "HTMLCharacterReference.gperf"
      {"&bscr;", "𝒷"},
#line 927 "HTMLCharacterReference.gperf"
      {"&lescc;", "⪨"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 995 "HTMLCharacterReference.gperf"
      {"&lotimes;", "⨴"},
#line 734 "HTMLCharacterReference.gperf"
      {"&iff;", "⇔"},
#line 2142 "HTMLCharacterReference.gperf"
      {"&zscr;", "𝓏"},
#line 1282 "HTMLCharacterReference.gperf"
      {"&nsucc;", "⊁"},
      {""}, {""}, {""},
#line 715 "HTMLCharacterReference.gperf"
      {"&hscr;", "𝒽"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2064 "HTMLCharacterReference.gperf"
      {"&Wfr;", "𝔚"},
      {""},
#line 1126 "HTMLCharacterReference.gperf"
      {"&nesim;", "≂̸"},
#line 700 "HTMLCharacterReference.gperf"
      {"&hercon;", "⊹"},
      {""}, {""},
#line 456 "HTMLCharacterReference.gperf"
      {"&Dscr;", "𝒟"},
      {""}, {""}, {""}, {""},
#line 319 "HTMLCharacterReference.gperf"
      {"&cscr;", "𝒸"},
      {""}, {""}, {""}, {""},
#line 669 "HTMLCharacterReference.gperf"
      {"&gtcc;", "⪧"},
      {""},
#line 1316 "HTMLCharacterReference.gperf"
      {"&nvrArr;", "⤃"},
      {""},
#line 950 "HTMLCharacterReference.gperf"
      {"&lgE;", "⪑"},
#line 1027 "HTMLCharacterReference.gperf"
      {"&ltcc;", "⪦"},
#line 1272 "HTMLCharacterReference.gperf"
      {"&nsmid;", "∤"},
      {""}, {""},
#line 1143 "HTMLCharacterReference.gperf"
      {"&ngt;", "≯"},
#line 1210 "HTMLCharacterReference.gperf"
      {"&NotLessGreater;", "≸"},
      {""}, {""}, {""}, {""},
#line 663 "HTMLCharacterReference.gperf"
      {"&gsim;", "≳"},
#line 1141 "HTMLCharacterReference.gperf"
      {"&ngsim;", "≵"},
      {""}, {""}, {""},
#line 1016 "HTMLCharacterReference.gperf"
      {"&lsim;", "≲"},
      {""}, {""}, {""}, {""},
#line 545 "HTMLCharacterReference.gperf"
      {"&esim;", "≂"},
      {""}, {""}, {""}, {""},
#line 1491 "HTMLCharacterReference.gperf"
      {"&Qscr;", "𝒬"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1208 "HTMLCharacterReference.gperf"
      {"&NotLess;", "≮"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1987 "HTMLCharacterReference.gperf"
      {"&Uscr;", "𝒰"},
#line 733 "HTMLCharacterReference.gperf"
      {"&iexcl;", "¡"},
      {""}, {""}, {""},
#line 808 "HTMLCharacterReference.gperf"
      {"&jscr;", "𝒿"},
      {""}, {""},
#line 527 "HTMLCharacterReference.gperf"
      {"&eqcolon;", "≕"},
#line 65 "HTMLCharacterReference.gperf"
      {"&angmsdah;", "⦯"},
      {""},
#line 201 "HTMLCharacterReference.gperf"
      {"&boxvL;", "╡"},
#line 697 "HTMLCharacterReference.gperf"
      {"&hearts;", "♥"},
      {""},
#line 331 "HTMLCharacterReference.gperf"
      {"&Cup;", "⋓"},
#line 214 "HTMLCharacterReference.gperf"
      {"&bsim;", "∽"},
#line 665 "HTMLCharacterReference.gperf"
      {"&gsiml;", "⪐"},
#line 864 "HTMLCharacterReference.gperf"
      {"&lbrace;", "{"},
      {""},
#line 48 "HTMLCharacterReference.gperf"
      {"&And;", "⩓"},
#line 616 "HTMLCharacterReference.gperf"
      {"&Gdot;", "Ġ"},
      {""}, {""}, {""},
#line 1006 "HTMLCharacterReference.gperf"
      {"&lrcorner;", "⌟"},
#line 1012 "HTMLCharacterReference.gperf"
      {"&Lscr;", "ℒ"},
      {""},
#line 224 "HTMLCharacterReference.gperf"
      {"&Bumpeq;", "≎"},
      {""}, {""},
#line 2116 "HTMLCharacterReference.gperf"
      {"&Yscr;", "𝒴"},
#line 1285 "HTMLCharacterReference.gperf"
      {"&nsupE;", "⫆̸"},
#line 2130 "HTMLCharacterReference.gperf"
      {"&zeetrf;", "ℨ"},
      {""}, {""}, {""},
#line 372 "HTMLCharacterReference.gperf"
      {"&ddarr;", "⇊"},
      {""}, {""}, {""}, {""},
#line 414 "HTMLCharacterReference.gperf"
      {"&doteq;", "≐"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2117 "HTMLCharacterReference.gperf"
      {"&yscr;", "𝓎"},
#line 952 "HTMLCharacterReference.gperf"
      {"&lhard;", "↽"},
      {""}, {""}, {""},
#line 1144 "HTMLCharacterReference.gperf"
      {"&ngtr;", "≯"},
#line 199 "HTMLCharacterReference.gperf"
      {"&boxVL;", "╣"},
      {""}, {""}, {""}, {""},
#line 185 "HTMLCharacterReference.gperf"
      {"&boxUL;", "╝"},
#line 296 "HTMLCharacterReference.gperf"
      {"&commat;", "@"},
      {""},
#line 1135 "HTMLCharacterReference.gperf"
      {"&nge;", "≱"},
      {""}, {""}, {""}, {""}, {""},
#line 784 "HTMLCharacterReference.gperf"
      {"&iscr;", "𝒾"},
      {""}, {""},
#line 326 "HTMLCharacterReference.gperf"
      {"&cudarrr;", "⤵"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1629 "HTMLCharacterReference.gperf"
      {"&Rscr;", "ℛ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1953 "HTMLCharacterReference.gperf"
      {"&Union;", "⋃"},
      {""},
#line 770 "HTMLCharacterReference.gperf"
      {"&intprod;", "⨼"},
      {""},
#line 1276 "HTMLCharacterReference.gperf"
      {"&nsub;", "⊄"},
#line 321 "HTMLCharacterReference.gperf"
      {"&csube;", "⫑"},
      {""}, {""}, {""},
#line 459 "HTMLCharacterReference.gperf"
      {"&dscy;", "ѕ"},
      {""},
#line 1490 "HTMLCharacterReference.gperf"
      {"&qprime;", "⁗"},
      {""}, {""},
#line 661 "HTMLCharacterReference.gperf"
      {"&Gscr;", "𝒢"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1262 "HTMLCharacterReference.gperf"
      {"&nsc;", "⊁"},
#line 1755 "HTMLCharacterReference.gperf"
      {"&Sscr;", "𝒮"},
      {""},
#line 335 "HTMLCharacterReference.gperf"
      {"&cupcap;", "⩆"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 258 "HTMLCharacterReference.gperf"
      {"&Cfr;", "ℭ"},
      {""},
#line 976 "HTMLCharacterReference.gperf"
      {"&loang;", "⟬"},
      {""},
#line 1212 "HTMLCharacterReference.gperf"
      {"&NotLessSlantEqual;", "⩽̸"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1383 "HTMLCharacterReference.gperf"
      {"&Oscr;", "𝒪"},
      {""}, {""}, {""}, {""}, {""},
#line 1117 "HTMLCharacterReference.gperf"
      {"&nearr;", "↗"},
      {""},
#line 867 "HTMLCharacterReference.gperf"
      {"&lbrksld;", "⦏"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 2072 "HTMLCharacterReference.gperf"
      {"&wscr;", "𝓌"},
#line 165 "HTMLCharacterReference.gperf"
      {"&boxDl;", "╖"},
#line 1256 "HTMLCharacterReference.gperf"
      {"&nrarrc;", "⤳̸"},
      {""}, {""}, {""}, {""}, {""},
#line 640 "HTMLCharacterReference.gperf"
      {"&gl;", "≷"},
      {""},
#line 601 "HTMLCharacterReference.gperf"
      {"&Fscr;", "ℱ"},
      {""}, {""},
#line 959 "HTMLCharacterReference.gperf"
      {"&ll;", "≪"},
      {""}, {""},
#line 169 "HTMLCharacterReference.gperf"
      {"&boxDr;", "╓"},
      {""},
#line 496 "HTMLCharacterReference.gperf"
      {"&el;", "⪙"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 677 "HTMLCharacterReference.gperf"
      {"&gtreqless;", "⋛"},
      {""}, {""}, {""}, {""},
#line 785 "HTMLCharacterReference.gperf"
      {"&isin;", "∈"},
      {""},
#line 1037 "HTMLCharacterReference.gperf"
      {"&ltrPar;", "⦖"},
#line 1622 "HTMLCharacterReference.gperf"
      {"&RoundImplies;", "⥰"},
      {""},
#line 172 "HTMLCharacterReference.gperf"
      {"&boxH;", "═"},
#line 790 "HTMLCharacterReference.gperf"
      {"&isinv;", "∈"},
#line 1744 "HTMLCharacterReference.gperf"
      {"&Square;", "□"},
#line 1693 "HTMLCharacterReference.gperf"
      {"&ShortUpArrow;", "↑"},
      {""},
#line 1266 "HTMLCharacterReference.gperf"
      {"&nscr;", "𝓃"},
#line 252 "HTMLCharacterReference.gperf"
      {"&cedil;", "¸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1136 "HTMLCharacterReference.gperf"
      {"&ngeq;", "≱"},
#line 1137 "HTMLCharacterReference.gperf"
      {"&ngeqq;", "≧̸"},
      {""}, {""}, {""},
#line 1492 "HTMLCharacterReference.gperf"
      {"&qscr;", "𝓆"},
      {""}, {""}, {""},
#line 500 "HTMLCharacterReference.gperf"
      {"&els;", "⪕"},
      {""},
#line 664 "HTMLCharacterReference.gperf"
      {"&gsime;", "⪎"},
#line 304 "HTMLCharacterReference.gperf"
      {"&Conint;", "∯"},
      {""}, {""}, {""},
#line 1017 "HTMLCharacterReference.gperf"
      {"&lsime;", "⪍"},
      {""}, {""}, {""},
#line 55 "HTMLCharacterReference.gperf"
      {"&ange;", "⦤"},
      {""},
#line 1102 "HTMLCharacterReference.gperf"
      {"&nbumpe;", "≏̸"},
      {""},
#line 2078 "HTMLCharacterReference.gperf"
      {"&xfr;", "𝔵"},
      {""}, {""}, {""},
#line 1183 "HTMLCharacterReference.gperf"
      {"&NotCongruent;", "≢"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1265 "HTMLCharacterReference.gperf"
      {"&Nscr;", "𝒩"},
      {""},
#line 1031 "HTMLCharacterReference.gperf"
      {"&ltimes;", "⋉"},
      {""}, {""},
#line 1081 "HTMLCharacterReference.gperf"
      {"&Mscr;", "ℳ"},
#line 98 "HTMLCharacterReference.gperf"
      {"&awint;", "⨑"},
      {""}, {""}, {""}, {""}, {""},
#line 1130 "HTMLCharacterReference.gperf"
      {"&nexist;", "∄"},
      {""},
#line 1171 "HTMLCharacterReference.gperf"
      {"&nLt;", "≪⃒"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 215 "HTMLCharacterReference.gperf"
      {"&bsime;", "⋍"},
#line 764 "HTMLCharacterReference.gperf"
      {"&intcal;", "⊺"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 415 "HTMLCharacterReference.gperf"
      {"&doteqdot;", "≑"},
      {""}, {""}, {""}, {""}, {""},
#line 2119 "HTMLCharacterReference.gperf"
      {"&yucy;", "ю"},
      {""}, {""}, {""},
#line 1134 "HTMLCharacterReference.gperf"
      {"&ngE;", "≧̸"},
#line 774 "HTMLCharacterReference.gperf"
      {"&iocy;", "ё"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 265 "HTMLCharacterReference.gperf"
      {"&chi;", "χ"},
#line 2132 "HTMLCharacterReference.gperf"
      {"&Zeta;", "Ζ"},
      {""}, {""}, {""}, {""},
#line 1269 "HTMLCharacterReference.gperf"
      {"&nsim;", "≁"},
#line 352 "HTMLCharacterReference.gperf"
      {"&cwint;", "∱"},
      {""}, {""},
#line 62 "HTMLCharacterReference.gperf"
      {"&angmsdae;", "⦬"},
      {""}, {""}, {""}, {""},
#line 989 "HTMLCharacterReference.gperf"
      {"&looparrowleft;", "↫"},
      {""}, {""}, {""},
#line 24 "HTMLCharacterReference.gperf"
      {"&ac;", "∾"},
      {""},
#line 87 "HTMLCharacterReference.gperf"
      {"&Ascr;", "𝒜"},
      {""}, {""},
#line 958 "HTMLCharacterReference.gperf"
      {"&Ll;", "⋘"},
      {""},
#line 714 "HTMLCharacterReference.gperf"
      {"&Hscr;", "ℋ"},
      {""},
#line 493 "HTMLCharacterReference.gperf"
      {"&egrave;", "è"},
      {""}, {""},
#line 730 "HTMLCharacterReference.gperf"
      {"&Idot;", "İ"},
#line 2093 "HTMLCharacterReference.gperf"
      {"&xrarr;", "⟶"},
      {""}, {""}, {""}, {""},
#line 174 "HTMLCharacterReference.gperf"
      {"&boxHD;", "╦"},
      {""}, {""},
#line 64 "HTMLCharacterReference.gperf"
      {"&angmsdag;", "⦮"},
#line 301 "HTMLCharacterReference.gperf"
      {"&cong;", "≅"},
#line 208 "HTMLCharacterReference.gperf"
      {"&Breve;", "˘"},
      {""}, {""},
#line 641 "HTMLCharacterReference.gperf"
      {"&gla;", "⪥"},
#line 572 "HTMLCharacterReference.gperf"
      {"&flat;", "♭"},
      {""},
#line 38 "HTMLCharacterReference.gperf"
      {"&agrave;", "à"},
      {""}, {""},
#line 300 "HTMLCharacterReference.gperf"
      {"&complexes;", "ℂ"},
#line 2047 "HTMLCharacterReference.gperf"
      {"&vprop;", "∝"},
      {""}, {""}, {""},
#line 123 "HTMLCharacterReference.gperf"
      {"&Beta;", "Β"},
#line 1278 "HTMLCharacterReference.gperf"
      {"&nsube;", "⊈"},
#line 562 "HTMLCharacterReference.gperf"
      {"&female;", "♀"},
      {""}, {""},
#line 2137 "HTMLCharacterReference.gperf"
      {"&zhcy;", "ж"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2128 "HTMLCharacterReference.gperf"
      {"&Zdot;", "Ż"},
      {""},
#line 526 "HTMLCharacterReference.gperf"
      {"&eqcirc;", "≖"},
      {""}, {""},
#line 1251 "HTMLCharacterReference.gperf"
      {"&npre;", "⪯̸"},
#line 299 "HTMLCharacterReference.gperf"
      {"&complement;", "∁"},
      {""}, {""}, {""}, {""},
#line 1010 "HTMLCharacterReference.gperf"
      {"&lrtri;", "⊿"},
      {""},
#line 379 "HTMLCharacterReference.gperf"
      {"&demptyv;", "⦱"},
      {""},
#line 261 "HTMLCharacterReference.gperf"
      {"&chcy;", "ч"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1480 "HTMLCharacterReference.gperf"
      {"&Pscr;", "𝒫"},
#line 788 "HTMLCharacterReference.gperf"
      {"&isins;", "⋴"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 413 "HTMLCharacterReference.gperf"
      {"&DotDot;", "◌⃜"},
#line 119 "HTMLCharacterReference.gperf"
      {"&bemptyv;", "⦰"},
      {""},
#line 657 "HTMLCharacterReference.gperf"
      {"&GreaterGreater;", "⪢"},
#line 149 "HTMLCharacterReference.gperf"
      {"&blank;", "␣"},
      {""}, {""}, {""},
#line 783 "HTMLCharacterReference.gperf"
      {"&Iscr;", "ℐ"},
      {""},
#line 1301 "HTMLCharacterReference.gperf"
      {"&numero;", "№"},
      {""}, {""},
#line 458 "HTMLCharacterReference.gperf"
      {"&DScy;", "Ѕ"},
#line 213 "HTMLCharacterReference.gperf"
      {"&bsemi;", "⁏"},
      {""}, {""}, {""},
#line 2094 "HTMLCharacterReference.gperf"
      {"&Xscr;", "𝒳"},
      {""},
#line 712 "HTMLCharacterReference.gperf"
      {"&horbar;", "―"},
      {""},
#line 2041 "HTMLCharacterReference.gperf"
      {"&vfr;", "𝔳"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1131 "HTMLCharacterReference.gperf"
      {"&nexists;", "∄"},
      {""},
#line 2103 "HTMLCharacterReference.gperf"
      {"&YAcy;", "Я"},
      {""},
#line 1936 "HTMLCharacterReference.gperf"
      {"&Ugrave;", "Ù"},
#line 254 "HTMLCharacterReference.gperf"
      {"&cemptyv;", "⦲"},
      {""}, {""},
#line 349 "HTMLCharacterReference.gperf"
      {"&cuvee;", "⋎"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2141 "HTMLCharacterReference.gperf"
      {"&Zscr;", "𝒵"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 529 "HTMLCharacterReference.gperf"
      {"&eqslantgtr;", "⪖"},
      {""}, {""}, {""}, {""}, {""},
#line 210 "HTMLCharacterReference.gperf"
      {"&brvbar;", "¦"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 480 "HTMLCharacterReference.gperf"
      {"&ecolon;", "≕"},
#line 725 "HTMLCharacterReference.gperf"
      {"&ic;", "⁣"},
      {""},
#line 257 "HTMLCharacterReference.gperf"
      {"&centerdot;", "·"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 960 "HTMLCharacterReference.gperf"
      {"&llarr;", "⇇"},
      {""}, {""},
#line 1454 "HTMLCharacterReference.gperf"
      {"&Precedes;", "≺"},
      {""},
#line 978 "HTMLCharacterReference.gperf"
      {"&lobrk;", "⟦"},
      {""},
#line 1747 "HTMLCharacterReference.gperf"
      {"&SquareSubset;", "⊏"},
#line 333 "HTMLCharacterReference.gperf"
      {"&cupbrcap;", "⩈"},
#line 469 "HTMLCharacterReference.gperf"
      {"&DZcy;", "Џ"},
#line 574 "HTMLCharacterReference.gperf"
      {"&fltns;", "▱"},
      {""}, {""},
#line 61 "HTMLCharacterReference.gperf"
      {"&angmsdad;", "⦫"},
#line 211 "HTMLCharacterReference.gperf"
      {"&Bscr;", "ℬ"},
      {""},
#line 738 "HTMLCharacterReference.gperf"
      {"&igrave;", "ì"},
      {""},
#line 184 "HTMLCharacterReference.gperf"
      {"&boxtimes;", "⊠"},
      {""}, {""},
#line 89 "HTMLCharacterReference.gperf"
      {"&Assign;", "≔"},
      {""},
#line 1813 "HTMLCharacterReference.gperf"
      {"&Superset;", "⊃"},
      {""},
#line 1119 "HTMLCharacterReference.gperf"
      {"&nedot;", "≐̸"},
#line 1390 "HTMLCharacterReference.gperf"
      {"&Otimes;", "⨷"},
#line 1748 "HTMLCharacterReference.gperf"
      {"&SquareSubsetEqual;", "⊑"},
#line 1482 "HTMLCharacterReference.gperf"
      {"&Psi;", "Ψ"},
      {""},
#line 873 "HTMLCharacterReference.gperf"
      {"&lceil;", "⌈"},
      {""}, {""},
#line 2040 "HTMLCharacterReference.gperf"
      {"&Vfr;", "𝔙"},
      {""},
#line 1252 "HTMLCharacterReference.gperf"
      {"&nprec;", "⊀"},
      {""},
#line 1648 "HTMLCharacterReference.gperf"
      {"&Sc;", "⪼"},
      {""},
#line 1688 "HTMLCharacterReference.gperf"
      {"&ShortDownArrow;", "↓"},
      {""},
#line 534 "HTMLCharacterReference.gperf"
      {"&equest;", "≟"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1749 "HTMLCharacterReference.gperf"
      {"&SquareSuperset;", "⊐"},
      {""}, {""},
#line 618 "HTMLCharacterReference.gperf"
      {"&gE;", "≧"},
      {""},
#line 1750 "HTMLCharacterReference.gperf"
      {"&SquareSupersetEqual;", "⊒"},
      {""}, {""},
#line 883 "HTMLCharacterReference.gperf"
      {"&lE;", "≦"},
      {""},
#line 1175 "HTMLCharacterReference.gperf"
      {"&nLtv;", "≪̸"},
#line 1277 "HTMLCharacterReference.gperf"
      {"&nsubE;", "⫅̸"},
      {""}, {""}, {""}, {""},
#line 1270 "HTMLCharacterReference.gperf"
      {"&nsime;", "≄"},
      {""}, {""}, {""},
#line 990 "HTMLCharacterReference.gperf"
      {"&looparrowright;", "↬"},
      {""}, {""}, {""}, {""}, {""},
#line 168 "HTMLCharacterReference.gperf"
      {"&boxDR;", "╔"},
      {""}, {""}, {""}, {""},
#line 2080 "HTMLCharacterReference.gperf"
      {"&xharr;", "⟷"},
      {""}, {""}, {""}, {""},
#line 153 "HTMLCharacterReference.gperf"
      {"&block;", "█"},
      {""}, {""}, {""}, {""},
#line 863 "HTMLCharacterReference.gperf"
      {"&lbbrk;", "❲"},
      {""},
#line 659 "HTMLCharacterReference.gperf"
      {"&GreaterSlantEqual;", "⩾"},
      {""}, {""}, {""},
#line 1344 "HTMLCharacterReference.gperf"
      {"&Ograve;", "Ò"},
#line 1287 "HTMLCharacterReference.gperf"
      {"&nsupset;", "⊃⃒"},
#line 182 "HTMLCharacterReference.gperf"
      {"&boxminus;", "⊟"},
      {""},
#line 955 "HTMLCharacterReference.gperf"
      {"&lhblk;", "▄"},
      {""}, {""},
#line 642 "HTMLCharacterReference.gperf"
      {"&glE;", "⪒"},
#line 2005 "HTMLCharacterReference.gperf"
      {"&vArr;", "⇕"},
      {""},
#line 1271 "HTMLCharacterReference.gperf"
      {"&nsimeq;", "≄"},
#line 468 "HTMLCharacterReference.gperf"
      {"&dwangle;", "⦦"},
      {""},
#line 1138 "HTMLCharacterReference.gperf"
      {"&ngeqslant;", "⩾̸"},
      {""},
#line 1720 "HTMLCharacterReference.gperf"
      {"&SOFTcy;", "Ь"},
      {""},
#line 1172 "HTMLCharacterReference.gperf"
      {"&nlt;", "≮"},
#line 1805 "HTMLCharacterReference.gperf"
      {"&sup1;", "¹"},
      {""}, {""},
#line 1213 "HTMLCharacterReference.gperf"
      {"&NotLessTilde;", "≴"},
      {""},
#line 1497 "HTMLCharacterReference.gperf"
      {"&QUOT;", "\""},
#line 1170 "HTMLCharacterReference.gperf"
      {"&nlsim;", "≴"},
      {""}, {""},
#line 417 "HTMLCharacterReference.gperf"
      {"&dotminus;", "∸"},
#line 877 "HTMLCharacterReference.gperf"
      {"&ldca;", "⤶"},
      {""}, {""}, {""}, {""},
#line 1806 "HTMLCharacterReference.gperf"
      {"&sup2;", "²"},
      {""}, {""}, {""}, {""},
#line 2071 "HTMLCharacterReference.gperf"
      {"&Wscr;", "𝒲"},
      {""}, {""}, {""}, {""},
#line 886 "HTMLCharacterReference.gperf"
      {"&LeftArrow;", "←"},
      {""},
#line 2053 "HTMLCharacterReference.gperf"
      {"&vsupnE;", "⫌︀"},
      {""},
#line 1766 "HTMLCharacterReference.gperf"
      {"&Sub;", "⋐"},
#line 682 "HTMLCharacterReference.gperf"
      {"&gvnE;", "≩︀"},
      {""}, {""}, {""}, {""},
#line 1041 "HTMLCharacterReference.gperf"
      {"&lvnE;", "≨︀"},
      {""},
#line 870 "HTMLCharacterReference.gperf"
      {"&lcaron;", "ľ"},
#line 889 "HTMLCharacterReference.gperf"
      {"&LeftArrowBar;", "⇤"},
#line 708 "HTMLCharacterReference.gperf"
      {"&hookleftarrow;", "↩"},
      {""},
#line 1216 "HTMLCharacterReference.gperf"
      {"&notni;", "∌"},
#line 476 "HTMLCharacterReference.gperf"
      {"&ecaron;", "ě"},
      {""},
#line 1804 "HTMLCharacterReference.gperf"
      {"&sup;", "⊃"},
      {""},
#line 787 "HTMLCharacterReference.gperf"
      {"&isinE;", "⋹"},
#line 366 "HTMLCharacterReference.gperf"
      {"&dcaron;", "ď"},
      {""}, {""},
#line 2118 "HTMLCharacterReference.gperf"
      {"&YUcy;", "Ю"},
      {""},
#line 857 "HTMLCharacterReference.gperf"
      {"&lAtail;", "⤛"},
      {""},
#line 549 "HTMLCharacterReference.gperf"
      {"&eth;", "ð"},
#line 1807 "HTMLCharacterReference.gperf"
      {"&sup3;", "³"},
      {""},
#line 77 "HTMLCharacterReference.gperf"
      {"&apacir;", "⩯"},
      {""},
#line 1801 "HTMLCharacterReference.gperf"
      {"&sum;", "∑"},
      {""},
#line 399 "HTMLCharacterReference.gperf"
      {"&disin;", "⋲"},
#line 2070 "HTMLCharacterReference.gperf"
      {"&wreath;", "≀"},
      {""},
#line 46 "HTMLCharacterReference.gperf"
      {"&AMP;", "&"},
      {""}, {""}, {""}, {""}, {""},
#line 2016 "HTMLCharacterReference.gperf"
      {"&Vbar;", "⫫"},
      {""},
#line 610 "HTMLCharacterReference.gperf"
      {"&gbreve;", "ğ"},
      {""}, {""}, {""},
#line 1036 "HTMLCharacterReference.gperf"
      {"&ltrif;", "◂"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 465 "HTMLCharacterReference.gperf"
      {"&dtrif;", "▾"},
#line 2125 "HTMLCharacterReference.gperf"
      {"&zcaron;", "ž"},
      {""},
#line 1015 "HTMLCharacterReference.gperf"
      {"&lsh;", "↰"},
      {""}, {""}, {""},
#line 1283 "HTMLCharacterReference.gperf"
      {"&nsucceq;", "⪰̸"},
      {""},
#line 1689 "HTMLCharacterReference.gperf"
      {"&ShortLeftArrow;", "←"},
#line 540 "HTMLCharacterReference.gperf"
      {"&erDot;", "≓"},
      {""}, {""},
#line 1159 "HTMLCharacterReference.gperf"
      {"&nle;", "≰"},
#line 1308 "HTMLCharacterReference.gperf"
      {"&nvge;", "≥⃒"},
#line 1260 "HTMLCharacterReference.gperf"
      {"&nrtri;", "⋫"},
#line 23 "HTMLCharacterReference.gperf"
      {"&abreve;", "ă"},
      {""}, {""}, {""}, {""},
#line 365 "HTMLCharacterReference.gperf"
      {"&Dcaron;", "Ď"},
      {""}, {""}, {""},
#line 1463 "HTMLCharacterReference.gperf"
      {"&Prime;", "″"},
#line 242 "HTMLCharacterReference.gperf"
      {"&ccaron;", "č"},
      {""}, {""},
#line 311 "HTMLCharacterReference.gperf"
      {"&COPY;", "©"},
      {""},
#line 782 "HTMLCharacterReference.gperf"
      {"&iquest;", "¿"},
      {""}, {""}, {""}, {""},
#line 758 "HTMLCharacterReference.gperf"
      {"&incare;", "℅"},
      {""}, {""},
#line 931 "HTMLCharacterReference.gperf"
      {"&lesg;", "⋚︀"},
#line 2087 "HTMLCharacterReference.gperf"
      {"&xodot;", "⨀"},
      {""}, {""}, {""},
#line 250 "HTMLCharacterReference.gperf"
      {"&Cdot;", "Ċ"},
      {""}, {""}, {""},
#line 1416 "HTMLCharacterReference.gperf"
      {"&Phi;", "Φ"},
      {""}, {""}, {""}, {""},
#line 26 "HTMLCharacterReference.gperf"
      {"&acE;", "∾̳"},
      {""},
#line 167 "HTMLCharacterReference.gperf"
      {"&boxdl;", "┐"},
      {""}, {""}, {""},
#line 1686 "HTMLCharacterReference.gperf"
      {"&SHcy;", "Ш"},
#line 240 "HTMLCharacterReference.gperf"
      {"&ccaps;", "⩍"},
      {""}, {""}, {""}, {""},
#line 164 "HTMLCharacterReference.gperf"
      {"&boxDL;", "╗"},
      {""}, {""}, {""},
#line 391 "HTMLCharacterReference.gperf"
      {"&diam;", "⋄"},
#line 171 "HTMLCharacterReference.gperf"
      {"&boxdr;", "┌"},
#line 37 "HTMLCharacterReference.gperf"
      {"&Agrave;", "À"},
      {""}, {""}, {""}, {""}, {""},
#line 374 "HTMLCharacterReference.gperf"
      {"&ddotseq;", "⩷"},
#line 266 "HTMLCharacterReference.gperf"
      {"&cir;", "○"},
      {""}, {""},
#line 1310 "HTMLCharacterReference.gperf"
      {"&nvHarr;", "⤄"},
      {""},
#line 1681 "HTMLCharacterReference.gperf"
      {"&sfr;", "𝔰"},
      {""},
#line 29 "HTMLCharacterReference.gperf"
      {"&acute;", "´"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 175 "HTMLCharacterReference.gperf"
      {"&boxHd;", "╤"},
      {""}, {""},
#line 1743 "HTMLCharacterReference.gperf"
      {"&squ;", "□"},
#line 2034 "HTMLCharacterReference.gperf"
      {"&vert;", "|"},
      {""},
#line 869 "HTMLCharacterReference.gperf"
      {"&Lcaron;", "Ľ"},
      {""}, {""},
#line 1761 "HTMLCharacterReference.gperf"
      {"&star;", "☆"},
      {""},
#line 2091 "HTMLCharacterReference.gperf"
      {"&xotime;", "⨂"},
      {""},
#line 400 "HTMLCharacterReference.gperf"
      {"&div;", "÷"},
#line 732 "HTMLCharacterReference.gperf"
      {"&iecy;", "е"},
      {""}, {""},
#line 739 "HTMLCharacterReference.gperf"
      {"&ii;", "ⅈ"},
      {""}, {""}, {""},
#line 1923 "HTMLCharacterReference.gperf"
      {"&Ubreve;", "Ŭ"},
      {""}, {""},
#line 1103 "HTMLCharacterReference.gperf"
      {"&ncap;", "⩃"},
#line 1765 "HTMLCharacterReference.gperf"
      {"&strns;", "¯"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 248 "HTMLCharacterReference.gperf"
      {"&ccups;", "⩌"},
      {""}, {""},
#line 396 "HTMLCharacterReference.gperf"
      {"&die;", "¨"},
#line 318 "HTMLCharacterReference.gperf"
      {"&Cscr;", "𝒞"},
#line 1156 "HTMLCharacterReference.gperf"
      {"&nlarr;", "↚"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 550 "HTMLCharacterReference.gperf"
      {"&Euml;", "Ë"},
#line 1978 "HTMLCharacterReference.gperf"
      {"&UpTee;", "⊥"},
      {""}, {""}, {""}, {""},
#line 1007 "HTMLCharacterReference.gperf"
      {"&lrhar;", "⇋"},
      {""}, {""},
#line 1014 "HTMLCharacterReference.gperf"
      {"&Lsh;", "↰"},
      {""}, {""},
#line 1539 "HTMLCharacterReference.gperf"
      {"&Rcaron;", "Ř"},
      {""}, {""}, {""},
#line 488 "HTMLCharacterReference.gperf"
      {"&efDot;", "≒"},
      {""}, {""},
#line 921 "HTMLCharacterReference.gperf"
      {"&lEg;", "⪋"},
      {""}, {""},
#line 676 "HTMLCharacterReference.gperf"
      {"&gtrdot;", "⋗"},
      {""}, {""},
#line 1164 "HTMLCharacterReference.gperf"
      {"&nleq;", "≰"},
#line 1165 "HTMLCharacterReference.gperf"
      {"&nleqq;", "≦̸"},
#line 737 "HTMLCharacterReference.gperf"
      {"&Igrave;", "Ì"},
      {""}, {""}, {""},
#line 1754 "HTMLCharacterReference.gperf"
      {"&srarr;", "→"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1729 "HTMLCharacterReference.gperf"
      {"&spar;", "∥"},
      {""}, {""}, {""}, {""},
#line 1110 "HTMLCharacterReference.gperf"
      {"&ncup;", "⩂"},
      {""},
#line 1211 "HTMLCharacterReference.gperf"
      {"&NotLessLess;", "≪̸"},
      {""}, {""}, {""}, {""},
#line 1651 "HTMLCharacterReference.gperf"
      {"&Scaron;", "Š"},
      {""}, {""},
#line 2033 "HTMLCharacterReference.gperf"
      {"&Vert;", "‖"},
#line 1979 "HTMLCharacterReference.gperf"
      {"&UpTeeArrow;", "↥"},
      {""}, {""}, {""}, {""}, {""},
#line 1757 "HTMLCharacterReference.gperf"
      {"&ssetmn;", "∖"},
      {""},
#line 1631 "HTMLCharacterReference.gperf"
      {"&Rsh;", "↱"},
      {""},
#line 467 "HTMLCharacterReference.gperf"
      {"&duhar;", "⥯"},
#line 298 "HTMLCharacterReference.gperf"
      {"&compfn;", "∘"},
      {""},
#line 546 "HTMLCharacterReference.gperf"
      {"&Eta;", "Η"},
      {""},
#line 1820 "HTMLCharacterReference.gperf"
      {"&supne;", "⊋"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 609 "HTMLCharacterReference.gperf"
      {"&Gbreve;", "Ğ"},
      {""},
#line 25 "HTMLCharacterReference.gperf"
      {"&acd;", "∿"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 2095 "HTMLCharacterReference.gperf"
      {"&xscr;", "𝓍"},
      {""}, {""}, {""},
#line 1158 "HTMLCharacterReference.gperf"
      {"&nlE;", "≦̸"},
      {""}, {""}, {""}, {""}, {""},
#line 888 "HTMLCharacterReference.gperf"
      {"&leftarrow;", "←"},
      {""},
#line 144 "HTMLCharacterReference.gperf"
      {"&blacksquare;", "▪"},
#line 1149 "HTMLCharacterReference.gperf"
      {"&ni;", "∋"},
      {""}, {""}, {""}, {""}, {""},
#line 2027 "HTMLCharacterReference.gperf"
      {"&vee;", "∨"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1018 "HTMLCharacterReference.gperf"
      {"&lsimg;", "⪏"},
      {""}, {""}, {""}, {""},
#line 395 "HTMLCharacterReference.gperf"
      {"&diams;", "♦"},
#line 1751 "HTMLCharacterReference.gperf"
      {"&SquareUnion;", "⊔"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 337 "HTMLCharacterReference.gperf"
      {"&cupdot;", "⊍"},
      {""},
#line 489 "HTMLCharacterReference.gperf"
      {"&Efr;", "𝔈"},
#line 1288 "HTMLCharacterReference.gperf"
      {"&nsupseteq;", "⊉"},
#line 1289 "HTMLCharacterReference.gperf"
      {"&nsupseteqq;", "⫆̸"},
#line 1105 "HTMLCharacterReference.gperf"
      {"&ncaron;", "ň"},
      {""}, {""}, {""},
#line 86 "HTMLCharacterReference.gperf"
      {"&aring;", "å"},
      {""}, {""}, {""},
#line 267 "HTMLCharacterReference.gperf"
      {"&circ;", "ˆ"},
      {""}, {""}, {""},
#line 1150 "HTMLCharacterReference.gperf"
      {"&nis;", "⋼"},
      {""},
#line 1833 "HTMLCharacterReference.gperf"
      {"&swarr;", "↙"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1248 "HTMLCharacterReference.gperf"
      {"&npolint;", "⨔"},
      {""}, {""},
#line 483 "HTMLCharacterReference.gperf"
      {"&eDDot;", "⩷"},
#line 1008 "HTMLCharacterReference.gperf"
      {"&lrhard;", "⥭"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1487 "HTMLCharacterReference.gperf"
      {"&qint;", "⨌"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1104 "HTMLCharacterReference.gperf"
      {"&Ncaron;", "Ň"},
      {""}, {""}, {""}, {""}, {""},
#line 1821 "HTMLCharacterReference.gperf"
      {"&supplus;", "⫀"},
      {""},
#line 639 "HTMLCharacterReference.gperf"
      {"&gjcy;", "ѓ"},
      {""}, {""},
#line 1494 "HTMLCharacterReference.gperf"
      {"&quatint;", "⨖"},
      {""},
#line 957 "HTMLCharacterReference.gperf"
      {"&ljcy;", "љ"},
      {""}, {""},
#line 786 "HTMLCharacterReference.gperf"
      {"&isindot;", "⋵"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 405 "HTMLCharacterReference.gperf"
      {"&djcy;", "ђ"},
      {""}, {""}, {""},
#line 2026 "HTMLCharacterReference.gperf"
      {"&Vee;", "⋁"},
      {""},
#line 515 "HTMLCharacterReference.gperf"
      {"&Eogon;", "Ę"},
#line 932 "HTMLCharacterReference.gperf"
      {"&lesges;", "⪓"},
      {""}, {""},
#line 1264 "HTMLCharacterReference.gperf"
      {"&nsce;", "⪰̸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 660 "HTMLCharacterReference.gperf"
      {"&GreaterTilde;", "≳"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 874 "HTMLCharacterReference.gperf"
      {"&lcub;", "{"},
#line 1984 "HTMLCharacterReference.gperf"
      {"&Uring;", "Ů"},
      {""}, {""}, {""}, {""}, {""},
#line 406 "HTMLCharacterReference.gperf"
      {"&dlcorn;", "⌞"},
      {""}, {""},
#line 887 "HTMLCharacterReference.gperf"
      {"&Leftarrow;", "⇐"},
#line 1190 "HTMLCharacterReference.gperf"
      {"&NotGreater;", "≯"},
#line 2054 "HTMLCharacterReference.gperf"
      {"&vsupne;", "⊋︀"},
#line 249 "HTMLCharacterReference.gperf"
      {"&ccupssm;", "⩐"},
      {""},
#line 2136 "HTMLCharacterReference.gperf"
      {"&ZHcy;", "Ж"},
#line 1683 "HTMLCharacterReference.gperf"
      {"&sharp;", "♯"},
      {""}, {""}, {""}, {""}, {""},
#line 407 "HTMLCharacterReference.gperf"
      {"&dlcrop;", "⌍"},
      {""},
#line 60 "HTMLCharacterReference.gperf"
      {"&angmsdac;", "⦪"},
#line 1679 "HTMLCharacterReference.gperf"
      {"&sext;", "✶"},
      {""}, {""}, {""}, {""}, {""},
#line 1819 "HTMLCharacterReference.gperf"
      {"&supnE;", "⫌"},
#line 1759 "HTMLCharacterReference.gperf"
      {"&sstarf;", "⋆"},
      {""}, {""}, {""}, {""},
#line 994 "HTMLCharacterReference.gperf"
      {"&loplus;", "⨭"},
      {""}, {""},
#line 1971 "HTMLCharacterReference.gperf"
      {"&UpperLeftArrow;", "↖"},
      {""}, {""},
#line 1279 "HTMLCharacterReference.gperf"
      {"&nsubset;", "⊂⃒"},
#line 1152 "HTMLCharacterReference.gperf"
      {"&niv;", "∋"},
#line 2050 "HTMLCharacterReference.gperf"
      {"&vscr;", "𝓋"},
      {""},
#line 761 "HTMLCharacterReference.gperf"
      {"&inodot;", "ı"},
#line 523 "HTMLCharacterReference.gperf"
      {"&Epsilon;", "Ε"},
      {""},
#line 339 "HTMLCharacterReference.gperf"
      {"&cups;", "∪︀"},
      {""},
#line 22 "HTMLCharacterReference.gperf"
      {"&Abreve;", "Ă"},
#line 1421 "HTMLCharacterReference.gperf"
      {"&Pi;", "Π"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 94 "HTMLCharacterReference.gperf"
      {"&atilde;", "ã"},
      {""},
#line 1949 "HTMLCharacterReference.gperf"
      {"&UnderBar;", "_"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 531 "HTMLCharacterReference.gperf"
      {"&Equal;", "⩵"},
      {""},
#line 433 "HTMLCharacterReference.gperf"
      {"&DoubleUpDownArrow;", "⇕"},
#line 538 "HTMLCharacterReference.gperf"
      {"&eqvparsl;", "⧥"},
#line 1675 "HTMLCharacterReference.gperf"
      {"&semi;", ";"},
      {""}, {""}, {""}, {""}, {""},
#line 170 "HTMLCharacterReference.gperf"
      {"&boxdR;", "╒"},
      {""}, {""}, {""},
#line 81 "HTMLCharacterReference.gperf"
      {"&apos;", "'"},
      {""},
#line 2051 "HTMLCharacterReference.gperf"
      {"&vsubnE;", "⫋︀"},
#line 2081 "HTMLCharacterReference.gperf"
      {"&Xi;", "Ξ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 264 "HTMLCharacterReference.gperf"
      {"&Chi;", "Χ"},
      {""},
#line 1678 "HTMLCharacterReference.gperf"
      {"&setmn;", "∖"},
      {""},
#line 302 "HTMLCharacterReference.gperf"
      {"&congdot;", "⩭"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 908 "HTMLCharacterReference.gperf"
      {"&LeftTee;", "⊣"},
      {""}, {""}, {""},
#line 1323 "HTMLCharacterReference.gperf"
      {"&nwnear;", "⤧"},
      {""}, {""},
#line 1157 "HTMLCharacterReference.gperf"
      {"&nldr;", "‥"},
#line 599 "HTMLCharacterReference.gperf"
      {"&frasl;", "⁄"},
#line 334 "HTMLCharacterReference.gperf"
      {"&CupCap;", "≍"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 821 "HTMLCharacterReference.gperf"
      {"&kfr;", "𝔨"},
      {""}, {""}, {""}, {""},
#line 133 "HTMLCharacterReference.gperf"
      {"&bigoplus;", "⨁"},
#line 2049 "HTMLCharacterReference.gperf"
      {"&Vscr;", "𝒱"},
      {""},
#line 1752 "HTMLCharacterReference.gperf"
      {"&squarf;", "▪"},
      {""}, {""},
#line 51 "HTMLCharacterReference.gperf"
      {"&andd;", "⩜"},
      {""},
#line 2124 "HTMLCharacterReference.gperf"
      {"&Zcaron;", "Ž"},
      {""},
#line 1214 "HTMLCharacterReference.gperf"
      {"&NotNestedGreaterGreater;", "⪢̸"},
#line 890 "HTMLCharacterReference.gperf"
      {"&LeftArrowRightArrow;", "⇆"},
#line 288 "HTMLCharacterReference.gperf"
      {"&clubs;", "♣"},
      {""}, {""}, {""},
#line 773 "HTMLCharacterReference.gperf"
      {"&IOcy;", "Ё"},
      {""},
#line 1990 "HTMLCharacterReference.gperf"
      {"&Utilde;", "Ũ"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 961 "HTMLCharacterReference.gperf"
      {"&llcorner;", "⌞"},
      {""}, {""}, {""}, {""}, {""},
#line 1166 "HTMLCharacterReference.gperf"
      {"&nleqslant;", "⩽̸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1558 "HTMLCharacterReference.gperf"
      {"&REG;", "®"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1758 "HTMLCharacterReference.gperf"
      {"&ssmile;", "⌣"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 268 "HTMLCharacterReference.gperf"
      {"&circeq;", "≗"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2112 "HTMLCharacterReference.gperf"
      {"&YIcy;", "Ї"},
      {""},
#line 530 "HTMLCharacterReference.gperf"
      {"&eqslantless;", "⪕"},
      {""}, {""}, {""}, {""},
#line 1261 "HTMLCharacterReference.gperf"
      {"&nrtrie;", "⋭"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 793 "HTMLCharacterReference.gperf"
      {"&itilde;", "ĩ"},
#line 1253 "HTMLCharacterReference.gperf"
      {"&npreceq;", "⪯̸"},
      {""}, {""}, {""}, {""}, {""},
#line 645 "HTMLCharacterReference.gperf"
      {"&gnapprox;", "⪊"},
      {""}, {""}, {""}, {""},
#line 970 "HTMLCharacterReference.gperf"
      {"&lnapprox;", "⪉"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 432 "HTMLCharacterReference.gperf"
      {"&DoubleUpArrow;", "⇑"},
      {""}, {""},
#line 658 "HTMLCharacterReference.gperf"
      {"&GreaterLess;", "≷"},
      {""},
#line 820 "HTMLCharacterReference.gperf"
      {"&Kfr;", "𝔎"},
#line 2099 "HTMLCharacterReference.gperf"
      {"&xvee;", "⋁"},
      {""}, {""}, {""}, {""}, {""},
#line 317 "HTMLCharacterReference.gperf"
      {"&cross;", "✗"},
      {""}, {""}, {""}, {""},
#line 166 "HTMLCharacterReference.gperf"
      {"&boxdL;", "╕"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1475 "HTMLCharacterReference.gperf"
      {"&Proportion;", "∷"},
      {""}, {""}, {""}, {""}, {""},
#line 1011 "HTMLCharacterReference.gperf"
      {"&lsaquo;", "‹"},
      {""}, {""},
#line 1154 "HTMLCharacterReference.gperf"
      {"&njcy;", "њ"},
      {""},
#line 1307 "HTMLCharacterReference.gperf"
      {"&nvdash;", "⊬"},
      {""},
#line 453 "HTMLCharacterReference.gperf"
      {"&drbkarow;", "⤐"},
#line 419 "HTMLCharacterReference.gperf"
      {"&dotsquare;", "⊡"},
      {""}, {""},
#line 537 "HTMLCharacterReference.gperf"
      {"&equivDD;", "⩸"},
      {""}, {""}, {""},
#line 627 "HTMLCharacterReference.gperf"
      {"&gesdot;", "⪀"},
      {""}, {""},
#line 1667 "HTMLCharacterReference.gperf"
      {"&sdot;", "⋅"},
      {""},
#line 928 "HTMLCharacterReference.gperf"
      {"&lesdot;", "⩿"},
      {""}, {""}, {""}, {""},
#line 1388 "HTMLCharacterReference.gperf"
      {"&Otilde;", "Õ"},
#line 909 "HTMLCharacterReference.gperf"
      {"&LeftTeeArrow;", "↤"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 336 "HTMLCharacterReference.gperf"
      {"&cupcup;", "⩊"},
      {""}, {""}, {""},
#line 85 "HTMLCharacterReference.gperf"
      {"&Aring;", "Å"},
      {""}, {""}, {""},
#line 900 "HTMLCharacterReference.gperf"
      {"&leftleftarrows;", "⇇"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 477 "HTMLCharacterReference.gperf"
      {"&ecir;", "≖"},
#line 1672 "HTMLCharacterReference.gperf"
      {"&searr;", "↘"},
#line 495 "HTMLCharacterReference.gperf"
      {"&egsdot;", "⪘"},
      {""}, {""}, {""}, {""}, {""},
#line 253 "HTMLCharacterReference.gperf"
      {"&Cedilla;", "¸"},
      {""}, {""},
#line 70 "HTMLCharacterReference.gperf"
      {"&angst;", "Å"},
      {""}, {""}, {""}, {""},
#line 328 "HTMLCharacterReference.gperf"
      {"&cuesc;", "⋟"},
      {""}, {""}, {""}, {""},
#line 112 "HTMLCharacterReference.gperf"
      {"&bcong;", "≌"},
      {""}, {""}, {""}, {""},
#line 1302 "HTMLCharacterReference.gperf"
      {"&numsp;", " "},
      {""}, {""},
#line 1457 "HTMLCharacterReference.gperf"
      {"&PrecedesTilde;", "≾"},
      {""}, {""},
#line 1292 "HTMLCharacterReference.gperf"
      {"&ntilde;", "ñ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 260 "HTMLCharacterReference.gperf"
      {"&CHcy;", "Ч"},
#line 151 "HTMLCharacterReference.gperf"
      {"&blk14;", "░"},
      {""}, {""}, {""},
#line 1756 "HTMLCharacterReference.gperf"
      {"&sscr;", "𝓈"},
#line 1127 "HTMLCharacterReference.gperf"
      {"&NestedGreaterGreater;", "≫"},
      {""},
#line 398 "HTMLCharacterReference.gperf"
      {"&digamma;", "ϝ"},
#line 930 "HTMLCharacterReference.gperf"
      {"&lesdotor;", "⪃"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 582 "HTMLCharacterReference.gperf"
      {"&Fouriertrf;", "ℱ"},
      {""}, {""}, {""},
#line 897 "HTMLCharacterReference.gperf"
      {"&LeftFloor;", "⌊"},
#line 2098 "HTMLCharacterReference.gperf"
      {"&xutri;", "△"},
      {""}, {""}, {""}, {""}, {""},
#line 1291 "HTMLCharacterReference.gperf"
      {"&Ntilde;", "Ñ"},
      {""}, {""}, {""},
#line 150 "HTMLCharacterReference.gperf"
      {"&blk12;", "▒"},
      {""}, {""}, {""}, {""},
#line 953 "HTMLCharacterReference.gperf"
      {"&lharu;", "↼"},
      {""}, {""}, {""},
#line 1280 "HTMLCharacterReference.gperf"
      {"&nsubseteq;", "⊈"},
#line 1281 "HTMLCharacterReference.gperf"
      {"&nsubseteqq;", "⫅̸"},
      {""}, {""}, {""}, {""},
#line 1730 "HTMLCharacterReference.gperf"
      {"&sqcap;", "⊓"},
#line 687 "HTMLCharacterReference.gperf"
      {"&HARDcy;", "Ъ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 152 "HTMLCharacterReference.gperf"
      {"&blk34;", "▓"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 435 "HTMLCharacterReference.gperf"
      {"&DownArrow;", "↓"},
      {""},
#line 954 "HTMLCharacterReference.gperf"
      {"&lharul;", "⥪"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 760 "HTMLCharacterReference.gperf"
      {"&infintie;", "⧝"},
#line 2073 "HTMLCharacterReference.gperf"
      {"&xcap;", "⋂"},
      {""}, {""},
#line 438 "HTMLCharacterReference.gperf"
      {"&DownArrowBar;", "⤓"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 2084 "HTMLCharacterReference.gperf"
      {"&xlarr;", "⟵"},
#line 1745 "HTMLCharacterReference.gperf"
      {"&square;", "□"},
      {""}, {""}, {""}, {""},
#line 93 "HTMLCharacterReference.gperf"
      {"&Atilde;", "Ã"},
      {""}, {""}, {""},
#line 613 "HTMLCharacterReference.gperf"
      {"&gcirc;", "ĝ"},
      {""}, {""}, {""}, {""},
#line 861 "HTMLCharacterReference.gperf"
      {"&lBarr;", "⤎"},
#line 1731 "HTMLCharacterReference.gperf"
      {"&sqcaps;", "⊓︀"},
      {""}, {""}, {""},
#line 479 "HTMLCharacterReference.gperf"
      {"&ecirc;", "ê"},
#line 1125 "HTMLCharacterReference.gperf"
      {"&nesear;", "⤨"},
      {""}, {""},
#line 484 "HTMLCharacterReference.gperf"
      {"&Edot;", "Ė"},
#line 1205 "HTMLCharacterReference.gperf"
      {"&NotLeftTriangle;", "⋪"},
#line 996 "HTMLCharacterReference.gperf"
      {"&lowast;", "∗"},
      {""},
#line 1206 "HTMLCharacterReference.gperf"
      {"&NotLeftTriangleBar;", "⧏̸"},
#line 404 "HTMLCharacterReference.gperf"
      {"&DJcy;", "Ђ"},
#line 1207 "HTMLCharacterReference.gperf"
      {"&NotLeftTriangleEqual;", "⋬"},
      {""}, {""}, {""}, {""},
#line 798 "HTMLCharacterReference.gperf"
      {"&Jcirc;", "Ĵ"},
      {""},
#line 393 "HTMLCharacterReference.gperf"
      {"&diamond;", "⋄"},
      {""},
#line 1811 "HTMLCharacterReference.gperf"
      {"&supe;", "⊇"},
#line 28 "HTMLCharacterReference.gperf"
      {"&acirc;", "â"},
#line 1030 "HTMLCharacterReference.gperf"
      {"&lthree;", "⋋"},
#line 1215 "HTMLCharacterReference.gperf"
      {"&NotNestedLessLess;", "⪡̸"},
      {""}, {""}, {""},
#line 241 "HTMLCharacterReference.gperf"
      {"&Ccaron;", "Č"},
      {""}, {""},
#line 303 "HTMLCharacterReference.gperf"
      {"&Congruent;", "≡"},
#line 964 "HTMLCharacterReference.gperf"
      {"&lltri;", "◺"},
      {""},
#line 1817 "HTMLCharacterReference.gperf"
      {"&suplarr;", "⥻"},
      {""},
#line 2075 "HTMLCharacterReference.gperf"
      {"&xcup;", "⋃"},
      {""}, {""}, {""}, {""},
#line 1787 "HTMLCharacterReference.gperf"
      {"&succ;", "≻"},
      {""},
#line 1684 "HTMLCharacterReference.gperf"
      {"&SHCHcy;", "Щ"},
      {""}, {""}, {""}, {""},
#line 2052 "HTMLCharacterReference.gperf"
      {"&vsubne;", "⊊︀"},
      {""}, {""}, {""},
#line 696 "HTMLCharacterReference.gperf"
      {"&hcirc;", "ĥ"},
      {""}, {""}, {""}, {""},
#line 2048 "HTMLCharacterReference.gperf"
      {"&vrtri;", "⊳"},
      {""}, {""}, {""}, {""}, {""},
#line 394 "HTMLCharacterReference.gperf"
      {"&diamondsuit;", "♦"},
#line 628 "HTMLCharacterReference.gperf"
      {"&gesdoto;", "⪂"},
      {""}, {""},
#line 1669 "HTMLCharacterReference.gperf"
      {"&sdote;", "⩦"},
#line 1493 "HTMLCharacterReference.gperf"
      {"&quaternions;", "ℍ"},
#line 929 "HTMLCharacterReference.gperf"
      {"&lesdoto;", "⪁"},
      {""}, {""},
#line 246 "HTMLCharacterReference.gperf"
      {"&ccirc;", "ĉ"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 392 "HTMLCharacterReference.gperf"
      {"&Diamond;", "⋄"},
      {""}, {""},
#line 743 "HTMLCharacterReference.gperf"
      {"&iiota;", "℩"},
      {""}, {""}, {""}, {""},
#line 1692 "HTMLCharacterReference.gperf"
      {"&ShortRightArrow;", "→"},
#line 792 "HTMLCharacterReference.gperf"
      {"&Itilde;", "Ĩ"},
      {""}, {""},
#line 956 "HTMLCharacterReference.gperf"
      {"&LJcy;", "Љ"},
      {""}, {""}, {""}, {""},
#line 541 "HTMLCharacterReference.gperf"
      {"&Escr;", "ℰ"},
#line 637 "HTMLCharacterReference.gperf"
      {"&gimel;", "ℷ"},
#line 1721 "HTMLCharacterReference.gperf"
      {"&softcy;", "ь"},
#line 2082 "HTMLCharacterReference.gperf"
      {"&xi;", "ξ"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 347 "HTMLCharacterReference.gperf"
      {"&curvearrowleft;", "↶"},
#line 1495 "HTMLCharacterReference.gperf"
      {"&quest;", "?"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 512 "HTMLCharacterReference.gperf"
      {"&ENG;", "Ŋ"},
      {""}, {""}, {""}, {""}, {""},
#line 1194 "HTMLCharacterReference.gperf"
      {"&NotGreaterLess;", "≹"},
#line 1925 "HTMLCharacterReference.gperf"
      {"&Ucirc;", "Û"},
#line 2059 "HTMLCharacterReference.gperf"
      {"&wedbar;", "⩟"},
      {""}, {""}, {""},
#line 799 "HTMLCharacterReference.gperf"
      {"&jcirc;", "ĵ"},
      {""},
#line 1960 "HTMLCharacterReference.gperf"
      {"&Uparrow;", "⇑"},
      {""},
#line 80 "HTMLCharacterReference.gperf"
      {"&apid;", "≋"},
      {""}, {""}, {""},
#line 1790 "HTMLCharacterReference.gperf"
      {"&Succeeds;", "≻"},
      {""}, {""}, {""}, {""}, {""},
#line 281 "HTMLCharacterReference.gperf"
      {"&cire;", "≗"},
      {""}, {""}, {""},
#line 981 "HTMLCharacterReference.gperf"
      {"&longleftarrow;", "⟵"},
      {""}, {""},
#line 1305 "HTMLCharacterReference.gperf"
      {"&nVdash;", "⊮"},
      {""}, {""}, {""},
#line 2105 "HTMLCharacterReference.gperf"
      {"&Ycirc;", "Ŷ"},
      {""},
#line 1085 "HTMLCharacterReference.gperf"
      {"&mu;", "μ"},
#line 1992 "HTMLCharacterReference.gperf"
      {"&utri;", "▵"},
      {""},
#line 1108 "HTMLCharacterReference.gperf"
      {"&ncong;", "≇"},
      {""}, {""}, {""}, {""},
#line 1020 "HTMLCharacterReference.gperf"
      {"&lsquo;", "‘"},
#line 1021 "HTMLCharacterReference.gperf"
      {"&lsquor;", "‚"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2106 "HTMLCharacterReference.gperf"
      {"&ycirc;", "ŷ"},
      {""},
#line 1080 "HTMLCharacterReference.gperf"
      {"&mp;", "∓"},
      {""},
#line 638 "HTMLCharacterReference.gperf"
      {"&GJcy;", "Ѓ"},
#line 919 "HTMLCharacterReference.gperf"
      {"&LeftVector;", "↼"},
      {""}, {""}, {""}, {""}, {""},
#line 1822 "HTMLCharacterReference.gperf"
      {"&Supset;", "⋑"},
#line 132 "HTMLCharacterReference.gperf"
      {"&bigodot;", "⨀"},
      {""}, {""},
#line 985 "HTMLCharacterReference.gperf"
      {"&longmapsto;", "⟼"},
      {""}, {""},
#line 1996 "HTMLCharacterReference.gperf"
      {"&uuml;", "ü"},
#line 553 "HTMLCharacterReference.gperf"
      {"&excl;", "!"},
#line 727 "HTMLCharacterReference.gperf"
      {"&icirc;", "î"},
      {""}, {""},
#line 1974 "HTMLCharacterReference.gperf"
      {"&upsi;", "υ"},
#line 544 "HTMLCharacterReference.gperf"
      {"&Esim;", "⩳"},
      {""}, {""},
#line 1202 "HTMLCharacterReference.gperf"
      {"&notinva;", "∉"},
      {""}, {""},
#line 1530 "HTMLCharacterReference.gperf"
      {"&RBarr;", "⤐"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1254 "HTMLCharacterReference.gperf"
      {"&nrArr;", "⇏"},
#line 439 "HTMLCharacterReference.gperf"
      {"&DownArrowUpArrow;", "⇵"},
      {""}, {""}, {""},
#line 91 "HTMLCharacterReference.gperf"
      {"&asymp;", "≈"},
      {""}, {""},
#line 910 "HTMLCharacterReference.gperf"
      {"&LeftTeeVector;", "⥚"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 612 "HTMLCharacterReference.gperf"
      {"&Gcirc;", "Ĝ"},
      {""}, {""}, {""}, {""},
#line 188 "HTMLCharacterReference.gperf"
      {"&boxul;", "┘"},
      {""}, {""}, {""},
#line 310 "HTMLCharacterReference.gperf"
      {"&Coproduct;", "∐"},
#line 1658 "HTMLCharacterReference.gperf"
      {"&Scirc;", "Ŝ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 192 "HTMLCharacterReference.gperf"
      {"&boxur;", "└"},
#line 2063 "HTMLCharacterReference.gperf"
      {"&weierp;", "℘"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 2061 "HTMLCharacterReference.gperf"
      {"&wedge;", "∧"},
      {""}, {""}, {""}, {""},
#line 1328 "HTMLCharacterReference.gperf"
      {"&Ocirc;", "Ô"},
      {""},
#line 719 "HTMLCharacterReference.gperf"
      {"&HumpDownHump;", "≎"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 2058 "HTMLCharacterReference.gperf"
      {"&wcirc;", "ŵ"},
      {""}, {""},
#line 765 "HTMLCharacterReference.gperf"
      {"&integers;", "ℤ"},
#line 1674 "HTMLCharacterReference.gperf"
      {"&sect;", "§"},
      {""}, {""}, {""},
#line 980 "HTMLCharacterReference.gperf"
      {"&Longleftarrow;", "⟸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1802 "HTMLCharacterReference.gperf"
      {"&sung;", "♪"},
      {""},
#line 2062 "HTMLCharacterReference.gperf"
      {"&wedgeq;", "≙"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 698 "HTMLCharacterReference.gperf"
      {"&heartsuit;", "♥"},
#line 1695 "HTMLCharacterReference.gperf"
      {"&Sigma;", "Σ"},
      {""},
#line 1649 "HTMLCharacterReference.gperf"
      {"&sc;", "≻"},
      {""},
#line 1153 "HTMLCharacterReference.gperf"
      {"&NJcy;", "Њ"},
#line 2029 "HTMLCharacterReference.gperf"
      {"&veeeq;", "≚"},
      {""}, {""}, {""},
#line 1367 "HTMLCharacterReference.gperf"
      {"&OpenCurlyQuote;", "‘"},
      {""},
#line 1083 "HTMLCharacterReference.gperf"
      {"&mstpos;", "∾"},
#line 1935 "HTMLCharacterReference.gperf"
      {"&ufr;", "𝔲"},
      {""},
#line 2113 "HTMLCharacterReference.gperf"
      {"&yicy;", "ї"},
      {""}, {""}, {""}, {""},
#line 1687 "HTMLCharacterReference.gperf"
      {"&shcy;", "ш"},
#line 1320 "HTMLCharacterReference.gperf"
      {"&nwArr;", "⇖"},
      {""}, {""},
#line 1062 "HTMLCharacterReference.gperf"
      {"&mfr;", "𝔪"},
#line 830 "HTMLCharacterReference.gperf"
      {"&kscr;", "𝓀"},
      {""},
#line 1794 "HTMLCharacterReference.gperf"
      {"&succeq;", "⪰"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 437 "HTMLCharacterReference.gperf"
      {"&downarrow;", "↓"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1173 "HTMLCharacterReference.gperf"
      {"&nltri;", "⋪"},
#line 83 "HTMLCharacterReference.gperf"
      {"&approx;", "≈"},
      {""}, {""}, {""},
#line 1668 "HTMLCharacterReference.gperf"
      {"&sdotb;", "⊡"},
#line 129 "HTMLCharacterReference.gperf"
      {"&bigcap;", "⋂"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1229 "HTMLCharacterReference.gperf"
      {"&NotSquareSuperset;", "⊐̸"},
      {""}, {""},
#line 1087 "HTMLCharacterReference.gperf"
      {"&mumap;", "⊸"},
      {""},
#line 1230 "HTMLCharacterReference.gperf"
      {"&NotSquareSupersetEqual;", "⋣"},
#line 1239 "HTMLCharacterReference.gperf"
      {"&NotTilde;", "≁"},
      {""}, {""}, {""},
#line 1322 "HTMLCharacterReference.gperf"
      {"&nwarrow;", "↖"},
      {""}, {""},
#line 741 "HTMLCharacterReference.gperf"
      {"&iiint;", "∭"},
#line 401 "HTMLCharacterReference.gperf"
      {"&divide;", "÷"},
      {""},
#line 1109 "HTMLCharacterReference.gperf"
      {"&ncongdot;", "⩭̸"},
      {""}, {""}, {""}, {""}, {""},
#line 1956 "HTMLCharacterReference.gperf"
      {"&uogon;", "ų"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 436 "HTMLCharacterReference.gperf"
      {"&Downarrow;", "⇓"},
      {""},
#line 421 "HTMLCharacterReference.gperf"
      {"&DoubleContourIntegral;", "∯"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1767 "HTMLCharacterReference.gperf"
      {"&sub;", "⊂"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1664 "HTMLCharacterReference.gperf"
      {"&scsim;", "≿"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 498 "HTMLCharacterReference.gperf"
      {"&elinters;", "⏧"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 27 "HTMLCharacterReference.gperf"
      {"&Acirc;", "Â"},
      {""}, {""},
#line 1294 "HTMLCharacterReference.gperf"
      {"&ntriangleleft;", "⋪"},
      {""},
#line 695 "HTMLCharacterReference.gperf"
      {"&Hcirc;", "Ĥ"},
      {""}, {""}, {""}, {""}, {""},
#line 1220 "HTMLCharacterReference.gperf"
      {"&NotPrecedes;", "⊀"},
      {""}, {""},
#line 829 "HTMLCharacterReference.gperf"
      {"&Kscr;", "𝒦"},
#line 1146 "HTMLCharacterReference.gperf"
      {"&nhArr;", "⇎"},
      {""}, {""},
#line 1691 "HTMLCharacterReference.gperf"
      {"&shortparallel;", "∥"},
      {""}, {""},
#line 740 "HTMLCharacterReference.gperf"
      {"&iiiint;", "⨌"},
#line 2138 "HTMLCharacterReference.gperf"
      {"&zigrarr;", "⇝"},
#line 1038 "HTMLCharacterReference.gperf"
      {"&lurdshar;", "⥊"},
#line 1994 "HTMLCharacterReference.gperf"
      {"&uuarr;", "⇈"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1063 "HTMLCharacterReference.gperf"
      {"&mho;", "℧"},
      {""}, {""},
#line 1977 "HTMLCharacterReference.gperf"
      {"&upsilon;", "υ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1139 "HTMLCharacterReference.gperf"
      {"&nges;", "⩾̸"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 501 "HTMLCharacterReference.gperf"
      {"&elsdot;", "⪗"},
      {""},
#line 1918 "HTMLCharacterReference.gperf"
      {"&uArr;", "⇑"},
      {""}, {""}, {""}, {""}, {""},
#line 1810 "HTMLCharacterReference.gperf"
      {"&supE;", "⫆"},
#line 1972 "HTMLCharacterReference.gperf"
      {"&UpperRightArrow;", "↗"},
      {""}, {""},
#line 416 "HTMLCharacterReference.gperf"
      {"&DotEqual;", "≐"},
#line 1650 "HTMLCharacterReference.gperf"
      {"&scap;", "⪸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 451 "HTMLCharacterReference.gperf"
      {"&DownTee;", "⊤"},
      {""}, {""}, {""}, {""},
#line 893 "HTMLCharacterReference.gperf"
      {"&LeftDoubleBracket;", "⟦"},
#line 1655 "HTMLCharacterReference.gperf"
      {"&sce;", "⪰"},
      {""},
#line 1710 "HTMLCharacterReference.gperf"
      {"&slarr;", "←"},
#line 140 "HTMLCharacterReference.gperf"
      {"&bigvee;", "⋁"},
      {""}, {""},
#line 935 "HTMLCharacterReference.gperf"
      {"&lesseqgtr;", "⋚"},
      {""}, {""}, {""},
#line 548 "HTMLCharacterReference.gperf"
      {"&ETH;", "Ð"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1186 "HTMLCharacterReference.gperf"
      {"&NotElement;", "∉"},
      {""}, {""},
#line 84 "HTMLCharacterReference.gperf"
      {"&approxeq;", "≊"},
      {""},
#line 726 "HTMLCharacterReference.gperf"
      {"&Icirc;", "Î"},
#line 492 "HTMLCharacterReference.gperf"
      {"&Egrave;", "È"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1660 "HTMLCharacterReference.gperf"
      {"&scnap;", "⪺"},
#line 283 "HTMLCharacterReference.gperf"
      {"&cirmid;", "⫯"},
      {""}, {""},
#line 422 "HTMLCharacterReference.gperf"
      {"&DoubleDot;", "¨"},
      {""},
#line 2032 "HTMLCharacterReference.gperf"
      {"&verbar;", "|"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1456 "HTMLCharacterReference.gperf"
      {"&PrecedesSlantEqual;", "≼"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2090 "HTMLCharacterReference.gperf"
      {"&xoplus;", "⨁"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 2076 "HTMLCharacterReference.gperf"
      {"&xdtri;", "▽"},
#line 2097 "HTMLCharacterReference.gperf"
      {"&xuplus;", "⨄"},
      {""},
#line 767 "HTMLCharacterReference.gperf"
      {"&intercal;", "⊺"},
#line 280 "HTMLCharacterReference.gperf"
      {"&cirE;", "⧃"},
      {""}, {""}, {""},
#line 1879 "HTMLCharacterReference.gperf"
      {"&top;", "⊤"},
      {""},
#line 423 "HTMLCharacterReference.gperf"
      {"&DoubleDownArrow;", "⇓"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 731 "HTMLCharacterReference.gperf"
      {"&IEcy;", "Е"},
      {""}, {""},
#line 558 "HTMLCharacterReference.gperf"
      {"&exponentiale;", "ⅇ"},
      {""}, {""},
#line 191 "HTMLCharacterReference.gperf"
      {"&boxuR;", "╘"},
      {""}, {""}, {""},
#line 1939 "HTMLCharacterReference.gperf"
      {"&uharl;", "↿"},
#line 316 "HTMLCharacterReference.gperf"
      {"&Cross;", "⨯"},
      {""},
#line 768 "HTMLCharacterReference.gperf"
      {"&Intersection;", "⋂"},
      {""}, {""}, {""},
#line 963 "HTMLCharacterReference.gperf"
      {"&llhard;", "⥫"},
      {""},
#line 52 "HTMLCharacterReference.gperf"
      {"&andslope;", "⩘"},
      {""}, {""}, {""},
#line 673 "HTMLCharacterReference.gperf"
      {"&gtquest;", "⩼"},
      {""},
#line 1940 "HTMLCharacterReference.gperf"
      {"&uharr;", "↾"},
      {""},
#line 2031 "HTMLCharacterReference.gperf"
      {"&Verbar;", "‖"},
#line 1033 "HTMLCharacterReference.gperf"
      {"&ltquest;", "⩻"},
      {""}, {""},
#line 179 "HTMLCharacterReference.gperf"
      {"&boxHu;", "╧"},
#line 1652 "HTMLCharacterReference.gperf"
      {"&scaron;", "š"},
      {""},
#line 1796 "HTMLCharacterReference.gperf"
      {"&succneqq;", "⪶"},
      {""},
#line 1774 "HTMLCharacterReference.gperf"
      {"&subne;", "⊊"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1705 "HTMLCharacterReference.gperf"
      {"&siml;", "⪝"},
      {""},
#line 2028 "HTMLCharacterReference.gperf"
      {"&veebar;", "⊻"},
      {""}, {""},
#line 427 "HTMLCharacterReference.gperf"
      {"&DoubleLongLeftArrow;", "⟸"},
      {""}, {""}, {""}, {""},
#line 428 "HTMLCharacterReference.gperf"
      {"&DoubleLongLeftRightArrow;", "⟺"},
#line 424 "HTMLCharacterReference.gperf"
      {"&DoubleLeftArrow;", "⇐"},
      {""}, {""}, {""},
#line 1296 "HTMLCharacterReference.gperf"
      {"&ntriangleright;", "⋫"},
      {""},
#line 1297 "HTMLCharacterReference.gperf"
      {"&ntrianglerighteq;", "⋭"},
      {""}, {""}, {""}, {""},
#line 1250 "HTMLCharacterReference.gperf"
      {"&nprcue;", "⋠"},
      {""}, {""}, {""}, {""}, {""},
#line 452 "HTMLCharacterReference.gperf"
      {"&DownTeeArrow;", "↧"},
#line 1699 "HTMLCharacterReference.gperf"
      {"&sim;", "∼"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1885 "HTMLCharacterReference.gperf"
      {"&tosa;", "⤩"},
      {""},
#line 680 "HTMLCharacterReference.gperf"
      {"&gtrsim;", "≳"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2060 "HTMLCharacterReference.gperf"
      {"&Wedge;", "⋀"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 69 "HTMLCharacterReference.gperf"
      {"&angsph;", "∢"},
      {""},
#line 1654 "HTMLCharacterReference.gperf"
      {"&scE;", "⪴"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 2057 "HTMLCharacterReference.gperf"
      {"&Wcirc;", "Ŵ"},
      {""}, {""}, {""}, {""},
#line 1116 "HTMLCharacterReference.gperf"
      {"&neArr;", "⇗"},
#line 656 "HTMLCharacterReference.gperf"
      {"&GreaterFullEqual;", "≧"},
#line 1001 "HTMLCharacterReference.gperf"
      {"&lozenge;", "◊"},
#line 1851 "HTMLCharacterReference.gperf"
      {"&tfr;", "𝔱"},
      {""}, {""}, {""}, {""}, {""},
#line 1841 "HTMLCharacterReference.gperf"
      {"&tbrk;", "⎴"},
#line 933 "HTMLCharacterReference.gperf"
      {"&lessapprox;", "⪅"},
      {""}, {""}, {""},
#line 824 "HTMLCharacterReference.gperf"
      {"&khcy;", "х"},
      {""}, {""}, {""}, {""},
#line 1989 "HTMLCharacterReference.gperf"
      {"&utdot;", "⋰"},
      {""},
#line 1446 "HTMLCharacterReference.gperf"
      {"&pr;", "≺"},
      {""},
#line 289 "HTMLCharacterReference.gperf"
      {"&clubsuit;", "♣"},
      {""}, {""},
#line 2096 "HTMLCharacterReference.gperf"
      {"&xsqcup;", "⨆"},
      {""}, {""}, {""},
#line 178 "HTMLCharacterReference.gperf"
      {"&boxHU;", "╩"},
#line 163 "HTMLCharacterReference.gperf"
      {"&boxbox;", "⧉"},
      {""}, {""}, {""},
#line 1195 "HTMLCharacterReference.gperf"
      {"&NotGreaterSlantEqual;", "⩾̸"},
#line 270 "HTMLCharacterReference.gperf"
      {"&circlearrowright;", "↻"},
#line 1775 "HTMLCharacterReference.gperf"
      {"&subplus;", "⪿"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 997 "HTMLCharacterReference.gperf"
      {"&lowbar;", "_"},
      {""},
#line 1938 "HTMLCharacterReference.gperf"
      {"&uHar;", "⥣"},
      {""}, {""}, {""},
#line 934 "HTMLCharacterReference.gperf"
      {"&lessdot;", "⋖"},
#line 1187 "HTMLCharacterReference.gperf"
      {"&NotEqual;", "≠"},
      {""}, {""},
#line 2055 "HTMLCharacterReference.gperf"
      {"&Vvdash;", "⊪"},
#line 1118 "HTMLCharacterReference.gperf"
      {"&nearrow;", "↗"},
      {""}, {""}, {""}, {""}, {""},
#line 608 "HTMLCharacterReference.gperf"
      {"&gap;", "⪆"},
#line 1192 "HTMLCharacterReference.gperf"
      {"&NotGreaterFullEqual;", "≧̸"},
      {""}, {""}, {""},
#line 842 "HTMLCharacterReference.gperf"
      {"&lap;", "⪅"},
      {""},
#line 1295 "HTMLCharacterReference.gperf"
      {"&ntrianglelefteq;", "⋬"},
      {""},
#line 1204 "HTMLCharacterReference.gperf"
      {"&notinvc;", "⋶"},
      {""}, {""},
#line 187 "HTMLCharacterReference.gperf"
      {"&boxuL;", "╛"},
      {""}, {""},
#line 1371 "HTMLCharacterReference.gperf"
      {"&or;", "∨"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1788 "HTMLCharacterReference.gperf"
      {"&succapprox;", "⪸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 256 "HTMLCharacterReference.gperf"
      {"&CenterDot;", "·"},
      {""},
#line 1306 "HTMLCharacterReference.gperf"
      {"&nvDash;", "⊭"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 789 "HTMLCharacterReference.gperf"
      {"&isinsv;", "⋳"},
#line 1812 "HTMLCharacterReference.gperf"
      {"&supedot;", "⫄"},
      {""}, {""},
#line 936 "HTMLCharacterReference.gperf"
      {"&lesseqqgtr;", "⪋"},
#line 475 "HTMLCharacterReference.gperf"
      {"&Ecaron;", "Ě"},
      {""},
#line 856 "HTMLCharacterReference.gperf"
      {"&lat;", "⪫"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1773 "HTMLCharacterReference.gperf"
      {"&subnE;", "⫋"},
      {""}, {""}, {""},
#line 1878 "HTMLCharacterReference.gperf"
      {"&toea;", "⤨"},
#line 878 "HTMLCharacterReference.gperf"
      {"&ldquo;", "“"},
#line 879 "HTMLCharacterReference.gperf"
      {"&ldquor;", "„"},
#line 1496 "HTMLCharacterReference.gperf"
      {"&questeq;", "≟"},
      {""}, {""}, {""}, {""}, {""},
#line 979 "HTMLCharacterReference.gperf"
      {"&LongLeftArrow;", "⟵"},
#line 1478 "HTMLCharacterReference.gperf"
      {"&prsim;", "≾"},
      {""}, {""}, {""}, {""}, {""},
#line 238 "HTMLCharacterReference.gperf"
      {"&caron;", "ˇ"},
      {""}, {""},
#line 229 "HTMLCharacterReference.gperf"
      {"&cap;", "∩"},
      {""},
#line 1762 "HTMLCharacterReference.gperf"
      {"&starf;", "★"},
      {""}, {""}, {""},
#line 895 "HTMLCharacterReference.gperf"
      {"&LeftDownVector;", "⇃"},
      {""}, {""},
#line 896 "HTMLCharacterReference.gperf"
      {"&LeftDownVectorBar;", "⥙"},
#line 276 "HTMLCharacterReference.gperf"
      {"&circledS;", "Ⓢ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1474 "HTMLCharacterReference.gperf"
      {"&prop;", "∝"},
      {""},
#line 115 "HTMLCharacterReference.gperf"
      {"&bdquo;", "„"},
#line 1808 "HTMLCharacterReference.gperf"
      {"&supdot;", "⪾"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1394 "HTMLCharacterReference.gperf"
      {"&ouml;", "ö"},
      {""}, {""}, {""}, {""},
#line 2092 "HTMLCharacterReference.gperf"
      {"&xrArr;", "⟹"},
      {""}, {""}, {""},
#line 847 "HTMLCharacterReference.gperf"
      {"&larr;", "←"},
      {""},
#line 1930 "HTMLCharacterReference.gperf"
      {"&Udblac;", "Ű"},
      {""}, {""}, {""}, {""}, {""},
#line 1177 "HTMLCharacterReference.gperf"
      {"&NoBreak;", "⁠"},
      {""},
#line 359 "HTMLCharacterReference.gperf"
      {"&darr;", "↓"},
#line 245 "HTMLCharacterReference.gperf"
      {"&Ccirc;", "Ĉ"},
#line 1174 "HTMLCharacterReference.gperf"
      {"&nltrie;", "⋬"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1387 "HTMLCharacterReference.gperf"
      {"&osol;", "⊘"},
      {""}, {""}, {""}, {""},
#line 1707 "HTMLCharacterReference.gperf"
      {"&simne;", "≆"},
      {""}, {""},
#line 1447 "HTMLCharacterReference.gperf"
      {"&prap;", "⪷"},
      {""}, {""},
#line 600 "HTMLCharacterReference.gperf"
      {"&frown;", "⌢"},
      {""}, {""}, {""},
#line 1227 "HTMLCharacterReference.gperf"
      {"&NotSquareSubset;", "⊏̸"},
      {""}, {""}, {""}, {""},
#line 1228 "HTMLCharacterReference.gperf"
      {"&NotSquareSubsetEqual;", "⋢"},
      {""},
#line 1450 "HTMLCharacterReference.gperf"
      {"&pre;", "⪯"},
      {""}, {""}, {""},
#line 116 "HTMLCharacterReference.gperf"
      {"&becaus;", "∵"},
      {""}, {""}, {""}, {""}, {""},
#line 92 "HTMLCharacterReference.gperf"
      {"&asympeq;", "≍"},
      {""},
#line 690 "HTMLCharacterReference.gperf"
      {"&harr;", "↔"},
#line 1981 "HTMLCharacterReference.gperf"
      {"&urcorn;", "⌝"},
#line 852 "HTMLCharacterReference.gperf"
      {"&larrlp;", "↫"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1850 "HTMLCharacterReference.gperf"
      {"&Tfr;", "𝔗"},
      {""},
#line 1867 "HTMLCharacterReference.gperf"
      {"&thorn;", "þ"},
      {""}, {""}, {""},
#line 357 "HTMLCharacterReference.gperf"
      {"&Darr;", "↡"},
#line 1983 "HTMLCharacterReference.gperf"
      {"&urcrop;", "⌎"},
#line 853 "HTMLCharacterReference.gperf"
      {"&larrpl;", "⤹"},
#line 1415 "HTMLCharacterReference.gperf"
      {"&pfr;", "𝔭"},
      {""},
#line 1381 "HTMLCharacterReference.gperf"
      {"&orv;", "⩛"},
      {""}, {""}, {""},
#line 1988 "HTMLCharacterReference.gperf"
      {"&uscr;", "𝓊"},
      {""},
#line 1366 "HTMLCharacterReference.gperf"
      {"&OpenCurlyDoubleQuote;", "“"},
      {""},
#line 1476 "HTMLCharacterReference.gperf"
      {"&Proportional;", "∝"},
      {""},
#line 1466 "HTMLCharacterReference.gperf"
      {"&prnap;", "⪹"},
      {""}, {""}, {""}, {""},
#line 1082 "HTMLCharacterReference.gperf"
      {"&mscr;", "𝓂"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 474 "HTMLCharacterReference.gperf"
      {"&easter;", "⩮"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 285 "HTMLCharacterReference.gperf"
      {"&ClockwiseContourIntegral;", "∲"},
      {""}, {""}, {""}, {""},
#line 1151 "HTMLCharacterReference.gperf"
      {"&nisd;", "⋺"},
#line 1379 "HTMLCharacterReference.gperf"
      {"&oror;", "⩖"},
      {""}, {""}, {""}, {""},
#line 2074 "HTMLCharacterReference.gperf"
      {"&xcirc;", "◯"},
      {""}, {""}, {""},
#line 1128 "HTMLCharacterReference.gperf"
      {"&NestedLessLess;", "≪"},
      {""}, {""}, {""}, {""},
#line 1917 "HTMLCharacterReference.gperf"
      {"&Uarr;", "↟"},
      {""}, {""}, {""}, {""},
#line 1342 "HTMLCharacterReference.gperf"
      {"&ofr;", "𝔬"},
#line 1477 "HTMLCharacterReference.gperf"
      {"&propto;", "∝"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1793 "HTMLCharacterReference.gperf"
      {"&SucceedsTilde;", "≿"},
      {""}, {""},
#line 556 "HTMLCharacterReference.gperf"
      {"&expectation;", "ℰ"},
      {""}, {""}, {""}, {""}, {""},
#line 1708 "HTMLCharacterReference.gperf"
      {"&simplus;", "⨤"},
      {""},
#line 845 "HTMLCharacterReference.gperf"
      {"&Larr;", "↞"},
      {""},
#line 1333 "HTMLCharacterReference.gperf"
      {"&Odblac;", "Ő"},
      {""}, {""},
#line 823 "HTMLCharacterReference.gperf"
      {"&KHcy;", "Х"},
      {""}, {""},
#line 1209 "HTMLCharacterReference.gperf"
      {"&NotLessEqual;", "≰"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 855 "HTMLCharacterReference.gperf"
      {"&larrtl;", "↢"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 350 "HTMLCharacterReference.gperf"
      {"&cuwed;", "⋏"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 629 "HTMLCharacterReference.gperf"
      {"&gesdotol;", "⪄"},
      {""}, {""},
#line 686 "HTMLCharacterReference.gperf"
      {"&hamilt;", "ℋ"},
      {""},
#line 1382 "HTMLCharacterReference.gperf"
      {"&oS;", "Ⓢ"},
      {""}, {""}, {""}, {""}, {""},
#line 1167 "HTMLCharacterReference.gperf"
      {"&nles;", "⩽̸"},
      {""},
#line 430 "HTMLCharacterReference.gperf"
      {"&DoubleRightArrow;", "⇒"},
      {""}, {""}, {""}, {""}, {""},
#line 497 "HTMLCharacterReference.gperf"
      {"&Element;", "∈"},
      {""},
#line 1511 "HTMLCharacterReference.gperf"
      {"&Rarr;", "↠"},
#line 1706 "HTMLCharacterReference.gperf"
      {"&simlE;", "⪟"},
      {""}, {""},
#line 1677 "HTMLCharacterReference.gperf"
      {"&setminus;", "∖"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1092 "HTMLCharacterReference.gperf"
      {"&nap;", "≉"},
#line 1348 "HTMLCharacterReference.gperf"
      {"&ohm;", "Ω"},
      {""},
#line 1372 "HTMLCharacterReference.gperf"
      {"&orarr;", "↻"},
      {""}, {""}, {""}, {""},
#line 1727 "HTMLCharacterReference.gperf"
      {"&spades;", "♠"},
#line 1982 "HTMLCharacterReference.gperf"
      {"&urcorner;", "⌝"},
#line 982 "HTMLCharacterReference.gperf"
      {"&LongLeftRightArrow;", "⟷"},
#line 1929 "HTMLCharacterReference.gperf"
      {"&udarr;", "⇅"},
#line 1365 "HTMLCharacterReference.gperf"
      {"&opar;", "⦷"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 2079 "HTMLCharacterReference.gperf"
      {"&xhArr;", "⟺"},
      {""}, {""}, {""},
#line 2017 "HTMLCharacterReference.gperf"
      {"&vBar;", "⫨"},
      {""},
#line 230 "HTMLCharacterReference.gperf"
      {"&capand;", "⩄"},
      {""}, {""}, {""},
#line 2018 "HTMLCharacterReference.gperf"
      {"&vBarv;", "⫩"},
      {""},
#line 363 "HTMLCharacterReference.gperf"
      {"&dbkarow;", "⤏"},
#line 1451 "HTMLCharacterReference.gperf"
      {"&prec;", "≺"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1304 "HTMLCharacterReference.gperf"
      {"&nVDash;", "⊯"},
      {""},
#line 1046 "HTMLCharacterReference.gperf"
      {"&Map;", "⤅"},
      {""}, {""}, {""},
#line 1449 "HTMLCharacterReference.gperf"
      {"&prE;", "⪳"},
#line 1380 "HTMLCharacterReference.gperf"
      {"&orslope;", "⩗"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 470 "HTMLCharacterReference.gperf"
      {"&dzcy;", "џ"},
      {""}, {""}, {""},
#line 891 "HTMLCharacterReference.gperf"
      {"&leftarrowtail;", "↢"},
      {""}, {""}, {""}, {""}, {""},
#line 1397 "HTMLCharacterReference.gperf"
      {"&OverBrace;", "⏞"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1368 "HTMLCharacterReference.gperf"
      {"&operp;", "⦹"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 402 "HTMLCharacterReference.gperf"
      {"&divideontimes;", "⋇"},
      {""}, {""}, {""}, {""}, {""},
#line 509 "HTMLCharacterReference.gperf"
      {"&emsp;", " "},
      {""}, {""}, {""}, {""}, {""},
#line 120 "HTMLCharacterReference.gperf"
      {"&bepsi;", "϶"},
      {""}, {""}, {""}, {""}, {""},
#line 1965 "HTMLCharacterReference.gperf"
      {"&Updownarrow;", "⇕"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 47 "HTMLCharacterReference.gperf"
      {"&amp;", "&"},
#line 1980 "HTMLCharacterReference.gperf"
      {"&upuparrows;", "⇈"},
      {""},
#line 511 "HTMLCharacterReference.gperf"
      {"&emsp14;", " "},
      {""}, {""},
#line 173 "HTMLCharacterReference.gperf"
      {"&boxh;", "─"},
      {""}, {""}, {""},
#line 1440 "HTMLCharacterReference.gperf"
      {"&Poincareplane;", "ℌ"},
#line 1638 "HTMLCharacterReference.gperf"
      {"&rtri;", "▹"},
#line 2042 "HTMLCharacterReference.gperf"
      {"&vltri;", "⊲"},
      {""}, {""}, {""}, {""},
#line 1562 "HTMLCharacterReference.gperf"
      {"&ReverseUpEquilibrium;", "⥯"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 371 "HTMLCharacterReference.gperf"
      {"&ddagger;", "‡"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 219 "HTMLCharacterReference.gperf"
      {"&bull;", "•"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1523 "HTMLCharacterReference.gperf"
      {"&Rarrtl;", "⤖"},
      {""}, {""}, {""},
#line 1095 "HTMLCharacterReference.gperf"
      {"&napos;", "ŉ"},
      {""}, {""}, {""}, {""}, {""},
#line 510 "HTMLCharacterReference.gperf"
      {"&emsp13;", " "},
#line 1396 "HTMLCharacterReference.gperf"
      {"&OverBar;", "‾"},
#line 693 "HTMLCharacterReference.gperf"
      {"&Hat;", "^"},
      {""},
#line 291 "HTMLCharacterReference.gperf"
      {"&colon;", ":"},
#line 162 "HTMLCharacterReference.gperf"
      {"&bowtie;", "⋈"},
#line 118 "HTMLCharacterReference.gperf"
      {"&because;", "∵"},
      {""}, {""}, {""},
#line 1852 "HTMLCharacterReference.gperf"
      {"&there4;", "∴"},
      {""},
#line 1242 "HTMLCharacterReference.gperf"
      {"&NotTildeTilde;", "≉"},
      {""},
#line 1168 "HTMLCharacterReference.gperf"
      {"&nless;", "≮"},
      {""}, {""},
#line 766 "HTMLCharacterReference.gperf"
      {"&Integral;", "∫"},
      {""}, {""},
#line 1886 "HTMLCharacterReference.gperf"
      {"&tprime;", "‴"},
      {""}, {""}, {""},
#line 1484 "HTMLCharacterReference.gperf"
      {"&puncsp;", " "},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 247 "HTMLCharacterReference.gperf"
      {"&Cconint;", "∰"},
      {""}, {""}, {""},
#line 822 "HTMLCharacterReference.gperf"
      {"&kgreen;", "ĸ"},
      {""}, {""}, {""}, {""},
#line 946 "HTMLCharacterReference.gperf"
      {"&lfloor;", "⌊"},
#line 1818 "HTMLCharacterReference.gperf"
      {"&supmult;", "⫂"},
#line 1690 "HTMLCharacterReference.gperf"
      {"&shortmid;", "∣"},
#line 1770 "HTMLCharacterReference.gperf"
      {"&sube;", "⊆"},
      {""}, {""}, {""}, {""},
#line 1848 "HTMLCharacterReference.gperf"
      {"&tdot;", "◌⃛"},
      {""},
#line 1952 "HTMLCharacterReference.gperf"
      {"&UnderParenthesis;", "⏝"},
#line 940 "HTMLCharacterReference.gperf"
      {"&lessgtr;", "≶"},
      {""}, {""}, {""}, {""}, {""},
#line 426 "HTMLCharacterReference.gperf"
      {"&DoubleLeftTee;", "⫤"},
      {""}, {""},
#line 1880 "HTMLCharacterReference.gperf"
      {"&topbot;", "⌶"},
      {""}, {""},
#line 1267 "HTMLCharacterReference.gperf"
      {"&nshortmid;", "∤"},
      {""},
#line 1921 "HTMLCharacterReference.gperf"
      {"&Ubrcy;", "Ў"},
      {""}, {""}, {""}, {""},
#line 1263 "HTMLCharacterReference.gperf"
      {"&nsccue;", "⋡"},
      {""},
#line 941 "HTMLCharacterReference.gperf"
      {"&LessLess;", "⪡"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 446 "HTMLCharacterReference.gperf"
      {"&DownLeftVector;", "↽"},
#line 1479 "HTMLCharacterReference.gperf"
      {"&prurel;", "⊰"},
      {""},
#line 447 "HTMLCharacterReference.gperf"
      {"&DownLeftVectorBar;", "⥖"},
      {""},
#line 1420 "HTMLCharacterReference.gperf"
      {"&phone;", "☎"},
#line 1377 "HTMLCharacterReference.gperf"
      {"&ordm;", "º"},
#line 1032 "HTMLCharacterReference.gperf"
      {"&ltlarr;", "⥶"},
      {""}, {""}, {""},
#line 176 "HTMLCharacterReference.gperf"
      {"&boxhD;", "╥"},
#line 1398 "HTMLCharacterReference.gperf"
      {"&OverBracket;", "⎴"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 624 "HTMLCharacterReference.gperf"
      {"&geqslant;", "⩾"},
      {""},
#line 2044 "HTMLCharacterReference.gperf"
      {"&vnsup;", "⊃⃒"},
      {""},
#line 1203 "HTMLCharacterReference.gperf"
      {"&notinvb;", "⋷"},
#line 925 "HTMLCharacterReference.gperf"
      {"&leqslant;", "⩽"},
      {""}, {""}, {""},
#line 1217 "HTMLCharacterReference.gperf"
      {"&notniva;", "∌"},
#line 1412 "HTMLCharacterReference.gperf"
      {"&perp;", "⊥"},
      {""}, {""},
#line 1023 "HTMLCharacterReference.gperf"
      {"&lstrok;", "ł"},
#line 1096 "HTMLCharacterReference.gperf"
      {"&napprox;", "≉"},
      {""}, {""},
#line 1155 "HTMLCharacterReference.gperf"
      {"&nlArr;", "⇍"},
      {""},
#line 917 "HTMLCharacterReference.gperf"
      {"&LeftUpVector;", "↿"},
      {""},
#line 1373 "HTMLCharacterReference.gperf"
      {"&ord;", "⩝"},
      {""},
#line 462 "HTMLCharacterReference.gperf"
      {"&dstrok;", "đ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1566 "HTMLCharacterReference.gperf"
      {"&rfr;", "𝔯"},
#line 1905 "HTMLCharacterReference.gperf"
      {"&tscr;", "𝓉"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1937 "HTMLCharacterReference.gperf"
      {"&ugrave;", "ù"},
      {""}, {""}, {""}, {""}, {""},
#line 563 "HTMLCharacterReference.gperf"
      {"&ffilig;", "ﬃ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2086 "HTMLCharacterReference.gperf"
      {"&xnis;", "⋻"},
#line 348 "HTMLCharacterReference.gperf"
      {"&curvearrowright;", "↷"},
#line 1059 "HTMLCharacterReference.gperf"
      {"&MediumSpace;", " "},
      {""}, {""}, {""}, {""},
#line 718 "HTMLCharacterReference.gperf"
      {"&hstrok;", "ħ"},
      {""}, {""}, {""},
#line 1617 "HTMLCharacterReference.gperf"
      {"&ropar;", "⦆"},
      {""},
#line 1776 "HTMLCharacterReference.gperf"
      {"&subrarr;", "⥹"},
#line 615 "HTMLCharacterReference.gperf"
      {"&gcy;", "г"},
      {""},
#line 1458 "HTMLCharacterReference.gperf"
      {"&preceq;", "⪯"},
      {""}, {""},
#line 876 "HTMLCharacterReference.gperf"
      {"&lcy;", "л"},
#line 826 "HTMLCharacterReference.gperf"
      {"&kjcy;", "ќ"},
#line 848 "HTMLCharacterReference.gperf"
      {"&larrb;", "⇤"},
#line 461 "HTMLCharacterReference.gperf"
      {"&Dstrok;", "Đ"},
      {""},
#line 482 "HTMLCharacterReference.gperf"
      {"&ecy;", "э"},
      {""}, {""}, {""}, {""},
#line 368 "HTMLCharacterReference.gperf"
      {"&dcy;", "д"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 800 "HTMLCharacterReference.gperf"
      {"&Jcy;", "Й"},
#line 1346 "HTMLCharacterReference.gperf"
      {"&ogt;", "⧁"},
      {""},
#line 1777 "HTMLCharacterReference.gperf"
      {"&Subset;", "⋐"},
#line 67 "HTMLCharacterReference.gperf"
      {"&angrtvb;", "⊾"},
#line 31 "HTMLCharacterReference.gperf"
      {"&acy;", "а"},
#line 1986 "HTMLCharacterReference.gperf"
      {"&urtri;", "◹"},
      {""}, {""}, {""},
#line 1746 "HTMLCharacterReference.gperf"
      {"&SquareIntersection;", "⊓"},
      {""}, {""}, {""}, {""},
#line 561 "HTMLCharacterReference.gperf"
      {"&fcy;", "ф"},
      {""},
#line 1343 "HTMLCharacterReference.gperf"
      {"&ogon;", "˛"},
#line 1823 "HTMLCharacterReference.gperf"
      {"&supset;", "⊃"},
      {""},
#line 114 "HTMLCharacterReference.gperf"
      {"&bcy;", "б"},
#line 440 "HTMLCharacterReference.gperf"
      {"&DownBreve;", "◌̑"},
#line 1639 "HTMLCharacterReference.gperf"
      {"&rtrie;", "⊵"},
#line 329 "HTMLCharacterReference.gperf"
      {"&cularr;", "↶"},
      {""}, {""}, {""},
#line 1626 "HTMLCharacterReference.gperf"
      {"&rrarr;", "⇉"},
      {""},
#line 654 "HTMLCharacterReference.gperf"
      {"&GreaterEqual;", "≥"},
#line 2127 "HTMLCharacterReference.gperf"
      {"&zcy;", "з"},
      {""}, {""},
#line 273 "HTMLCharacterReference.gperf"
      {"&circleddash;", "⊝"},
      {""}, {""},
#line 1623 "HTMLCharacterReference.gperf"
      {"&rpar;", ")"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 998 "HTMLCharacterReference.gperf"
      {"&LowerLeftArrow;", "↙"},
#line 605 "HTMLCharacterReference.gperf"
      {"&gamma;", "γ"},
      {""}, {""},
#line 367 "HTMLCharacterReference.gperf"
      {"&Dcy;", "Д"},
      {""},
#line 1410 "HTMLCharacterReference.gperf"
      {"&period;", "."},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 58 "HTMLCharacterReference.gperf"
      {"&angmsdaa;", "⦨"},
      {""},
#line 1615 "HTMLCharacterReference.gperf"
      {"&roarr;", "⇾"},
#line 1022 "HTMLCharacterReference.gperf"
      {"&Lstrok;", "Ł"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 269 "HTMLCharacterReference.gperf"
      {"&circlearrowleft;", "↺"},
#line 721 "HTMLCharacterReference.gperf"
      {"&hybull;", "⁃"},
      {""},
#line 1572 "HTMLCharacterReference.gperf"
      {"&rho;", "ρ"},
      {""},
#line 1659 "HTMLCharacterReference.gperf"
      {"&scirc;", "ŝ"},
      {""}, {""}, {""}, {""},
#line 1832 "HTMLCharacterReference.gperf"
      {"&swArr;", "⇙"},
#line 722 "HTMLCharacterReference.gperf"
      {"&hyphen;", "‐"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1927 "HTMLCharacterReference.gperf"
      {"&Ucy;", "У"},
#line 1512 "HTMLCharacterReference.gperf"
      {"&rArr;", "⇒"},
      {""}, {""}, {""},
#line 801 "HTMLCharacterReference.gperf"
      {"&jcy;", "й"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 503 "HTMLCharacterReference.gperf"
      {"&emacr;", "ē"},
      {""}, {""}, {""},
#line 1444 "HTMLCharacterReference.gperf"
      {"&pound;", "£"},
#line 364 "HTMLCharacterReference.gperf"
      {"&dblac;", "˝"},
      {""},
#line 1241 "HTMLCharacterReference.gperf"
      {"&NotTildeFullEqual;", "≇"},
#line 875 "HTMLCharacterReference.gperf"
      {"&Lcy;", "Л"},
      {""}, {""},
#line 131 "HTMLCharacterReference.gperf"
      {"&bigcup;", "⋃"},
      {""},
#line 2107 "HTMLCharacterReference.gperf"
      {"&Ycy;", "Ы"},
#line 1701 "HTMLCharacterReference.gperf"
      {"&sime;", "≃"},
#line 1702 "HTMLCharacterReference.gperf"
      {"&simeq;", "≃"},
      {""},
#line 1834 "HTMLCharacterReference.gperf"
      {"&swarrow;", "↙"},
      {""}, {""},
#line 44 "HTMLCharacterReference.gperf"
      {"&amacr;", "ā"},
      {""}, {""}, {""}, {""},
#line 1532 "HTMLCharacterReference.gperf"
      {"&rbarr;", "⤍"},
      {""}, {""}, {""}, {""},
#line 1696 "HTMLCharacterReference.gperf"
      {"&sigma;", "σ"},
      {""}, {""},
#line 2108 "HTMLCharacterReference.gperf"
      {"&ycy;", "ы"},
#line 1907 "HTMLCharacterReference.gperf"
      {"&tscy;", "ц"},
#line 1336 "HTMLCharacterReference.gperf"
      {"&odot;", "⊙"},
#line 865 "HTMLCharacterReference.gperf"
      {"&lbrack;", "["},
      {""}, {""},
#line 1854 "HTMLCharacterReference.gperf"
      {"&therefore;", "∴"},
      {""}, {""}, {""},
#line 373 "HTMLCharacterReference.gperf"
      {"&DDotrahd;", "⤑"},
#line 431 "HTMLCharacterReference.gperf"
      {"&DoubleRightTee;", "⊨"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1795 "HTMLCharacterReference.gperf"
      {"&succnapprox;", "⪺"},
      {""},
#line 729 "HTMLCharacterReference.gperf"
      {"&icy;", "и"},
      {""}, {""}, {""}, {""}, {""},
#line 1904 "HTMLCharacterReference.gperf"
      {"&Tscr;", "𝒯"},
      {""}, {""}, {""},
#line 1545 "HTMLCharacterReference.gperf"
      {"&Rcy;", "Р"},
      {""},
#line 1856 "HTMLCharacterReference.gperf"
      {"&theta;", "θ"},
      {""}, {""},
#line 1481 "HTMLCharacterReference.gperf"
      {"&pscr;", "𝓅"},
#line 1560 "HTMLCharacterReference.gperf"
      {"&ReverseElement;", "∋"},
      {""}, {""}, {""},
#line 1824 "HTMLCharacterReference.gperf"
      {"&supseteq;", "⊇"},
#line 1825 "HTMLCharacterReference.gperf"
      {"&supseteqq;", "⫆"},
      {""}, {""}, {""}, {""}, {""},
#line 1536 "HTMLCharacterReference.gperf"
      {"&rbrke;", "⦌"},
      {""}, {""}, {""},
#line 1314 "HTMLCharacterReference.gperf"
      {"&nvlt;", "<⃒"},
      {""},
#line 607 "HTMLCharacterReference.gperf"
      {"&gammad;", "ϝ"},
      {""}, {""}, {""}, {""}, {""},
#line 746 "HTMLCharacterReference.gperf"
      {"&Im;", "ℑ"},
#line 614 "HTMLCharacterReference.gperf"
      {"&Gcy;", "Г"},
      {""},
#line 1094 "HTMLCharacterReference.gperf"
      {"&napid;", "≋̸"},
      {""},
#line 330 "HTMLCharacterReference.gperf"
      {"&cularrp;", "⤽"},
      {""}, {""}, {""},
#line 1997 "HTMLCharacterReference.gperf"
      {"&uwangle;", "⦧"},
      {""},
#line 1665 "HTMLCharacterReference.gperf"
      {"&Scy;", "С"},
#line 1941 "HTMLCharacterReference.gperf"
      {"&uhblk;", "▀"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1739 "HTMLCharacterReference.gperf"
      {"&sqsup;", "⊐"},
      {""}, {""},
#line 1188 "HTMLCharacterReference.gperf"
      {"&NotEqualTilde;", "≂̸"},
      {""},
#line 1946 "HTMLCharacterReference.gperf"
      {"&Umacr;", "Ū"},
#line 220 "HTMLCharacterReference.gperf"
      {"&bullet;", "•"},
      {""}, {""}, {""},
#line 1384 "HTMLCharacterReference.gperf"
      {"&oscr;", "ℴ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1330 "HTMLCharacterReference.gperf"
      {"&Ocy;", "О"},
      {""},
#line 1499 "HTMLCharacterReference.gperf"
      {"&rAarr;", "⇛"},
      {""},
#line 130 "HTMLCharacterReference.gperf"
      {"&bigcirc;", "◯"},
      {""}, {""}, {""}, {""}, {""},
#line 228 "HTMLCharacterReference.gperf"
      {"&Cap;", "⋒"},
#line 105 "HTMLCharacterReference.gperf"
      {"&Barv;", "⫧"},
      {""}, {""},
#line 1709 "HTMLCharacterReference.gperf"
      {"&simrarr;", "⥲"},
#line 621 "HTMLCharacterReference.gperf"
      {"&gel;", "⋛"},
      {""}, {""},
#line 1077 "HTMLCharacterReference.gperf"
      {"&models;", "⊧"},
      {""},
#line 1792 "HTMLCharacterReference.gperf"
      {"&SucceedsSlantEqual;", "≽"},
      {""},
#line 478 "HTMLCharacterReference.gperf"
      {"&Ecirc;", "Ê"},
#line 1789 "HTMLCharacterReference.gperf"
      {"&succcurlyeq;", "≽"},
#line 1337 "HTMLCharacterReference.gperf"
      {"&odsold;", "⦼"},
#line 560 "HTMLCharacterReference.gperf"
      {"&Fcy;", "Ф"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 604 "HTMLCharacterReference.gperf"
      {"&Gamma;", "Γ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 859 "HTMLCharacterReference.gperf"
      {"&late;", "⪭"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 872 "HTMLCharacterReference.gperf"
      {"&lcedil;", "ļ"},
      {""}, {""},
#line 1661 "HTMLCharacterReference.gperf"
      {"&scnE;", "⪶"},
#line 748 "HTMLCharacterReference.gperf"
      {"&imacr;", "ī"},
      {""},
#line 1460 "HTMLCharacterReference.gperf"
      {"&precneqq;", "⪵"},
#line 1112 "HTMLCharacterReference.gperf"
      {"&ncy;", "н"},
#line 1993 "HTMLCharacterReference.gperf"
      {"&utrif;", "▴"},
#line 1924 "HTMLCharacterReference.gperf"
      {"&ubreve;", "ŭ"},
      {""}, {""}, {""}, {""}, {""},
#line 1340 "HTMLCharacterReference.gperf"
      {"&ofcir;", "⦿"},
#line 1483 "HTMLCharacterReference.gperf"
      {"&psi;", "ψ"},
      {""},
#line 1769 "HTMLCharacterReference.gperf"
      {"&subE;", "⫅"},
#line 1409 "HTMLCharacterReference.gperf"
      {"&percnt;", "%"},
      {""},
#line 343 "HTMLCharacterReference.gperf"
      {"&curlyeqsucc;", "⋟"},
      {""},
#line 125 "HTMLCharacterReference.gperf"
      {"&beth;", "ℶ"},
#line 1647 "HTMLCharacterReference.gperf"
      {"&sbquo;", "‚"},
      {""}, {""}, {""},
#line 1633 "HTMLCharacterReference.gperf"
      {"&rsqb;", "]"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 376 "HTMLCharacterReference.gperf"
      {"&Del;", "∇"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 177 "HTMLCharacterReference.gperf"
      {"&boxhd;", "┬"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1111 "HTMLCharacterReference.gperf"
      {"&Ncy;", "Н"},
      {""}, {""}, {""}, {""},
#line 1054 "HTMLCharacterReference.gperf"
      {"&Mcy;", "М"},
      {""}, {""},
#line 717 "HTMLCharacterReference.gperf"
      {"&Hstrok;", "Ħ"},
#line 342 "HTMLCharacterReference.gperf"
      {"&curlyeqprec;", "⋞"},
      {""},
#line 1573 "HTMLCharacterReference.gperf"
      {"&rhov;", "ϱ"},
      {""},
#line 232 "HTMLCharacterReference.gperf"
      {"&capcap;", "⩋"},
      {""}, {""}, {""},
#line 1613 "HTMLCharacterReference.gperf"
      {"&rnmid;", "⫮"},
      {""}, {""}, {""}, {""}, {""},
#line 244 "HTMLCharacterReference.gperf"
      {"&ccedil;", "ç"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1703 "HTMLCharacterReference.gperf"
      {"&simg;", "⪞"},
#line 1355 "HTMLCharacterReference.gperf"
      {"&Omacr;", "Ō"},
      {""}, {""}, {""}, {""}, {""},
#line 293 "HTMLCharacterReference.gperf"
      {"&colone;", "≔"},
      {""}, {""},
#line 1553 "HTMLCharacterReference.gperf"
      {"&real;", "ℜ"},
      {""}, {""}, {""}, {""},
#line 1853 "HTMLCharacterReference.gperf"
      {"&Therefore;", "∴"},
      {""}, {""},
#line 117 "HTMLCharacterReference.gperf"
      {"&Because;", "∵"},
      {""}, {""}, {""},
#line 1697 "HTMLCharacterReference.gperf"
      {"&sigmaf;", "ς"},
      {""}, {""}, {""}, {""}, {""},
#line 1900 "HTMLCharacterReference.gperf"
      {"&triplus;", "⨹"},
      {""},
#line 1567 "HTMLCharacterReference.gperf"
      {"&rHar;", "⥤"},
      {""},
#line 403 "HTMLCharacterReference.gperf"
      {"&divonx;", "⋇"},
#line 849 "HTMLCharacterReference.gperf"
      {"&larrbfs;", "⤟"},
      {""}, {""},
#line 2043 "HTMLCharacterReference.gperf"
      {"&vnsub;", "⊂⃒"},
      {""}, {""},
#line 30 "HTMLCharacterReference.gperf"
      {"&Acy;", "А"},
      {""},
#line 1855 "HTMLCharacterReference.gperf"
      {"&Theta;", "Θ"},
      {""}, {""}, {""},
#line 882 "HTMLCharacterReference.gperf"
      {"&ldsh;", "↲"},
      {""},
#line 345 "HTMLCharacterReference.gperf"
      {"&curlywedge;", "⋏"},
      {""}, {""}, {""}, {""},
#line 1221 "HTMLCharacterReference.gperf"
      {"&NotPrecedesEqual;", "⪯̸"},
      {""}, {""}, {""},
#line 1196 "HTMLCharacterReference.gperf"
      {"&NotGreaterTilde;", "≵"},
#line 606 "HTMLCharacterReference.gperf"
      {"&Gammad;", "Ϝ"},
      {""}, {""}, {""}, {""},
#line 2100 "HTMLCharacterReference.gperf"
      {"&xwedge;", "⋀"},
      {""}, {""}, {""}, {""},
#line 871 "HTMLCharacterReference.gperf"
      {"&Lcedil;", "Ļ"},
      {""},
#line 1741 "HTMLCharacterReference.gperf"
      {"&sqsupset;", "⊐"},
      {""},
#line 1742 "HTMLCharacterReference.gperf"
      {"&sqsupseteq;", "⊒"},
#line 1682 "HTMLCharacterReference.gperf"
      {"&sfrown;", "⌢"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 353 "HTMLCharacterReference.gperf"
      {"&cylcty;", "⌭"},
      {""},
#line 1694 "HTMLCharacterReference.gperf"
      {"&shy;", " "},
#line 1452 "HTMLCharacterReference.gperf"
      {"&precapprox;", "⪷"},
#line 237 "HTMLCharacterReference.gperf"
      {"&caret;", "⁁"},
#line 835 "HTMLCharacterReference.gperf"
      {"&lagran;", "ℒ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 860 "HTMLCharacterReference.gperf"
      {"&lates;", "⪭︀"},
#line 1862 "HTMLCharacterReference.gperf"
      {"&thinsp;", " "},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1407 "HTMLCharacterReference.gperf"
      {"&Pcy;", "П"},
      {""}, {""},
#line 408 "HTMLCharacterReference.gperf"
      {"&dollar;", "$"},
      {""}, {""},
#line 1075 "HTMLCharacterReference.gperf"
      {"&mldr;", "…"},
#line 1671 "HTMLCharacterReference.gperf"
      {"&seArr;", "⇘"},
#line 1541 "HTMLCharacterReference.gperf"
      {"&Rcedil;", "Ŗ"},
      {""}, {""}, {""},
#line 1419 "HTMLCharacterReference.gperf"
      {"&phmmat;", "ℳ"},
#line 106 "HTMLCharacterReference.gperf"
      {"&barvee;", "⊽"},
      {""}, {""},
#line 1464 "HTMLCharacterReference.gperf"
      {"&prime;", "′"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 728 "HTMLCharacterReference.gperf"
      {"&Icy;", "И"},
      {""}, {""},
#line 892 "HTMLCharacterReference.gperf"
      {"&LeftCeiling;", "⌈"},
      {""}, {""},
#line 839 "HTMLCharacterReference.gperf"
      {"&lang;", "⟨"},
      {""}, {""}, {""}, {""},
#line 2104 "HTMLCharacterReference.gperf"
      {"&yacy;", "я"},
      {""},
#line 611 "HTMLCharacterReference.gperf"
      {"&Gcedil;", "Ģ"},
#line 1417 "HTMLCharacterReference.gperf"
      {"&phi;", "φ"},
      {""}, {""}, {""}, {""},
#line 1219 "HTMLCharacterReference.gperf"
      {"&notnivc;", "⋽"},
      {""}, {""},
#line 429 "HTMLCharacterReference.gperf"
      {"&DoubleLongRightArrow;", "⟹"},
#line 1656 "HTMLCharacterReference.gperf"
      {"&Scedil;", "Ş"},
#line 1538 "HTMLCharacterReference.gperf"
      {"&rbrkslu;", "⦐"},
      {""},
#line 720 "HTMLCharacterReference.gperf"
      {"&HumpEqual;", "≏"},
      {""}, {""},
#line 1129 "HTMLCharacterReference.gperf"
      {"&NewLine;", "␊"},
#line 1559 "HTMLCharacterReference.gperf"
      {"&reg;", "®"},
      {""}, {""},
#line 1881 "HTMLCharacterReference.gperf"
      {"&topcir;", "⫱"},
      {""}, {""}, {""},
#line 43 "HTMLCharacterReference.gperf"
      {"&Amacr;", "Ā"},
      {""},
#line 1673 "HTMLCharacterReference.gperf"
      {"&searrow;", "↘"},
#line 2126 "HTMLCharacterReference.gperf"
      {"&Zcy;", "З"},
      {""}, {""}, {""},
#line 434 "HTMLCharacterReference.gperf"
      {"&DoubleVerticalBar;", "∥"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1465 "HTMLCharacterReference.gperf"
      {"&primes;", "ℙ"},
      {""}, {""}, {""},
#line 825 "HTMLCharacterReference.gperf"
      {"&KJcy;", "Ќ"},
      {""}, {""},
#line 294 "HTMLCharacterReference.gperf"
      {"&coloneq;", "≔"},
      {""},
#line 1985 "HTMLCharacterReference.gperf"
      {"&uring;", "ů"},
      {""},
#line 1781 "HTMLCharacterReference.gperf"
      {"&SubsetEqual;", "⊆"},
      {""},
#line 68 "HTMLCharacterReference.gperf"
      {"&angrtvbd;", "⦝"},
      {""}, {""},
#line 655 "HTMLCharacterReference.gperf"
      {"&GreaterEqualLess;", "⋛"},
      {""}, {""}, {""},
#line 1556 "HTMLCharacterReference.gperf"
      {"&reals;", "ℝ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 113 "HTMLCharacterReference.gperf"
      {"&Bcy;", "Б"},
      {""}, {""}, {""},
#line 1391 "HTMLCharacterReference.gperf"
      {"&otimes;", "⊗"},
      {""},
#line 1826 "HTMLCharacterReference.gperf"
      {"&supsetneq;", "⊋"},
#line 1827 "HTMLCharacterReference.gperf"
      {"&supsetneqq;", "⫌"},
#line 272 "HTMLCharacterReference.gperf"
      {"&circledcirc;", "⊚"},
      {""}, {""},
#line 1630 "HTMLCharacterReference.gperf"
      {"&rscr;", "𝓇"},
      {""},
#line 684 "HTMLCharacterReference.gperf"
      {"&hairsp;", " "},
      {""}, {""},
#line 1074 "HTMLCharacterReference.gperf"
      {"&mlcp;", "⫛"},
#line 1057 "HTMLCharacterReference.gperf"
      {"&mDDot;", "∺"},
#line 1107 "HTMLCharacterReference.gperf"
      {"&ncedil;", "ņ"},
      {""}, {""},
#line 943 "HTMLCharacterReference.gperf"
      {"&LessSlantEqual;", "⩽"},
      {""}, {""}, {""}, {""}, {""},
#line 2083 "HTMLCharacterReference.gperf"
      {"&xlArr;", "⟸"},
#line 1312 "HTMLCharacterReference.gperf"
      {"&nvlArr;", "⤂"},
      {""}, {""},
#line 1354 "HTMLCharacterReference.gperf"
      {"&olt;", "⧀"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1906 "HTMLCharacterReference.gperf"
      {"&TScy;", "Ц"},
#line 747 "HTMLCharacterReference.gperf"
      {"&Imacr;", "Ī"},
      {""},
#line 1275 "HTMLCharacterReference.gperf"
      {"&nsqsupe;", "⋣"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 2006 "HTMLCharacterReference.gperf"
      {"&varr;", "↕"},
      {""}, {""},
#line 1621 "HTMLCharacterReference.gperf"
      {"&rotimes;", "⨵"},
      {""},
#line 838 "HTMLCharacterReference.gperf"
      {"&Lang;", "⟪"},
      {""},
#line 1106 "HTMLCharacterReference.gperf"
      {"&Ncedil;", "Ņ"},
      {""}, {""},
#line 444 "HTMLCharacterReference.gperf"
      {"&DownLeftRightVector;", "⥐"},
#line 290 "HTMLCharacterReference.gperf"
      {"&Colon;", "∷"},
      {""},
#line 1771 "HTMLCharacterReference.gperf"
      {"&subedot;", "⫃"},
      {""}, {""}, {""}, {""},
#line 1345 "HTMLCharacterReference.gperf"
      {"&ograve;", "ò"},
      {""},
#line 2004 "HTMLCharacterReference.gperf"
      {"&varpropto;", "∝"},
#line 1864 "HTMLCharacterReference.gperf"
      {"&thkap;", "≈"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 135 "HTMLCharacterReference.gperf"
      {"&bigsqcup;", "⨆"},
      {""},
#line 1942 "HTMLCharacterReference.gperf"
      {"&ulcorn;", "⌜"},
      {""}, {""},
#line 881 "HTMLCharacterReference.gperf"
      {"&ldrushar;", "⥋"},
      {""}, {""},
#line 1843 "HTMLCharacterReference.gperf"
      {"&tcaron;", "ť"},
      {""},
#line 148 "HTMLCharacterReference.gperf"
      {"&blacktriangleright;", "▸"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1944 "HTMLCharacterReference.gperf"
      {"&ulcrop;", "⌏"},
      {""}, {""}, {""}, {""},
#line 278 "HTMLCharacterReference.gperf"
      {"&CirclePlus;", "⊕"},
      {""}, {""}, {""},
#line 1877 "HTMLCharacterReference.gperf"
      {"&tint;", "∭"},
      {""}, {""}, {""}, {""},
#line 1505 "HTMLCharacterReference.gperf"
      {"&Rang;", "⟫"},
      {""},
#line 858 "HTMLCharacterReference.gperf"
      {"&latail;", "⤙"},
      {""}, {""}, {""}, {""},
#line 1076 "HTMLCharacterReference.gperf"
      {"&mnplus;", "∓"},
      {""}, {""}, {""},
#line 1991 "HTMLCharacterReference.gperf"
      {"&utilde;", "ũ"},
#line 1768 "HTMLCharacterReference.gperf"
      {"&subdot;", "⪽"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 441 "HTMLCharacterReference.gperf"
      {"&downdownarrows;", "⇊"},
#line 1735 "HTMLCharacterReference.gperf"
      {"&sqsub;", "⊏"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 325 "HTMLCharacterReference.gperf"
      {"&cudarrl;", "⤸"},
      {""}, {""},
#line 1866 "HTMLCharacterReference.gperf"
      {"&THORN;", "Þ"},
      {""}, {""}, {""}, {""}, {""},
#line 1534 "HTMLCharacterReference.gperf"
      {"&rbrace;", "}"},
      {""},
#line 1467 "HTMLCharacterReference.gperf"
      {"&prnE;", "⪵"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 82 "HTMLCharacterReference.gperf"
      {"&ApplyFunction;", "⁡"},
      {""}, {""},
#line 1959 "HTMLCharacterReference.gperf"
      {"&UpArrow;", "↑"},
      {""},
#line 1392 "HTMLCharacterReference.gperf"
      {"&otimesas;", "⨶"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1568 "HTMLCharacterReference.gperf"
      {"&rhard;", "⇁"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 45 "HTMLCharacterReference.gperf"
      {"&amalg;", "⨿"},
#line 1740 "HTMLCharacterReference.gperf"
      {"&sqsupe;", "⊒"},
      {""},
#line 231 "HTMLCharacterReference.gperf"
      {"&capbrcup;", "⩉"},
      {""}, {""}, {""}, {""},
#line 1418 "HTMLCharacterReference.gperf"
      {"&phiv;", "ϕ"},
      {""},
#line 533 "HTMLCharacterReference.gperf"
      {"&EqualTilde;", "≂"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1828 "HTMLCharacterReference.gperf"
      {"&supsim;", "⫈"},
      {""}, {""}, {""},
#line 1704 "HTMLCharacterReference.gperf"
      {"&simgE;", "⪠"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1065 "HTMLCharacterReference.gperf"
      {"&mid;", "∣"},
      {""}, {""}, {""}, {""}, {""},
#line 1091 "HTMLCharacterReference.gperf"
      {"&nang;", "∠⃒"},
      {""}, {""}, {""},
#line 139 "HTMLCharacterReference.gperf"
      {"&biguplus;", "⨄"},
      {""}, {""},
#line 1350 "HTMLCharacterReference.gperf"
      {"&olarr;", "↺"},
      {""}, {""}, {""}, {""}, {""},
#line 1943 "HTMLCharacterReference.gperf"
      {"&ulcorner;", "⌜"},
#line 145 "HTMLCharacterReference.gperf"
      {"&blacktriangle;", "▴"},
      {""},
#line 564 "HTMLCharacterReference.gperf"
      {"&fflig;", "ﬀ"},
      {""}, {""}, {""}, {""}, {""},
#line 672 "HTMLCharacterReference.gperf"
      {"&gtlPar;", "⦕"},
      {""}, {""}, {""}, {""},
#line 811 "HTMLCharacterReference.gperf"
      {"&Jukcy;", "Є"},
      {""},
#line 275 "HTMLCharacterReference.gperf"
      {"&circledR;", "®"},
      {""},
#line 1614 "HTMLCharacterReference.gperf"
      {"&roang;", "⟭"},
#line 1237 "HTMLCharacterReference.gperf"
      {"&NotSuperset;", "⊃⃒"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1537 "HTMLCharacterReference.gperf"
      {"&rbrksld;", "⦎"},
      {""}, {""},
#line 843 "HTMLCharacterReference.gperf"
      {"&Laplacetrf;", "ℒ"},
      {""}, {""}, {""},
#line 2085 "HTMLCharacterReference.gperf"
      {"&xmap;", "⟼"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1374 "HTMLCharacterReference.gperf"
      {"&order;", "ℴ"},
      {""}, {""}, {""}, {""},
#line 1422 "HTMLCharacterReference.gperf"
      {"&pi;", "π"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1169 "HTMLCharacterReference.gperf"
      {"&nLl;", "⋘̸"},
      {""}, {""}, {""}, {""},
#line 99 "HTMLCharacterReference.gperf"
      {"&backcong;", "≌"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 143 "HTMLCharacterReference.gperf"
      {"&blacklozenge;", "⧫"},
      {""}, {""}, {""},
#line 1842 "HTMLCharacterReference.gperf"
      {"&Tcaron;", "Ť"},
      {""},
#line 499 "HTMLCharacterReference.gperf"
      {"&ell;", "ℓ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1470 "HTMLCharacterReference.gperf"
      {"&Product;", "∏"},
#line 1737 "HTMLCharacterReference.gperf"
      {"&sqsubset;", "⊏"},
      {""},
#line 1738 "HTMLCharacterReference.gperf"
      {"&sqsubseteq;", "⊑"},
      {""}, {""}, {""},
#line 1897 "HTMLCharacterReference.gperf"
      {"&trie;", "≜"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 71 "HTMLCharacterReference.gperf"
      {"&angzarr;", "⍼"},
#line 1455 "HTMLCharacterReference.gperf"
      {"&PrecedesEqual;", "⪯"},
      {""},
#line 1732 "HTMLCharacterReference.gperf"
      {"&sqcup;", "⊔"},
#line 812 "HTMLCharacterReference.gperf"
      {"&jukcy;", "є"},
      {""},
#line 1561 "HTMLCharacterReference.gperf"
      {"&ReverseEquilibrium;", "⇋"},
      {""},
#line 999 "HTMLCharacterReference.gperf"
      {"&LowerRightArrow;", "↘"},
      {""}, {""},
#line 1610 "HTMLCharacterReference.gperf"
      {"&rlm;", "‏"},
      {""},
#line 378 "HTMLCharacterReference.gperf"
      {"&delta;", "δ"},
#line 1637 "HTMLCharacterReference.gperf"
      {"&rtimes;", "⋊"},
      {""}, {""}, {""}, {""},
#line 355 "HTMLCharacterReference.gperf"
      {"&dagger;", "†"},
      {""},
#line 1814 "HTMLCharacterReference.gperf"
      {"&SupersetEqual;", "⊇"},
      {""}, {""}, {""},
#line 1968 "HTMLCharacterReference.gperf"
      {"&upharpoonleft;", "↿"},
#line 1469 "HTMLCharacterReference.gperf"
      {"&prod;", "∏"},
#line 1557 "HTMLCharacterReference.gperf"
      {"&rect;", "▭"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1243 "HTMLCharacterReference.gperf"
      {"&NotVerticalBar;", "∤"},
      {""}, {""}, {""}, {""}, {""},
#line 1349 "HTMLCharacterReference.gperf"
      {"&oint;", "∮"},
#line 1700 "HTMLCharacterReference.gperf"
      {"&simdot;", "⩪"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 126 "HTMLCharacterReference.gperf"
      {"&between;", "≬"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 554 "HTMLCharacterReference.gperf"
      {"&exist;", "∃"},
#line 1733 "HTMLCharacterReference.gperf"
      {"&sqcups;", "⊔︀"},
      {""}, {""},
#line 1093 "HTMLCharacterReference.gperf"
      {"&napE;", "⩰̸"},
#line 425 "HTMLCharacterReference.gperf"
      {"&DoubleLeftRightArrow;", "⇔"},
#line 795 "HTMLCharacterReference.gperf"
      {"&iukcy;", "і"},
#line 1809 "HTMLCharacterReference.gperf"
      {"&supdsub;", "⫘"},
      {""}, {""},
#line 377 "HTMLCharacterReference.gperf"
      {"&Delta;", "Δ"},
      {""}, {""}, {""}, {""}, {""},
#line 354 "HTMLCharacterReference.gperf"
      {"&Dagger;", "‡"},
      {""}, {""},
#line 274 "HTMLCharacterReference.gperf"
      {"&CircleDot;", "⊙"},
#line 1888 "HTMLCharacterReference.gperf"
      {"&trade;", "™"},
      {""}, {""}, {""}, {""}, {""},
#line 1395 "HTMLCharacterReference.gperf"
      {"&ovbar;", "⌽"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 716 "HTMLCharacterReference.gperf"
      {"&hslash;", "ℏ"},
#line 1424 "HTMLCharacterReference.gperf"
      {"&piv;", "ϖ"},
      {""}, {""},
#line 1399 "HTMLCharacterReference.gperf"
      {"&OverParenthesis;", "⏜"},
      {""}, {""},
#line 1920 "HTMLCharacterReference.gperf"
      {"&Uarrocir;", "⥉"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 840 "HTMLCharacterReference.gperf"
      {"&langd;", "⦑"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 104 "HTMLCharacterReference.gperf"
      {"&Backslash;", "∖"},
#line 1411 "HTMLCharacterReference.gperf"
      {"&permil;", "‰"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1218 "HTMLCharacterReference.gperf"
      {"&notnivb;", "⋾"},
      {""}, {""}, {""},
#line 1347 "HTMLCharacterReference.gperf"
      {"&ohbar;", "⦵"},
      {""}, {""}, {""}, {""},
#line 2035 "HTMLCharacterReference.gperf"
      {"&VerticalBar;", "∣"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 1064 "HTMLCharacterReference.gperf"
      {"&micro;", "µ"},
#line 850 "HTMLCharacterReference.gperf"
      {"&larrfs;", "⤝"},
      {""}, {""}, {""}, {""},
#line 841 "HTMLCharacterReference.gperf"
      {"&langle;", "⟨"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1963 "HTMLCharacterReference.gperf"
      {"&UpArrowDownArrow;", "⇅"},
      {""}, {""}, {""}, {""}, {""},
#line 1772 "HTMLCharacterReference.gperf"
      {"&submult;", "⫁"},
      {""},
#line 1313 "HTMLCharacterReference.gperf"
      {"&nvle;", "≤⃒"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1926 "HTMLCharacterReference.gperf"
      {"&ucirc;", "û"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1961 "HTMLCharacterReference.gperf"
      {"&uparrow;", "↑"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1624 "HTMLCharacterReference.gperf"
      {"&rpargt;", "⦔"},
      {""}, {""},
#line 1932 "HTMLCharacterReference.gperf"
      {"&udhar;", "⥮"},
      {""},
#line 234 "HTMLCharacterReference.gperf"
      {"&capdot;", "⩀"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 2020 "HTMLCharacterReference.gperf"
      {"&vcy;", "в"},
#line 1945 "HTMLCharacterReference.gperf"
      {"&ultri;", "◸"},
      {""}, {""}, {""}, {""},
#line 1293 "HTMLCharacterReference.gperf"
      {"&ntlg;", "≸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1608 "HTMLCharacterReference.gperf"
      {"&rlarr;", "⇄"},
      {""}, {""}, {""}, {""},
#line 1616 "HTMLCharacterReference.gperf"
      {"&robrk;", "⟧"},
#line 243 "HTMLCharacterReference.gperf"
      {"&Ccedil;", "Ç"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 33 "HTMLCharacterReference.gperf"
      {"&aelig;", "æ"},
      {""}, {""},
#line 1969 "HTMLCharacterReference.gperf"
      {"&upharpoonright;", "↾"},
      {""}, {""},
#line 292 "HTMLCharacterReference.gperf"
      {"&Colone;", "⩴"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1543 "HTMLCharacterReference.gperf"
      {"&rceil;", "⌉"},
      {""},
#line 894 "HTMLCharacterReference.gperf"
      {"&LeftDownTeeVector;", "⥡"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 678 "HTMLCharacterReference.gperf"
      {"&gtreqqless;", "⪌"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1385 "HTMLCharacterReference.gperf"
      {"&Oslash;", "Ø"},
      {""},
#line 620 "HTMLCharacterReference.gperf"
      {"&gEl;", "⪌"},
      {""},
#line 1459 "HTMLCharacterReference.gperf"
      {"&precnapprox;", "⪹"},
      {""}, {""},
#line 2019 "HTMLCharacterReference.gperf"
      {"&Vcy;", "В"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1533 "HTMLCharacterReference.gperf"
      {"&rbbrk;", "❳"},
#line 1736 "HTMLCharacterReference.gperf"
      {"&sqsube;", "⊑"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1722 "HTMLCharacterReference.gperf"
      {"&sol;", "/"},
      {""},
#line 1950 "HTMLCharacterReference.gperf"
      {"&UnderBrace;", "⏟"},
#line 1778 "HTMLCharacterReference.gperf"
      {"&subset;", "⊂"},
#line 1951 "HTMLCharacterReference.gperf"
      {"&UnderBracket;", "⎵"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1066 "HTMLCharacterReference.gperf"
      {"&midast;", "*"},
      {""},
#line 1717 "HTMLCharacterReference.gperf"
      {"&smt;", "⪪"},
      {""}, {""}, {""}, {""}, {""},
#line 1547 "HTMLCharacterReference.gperf"
      {"&rdca;", "⤷"},
      {""}, {""}, {""}, {""},
#line 236 "HTMLCharacterReference.gperf"
      {"&caps;", "∩︀"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1335 "HTMLCharacterReference.gperf"
      {"&odiv;", "⨸"},
      {""}, {""}, {""}, {""}, {""},
#line 1351 "HTMLCharacterReference.gperf"
      {"&olcir;", "⦾"},
      {""}, {""}, {""}, {""},
#line 1540 "HTMLCharacterReference.gperf"
      {"&rcaron;", "ř"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 794 "HTMLCharacterReference.gperf"
      {"&Iukcy;", "І"},
#line 1274 "HTMLCharacterReference.gperf"
      {"&nsqsube;", "⋢"},
      {""}, {""}, {""},
#line 1526 "HTMLCharacterReference.gperf"
      {"&rAtail;", "⤜"},
#line 1193 "HTMLCharacterReference.gperf"
      {"&NotGreaterGreater;", "≫̸"},
      {""}, {""},
#line 1191 "HTMLCharacterReference.gperf"
      {"&NotGreaterEqual;", "≱"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1453 "HTMLCharacterReference.gperf"
      {"&preccurlyeq;", "≼"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1640 "HTMLCharacterReference.gperf"
      {"&rtrif;", "▸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 755 "HTMLCharacterReference.gperf"
      {"&imped;", "Ƶ"},
      {""}, {""},
#line 1632 "HTMLCharacterReference.gperf"
      {"&rsh;", "↱"},
      {""}, {""},
#line 1053 "HTMLCharacterReference.gperf"
      {"&mcomma;", "⨩"},
      {""}, {""}, {""}, {""}, {""},
#line 146 "HTMLCharacterReference.gperf"
      {"&blacktriangledown;", "▾"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 683 "HTMLCharacterReference.gperf"
      {"&Hacek;", "ˇ"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1359 "HTMLCharacterReference.gperf"
      {"&Omicron;", "Ο"},
      {""}, {""},
#line 749 "HTMLCharacterReference.gperf"
      {"&image;", "ℑ"},
      {""},
#line 1389 "HTMLCharacterReference.gperf"
      {"&otilde;", "õ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 688 "HTMLCharacterReference.gperf"
      {"&hardcy;", "ъ"},
      {""}, {""}, {""},
#line 652 "HTMLCharacterReference.gperf"
      {"&gopf;", "𝕘"},
      {""}, {""}, {""},
#line 1728 "HTMLCharacterReference.gperf"
      {"&spadesuit;", "♠"},
#line 993 "HTMLCharacterReference.gperf"
      {"&lopf;", "𝕝"},
      {""},
#line 2036 "HTMLCharacterReference.gperf"
      {"&VerticalLine;", "|"},
      {""}, {""},
#line 518 "HTMLCharacterReference.gperf"
      {"&eopf;", "𝕖"},
      {""}, {""},
#line 1779 "HTMLCharacterReference.gperf"
      {"&subseteq;", "⊆"},
#line 1780 "HTMLCharacterReference.gperf"
      {"&subseteqq;", "⫅"},
#line 410 "HTMLCharacterReference.gperf"
      {"&dopf;", "𝕕"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 805 "HTMLCharacterReference.gperf"
      {"&Jopf;", "𝕁"},
#line 699 "HTMLCharacterReference.gperf"
      {"&hellip;", "…"},
      {""}, {""}, {""},
#line 75 "HTMLCharacterReference.gperf"
      {"&aopf;", "𝕒"},
#line 915 "HTMLCharacterReference.gperf"
      {"&LeftUpDownVector;", "⥑"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 577 "HTMLCharacterReference.gperf"
      {"&fopf;", "𝕗"},
      {""}, {""},
#line 1898 "HTMLCharacterReference.gperf"
      {"&triminus;", "⨺"},
      {""},
#line 159 "HTMLCharacterReference.gperf"
      {"&bopf;", "𝕓"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 136 "HTMLCharacterReference.gperf"
      {"&bigstar;", "★"},
      {""}, {""},
#line 2140 "HTMLCharacterReference.gperf"
      {"&zopf;", "𝕫"},
      {""}, {""},
#line 1666 "HTMLCharacterReference.gperf"
      {"&scy;", "с"},
      {""},
#line 711 "HTMLCharacterReference.gperf"
      {"&hopf;", "𝕙"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 409 "HTMLCharacterReference.gperf"
      {"&Dopf;", "𝔻"},
      {""}, {""}, {""}, {""},
#line 308 "HTMLCharacterReference.gperf"
      {"&copf;", "𝕔"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 575 "HTMLCharacterReference.gperf"
      {"&fnof;", "ƒ"},
#line 1413 "HTMLCharacterReference.gperf"
      {"&pertenk;", "‱"},
      {""}, {""}, {""}, {""},
#line 1830 "HTMLCharacterReference.gperf"
      {"&supsup;", "⫖"},
      {""}, {""}, {""}, {""}, {""},
#line 1902 "HTMLCharacterReference.gperf"
      {"&tritime;", "⨻"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1488 "HTMLCharacterReference.gperf"
      {"&Qopf;", "ℚ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1957 "HTMLCharacterReference.gperf"
      {"&Uopf;", "𝕌"},
#line 233 "HTMLCharacterReference.gperf"
      {"&capcup;", "⩇"},
      {""}, {""}, {""},
#line 806 "HTMLCharacterReference.gperf"
      {"&jopf;", "𝕛"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 992 "HTMLCharacterReference.gperf"
      {"&Lopf;", "𝕃"},
      {""}, {""}, {""}, {""},
#line 2114 "HTMLCharacterReference.gperf"
      {"&Yopf;", "𝕐"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1352 "HTMLCharacterReference.gperf"
      {"&olcross;", "⦻"},
#line 2143 "HTMLCharacterReference.gperf"
      {"&zwj;", "‍"},
      {""}, {""}, {""}, {""},
#line 1245 "HTMLCharacterReference.gperf"
      {"&nparallel;", "∦"},
#line 2115 "HTMLCharacterReference.gperf"
      {"&yopf;", "𝕪"},
      {""}, {""}, {""}, {""},
#line 571 "HTMLCharacterReference.gperf"
      {"&fjlig;", "fj"},
      {""}, {""},
#line 1375 "HTMLCharacterReference.gperf"
      {"&orderof;", "ℴ"},
      {""},
#line 42 "HTMLCharacterReference.gperf"
      {"&alpha;", "α"},
      {""}, {""},
#line 2039 "HTMLCharacterReference.gperf"
      {"&VeryThinSpace;", " "},
      {""},
#line 1088 "HTMLCharacterReference.gperf"
      {"&nabla;", "∇"},
      {""}, {""}, {""}, {""},
#line 778 "HTMLCharacterReference.gperf"
      {"&iopf;", "𝕚"},
#line 277 "HTMLCharacterReference.gperf"
      {"&CircleMinus;", "⊖"},
      {""}, {""}, {""}, {""},
#line 279 "HTMLCharacterReference.gperf"
      {"&CircleTimes;", "⊗"},
      {""},
#line 1641 "HTMLCharacterReference.gperf"
      {"&rtriltri;", "⧎"},
      {""},
#line 1618 "HTMLCharacterReference.gperf"
      {"&Ropf;", "ℝ"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1644 "HTMLCharacterReference.gperf"
      {"&rx;", "℞"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 916 "HTMLCharacterReference.gperf"
      {"&LeftUpTeeVector;", "⥠"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 651 "HTMLCharacterReference.gperf"
      {"&Gopf;", "𝔾"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1725 "HTMLCharacterReference.gperf"
      {"&Sopf;", "𝕊"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 481 "HTMLCharacterReference.gperf"
      {"&Ecy;", "Э"},
      {""}, {""}, {""},
#line 445 "HTMLCharacterReference.gperf"
      {"&DownLeftTeeVector;", "⥞"},
      {""}, {""},
#line 1327 "HTMLCharacterReference.gperf"
      {"&ocir;", "⊚"},
      {""},
#line 142 "HTMLCharacterReference.gperf"
      {"&bkarow;", "⤍"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1363 "HTMLCharacterReference.gperf"
      {"&Oopf;", "𝕆"},
      {""},
#line 707 "HTMLCharacterReference.gperf"
      {"&homtht;", "∻"},
      {""}, {""},
#line 181 "HTMLCharacterReference.gperf"
      {"&boxhu;", "┴"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1378 "HTMLCharacterReference.gperf"
      {"&origof;", "⊶"},
      {""}, {""},
#line 2067 "HTMLCharacterReference.gperf"
      {"&wopf;", "𝕨"},
#line 837 "HTMLCharacterReference.gperf"
      {"&lambda;", "λ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 576 "HTMLCharacterReference.gperf"
      {"&Fopf;", "𝔽"},
      {""}, {""}, {""}, {""}, {""},
#line 1353 "HTMLCharacterReference.gperf"
      {"&oline;", "‾"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1555 "HTMLCharacterReference.gperf"
      {"&realpart;", "ℜ"},
#line 1544 "HTMLCharacterReference.gperf"
      {"&rcub;", "}"},
#line 2003 "HTMLCharacterReference.gperf"
      {"&varpi;", "ϖ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1002 "HTMLCharacterReference.gperf"
      {"&lozf;", "⧫"},
      {""}, {""}, {""}, {""},
#line 1185 "HTMLCharacterReference.gperf"
      {"&NotDoubleVerticalBar;", "∦"},
      {""},
#line 1835 "HTMLCharacterReference.gperf"
      {"&swnwar;", "⤪"},
      {""},
#line 1231 "HTMLCharacterReference.gperf"
      {"&NotSubset;", "⊂⃒"},
#line 1180 "HTMLCharacterReference.gperf"
      {"&nopf;", "𝕟"},
      {""}, {""}, {""}, {""},
#line 745 "HTMLCharacterReference.gperf"
      {"&ijlig;", "ĳ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1489 "HTMLCharacterReference.gperf"
      {"&qopf;", "𝕢"},
#line 1620 "HTMLCharacterReference.gperf"
      {"&roplus;", "⨮"},
      {""},
#line 1799 "HTMLCharacterReference.gperf"
      {"&SuchThat;", "∋"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1896 "HTMLCharacterReference.gperf"
      {"&tridot;", "◬"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1873 "HTMLCharacterReference.gperf"
      {"&times;", "×"},
#line 198 "HTMLCharacterReference.gperf"
      {"&boxvh;", "┼"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1723 "HTMLCharacterReference.gperf"
      {"&solb;", "⧄"},
#line 1179 "HTMLCharacterReference.gperf"
      {"&Nopf;", "ℕ"},
#line 1657 "HTMLCharacterReference.gperf"
      {"&scedil;", "ş"},
      {""}, {""}, {""},
#line 1078 "HTMLCharacterReference.gperf"
      {"&Mopf;", "𝕄"},
      {""}, {""},
#line 2038 "HTMLCharacterReference.gperf"
      {"&VerticalTilde;", "≀"},
      {""},
#line 502 "HTMLCharacterReference.gperf"
      {"&Emacr;", "Ē"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1712 "HTMLCharacterReference.gperf"
      {"&smallsetminus;", "∖"},
      {""},
#line 180 "HTMLCharacterReference.gperf"
      {"&boxhU;", "╨"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 196 "HTMLCharacterReference.gperf"
      {"&boxVh;", "╫"},
#line 147 "HTMLCharacterReference.gperf"
      {"&blacktriangleleft;", "◂"},
      {""}, {""}, {""},
#line 1329 "HTMLCharacterReference.gperf"
      {"&ocirc;", "ô"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 573 "HTMLCharacterReference.gperf"
      {"&fllig;", "ﬂ"},
      {""}, {""}, {""}, {""}, {""},
#line 836 "HTMLCharacterReference.gperf"
      {"&Lambda;", "Λ"},
      {""}, {""},
#line 1782 "HTMLCharacterReference.gperf"
      {"&subsetneq;", "⊊"},
#line 1783 "HTMLCharacterReference.gperf"
      {"&subsetneqq;", "⫋"},
      {""}, {""}, {""}, {""},
#line 74 "HTMLCharacterReference.gperf"
      {"&Aopf;", "𝔸"},
      {""},
#line 557 "HTMLCharacterReference.gperf"
      {"&ExponentialE;", "ⅇ"},
      {""}, {""},
#line 710 "HTMLCharacterReference.gperf"
      {"&Hopf;", "ℍ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1575 "HTMLCharacterReference.gperf"
      {"&RightArrow;", "→"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 844 "HTMLCharacterReference.gperf"
      {"&laquo;", "«"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1060 "HTMLCharacterReference.gperf"
      {"&Mellintrf;", "ℳ"},
      {""}, {""}, {""},
#line 819 "HTMLCharacterReference.gperf"
      {"&kcy;", "к"},
      {""},
#line 1931 "HTMLCharacterReference.gperf"
      {"&udblac;", "ű"},
      {""}, {""}, {""},
#line 583 "HTMLCharacterReference.gperf"
      {"&fpartint;", "⨍"},
      {""}, {""}, {""}, {""},
#line 218 "HTMLCharacterReference.gperf"
      {"&bsolhsub;", "⟈"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1047 "HTMLCharacterReference.gperf"
      {"&map;", "↦"},
      {""}, {""},
#line 1998 "HTMLCharacterReference.gperf"
      {"&vangrt;", "⦜"},
      {""}, {""}, {""},
#line 1442 "HTMLCharacterReference.gperf"
      {"&Popf;", "ℙ"},
      {""}, {""}, {""}, {""}, {""},
#line 1876 "HTMLCharacterReference.gperf"
      {"&timesd;", "⨰"},
#line 1554 "HTMLCharacterReference.gperf"
      {"&realine;", "ℛ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 984 "HTMLCharacterReference.gperf"
      {"&longleftrightarrow;", "⟷"},
      {""},
#line 777 "HTMLCharacterReference.gperf"
      {"&Iopf;", "𝕀"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2088 "HTMLCharacterReference.gperf"
      {"&Xopf;", "𝕏"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1357 "HTMLCharacterReference.gperf"
      {"&Omega;", "Ω"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2139 "HTMLCharacterReference.gperf"
      {"&Zopf;", "ℤ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1628 "HTMLCharacterReference.gperf"
      {"&rsaquo;", "›"},
      {""}, {""}, {""},
#line 814 "HTMLCharacterReference.gperf"
      {"&kappa;", "κ"},
      {""},
#line 912 "HTMLCharacterReference.gperf"
      {"&LeftTriangle;", "⊲"},
#line 713 "HTMLCharacterReference.gperf"
      {"&HorizontalLine;", "─"},
      {""},
#line 41 "HTMLCharacterReference.gperf"
      {"&Alpha;", "Α"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 818 "HTMLCharacterReference.gperf"
      {"&Kcy;", "К"},
      {""}, {""}, {""}, {""},
#line 1919 "HTMLCharacterReference.gperf"
      {"&uarr;", "↑"},
      {""}, {""}, {""},
#line 914 "HTMLCharacterReference.gperf"
      {"&LeftTriangleEqual;", "⊴"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 158 "HTMLCharacterReference.gperf"
      {"&Bopf;", "𝔹"},
#line 1859 "HTMLCharacterReference.gperf"
      {"&thickapprox;", "≈"},
      {""}, {""}, {""},
#line 306 "HTMLCharacterReference.gperf"
      {"&ContourIntegral;", "∮"},
#line 1662 "HTMLCharacterReference.gperf"
      {"&scnsim;", "⋩"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1784 "HTMLCharacterReference.gperf"
      {"&subsim;", "⫇"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1698 "HTMLCharacterReference.gperf"
      {"&sigmav;", "ς"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1791 "HTMLCharacterReference.gperf"
      {"&SucceedsEqual;", "⪰"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 692 "HTMLCharacterReference.gperf"
      {"&harrw;", "↭"},
      {""}, {""},
#line 983 "HTMLCharacterReference.gperf"
      {"&Longleftrightarrow;", "⟺"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1858 "HTMLCharacterReference.gperf"
      {"&thetav;", "ϑ"},
      {""}, {""}, {""}, {""}, {""},
#line 1676 "HTMLCharacterReference.gperf"
      {"&seswar;", "⤩"},
      {""}, {""}, {""}, {""}, {""},
#line 920 "HTMLCharacterReference.gperf"
      {"&LeftVectorBar;", "⥒"},
      {""},
#line 813 "HTMLCharacterReference.gperf"
      {"&Kappa;", "Κ"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 756 "HTMLCharacterReference.gperf"
      {"&Implies;", "⇒"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1569 "HTMLCharacterReference.gperf"
      {"&rharu;", "⇀"},
#line 100 "HTMLCharacterReference.gperf"
      {"&backepsilon;", "϶"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 344 "HTMLCharacterReference.gperf"
      {"&curlyvee;", "⋎"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 471 "HTMLCharacterReference.gperf"
      {"&dzigrarr;", "⟿"},
      {""},
#line 2066 "HTMLCharacterReference.gperf"
      {"&Wopf;", "𝕎"},
      {""}, {""}, {""}, {""}, {""},
#line 1570 "HTMLCharacterReference.gperf"
      {"&rharul;", "⥬"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1067 "HTMLCharacterReference.gperf"
      {"&midcir;", "⫰"},
      {""}, {""},
#line 1718 "HTMLCharacterReference.gperf"
      {"&smte;", "⪬"},
#line 287 "HTMLCharacterReference.gperf"
      {"&CloseCurlyQuote;", "’"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1531 "HTMLCharacterReference.gperf"
      {"&rBarr;", "⤏"},
      {""}, {""}, {""}, {""},
#line 122 "HTMLCharacterReference.gperf"
      {"&Bernoullis;", "ℬ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1966 "HTMLCharacterReference.gperf"
      {"&updownarrow;", "↕"},
#line 817 "HTMLCharacterReference.gperf"
      {"&kcedil;", "ķ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1636 "HTMLCharacterReference.gperf"
      {"&rthree;", "⋌"},
      {""}, {""}, {""}, {""},
#line 1052 "HTMLCharacterReference.gperf"
      {"&marker;", "▮"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1829 "HTMLCharacterReference.gperf"
      {"&supsub;", "⫔"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 568 "HTMLCharacterReference.gperf"
      {"&filig;", "ﬁ"},
#line 1238 "HTMLCharacterReference.gperf"
      {"&NotSupersetEqual;", "⊉"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1764 "HTMLCharacterReference.gperf"
      {"&straightphi;", "ϕ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 286 "HTMLCharacterReference.gperf"
      {"&CloseCurlyDoubleQuote;", "”"},
      {""}, {""}, {""},
#line 1097 "HTMLCharacterReference.gperf"
      {"&natur;", "♮"},
      {""}, {""},
#line 1317 "HTMLCharacterReference.gperf"
      {"&nvrtrie;", "⊵⃒"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 97 "HTMLCharacterReference.gperf"
      {"&awconint;", "∳"},
#line 709 "HTMLCharacterReference.gperf"
      {"&hookrightarrow;", "↪"},
      {""}, {""},
#line 1240 "HTMLCharacterReference.gperf"
      {"&NotTildeEqual;", "≄"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1922 "HTMLCharacterReference.gperf"
      {"&ubrcy;", "ў"},
#line 816 "HTMLCharacterReference.gperf"
      {"&Kcedil;", "Ķ"},
#line 1198 "HTMLCharacterReference.gperf"
      {"&NotHumpEqual;", "≏̸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1222 "HTMLCharacterReference.gperf"
      {"&NotPrecedesSlantEqual;", "⋠"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 307 "HTMLCharacterReference.gperf"
      {"&Copf;", "ℂ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1634 "HTMLCharacterReference.gperf"
      {"&rsquo;", "’"},
#line 1635 "HTMLCharacterReference.gperf"
      {"&rsquor;", "’"},
      {""}, {""}, {""},
#line 271 "HTMLCharacterReference.gperf"
      {"&circledast;", "⊛"},
      {""}, {""}, {""},
#line 351 "HTMLCharacterReference.gperf"
      {"&cwconint;", "∲"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1719 "HTMLCharacterReference.gperf"
      {"&smtes;", "⪬︀"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 141 "HTMLCharacterReference.gperf"
      {"&bigwedge;", "⋀"},
#line 1716 "HTMLCharacterReference.gperf"
      {"&smile;", "⌣"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 966 "HTMLCharacterReference.gperf"
      {"&lmidot;", "ŀ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1184 "HTMLCharacterReference.gperf"
      {"&NotCupCap;", "≭"},
#line 1653 "HTMLCharacterReference.gperf"
      {"&sccue;", "≽"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1579 "HTMLCharacterReference.gperf"
      {"&RightArrowLeftArrow;", "⇄"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1863 "HTMLCharacterReference.gperf"
      {"&ThinSpace;", " "},
#line 1338 "HTMLCharacterReference.gperf"
      {"&OElig;", "Œ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2089 "HTMLCharacterReference.gperf"
      {"&xopf;", "𝕩"},
      {""}, {""}, {""},
#line 911 "HTMLCharacterReference.gperf"
      {"&leftthreetimes;", "⋋"},
      {""}, {""}, {""},
#line 834 "HTMLCharacterReference.gperf"
      {"&laemptyv;", "⦴"},
      {""}, {""},
#line 1233 "HTMLCharacterReference.gperf"
      {"&NotSucceeds;", "⊁"},
      {""}, {""},
#line 1899 "HTMLCharacterReference.gperf"
      {"&TripleDot;", "◌⃛"},
#line 1901 "HTMLCharacterReference.gperf"
      {"&trisb;", "⧍"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1468 "HTMLCharacterReference.gperf"
      {"&prnsim;", "⋨"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1840 "HTMLCharacterReference.gperf"
      {"&tau;", "τ"},
      {""}, {""}, {""}, {""}, {""},
#line 1606 "HTMLCharacterReference.gperf"
      {"&ring;", "˚"},
      {""},
#line 1874 "HTMLCharacterReference.gperf"
      {"&timesb;", "⊠"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1176 "HTMLCharacterReference.gperf"
      {"&nmid;", "∤"},
      {""},
#line 1724 "HTMLCharacterReference.gperf"
      {"&solbar;", "⌿"},
      {""},
#line 752 "HTMLCharacterReference.gperf"
      {"&imagpart;", "ℑ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1954 "HTMLCharacterReference.gperf"
      {"&UnionPlus;", "⊎"},
#line 521 "HTMLCharacterReference.gperf"
      {"&eplus;", "⩱"},
#line 965 "HTMLCharacterReference.gperf"
      {"&Lmidot;", "Ŀ"},
      {""}, {""},
#line 111 "HTMLCharacterReference.gperf"
      {"&bbrktbrk;", "⎶"},
#line 744 "HTMLCharacterReference.gperf"
      {"&IJlig;", "Ĳ"},
      {""}, {""}, {""}, {""}, {""},
#line 1068 "HTMLCharacterReference.gperf"
      {"&middot;", "·"},
#line 1928 "HTMLCharacterReference.gperf"
      {"&ucy;", "у"},
#line 1405 "HTMLCharacterReference.gperf"
      {"&part;", "∂"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1055 "HTMLCharacterReference.gperf"
      {"&mcy;", "м"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1326 "HTMLCharacterReference.gperf"
      {"&oast;", "⊛"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1428 "HTMLCharacterReference.gperf"
      {"&plus;", "+"},
      {""},
#line 1427 "HTMLCharacterReference.gperf"
      {"&plankv;", "ℏ"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1400 "HTMLCharacterReference.gperf"
      {"&par;", "∥"},
      {""},
#line 643 "HTMLCharacterReference.gperf"
      {"&glj;", "⪤"},
      {""},
#line 2030 "HTMLCharacterReference.gperf"
      {"&vellip;", "⋮"},
      {""}, {""}, {""},
#line 32 "HTMLCharacterReference.gperf"
      {"&AElig;", "Æ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1602 "HTMLCharacterReference.gperf"
      {"&RightUpVector;", "↾"},
      {""}, {""},
#line 1124 "HTMLCharacterReference.gperf"
      {"&nequiv;", "≢"},
#line 1334 "HTMLCharacterReference.gperf"
      {"&odblac;", "ő"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 2046 "HTMLCharacterReference.gperf"
      {"&vopf;", "𝕧"},
      {""}, {""}, {""}, {""}, {""},
#line 1685 "HTMLCharacterReference.gperf"
      {"&shchcy;", "щ"},
      {""}, {""}, {""}, {""},
#line 1601 "HTMLCharacterReference.gperf"
      {"&RightUpTeeVector;", "⥜"},
      {""}, {""}, {""}, {""},
#line 565 "HTMLCharacterReference.gperf"
      {"&ffllig;", "ﬄ"},
      {""},
#line 1875 "HTMLCharacterReference.gperf"
      {"&timesbar;", "⨱"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 1609 "HTMLCharacterReference.gperf"
      {"&rlhar;", "⇌"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1947 "HTMLCharacterReference.gperf"
      {"&umacr;", "ū"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1401 "HTMLCharacterReference.gperf"
      {"&para;", "¶"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1042 "HTMLCharacterReference.gperf"
      {"&macr;", "¯"},
#line 2045 "HTMLCharacterReference.gperf"
      {"&Vopf;", "𝕍"},
      {""}, {""}, {""}, {""}, {""},
#line 967 "HTMLCharacterReference.gperf"
      {"&lmoust;", "⎰"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1839 "HTMLCharacterReference.gperf"
      {"&Tau;", "Τ"},
      {""},
#line 1436 "HTMLCharacterReference.gperf"
      {"&plusmn;", "±"},
      {""}, {""}, {""}, {""}, {""},
#line 1711 "HTMLCharacterReference.gperf"
      {"&SmallCircle;", "∘"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 555 "HTMLCharacterReference.gperf"
      {"&Exists;", "∃"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1962 "HTMLCharacterReference.gperf"
      {"&UpArrowBar;", "⤒"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 40 "HTMLCharacterReference.gperf"
      {"&aleph;", "ℵ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1786 "HTMLCharacterReference.gperf"
      {"&subsup;", "⫓"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1964 "HTMLCharacterReference.gperf"
      {"&UpDownArrow;", "↕"},
      {""}, {""},
#line 1434 "HTMLCharacterReference.gperf"
      {"&pluse;", "⩲"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1448 "HTMLCharacterReference.gperf"
      {"&prcue;", "≼"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 750 "HTMLCharacterReference.gperf"
      {"&ImaginaryI;", "ⅈ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1911 "HTMLCharacterReference.gperf"
      {"&tstrok;", "ŧ"},
      {""}, {""}, {""}, {""},
#line 1439 "HTMLCharacterReference.gperf"
      {"&pm;", "±"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 360 "HTMLCharacterReference.gperf"
      {"&dash;", "‐"},
      {""}, {""}, {""}, {""}, {""},
#line 362 "HTMLCharacterReference.gperf"
      {"&dashv;", "⊣"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1726 "HTMLCharacterReference.gperf"
      {"&sopf;", "𝕤"},
      {""}, {""}, {""}, {""}, {""},
#line 1438 "HTMLCharacterReference.gperf"
      {"&plustwo;", "⨧"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1847 "HTMLCharacterReference.gperf"
      {"&tcy;", "т"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 361 "HTMLCharacterReference.gperf"
      {"&Dashv;", "⫤"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1232 "HTMLCharacterReference.gperf"
      {"&NotSubsetEqual;", "⊈"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 504 "HTMLCharacterReference.gperf"
      {"&empty;", "∅"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1528 "HTMLCharacterReference.gperf"
      {"&ratio;", "∶"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1816 "HTMLCharacterReference.gperf"
      {"&suphsub;", "⫗"},
#line 1593 "HTMLCharacterReference.gperf"
      {"&RightTee;", "⊢"},
      {""},
#line 1549 "HTMLCharacterReference.gperf"
      {"&rdquo;", "”"},
#line 1550 "HTMLCharacterReference.gperf"
      {"&rdquor;", "”"},
#line 108 "HTMLCharacterReference.gperf"
      {"&barwed;", "⌅"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1595 "HTMLCharacterReference.gperf"
      {"&RightTeeVector;", "⥛"},
      {""}, {""}, {""}, {""}, {""},
#line 1069 "HTMLCharacterReference.gperf"
      {"&minus;", "−"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 905 "HTMLCharacterReference.gperf"
      {"&leftrightharpoons;", "⇋"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 937 "HTMLCharacterReference.gperf"
      {"&LessEqualGreater;", "⋚"},
      {""}, {""},
#line 1513 "HTMLCharacterReference.gperf"
      {"&rarr;", "→"},
      {""}, {""}, {""}, {""}, {""},
#line 1753 "HTMLCharacterReference.gperf"
      {"&squf;", "▪"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 1430 "HTMLCharacterReference.gperf"
      {"&plusb;", "⊞"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1910 "HTMLCharacterReference.gperf"
      {"&Tstrok;", "Ŧ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 517 "HTMLCharacterReference.gperf"
      {"&Eopf;", "𝔼"},
      {""}, {""}, {""}, {""},
#line 918 "HTMLCharacterReference.gperf"
      {"&LeftUpVectorBar;", "⥘"},
#line 1520 "HTMLCharacterReference.gperf"
      {"&rarrlp;", "↬"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1432 "HTMLCharacterReference.gperf"
      {"&plusdo;", "∔"},
      {""}, {""}, {""}, {""}, {""},
#line 1521 "HTMLCharacterReference.gperf"
      {"&rarrpl;", "⥅"},
      {""}, {""}, {""},
#line 903 "HTMLCharacterReference.gperf"
      {"&leftrightarrow;", "↔"},
#line 904 "HTMLCharacterReference.gperf"
      {"&leftrightarrows;", "⇆"},
      {""}, {""}, {""},
#line 1576 "HTMLCharacterReference.gperf"
      {"&Rightarrow;", "⇒"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1846 "HTMLCharacterReference.gperf"
      {"&Tcy;", "Т"},
      {""}, {""}, {""}, {""}, {""},
#line 505 "HTMLCharacterReference.gperf"
      {"&emptyset;", "∅"},
      {""}, {""},
#line 1408 "HTMLCharacterReference.gperf"
      {"&pcy;", "п"},
      {""}, {""}, {""},
#line 1071 "HTMLCharacterReference.gperf"
      {"&minusd;", "∸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1797 "HTMLCharacterReference.gperf"
      {"&succnsim;", "⋩"},
#line 420 "HTMLCharacterReference.gperf"
      {"&doublebarwedge;", "⌆"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1517 "HTMLCharacterReference.gperf"
      {"&rarrc;", "⤳"},
      {""}, {""}, {""}, {""}, {""},
#line 1524 "HTMLCharacterReference.gperf"
      {"&rarrtl;", "↣"},
      {""},
#line 1594 "HTMLCharacterReference.gperf"
      {"&RightTeeArrow;", "↦"},
      {""}, {""}, {""}, {""}, {""},
#line 1331 "HTMLCharacterReference.gperf"
      {"&ocy;", "о"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1433 "HTMLCharacterReference.gperf"
      {"&plusdu;", "⨥"},
      {""}, {""}, {""}, {""}, {""},
#line 1845 "HTMLCharacterReference.gperf"
      {"&tcedil;", "ţ"},
      {""}, {""}, {""}, {""},
#line 603 "HTMLCharacterReference.gperf"
      {"&gacute;", "ǵ"},
      {""}, {""}, {""}, {""},
#line 833 "HTMLCharacterReference.gperf"
      {"&lacute;", "ĺ"},
      {""}, {""}, {""}, {""},
#line 473 "HTMLCharacterReference.gperf"
      {"&eacute;", "é"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 704 "HTMLCharacterReference.gperf"
      {"&hksearow;", "⤥"},
      {""}, {""}, {""},
#line 1714 "HTMLCharacterReference.gperf"
      {"&smeparsl;", "⧤"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 902 "HTMLCharacterReference.gperf"
      {"&Leftrightarrow;", "⇔"},
#line 21 "HTMLCharacterReference.gperf"
      {"&aacute;", "á"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 988 "HTMLCharacterReference.gperf"
      {"&longrightarrow;", "⟶"},
#line 2123 "HTMLCharacterReference.gperf"
      {"&zacute;", "ź"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 227 "HTMLCharacterReference.gperf"
      {"&cacute;", "ć"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 828 "HTMLCharacterReference.gperf"
      {"&kopf;", "𝕜"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 703 "HTMLCharacterReference.gperf"
      {"&HilbertSpace;", "ℋ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1356 "HTMLCharacterReference.gperf"
      {"&omacr;", "ō"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1915 "HTMLCharacterReference.gperf"
      {"&Uacute;", "Ú"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 832 "HTMLCharacterReference.gperf"
      {"&Lacute;", "Ĺ"},
      {""}, {""}, {""}, {""},
#line 2101 "HTMLCharacterReference.gperf"
      {"&Yacute;", "Ý"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 39 "HTMLCharacterReference.gperf"
      {"&alefsym;", "ℵ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 2102 "HTMLCharacterReference.gperf"
      {"&yacute;", "ý"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 724 "HTMLCharacterReference.gperf"
      {"&iacute;", "í"},
      {""}, {""},
#line 1912 "HTMLCharacterReference.gperf"
      {"&twixt;", "≬"},
#line 987 "HTMLCharacterReference.gperf"
      {"&Longrightarrow;", "⟹"},
      {""}, {""},
#line 1583 "HTMLCharacterReference.gperf"
      {"&RightDownTeeVector;", "⥝"},
      {""}, {""},
#line 1501 "HTMLCharacterReference.gperf"
      {"&Racute;", "Ŕ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 827 "HTMLCharacterReference.gperf"
      {"&Kopf;", "𝕂"},
      {""}, {""}, {""}, {""}, {""},
#line 1564 "HTMLCharacterReference.gperf"
      {"&rfloor;", "⌋"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1844 "HTMLCharacterReference.gperf"
      {"&Tcedil;", "Ţ"},
      {""}, {""}, {""}, {""},
#line 1785 "HTMLCharacterReference.gperf"
      {"&subsub;", "⫕"},
      {""}, {""}, {""}, {""},
#line 1645 "HTMLCharacterReference.gperf"
      {"&Sacute;", "Ś"},
      {""}, {""}, {""}, {""}, {""},
#line 1607 "HTMLCharacterReference.gperf"
      {"&risingdotseq;", "≓"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 239 "HTMLCharacterReference.gperf"
      {"&Cayleys;", "ℭ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 1324 "HTMLCharacterReference.gperf"
      {"&Oacute;", "Ó"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1189 "HTMLCharacterReference.gperf"
      {"&NotExists;", "∄"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1122 "HTMLCharacterReference.gperf"
      {"&NegativeThinSpace;", "​"},
      {""}, {""}, {""},
#line 1072 "HTMLCharacterReference.gperf"
      {"&minusdu;", "⨪"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1090 "HTMLCharacterReference.gperf"
      {"&nacute;", "ń"},
      {""}, {""}, {""}, {""},
#line 939 "HTMLCharacterReference.gperf"
      {"&LessGreater;", "≶"},
      {""}, {""}, {""}, {""}, {""},
#line 107 "HTMLCharacterReference.gperf"
      {"&Barwed;", "⌆"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1865 "HTMLCharacterReference.gperf"
      {"&thksim;", "∼"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1546 "HTMLCharacterReference.gperf"
      {"&rcy;", "р"},
      {""},
#line 1515 "HTMLCharacterReference.gperf"
      {"&rarrb;", "⇥"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1503 "HTMLCharacterReference.gperf"
      {"&radic;", "√"},
#line 1089 "HTMLCharacterReference.gperf"
      {"&Nacute;", "Ń"},
      {""}, {""}, {""},
#line 1999 "HTMLCharacterReference.gperf"
      {"&varepsilon;", "ϵ"},
      {""}, {""}, {""}, {""},
#line 906 "HTMLCharacterReference.gperf"
      {"&leftrightsquigarrow;", "↭"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 901 "HTMLCharacterReference.gperf"
      {"&LeftRightArrow;", "↔"},
      {""},
#line 1223 "HTMLCharacterReference.gperf"
      {"&NotReverseElement;", "∌"},
      {""}, {""}, {""},
#line 1236 "HTMLCharacterReference.gperf"
      {"&NotSucceedsTilde;", "≿̸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 138 "HTMLCharacterReference.gperf"
      {"&bigtriangleup;", "△"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 20 "HTMLCharacterReference.gperf"
      {"&Aacute;", "Á"},
      {""}, {""}, {""}, {""}, {""},
#line 1123 "HTMLCharacterReference.gperf"
      {"&NegativeVeryThinSpace;", "​"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1715 "HTMLCharacterReference.gperf"
      {"&smid;", "∣"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1584 "HTMLCharacterReference.gperf"
      {"&RightDownVector;", "⇂"},
      {""},
#line 1574 "HTMLCharacterReference.gperf"
      {"&RightAngleBracket;", "⟩"},
#line 1585 "HTMLCharacterReference.gperf"
      {"&RightDownVectorBar;", "⥕"},
      {""}, {""}, {""}, {""},
#line 1086 "HTMLCharacterReference.gperf"
      {"&multimap;", "⊸"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1763 "HTMLCharacterReference.gperf"
      {"&straightepsilon;", "ϵ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1070 "HTMLCharacterReference.gperf"
      {"&minusb;", "⊟"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1535 "HTMLCharacterReference.gperf"
      {"&rbrack;", "]"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 723 "HTMLCharacterReference.gperf"
      {"&Iacute;", "Í"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 109 "HTMLCharacterReference.gperf"
      {"&barwedge;", "⌅"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1514 "HTMLCharacterReference.gperf"
      {"&rarrap;", "⥵"},
      {""},
#line 1837 "HTMLCharacterReference.gperf"
      {"&Tab;", "␉"},
      {""}, {""}, {""}, {""}, {""},
#line 1268 "HTMLCharacterReference.gperf"
      {"&nshortparallel;", "∦"},
      {""},
#line 2122 "HTMLCharacterReference.gperf"
      {"&Zacute;", "Ź"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 986 "HTMLCharacterReference.gperf"
      {"&LongRightArrow;", "⟶"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1234 "HTMLCharacterReference.gperf"
      {"&NotSucceedsEqual;", "⪰̸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1838 "HTMLCharacterReference.gperf"
      {"&target;", "⌖"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2001 "HTMLCharacterReference.gperf"
      {"&varnothing;", "∅"},
      {""}, {""}, {""}, {""}, {""},
#line 1542 "HTMLCharacterReference.gperf"
      {"&rcedil;", "ŗ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 134 "HTMLCharacterReference.gperf"
      {"&bigotimes;", "⨂"},
#line 815 "HTMLCharacterReference.gperf"
      {"&kappav;", "ϰ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1048 "HTMLCharacterReference.gperf"
      {"&mapsto;", "↦"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 1197 "HTMLCharacterReference.gperf"
      {"&NotHumpDownHump;", "≎̸"},
      {""},
#line 1600 "HTMLCharacterReference.gperf"
      {"&RightUpDownVector;", "⥏"},
      {""},
#line 769 "HTMLCharacterReference.gperf"
      {"&intlarhk;", "⨗"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1958 "HTMLCharacterReference.gperf"
      {"&uopf;", "𝕦"},
      {""}, {""}, {""},
#line 155 "HTMLCharacterReference.gperf"
      {"&bnequiv;", "≡⃥"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1079 "HTMLCharacterReference.gperf"
      {"&mopf;", "𝕞"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1516 "HTMLCharacterReference.gperf"
      {"&rarrbfs;", "⤠"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1551 "HTMLCharacterReference.gperf"
      {"&rdsh;", "↳"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1975 "HTMLCharacterReference.gperf"
      {"&upsih;", "ϒ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 1402 "HTMLCharacterReference.gperf"
      {"&parallel;", "∥"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1849 "HTMLCharacterReference.gperf"
      {"&telrec;", "⌕"},
      {""}, {""},
#line 899 "HTMLCharacterReference.gperf"
      {"&leftharpoonup;", "↼"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1908 "HTMLCharacterReference.gperf"
      {"&TSHcy;", "Ћ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 968 "HTMLCharacterReference.gperf"
      {"&lmoustache;", "⎰"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1506 "HTMLCharacterReference.gperf"
      {"&rang;", "⟩"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1160 "HTMLCharacterReference.gperf"
      {"&nLeftarrow;", "⇍"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1627 "HTMLCharacterReference.gperf"
      {"&Rrightarrow;", "⇛"},
      {""}, {""}, {""},
#line 226 "HTMLCharacterReference.gperf"
      {"&Cacute;", "Ć"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1909 "HTMLCharacterReference.gperf"
      {"&tshcy;", "ћ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1051 "HTMLCharacterReference.gperf"
      {"&mapstoup;", "↥"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 942 "HTMLCharacterReference.gperf"
      {"&lesssim;", "≲"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1386 "HTMLCharacterReference.gperf"
      {"&oslash;", "ø"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1663 "HTMLCharacterReference.gperf"
      {"&scpolint;", "⨓"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 1259 "HTMLCharacterReference.gperf"
      {"&nrightarrow;", "↛"},
#line 1798 "HTMLCharacterReference.gperf"
      {"&succsim;", "≿"},
      {""}, {""},
#line 962 "HTMLCharacterReference.gperf"
      {"&Lleftarrow;", "⇚"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1461 "HTMLCharacterReference.gperf"
      {"&precnsim;", "⋨"},
      {""}, {""}, {""},
#line 137 "HTMLCharacterReference.gperf"
      {"&bigtriangledown;", "▽"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1527 "HTMLCharacterReference.gperf"
      {"&ratail;", "⤚"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1162 "HTMLCharacterReference.gperf"
      {"&nLeftrightarrow;", "⇎"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1508 "HTMLCharacterReference.gperf"
      {"&range;", "⦥"},
      {""}, {""}, {""}, {""},
#line 1425 "HTMLCharacterReference.gperf"
      {"&planck;", "ℏ"},
      {""}, {""}, {""},
#line 1529 "HTMLCharacterReference.gperf"
      {"&rationals;", "ℚ"},
      {""}, {""}, {""}, {""}, {""},
#line 1861 "HTMLCharacterReference.gperf"
      {"&ThickSpace;", "  "},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 1360 "HTMLCharacterReference.gperf"
      {"&omicron;", "ο"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1883 "HTMLCharacterReference.gperf"
      {"&topf;", "𝕥"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 742 "HTMLCharacterReference.gperf"
      {"&iinfin;", "⧜"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1339 "HTMLCharacterReference.gperf"
      {"&oelig;", "œ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1113 "HTMLCharacterReference.gperf"
      {"&ndash;", "–"},
      {""}, {""}, {""}, {""},
#line 1161 "HTMLCharacterReference.gperf"
      {"&nleftarrow;", "↚"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1404 "HTMLCharacterReference.gperf"
      {"&parsl;", "⫽"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1049 "HTMLCharacterReference.gperf"
      {"&mapstodown;", "↧"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2000 "HTMLCharacterReference.gperf"
      {"&varkappa;", "ϰ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1500 "HTMLCharacterReference.gperf"
      {"&race;", "∽̱"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 945 "HTMLCharacterReference.gperf"
      {"&lfisht;", "⥼"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 380 "HTMLCharacterReference.gperf"
      {"&dfisht;", "⥿"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1603 "HTMLCharacterReference.gperf"
      {"&RightUpVectorBar;", "⥔"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1887 "HTMLCharacterReference.gperf"
      {"&TRADE;", "™"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1311 "HTMLCharacterReference.gperf"
      {"&nvinfin;", "⧞"},
      {""},
#line 1882 "HTMLCharacterReference.gperf"
      {"&Topf;", "𝕋"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1443 "HTMLCharacterReference.gperf"
      {"&popf;", "𝕡"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1224 "HTMLCharacterReference.gperf"
      {"&NotRightTriangle;", "⋫"},
      {""}, {""},
#line 1225 "HTMLCharacterReference.gperf"
      {"&NotRightTriangleBar;", "⧐̸"},
      {""},
#line 1226 "HTMLCharacterReference.gperf"
      {"&NotRightTriangleEqual;", "⋭"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1163 "HTMLCharacterReference.gperf"
      {"&nleftrightarrow;", "↮"},
      {""}, {""}, {""},
#line 1507 "HTMLCharacterReference.gperf"
      {"&rangd;", "⦒"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 101 "HTMLCharacterReference.gperf"
      {"&backprime;", "‵"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1364 "HTMLCharacterReference.gperf"
      {"&oopf;", "𝕠"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 898 "HTMLCharacterReference.gperf"
      {"&leftharpoondown;", "↽"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1518 "HTMLCharacterReference.gperf"
      {"&rarrfs;", "⤞"},
      {""}, {""}, {""}, {""},
#line 1509 "HTMLCharacterReference.gperf"
      {"&rangle;", "⟩"},
      {""}, {""}, {""},
#line 1836 "HTMLCharacterReference.gperf"
      {"&szlig;", "ß"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 1429 "HTMLCharacterReference.gperf"
      {"&plusacir;", "⨣"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1646 "HTMLCharacterReference.gperf"
      {"&sacute;", "ś"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 535 "HTMLCharacterReference.gperf"
      {"&Equilibrium;", "⇌"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 907 "HTMLCharacterReference.gperf"
      {"&LeftRightVector;", "⥎"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 314 "HTMLCharacterReference.gperf"
      {"&CounterClockwiseContourIntegral;", "∳"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 356 "HTMLCharacterReference.gperf"
      {"&daleth;", "ℸ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2023 "HTMLCharacterReference.gperf"
      {"&vDash;", "⊨"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 507 "HTMLCharacterReference.gperf"
      {"&emptyv;", "∅"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2011 "HTMLCharacterReference.gperf"
      {"&varsupsetneq;", "⊋︀"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1914 "HTMLCharacterReference.gperf"
      {"&twoheadrightarrow;", "↠"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 472 "HTMLCharacterReference.gperf"
      {"&Eacute;", "É"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1358 "HTMLCharacterReference.gperf"
      {"&omega;", "ω"},
      {""}, {""}, {""}, {""},
#line 2021 "HTMLCharacterReference.gperf"
      {"&VDash;", "⊫"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1121 "HTMLCharacterReference.gperf"
      {"&NegativeThickSpace;", "​"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1619 "HTMLCharacterReference.gperf"
      {"&ropf;", "𝕣"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 2012 "HTMLCharacterReference.gperf"
      {"&varsupsetneqq;", "⫌︀"},
      {""}, {""}, {""}, {""},
#line 1860 "HTMLCharacterReference.gperf"
      {"&thicksim;", "∼"},
#line 1970 "HTMLCharacterReference.gperf"
      {"&uplus;", "⊎"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 854 "HTMLCharacterReference.gperf"
      {"&larrsim;", "⥳"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 691 "HTMLCharacterReference.gperf"
      {"&harrcir;", "⥈"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1473 "HTMLCharacterReference.gperf"
      {"&profsurf;", "⌓"},
      {""}, {""},
#line 1376 "HTMLCharacterReference.gperf"
      {"&ordf;", "ª"},
      {""}, {""},
#line 1625 "HTMLCharacterReference.gperf"
      {"&rppolint;", "⨒"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 880 "HTMLCharacterReference.gperf"
      {"&ldrdhar;", "⥧"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 913 "HTMLCharacterReference.gperf"
      {"&LeftTriangleBar;", "⧏"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 804 "HTMLCharacterReference.gperf"
      {"&jmath;", "ȷ"},
      {""}, {""}, {""}, {""}, {""},
#line 1815 "HTMLCharacterReference.gperf"
      {"&suphsol;", "⟉"},
      {""}, {""},
#line 754 "HTMLCharacterReference.gperf"
      {"&imof;", "⊷"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1578 "HTMLCharacterReference.gperf"
      {"&RightArrowBar;", "⇥"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 753 "HTMLCharacterReference.gperf"
      {"&imath;", "ı"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1597 "HTMLCharacterReference.gperf"
      {"&RightTriangle;", "⊳"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 2024 "HTMLCharacterReference.gperf"
      {"&vdash;", "⊢"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 397 "HTMLCharacterReference.gperf"
      {"&DifferentialD;", "ⅆ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 284 "HTMLCharacterReference.gperf"
      {"&cirscir;", "⧂"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 443 "HTMLCharacterReference.gperf"
      {"&downharpoonright;", "⇂"},
      {""}, {""}, {""}, {""}, {""},
#line 1058 "HTMLCharacterReference.gperf"
      {"&measuredangle;", "∡"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 386 "HTMLCharacterReference.gperf"
      {"&DiacriticalAcute;", "´"},
      {""}, {""}, {""}, {""},
#line 1462 "HTMLCharacterReference.gperf"
      {"&precsim;", "≾"},
      {""}, {""},
#line 387 "HTMLCharacterReference.gperf"
      {"&DiacriticalDot;", "˙"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 2022 "HTMLCharacterReference.gperf"
      {"&Vdash;", "⊩"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 448 "HTMLCharacterReference.gperf"
      {"&DownRightTeeVector;", "⥟"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 388 "HTMLCharacterReference.gperf"
      {"&DiacriticalDoubleAcute;", "˝"},
      {""},
#line 1592 "HTMLCharacterReference.gperf"
      {"&rightsquigarrow;", "↝"},
      {""},
#line 2025 "HTMLCharacterReference.gperf"
      {"&Vdashl;", "⫦"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1361 "HTMLCharacterReference.gperf"
      {"&omid;", "⦶"},
      {""}, {""}, {""}, {""}, {""},
#line 1604 "HTMLCharacterReference.gperf"
      {"&RightVector;", "⇀"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1044 "HTMLCharacterReference.gperf"
      {"&malt;", "✠"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1510 "HTMLCharacterReference.gperf"
      {"&raquo;", "»"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 1039 "HTMLCharacterReference.gperf"
      {"&luruhar;", "⥦"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1471 "HTMLCharacterReference.gperf"
      {"&profalar;", "⌮"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 705 "HTMLCharacterReference.gperf"
      {"&hkswarow;", "⤦"},
      {""},
#line 2009 "HTMLCharacterReference.gperf"
      {"&varsubsetneq;", "⊊︀"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1581 "HTMLCharacterReference.gperf"
      {"&RightCeiling;", "⌉"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1590 "HTMLCharacterReference.gperf"
      {"&rightleftharpoons;", "⇌"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1525 "HTMLCharacterReference.gperf"
      {"&rarrw;", "↝"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1319 "HTMLCharacterReference.gperf"
      {"&nwarhk;", "⤣"},
      {""}, {""},
#line 1403 "HTMLCharacterReference.gperf"
      {"&parsim;", "⫳"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 449 "HTMLCharacterReference.gperf"
      {"&DownRightVector;", "⇁"},
      {""}, {""},
#line 450 "HTMLCharacterReference.gperf"
      {"&DownRightVectorBar;", "⥗"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 1589 "HTMLCharacterReference.gperf"
      {"&rightleftarrows;", "⇄"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 1884 "HTMLCharacterReference.gperf"
      {"&topfork;", "⫚"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1441 "HTMLCharacterReference.gperf"
      {"&pointint;", "⨕"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1235 "HTMLCharacterReference.gperf"
      {"&NotSucceedsSlantEqual;", "⋡"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1369 "HTMLCharacterReference.gperf"
      {"&oplus;", "⊕"},
      {""},
#line 2010 "HTMLCharacterReference.gperf"
      {"&varsubsetneqq;", "⫋︀"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 102 "HTMLCharacterReference.gperf"
      {"&backsim;", "∽"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 1916 "HTMLCharacterReference.gperf"
      {"&uacute;", "ú"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 559 "HTMLCharacterReference.gperf"
      {"&fallingdotseq;", "≒"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1315 "HTMLCharacterReference.gperf"
      {"&nvltrie;", "⊴⃒"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1889 "HTMLCharacterReference.gperf"
      {"&triangle;", "▵"},
#line 1893 "HTMLCharacterReference.gperf"
      {"&triangleq;", "≜"},
      {""}, {""},
#line 1891 "HTMLCharacterReference.gperf"
      {"&triangleleft;", "◃"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1892 "HTMLCharacterReference.gperf"
      {"&trianglelefteq;", "⊴"},
      {""}, {""},
#line 1948 "HTMLCharacterReference.gperf"
      {"&uml;", "¨"},
      {""}, {""}, {""}, {""},
#line 1362 "HTMLCharacterReference.gperf"
      {"&ominus;", "⊖"},
      {""}, {""}, {""}, {""},
#line 1890 "HTMLCharacterReference.gperf"
      {"&triangledown;", "▿"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1586 "HTMLCharacterReference.gperf"
      {"&RightFloor;", "⌋"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2007 "HTMLCharacterReference.gperf"
      {"&varrho;", "ϱ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1258 "HTMLCharacterReference.gperf"
      {"&nRightarrow;", "⇏"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1869 "HTMLCharacterReference.gperf"
      {"&tilde;", "˜"},
      {""}, {""},
#line 1504 "HTMLCharacterReference.gperf"
      {"&raemptyv;", "⦳"},
      {""}, {""}, {""},
#line 1115 "HTMLCharacterReference.gperf"
      {"&nearhk;", "⤤"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1045 "HTMLCharacterReference.gperf"
      {"&maltese;", "✠"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 442 "HTMLCharacterReference.gperf"
      {"&downharpoonleft;", "⇃"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1426 "HTMLCharacterReference.gperf"
      {"&planckh;", "ℎ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 569 "HTMLCharacterReference.gperf"
      {"&FilledSmallSquare;", "◼"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 1435 "HTMLCharacterReference.gperf"
      {"&PlusMinus;", "±"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1406 "HTMLCharacterReference.gperf"
      {"&PartialD;", "∂"},
      {""},
#line 103 "HTMLCharacterReference.gperf"
      {"&backsimeq;", "⋍"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1178 "HTMLCharacterReference.gperf"
      {"&NonBreakingSpace;", " "},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1073 "HTMLCharacterReference.gperf"
      {"&MinusPlus;", "∓"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 1894 "HTMLCharacterReference.gperf"
      {"&triangleright;", "▹"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1868 "HTMLCharacterReference.gperf"
      {"&Tilde;", "∼"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1611 "HTMLCharacterReference.gperf"
      {"&rmoust;", "⎱"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1043 "HTMLCharacterReference.gperf"
      {"&male;", "♂"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1587 "HTMLCharacterReference.gperf"
      {"&rightharpoondown;", "⇁"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1857 "HTMLCharacterReference.gperf"
      {"&thetasym;", "ϑ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1588 "HTMLCharacterReference.gperf"
      {"&rightharpoonup;", "⇀"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 390 "HTMLCharacterReference.gperf"
      {"&DiacriticalTilde;", "˜"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1870 "HTMLCharacterReference.gperf"
      {"&TildeEqual;", "≃"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1325 "HTMLCharacterReference.gperf"
      {"&oacute;", "ó"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 2015 "HTMLCharacterReference.gperf"
      {"&vartriangleright;", "⊳"},
      {""}, {""},
#line 1577 "HTMLCharacterReference.gperf"
      {"&rightarrow;", "→"},
      {""}, {""}, {""}, {""},
#line 235 "HTMLCharacterReference.gperf"
      {"&CapitalDifferentialD;", "ⅅ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1895 "HTMLCharacterReference.gperf"
      {"&trianglerighteq;", "⊵"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1599 "HTMLCharacterReference.gperf"
      {"&RightTriangleEqual;", "⊵"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1056 "HTMLCharacterReference.gperf"
      {"&mdash;", "—"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1423 "HTMLCharacterReference.gperf"
      {"&pitchfork;", "⋔"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1591 "HTMLCharacterReference.gperf"
      {"&rightrightarrows;", "⇉"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2056 "HTMLCharacterReference.gperf"
      {"&vzigzag;", "⦚"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1831 "HTMLCharacterReference.gperf"
      {"&swarhk;", "⤦"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1502 "HTMLCharacterReference.gperf"
      {"&racute;", "ŕ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 751 "HTMLCharacterReference.gperf"
      {"&imagline;", "ℐ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 2131 "HTMLCharacterReference.gperf"
      {"&ZeroWidthSpace;", "​"},
      {""}, {""}, {""}, {""}, {""},
#line 1933 "HTMLCharacterReference.gperf"
      {"&ufisht;", "⥾"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1099 "HTMLCharacterReference.gperf"
      {"&naturals;", "ℕ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1670 "HTMLCharacterReference.gperf"
      {"&searhk;", "⤥"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 570 "HTMLCharacterReference.gperf"
      {"&FilledVerySmallSquare;", "▪"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1598 "HTMLCharacterReference.gperf"
      {"&RightTriangleBar;", "⧐"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 2037 "HTMLCharacterReference.gperf"
      {"&VerticalSeparator;", "❘"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2014 "HTMLCharacterReference.gperf"
      {"&vartriangleleft;", "⊲"},
      {""}, {""}, {""}, {""}, {""},
#line 1098 "HTMLCharacterReference.gperf"
      {"&natural;", "♮"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1332 "HTMLCharacterReference.gperf"
      {"&odash;", "⊝"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1605 "HTMLCharacterReference.gperf"
      {"&RightVectorBar;", "⥓"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 885 "HTMLCharacterReference.gperf"
      {"&LeftAngleBracket;", "⟨"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 1582 "HTMLCharacterReference.gperf"
      {"&RightDoubleBracket;", "⟧"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1612 "HTMLCharacterReference.gperf"
      {"&rmoustache;", "⎱"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 938 "HTMLCharacterReference.gperf"
      {"&LessFullEqual;", "≦"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1050 "HTMLCharacterReference.gperf"
      {"&mapstoleft;", "↤"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1563 "HTMLCharacterReference.gperf"
      {"&rfisht;", "⥽"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1431 "HTMLCharacterReference.gperf"
      {"&pluscir;", "⨢"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 685 "HTMLCharacterReference.gperf"
      {"&half;", "½"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1437 "HTMLCharacterReference.gperf"
      {"&plussim;", "⨦"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 944 "HTMLCharacterReference.gperf"
      {"&LessTilde;", "≲"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 772 "HTMLCharacterReference.gperf"
      {"&InvisibleTimes;", "⁢"},
      {""},
#line 851 "HTMLCharacterReference.gperf"
      {"&larrhk;", "↩"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 2008 "HTMLCharacterReference.gperf"
      {"&varsigma;", "ς"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 282 "HTMLCharacterReference.gperf"
      {"&cirfnint;", "⨐"},
      {""},
#line 389 "HTMLCharacterReference.gperf"
      {"&DiacriticalGrave;", "`"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1642 "HTMLCharacterReference.gperf"
      {"&RuleDelayed;", "⧴"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1713 "HTMLCharacterReference.gperf"
      {"&smashp;", "⨳"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1120 "HTMLCharacterReference.gperf"
      {"&NegativeMediumSpace;", "​"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1522 "HTMLCharacterReference.gperf"
      {"&rarrsim;", "⥴"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1872 "HTMLCharacterReference.gperf"
      {"&TildeTilde;", "≈"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1596 "HTMLCharacterReference.gperf"
      {"&rightthreetimes;", "⋌"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1913 "HTMLCharacterReference.gperf"
      {"&twoheadleftarrow;", "↞"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1871 "HTMLCharacterReference.gperf"
      {"&TildeFullEqual;", "≅"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1967 "HTMLCharacterReference.gperf"
      {"&UpEquilibrium;", "⥮"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2013 "HTMLCharacterReference.gperf"
      {"&vartheta;", "ϑ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1472 "HTMLCharacterReference.gperf"
      {"&profline;", "⌒"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 1580 "HTMLCharacterReference.gperf"
      {"&rightarrowtail;", "↣"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 2002 "HTMLCharacterReference.gperf"
      {"&varphi;", "ϕ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 508 "HTMLCharacterReference.gperf"
      {"&EmptyVerySmallSquare;", "▫"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 771 "HTMLCharacterReference.gperf"
      {"&InvisibleComma;", "⁣"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 506 "HTMLCharacterReference.gperf"
      {"&EmptySmallSquare;", "◻"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1519 "HTMLCharacterReference.gperf"
      {"&rarrhk;", "↪"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 1903 "HTMLCharacterReference.gperf"
      {"&trpezium;", "⏢"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1548 "HTMLCharacterReference.gperf"
      {"&rdldhar;", "⥩"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1643 "HTMLCharacterReference.gperf"
      {"&ruluhar;", "⥨"}
    };

  if (len <= MAX_WORD_LENGTH && len >= MIN_WORD_LENGTH)
    {
      unsigned int key = hash (str, len);

      if (key <= MAX_HASH_VALUE)
        {
          const char *s = wordlist[key].name;

          if (*str == *s && !strncmp (str + 1, s + 1, len - 1) && s[len] == '\0')
            return &wordlist[key];
        }
    }
  return 0;
}
#line 2145 "HTMLCharacterReference.gperf"

