/* C++ code produced by gperf version 3.1 */
/* Command-line: gperf -t --output-file=HTMLCharacterReference.generated.cpp HTMLCharacterReference.gperf  */
/* Computed positions: -k'1-8,12,14' */

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

#define TOTAL_KEYWORDS 2231
#define MIN_WORD_LENGTH 2
#define MAX_WORD_LENGTH 32
#define MIN_HASH_VALUE 2
#define MAX_HASH_VALUE 15511
/* maximum key range = 15510, duplicates = 0 */

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
      15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512,
      15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512,
      15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512,
      15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512,
      15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512,
      15512, 15512,     0,    60,    15,    20,    25, 15512,    90,   280,
          0,     0,     0, 15512,     5,  3060,  3035,    30,   230,  2900,
       1985,  3425,   320,   185,  3555,     0,   420,  1685,   970,  1835,
       1850,   430,   745,   210,   770,   205,   590,   480,  1595,   290,
        350,   900,  3370,  1240,    90,   730,   545,  1210,    30,  1340,
       1135,   500,   250,   645,   190,  2210,   820,  3260,  2230,  3545,
         20,   145,    15,    50,    10,   100,     0,    55,   220,    25,
       2440,     5,  1570,   610,  3951,  4666,   320,  3633,  3130,  2755,
       3874,   120,   110,   755,  1430,  1250, 15512, 15512, 15512, 15512,
      15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512,
      15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512,
      15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512,
      15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512,
      15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512,
      15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512,
      15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512,
      15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512,
      15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512,
      15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512,
      15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512,
      15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512,
      15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512, 15512
    };
  unsigned int hval = len;

  switch (hval)
    {
      default:
        hval += asso_values[static_cast<unsigned char>(str[13])];
      /*FALLTHROUGH*/
      case 13:
      case 12:
        hval += asso_values[static_cast<unsigned char>(str[11])];
      /*FALLTHROUGH*/
      case 11:
      case 10:
      case 9:
      case 8:
        hval += asso_values[static_cast<unsigned char>(str[7])];
      /*FALLTHROUGH*/
      case 7:
        hval += asso_values[static_cast<unsigned char>(str[6]+1)];
      /*FALLTHROUGH*/
      case 6:
        hval += asso_values[static_cast<unsigned char>(str[5]+2)];
      /*FALLTHROUGH*/
      case 5:
        hval += asso_values[static_cast<unsigned char>(str[4]+3)];
      /*FALLTHROUGH*/
      case 4:
        hval += asso_values[static_cast<unsigned char>(str[3]+5)];
      /*FALLTHROUGH*/
      case 3:
        hval += asso_values[static_cast<unsigned char>(str[2]+1)];
      /*FALLTHROUGH*/
      case 2:
        hval += asso_values[static_cast<unsigned char>(str[1])];
      /*FALLTHROUGH*/
      case 1:
        hval += asso_values[static_cast<unsigned char>(str[0]+13)];
        break;
    }
  return hval;
}

const struct NameAndGlyph *
HTMLCharacterHash::Lookup (const char *str, size_t len)
{
  static const struct NameAndGlyph wordlist[] =
    {
      {""}, {""},
#line 1154 "HTMLCharacterReference.gperf"
      {"gt", ">"},
#line 1155 "HTMLCharacterReference.gperf"
      {"gt;", ">"},
      {""}, {""}, {""},
#line 1409 "HTMLCharacterReference.gperf"
      {"lt", "<"},
#line 1410 "HTMLCharacterReference.gperf"
      {"lt;", "<"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 703 "HTMLCharacterReference.gperf"
      {"ap;", "≈"},
      {""}, {""}, {""}, {""}, {""},
#line 1397 "HTMLCharacterReference.gperf"
      {"lrm;", "‎"},
      {""}, {""}, {""}, {""},
#line 1061 "HTMLCharacterReference.gperf"
      {"eta;", "η"},
#line 1043 "HTMLCharacterReference.gperf"
      {"epsi;", "ε"},
      {""}, {""}, {""}, {""}, {""},
#line 1045 "HTMLCharacterReference.gperf"
      {"epsiv;", "ϵ"},
      {""}, {""}, {""}, {""},
#line 1147 "HTMLCharacterReference.gperf"
      {"gnsim;", "⋧"},
      {""}, {""}, {""}, {""},
#line 1372 "HTMLCharacterReference.gperf"
      {"lnsim;", "⋦"},
      {""}, {""}, {""},
#line 600 "HTMLCharacterReference.gperf"
      {"Upsi;", "ϒ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1391 "HTMLCharacterReference.gperf"
      {"lpar;", "("},
      {""}, {""}, {""}, {""},
#line 1040 "HTMLCharacterReference.gperf"
      {"epar;", "⋕"},
      {""}, {""}, {""}, {""},
#line 1037 "HTMLCharacterReference.gperf"
      {"ensp;", " "},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1048 "HTMLCharacterReference.gperf"
      {"eqsim;", "≂"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1142 "HTMLCharacterReference.gperf"
      {"gnap;", "⪊"},
      {""}, {""}, {""}, {""},
#line 1367 "HTMLCharacterReference.gperf"
      {"lnap;", "⪉"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2197 "HTMLCharacterReference.gperf"
      {"wr;", "≀"},
      {""}, {""}, {""}, {""},
#line 2196 "HTMLCharacterReference.gperf"
      {"wp;", "℘"},
#line 916 "HTMLCharacterReference.gperf"
      {"cup;", "∪"},
#line 1419 "HTMLCharacterReference.gperf"
      {"ltri;", "◃"},
#line 1393 "HTMLCharacterReference.gperf"
      {"lrarr;", "⇆"},
      {""}, {""}, {""}, {""},
#line 1057 "HTMLCharacterReference.gperf"
      {"erarr;", "⥱"},
      {""}, {""},
#line 1064 "HTMLCharacterReference.gperf"
      {"euml", "ë"},
#line 1065 "HTMLCharacterReference.gperf"
      {"euml;", "ë"},
#line 902 "HTMLCharacterReference.gperf"
      {"crarr;", "↵"},
      {""}, {""}, {""},
#line 1178 "HTMLCharacterReference.gperf"
      {"hbar;", "ℏ"},
      {""}, {""}, {""},
#line 719 "HTMLCharacterReference.gperf"
      {"auml", "ä"},
#line 720 "HTMLCharacterReference.gperf"
      {"auml;", "ä"},
#line 1302 "HTMLCharacterReference.gperf"
      {"lbarr;", "⤌"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 605 "HTMLCharacterReference.gperf"
      {"Uuml", "Ü"},
#line 606 "HTMLCharacterReference.gperf"
      {"Uuml;", "Ü"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1066 "HTMLCharacterReference.gperf"
      {"euro;", "€"},
      {""}, {""}, {""}, {""},
#line 997 "HTMLCharacterReference.gperf"
      {"dtri;", "▿"},
      {""}, {""}, {""}, {""}, {""},
#line 921 "HTMLCharacterReference.gperf"
      {"cupor;", "⩅"},
      {""}, {""},
#line 714 "HTMLCharacterReference.gperf"
      {"ast;", "*"},
      {""}, {""}, {""}, {""}, {""},
#line 773 "HTMLCharacterReference.gperf"
      {"bnot;", "⌐"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 994 "HTMLCharacterReference.gperf"
      {"dsol;", "⧶"},
#line 999 "HTMLCharacterReference.gperf"
      {"duarr;", "⇵"},
      {""},
#line 1249 "HTMLCharacterReference.gperf"
      {"it;", "⁢"},
      {""}, {""}, {""}, {""}, {""},
#line 1036 "HTMLCharacterReference.gperf"
      {"eng;", "ŋ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 683 "HTMLCharacterReference.gperf"
      {"ang;", "∠"},
#line 890 "HTMLCharacterReference.gperf"
      {"comp;", "∁"},
      {""}, {""},
#line 1224 "HTMLCharacterReference.gperf"
      {"in;", "∈"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 978 "HTMLCharacterReference.gperf"
      {"dot;", "˙"},
      {""}, {""}, {""}, {""}, {""},
#line 1005 "HTMLCharacterReference.gperf"
      {"eDot;", "≑"},
#line 1374 "HTMLCharacterReference.gperf"
      {"loarr;", "⇽"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 667 "HTMLCharacterReference.gperf"
      {"af;", "⁡"},
      {""}, {""}, {""}, {""}, {""},
#line 1144 "HTMLCharacterReference.gperf"
      {"gne;", "⪈"},
#line 835 "HTMLCharacterReference.gperf"
      {"bump;", "≎"},
      {""}, {""}, {""},
#line 1369 "HTMLCharacterReference.gperf"
      {"lne;", "⪇"},
      {""},
#line 695 "HTMLCharacterReference.gperf"
      {"angrt;", "∟"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 324 "HTMLCharacterReference.gperf"
      {"Lt;", "≪"},
#line 706 "HTMLCharacterReference.gperf"
      {"ape;", "≊"},
#line 732 "HTMLCharacterReference.gperf"
      {"bbrk;", "⎵"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1229 "HTMLCharacterReference.gperf"
      {"int;", "∫"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1053 "HTMLCharacterReference.gperf"
      {"equiv;", "≡"},
      {""}, {""}, {""},
#line 830 "HTMLCharacterReference.gperf"
      {"bsol;", "\\"},
#line 1187 "HTMLCharacterReference.gperf"
      {"hoarr;", "⇿"},
      {""}, {""}, {""}, {""},
#line 1420 "HTMLCharacterReference.gperf"
      {"ltrie;", "⊴"},
      {""}, {""}, {""}, {""}, {""},
#line 1041 "HTMLCharacterReference.gperf"
      {"eparsl;", "⧣"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1089 "HTMLCharacterReference.gperf"
      {"frac12", "½"},
#line 1090 "HTMLCharacterReference.gperf"
      {"frac12;", "½"},
      {""}, {""}, {""},
#line 2181 "HTMLCharacterReference.gperf"
      {"vprop;", "∝"},
      {""}, {""}, {""}, {""},
#line 1306 "HTMLCharacterReference.gperf"
      {"lbrke;", "⦋"},
      {""}, {""},
#line 1252 "HTMLCharacterReference.gperf"
      {"iuml", "ï"},
#line 1253 "HTMLCharacterReference.gperf"
      {"iuml;", "ï"},
#line 1092 "HTMLCharacterReference.gperf"
      {"frac14", "¼"},
#line 1093 "HTMLCharacterReference.gperf"
      {"frac14;", "¼"},
      {""},
#line 775 "HTMLCharacterReference.gperf"
      {"bot;", "⊥"},
      {""},
#line 960 "HTMLCharacterReference.gperf"
      {"dharr;", "⇂"},
#line 1094 "HTMLCharacterReference.gperf"
      {"frac15;", "⅕"},
      {""},
#line 1132 "HTMLCharacterReference.gperf"
      {"gfr;", "𝔤"},
      {""}, {""},
#line 1095 "HTMLCharacterReference.gperf"
      {"frac16;", "⅙"},
      {""},
#line 1350 "HTMLCharacterReference.gperf"
      {"lfr;", "𝔩"},
#line 1086 "HTMLCharacterReference.gperf"
      {"fork;", "⋔"},
#line 1099 "HTMLCharacterReference.gperf"
      {"frac34", "¾"},
#line 1100 "HTMLCharacterReference.gperf"
      {"frac34;", "¾"},
      {""},
#line 1018 "HTMLCharacterReference.gperf"
      {"efr;", "𝔢"},
      {""},
#line 1087 "HTMLCharacterReference.gperf"
      {"forkv;", "⫙"},
#line 1101 "HTMLCharacterReference.gperf"
      {"frac35;", "⅗"},
      {""},
#line 863 "HTMLCharacterReference.gperf"
      {"cfr;", "𝔠"},
      {""}, {""},
#line 1103 "HTMLCharacterReference.gperf"
      {"frac45;", "⅘"},
      {""},
#line 668 "HTMLCharacterReference.gperf"
      {"afr;", "𝔞"},
#line 643 "HTMLCharacterReference.gperf"
      {"Yuml;", "Ÿ"},
      {""}, {""}, {""},
#line 1256 "HTMLCharacterReference.gperf"
      {"jfr;", "𝔧"},
#line 1278 "HTMLCharacterReference.gperf"
      {"lHar;", "⥢"},
      {""},
#line 1104 "HTMLCharacterReference.gperf"
      {"frac56;", "⅚"},
      {""},
#line 577 "HTMLCharacterReference.gperf"
      {"Ufr;", "𝔘"},
      {""}, {""}, {""}, {""}, {""},
#line 907 "HTMLCharacterReference.gperf"
      {"csup;", "⫐"},
      {""},
#line 1091 "HTMLCharacterReference.gperf"
      {"frac13;", "⅓"},
      {""},
#line 1773 "HTMLCharacterReference.gperf"
      {"quot", "\""},
#line 1774 "HTMLCharacterReference.gperf"
      {"quot;", "\""},
#line 1038 "HTMLCharacterReference.gperf"
      {"eogon;", "ę"},
      {""}, {""}, {""}, {""},
#line 929 "HTMLCharacterReference.gperf"
      {"curren", "¤"},
#line 930 "HTMLCharacterReference.gperf"
      {"curren;", "¤"},
#line 333 "HTMLCharacterReference.gperf"
      {"Mu;", "Μ"},
#line 958 "HTMLCharacterReference.gperf"
      {"dfr;", "𝔡"},
      {""},
#line 701 "HTMLCharacterReference.gperf"
      {"aogon;", "ą"},
#line 1162 "HTMLCharacterReference.gperf"
      {"gtrarr;", "⥸"},
      {""},
#line 1184 "HTMLCharacterReference.gperf"
      {"hfr;", "𝔥"},
      {""}, {""},
#line 1098 "HTMLCharacterReference.gperf"
      {"frac25;", "⅖"},
      {""}, {""}, {""},
#line 587 "HTMLCharacterReference.gperf"
      {"Uogon;", "Ų"},
      {""}, {""},
#line 771 "HTMLCharacterReference.gperf"
      {"bne;", "=⃥"},
      {""}, {""},
#line 1096 "HTMLCharacterReference.gperf"
      {"frac18;", "⅛"},
      {""}, {""},
#line 939 "HTMLCharacterReference.gperf"
      {"dHar;", "⥥"},
      {""}, {""}, {""}, {""}, {""},
#line 912 "HTMLCharacterReference.gperf"
      {"cuepr;", "⋞"},
      {""}, {""}, {""}, {""}, {""},
#line 1102 "HTMLCharacterReference.gperf"
      {"frac38;", "⅜"},
      {""}, {""}, {""},
#line 959 "HTMLCharacterReference.gperf"
      {"dharl;", "⇃"},
#line 1392 "HTMLCharacterReference.gperf"
      {"lparlt;", "⦓"},
      {""},
#line 456 "HTMLCharacterReference.gperf"
      {"Qfr;", "𝔔"},
      {""}, {""},
#line 1105 "HTMLCharacterReference.gperf"
      {"frac58;", "⅝"},
      {""}, {""}, {""}, {""},
#line 1097 "HTMLCharacterReference.gperf"
      {"frac23;", "⅔"},
      {""},
#line 1077 "HTMLCharacterReference.gperf"
      {"ffr;", "𝔣"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2194 "HTMLCharacterReference.gperf"
      {"wfr;", "𝔴"},
      {""},
#line 837 "HTMLCharacterReference.gperf"
      {"bumpe;", "≏"},
      {""}, {""}, {""}, {""},
#line 685 "HTMLCharacterReference.gperf"
      {"angle;", "∠"},
      {""}, {""},
#line 2176 "HTMLCharacterReference.gperf"
      {"vfr;", "𝔳"},
      {""}, {""},
#line 923 "HTMLCharacterReference.gperf"
      {"curarr;", "↷"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1145 "HTMLCharacterReference.gperf"
      {"gneq;", "⪈"},
#line 1146 "HTMLCharacterReference.gperf"
      {"gneqq;", "≩"},
      {""}, {""}, {""},
#line 1370 "HTMLCharacterReference.gperf"
      {"lneq;", "⪇"},
#line 1371 "HTMLCharacterReference.gperf"
      {"lneqq;", "≨"},
#line 898 "HTMLCharacterReference.gperf"
      {"coprod;", "∐"},
#line 1120 "HTMLCharacterReference.gperf"
      {"ge;", "≥"},
#line 745 "HTMLCharacterReference.gperf"
      {"bfr;", "𝔟"},
      {""}, {""}, {""},
#line 1320 "HTMLCharacterReference.gperf"
      {"le;", "≤"},
#line 1125 "HTMLCharacterReference.gperf"
      {"ges;", "⩾"},
      {""},
#line 1382 "HTMLCharacterReference.gperf"
      {"lopar;", "⦅"},
#line 776 "HTMLCharacterReference.gperf"
      {"bottom;", "⊥"},
#line 1016 "HTMLCharacterReference.gperf"
      {"ee;", "ⅇ"},
#line 1335 "HTMLCharacterReference.gperf"
      {"les;", "⩽"},
      {""}, {""},
#line 1106 "HTMLCharacterReference.gperf"
      {"frac78;", "⅞"},
      {""},
#line 1122 "HTMLCharacterReference.gperf"
      {"geq;", "≥"},
      {""}, {""}, {""}, {""},
#line 1332 "HTMLCharacterReference.gperf"
      {"leq;", "≤"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1158 "HTMLCharacterReference.gperf"
      {"gtdot;", "⋗"},
      {""}, {""},
#line 899 "HTMLCharacterReference.gperf"
      {"copy", "©"},
#line 900 "HTMLCharacterReference.gperf"
      {"copy;", "©"},
#line 1413 "HTMLCharacterReference.gperf"
      {"ltdot;", "⋖"},
      {""}, {""}, {""}, {""},
#line 781 "HTMLCharacterReference.gperf"
      {"boxDr;", "╓"},
      {""}, {""}, {""}, {""},
#line 909 "HTMLCharacterReference.gperf"
      {"ctdot;", "⋯"},
      {""}, {""},
#line 678 "HTMLCharacterReference.gperf"
      {"and;", "∧"},
      {""}, {""}, {""}, {""}, {""},
#line 1130 "HTMLCharacterReference.gperf"
      {"gesl;", "⋛︀"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 213 "HTMLCharacterReference.gperf"
      {"Hfr;", "ℌ"},
      {""}, {""}, {""}, {""},
#line 180 "HTMLCharacterReference.gperf"
      {"Ffr;", "𝔉"},
      {""}, {""},
#line 838 "HTMLCharacterReference.gperf"
      {"bumpeq;", "≏"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1208 "HTMLCharacterReference.gperf"
      {"ifr;", "𝔦"},
      {""},
#line 996 "HTMLCharacterReference.gperf"
      {"dtdot;", "⋱"},
      {""}, {""}, {""}, {""},
#line 908 "HTMLCharacterReference.gperf"
      {"csupe;", "⫒"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 836 "HTMLCharacterReference.gperf"
      {"bumpE;", "⪮"},
#line 896 "HTMLCharacterReference.gperf"
      {"conint;", "∮"},
      {""}, {""},
#line 530 "HTMLCharacterReference.gperf"
      {"Star;", "⋆"},
      {""}, {""}, {""},
#line 640 "HTMLCharacterReference.gperf"
      {"Yfr;", "𝔜"},
      {""},
#line 1236 "HTMLCharacterReference.gperf"
      {"iogon;", "į"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 539 "HTMLCharacterReference.gperf"
      {"Sum;", "∑"},
      {""},
#line 780 "HTMLCharacterReference.gperf"
      {"boxDl;", "╖"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 116 "HTMLCharacterReference.gperf"
      {"Dot;", "¨"},
      {""},
#line 1059 "HTMLCharacterReference.gperf"
      {"esdot;", "≐"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1133 "HTMLCharacterReference.gperf"
      {"gg;", "≫"},
#line 308 "HTMLCharacterReference.gperf"
      {"Lfr;", "𝔏"},
      {""}, {""}, {""},
#line 1351 "HTMLCharacterReference.gperf"
      {"lg;", "≶"},
      {""}, {""}, {""}, {""},
#line 1019 "HTMLCharacterReference.gperf"
      {"eg;", "⪚"},
#line 540 "HTMLCharacterReference.gperf"
      {"Sup;", "⋑"},
      {""}, {""}, {""},
#line 95 "HTMLCharacterReference.gperf"
      {"DD;", "ⅅ"},
#line 1022 "HTMLCharacterReference.gperf"
      {"egs;", "⪖"},
      {""}, {""}, {""},
#line 1611 "HTMLCharacterReference.gperf"
      {"nu;", "ν"},
#line 860 "HTMLCharacterReference.gperf"
      {"cent", "¢"},
#line 861 "HTMLCharacterReference.gperf"
      {"cent;", "¢"},
#line 865 "HTMLCharacterReference.gperf"
      {"check;", "✓"},
      {""},
#line 1044 "HTMLCharacterReference.gperf"
      {"epsilon;", "ε"},
#line 237 "HTMLCharacterReference.gperf"
      {"Int;", "∬"},
#line 1238 "HTMLCharacterReference.gperf"
      {"iota;", "ι"},
      {""}, {""}, {""},
#line 1764 "HTMLCharacterReference.gperf"
      {"qfr;", "𝔮"},
      {""}, {""}, {""},
#line 1166 "HTMLCharacterReference.gperf"
      {"gtrless;", "≷"},
      {""},
#line 1559 "HTMLCharacterReference.gperf"
      {"npar;", "∦"},
      {""}, {""}, {""}, {""}, {""},
#line 779 "HTMLCharacterReference.gperf"
      {"boxDR;", "╔"},
      {""},
#line 601 "HTMLCharacterReference.gperf"
      {"Upsilon;", "Υ"},
#line 1612 "HTMLCharacterReference.gperf"
      {"num;", "#"},
      {""}, {""},
#line 1180 "HTMLCharacterReference.gperf"
      {"hearts;", "♥"},
      {""},
#line 1488 "HTMLCharacterReference.gperf"
      {"nbsp", " "},
#line 1489 "HTMLCharacterReference.gperf"
      {"nbsp;", " "},
      {""}, {""}, {""},
#line 1331 "HTMLCharacterReference.gperf"
      {"leg;", "⋚"},
#line 521 "HTMLCharacterReference.gperf"
      {"Sqrt;", "√"},
#line 790 "HTMLCharacterReference.gperf"
      {"boxUr;", "╙"},
      {""}, {""},
#line 329 "HTMLCharacterReference.gperf"
      {"Mfr;", "𝔐"},
      {""},
#line 1562 "HTMLCharacterReference.gperf"
      {"npart;", "∂̸"},
      {""}, {""}, {""},
#line 1161 "HTMLCharacterReference.gperf"
      {"gtrapprox;", "⪆"},
      {""},
#line 686 "HTMLCharacterReference.gperf"
      {"angmsd;", "∡"},
      {""},
#line 248 "HTMLCharacterReference.gperf"
      {"Iuml", "Ï"},
#line 249 "HTMLCharacterReference.gperf"
      {"Iuml;", "Ï"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2175 "HTMLCharacterReference.gperf"
      {"vert;", "|"},
      {""}, {""}, {""},
#line 1564 "HTMLCharacterReference.gperf"
      {"npr;", "⊀"},
#line 1123 "HTMLCharacterReference.gperf"
      {"geqq;", "≧"},
#line 1570 "HTMLCharacterReference.gperf"
      {"nrarr;", "↛"},
#line 1051 "HTMLCharacterReference.gperf"
      {"equals;", "="},
      {""}, {""},
#line 1333 "HTMLCharacterReference.gperf"
      {"leqq;", "≦"},
      {""},
#line 1572 "HTMLCharacterReference.gperf"
      {"nrarrw;", "↝̸"},
#line 953 "HTMLCharacterReference.gperf"
      {"deg", "°"},
      {""}, {""}, {""}, {""}, {""},
#line 954 "HTMLCharacterReference.gperf"
      {"deg;", "°"},
      {""},
#line 1631 "HTMLCharacterReference.gperf"
      {"nwarr;", "↖"},
      {""}, {""}, {""}, {""}, {""},
#line 901 "HTMLCharacterReference.gperf"
      {"copysr;", "℗"},
      {""}, {""}, {""}, {""}, {""},
#line 982 "HTMLCharacterReference.gperf"
      {"dotplus;", "∔"},
      {""},
#line 1405 "HTMLCharacterReference.gperf"
      {"lsqb;", "["},
      {""},
#line 1085 "HTMLCharacterReference.gperf"
      {"forall;", "∀"},
      {""},
#line 1388 "HTMLCharacterReference.gperf"
      {"loz;", "◊"},
      {""}, {""}, {""},
#line 208 "HTMLCharacterReference.gperf"
      {"Gt;", "≫"},
      {""}, {""}, {""}, {""}, {""},
#line 688 "HTMLCharacterReference.gperf"
      {"angmsdab;", "⦩"},
      {""}, {""}, {""},
#line 924 "HTMLCharacterReference.gperf"
      {"curarrm;", "⤼"},
#line 174 "HTMLCharacterReference.gperf"
      {"Eta;", "Η"},
      {""}, {""}, {""}, {""},
#line 107 "HTMLCharacterReference.gperf"
      {"Dfr;", "𝔇"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 789 "HTMLCharacterReference.gperf"
      {"boxUl;", "╜"},
      {""}, {""}, {""}, {""}, {""},
#line 1131 "HTMLCharacterReference.gperf"
      {"gesles;", "⪔"},
      {""}, {""}, {""}, {""}, {""},
#line 809 "HTMLCharacterReference.gperf"
      {"boxplus;", "⊞"},
      {""}, {""}, {""}, {""},
#line 1547 "HTMLCharacterReference.gperf"
      {"not", "¬"},
      {""}, {""},
#line 831 "HTMLCharacterReference.gperf"
      {"bsolb;", "⧅"},
      {""}, {""},
#line 1548 "HTMLCharacterReference.gperf"
      {"not;", "¬"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 252 "HTMLCharacterReference.gperf"
      {"Jfr;", "𝔍"},
      {""}, {""}, {""}, {""},
#line 1134 "HTMLCharacterReference.gperf"
      {"ggg;", "⋙"},
#line 1168 "HTMLCharacterReference.gperf"
      {"gvertneqq;", "≩︀"},
#line 1149 "HTMLCharacterReference.gperf"
      {"grave;", "`"},
      {""}, {""}, {""},
#line 1424 "HTMLCharacterReference.gperf"
      {"lvertneqq;", "≨︀"},
      {""}, {""}, {""}, {""},
#line 1603 "HTMLCharacterReference.gperf"
      {"ntgl;", "≹"},
#line 788 "HTMLCharacterReference.gperf"
      {"boxUR;", "╚"},
      {""}, {""},
#line 629 "HTMLCharacterReference.gperf"
      {"Xfr;", "𝔛"},
#line 866 "HTMLCharacterReference.gperf"
      {"checkmark;", "✓"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1628 "HTMLCharacterReference.gperf"
      {"nvsim;", "∼⃒"},
      {""}, {""},
#line 175 "HTMLCharacterReference.gperf"
      {"Euml", "Ë"},
#line 176 "HTMLCharacterReference.gperf"
      {"Euml;", "Ë"},
      {""},
#line 1183 "HTMLCharacterReference.gperf"
      {"hercon;", "⊹"},
      {""},
#line 2170 "HTMLCharacterReference.gperf"
      {"vee;", "∨"},
      {""},
#line 2217 "HTMLCharacterReference.gperf"
      {"xrarr;", "⟶"},
      {""}, {""}, {""}, {""},
#line 1549 "HTMLCharacterReference.gperf"
      {"notin;", "∉"},
#line 741 "HTMLCharacterReference.gperf"
      {"bernou;", "ℬ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1308 "HTMLCharacterReference.gperf"
      {"lbrkslu;", "⦍"},
      {""}, {""},
#line 1353 "HTMLCharacterReference.gperf"
      {"lhard;", "↽"},
      {""}, {""},
#line 513 "HTMLCharacterReference.gperf"
      {"Sfr;", "𝔖"},
      {""}, {""}, {""}, {""}, {""},
#line 815 "HTMLCharacterReference.gperf"
      {"boxv;", "│"},
#line 816 "HTMLCharacterReference.gperf"
      {"boxvH;", "╪"},
      {""}, {""}, {""}, {""},
#line 1522 "HTMLCharacterReference.gperf"
      {"nharr;", "↮"},
      {""}, {""}, {""},
#line 1617 "HTMLCharacterReference.gperf"
      {"nvap;", "≍⃒"},
      {""}, {""}, {""}, {""}, {""},
#line 585 "HTMLCharacterReference.gperf"
      {"Union;", "⋃"},
#line 1561 "HTMLCharacterReference.gperf"
      {"nparsl;", "⫽⃥"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1151 "HTMLCharacterReference.gperf"
      {"gsim;", "≳"},
      {""}, {""}, {""},
#line 694 "HTMLCharacterReference.gperf"
      {"angmsdah;", "⦯"},
#line 1402 "HTMLCharacterReference.gperf"
      {"lsim;", "≲"},
      {""}, {""}, {""},
#line 474 "HTMLCharacterReference.gperf"
      {"Rho;", "Ρ"},
#line 1060 "HTMLCharacterReference.gperf"
      {"esim;", "≂"},
#line 1126 "HTMLCharacterReference.gperf"
      {"gescc;", "⪩"},
#line 822 "HTMLCharacterReference.gperf"
      {"bprime;", "‵"},
      {""},
#line 230 "HTMLCharacterReference.gperf"
      {"Ifr;", "ℑ"},
      {""},
#line 1336 "HTMLCharacterReference.gperf"
      {"lescc;", "⪨"},
      {""}, {""}, {""},
#line 742 "HTMLCharacterReference.gperf"
      {"beta;", "β"},
      {""}, {""},
#line 405 "HTMLCharacterReference.gperf"
      {"Nu;", "Ν"},
      {""}, {""},
#line 1226 "HTMLCharacterReference.gperf"
      {"infin;", "∞"},
      {""}, {""}, {""}, {""},
#line 821 "HTMLCharacterReference.gperf"
      {"boxvr;", "├"},
      {""}, {""},
#line 1511 "HTMLCharacterReference.gperf"
      {"nfr;", "𝔫"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1490 "HTMLCharacterReference.gperf"
      {"nbump;", "≎̸"},
      {""}, {""}, {""}, {""},
#line 242 "HTMLCharacterReference.gperf"
      {"Iogon;", "Į"},
      {""}, {""}, {""},
#line 905 "HTMLCharacterReference.gperf"
      {"csub;", "⫏"},
#line 1239 "HTMLCharacterReference.gperf"
      {"iprod;", "⨼"},
      {""}, {""}, {""},
#line 1597 "HTMLCharacterReference.gperf"
      {"nsup;", "⊅"},
#line 936 "HTMLCharacterReference.gperf"
      {"cwint;", "∱"},
      {""}, {""}, {""}, {""},
#line 722 "HTMLCharacterReference.gperf"
      {"awint;", "⨑"},
      {""}, {""},
#line 692 "HTMLCharacterReference.gperf"
      {"angmsdaf;", "⦭"},
      {""}, {""}, {""}, {""},
#line 651 "HTMLCharacterReference.gperf"
      {"Zfr;", "ℨ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1620 "HTMLCharacterReference.gperf"
      {"nvgt;", ">⃒"},
      {""}, {""}, {""},
#line 867 "HTMLCharacterReference.gperf"
      {"chi;", "χ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 244 "HTMLCharacterReference.gperf"
      {"Iota;", "Ι"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 117 "HTMLCharacterReference.gperf"
      {"DotDot;", "⃜"},
      {""}, {""},
#line 684 "HTMLCharacterReference.gperf"
      {"ange;", "⦤"},
#line 820 "HTMLCharacterReference.gperf"
      {"boxvl;", "┤"},
      {""}, {""}, {""}, {""},
#line 2206 "HTMLCharacterReference.gperf"
      {"xharr;", "⟷"},
#line 266 "HTMLCharacterReference.gperf"
      {"LT", "<"},
#line 267 "HTMLCharacterReference.gperf"
      {"LT;", "<"},
      {""},
#line 1205 "HTMLCharacterReference.gperf"
      {"iexcl", "¡"},
#line 1206 "HTMLCharacterReference.gperf"
      {"iexcl;", "¡"},
      {""}, {""}, {""}, {""},
#line 1586 "HTMLCharacterReference.gperf"
      {"nspar;", "∦"},
      {""}, {""}, {""}, {""},
#line 979 "HTMLCharacterReference.gperf"
      {"doteq;", "≐"},
      {""}, {""}, {""},
#line 828 "HTMLCharacterReference.gperf"
      {"bsim;", "∽"},
#line 1153 "HTMLCharacterReference.gperf"
      {"gsiml;", "⪐"},
      {""}, {""}, {""}, {""}, {""},
#line 1415 "HTMLCharacterReference.gperf"
      {"ltimes;", "⋉"},
      {""},
#line 473 "HTMLCharacterReference.gperf"
      {"Rfr;", "ℜ"},
      {""}, {""}, {""}, {""},
#line 1472 "HTMLCharacterReference.gperf"
      {"nLt;", "≪⃒"},
      {""}, {""},
#line 679 "HTMLCharacterReference.gperf"
      {"andand;", "⩕"},
      {""},
#line 45 "HTMLCharacterReference.gperf"
      {"Auml", "Ä"},
#line 46 "HTMLCharacterReference.gperf"
      {"Auml;", "Ä"},
      {""}, {""}, {""},
#line 159 "HTMLCharacterReference.gperf"
      {"Efr;", "𝔈"},
      {""},
#line 1373 "HTMLCharacterReference.gperf"
      {"loang;", "⟬"},
      {""}, {""},
#line 350 "HTMLCharacterReference.gperf"
      {"Not;", "⫬"},
      {""},
#line 933 "HTMLCharacterReference.gperf"
      {"cuvee;", "⋎"},
      {""},
#line 1500 "HTMLCharacterReference.gperf"
      {"ne;", "≠"},
#line 2204 "HTMLCharacterReference.gperf"
      {"xfr;", "𝔵"},
      {""},
#line 818 "HTMLCharacterReference.gperf"
      {"boxvR;", "╞"},
      {""}, {""}, {""}, {""}, {""},
#line 1767 "HTMLCharacterReference.gperf"
      {"qprime;", "⁗"},
      {""},
#line 1207 "HTMLCharacterReference.gperf"
      {"iff;", "⇔"},
      {""},
#line 1152 "HTMLCharacterReference.gperf"
      {"gsime;", "⪎"},
      {""}, {""},
#line 197 "HTMLCharacterReference.gperf"
      {"Gfr;", "𝔊"},
      {""},
#line 1403 "HTMLCharacterReference.gperf"
      {"lsime;", "⪍"},
      {""}, {""}, {""}, {""},
#line 166 "HTMLCharacterReference.gperf"
      {"Eogon;", "Ę"},
      {""}, {""}, {""},
#line 723 "HTMLCharacterReference.gperf"
      {"bNot;", "⫭"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1508 "HTMLCharacterReference.gperf"
      {"nesim;", "≂̸"},
      {""}, {""},
#line 1551 "HTMLCharacterReference.gperf"
      {"notindot;", "⋵̸"},
#line 682 "HTMLCharacterReference.gperf"
      {"andv;", "⩚"},
      {""}, {""}, {""}, {""},
#line 1119 "HTMLCharacterReference.gperf"
      {"gdot;", "ġ"},
#line 1523 "HTMLCharacterReference.gperf"
      {"nhpar;", "⫲"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1015 "HTMLCharacterReference.gperf"
      {"edot;", "ė"},
      {""}, {""},
#line 1385 "HTMLCharacterReference.gperf"
      {"lotimes;", "⨴"},
      {""},
#line 856 "HTMLCharacterReference.gperf"
      {"cdot;", "ċ"},
#line 906 "HTMLCharacterReference.gperf"
      {"csube;", "⫑"},
      {""},
#line 1307 "HTMLCharacterReference.gperf"
      {"lbrksld;", "⦏"},
      {""}, {""},
#line 1599 "HTMLCharacterReference.gperf"
      {"nsupe;", "⊉"},
      {""}, {""}, {""},
#line 857 "HTMLCharacterReference.gperf"
      {"cedil", "¸"},
#line 858 "HTMLCharacterReference.gperf"
      {"cedil;", "¸"},
      {""},
#line 949 "HTMLCharacterReference.gperf"
      {"dd;", "ⅆ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2187 "HTMLCharacterReference.gperf"
      {"vsupne;", "⊋︀"},
      {""}, {""},
#line 791 "HTMLCharacterReference.gperf"
      {"boxV;", "║"},
#line 792 "HTMLCharacterReference.gperf"
      {"boxVH;", "╬"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 823 "HTMLCharacterReference.gperf"
      {"breve;", "˘"},
      {""}, {""}, {""}, {""},
#line 1157 "HTMLCharacterReference.gperf"
      {"gtcir;", "⩺"},
      {""}, {""}, {""}, {""},
#line 1412 "HTMLCharacterReference.gperf"
      {"ltcir;", "⩹"},
      {""}, {""}, {""}, {""},
#line 1503 "HTMLCharacterReference.gperf"
      {"nearr;", "↗"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 469 "HTMLCharacterReference.gperf"
      {"Re;", "ℜ"},
      {""}, {""}, {""},
#line 1571 "HTMLCharacterReference.gperf"
      {"nrarrc;", "⤳̸"},
      {""}, {""}, {""}, {""}, {""},
#line 1234 "HTMLCharacterReference.gperf"
      {"intprod;", "⨼"},
      {""},
#line 1243 "HTMLCharacterReference.gperf"
      {"isin;", "∈"},
      {""},
#line 991 "HTMLCharacterReference.gperf"
      {"drcrop;", "⌌"},
      {""},
#line 691 "HTMLCharacterReference.gperf"
      {"angmsdae;", "⦬"},
      {""},
#line 1248 "HTMLCharacterReference.gperf"
      {"isinv;", "∈"},
      {""},
#line 443 "HTMLCharacterReference.gperf"
      {"Pr;", "⪻"},
      {""}, {""},
#line 797 "HTMLCharacterReference.gperf"
      {"boxVr;", "╟"},
      {""}, {""},
#line 346 "HTMLCharacterReference.gperf"
      {"Nfr;", "𝔑"},
      {""}, {""}, {""},
#line 911 "HTMLCharacterReference.gperf"
      {"cudarrr;", "⤵"},
      {""},
#line 1150 "HTMLCharacterReference.gperf"
      {"gscr;", "ℊ"},
#line 594 "HTMLCharacterReference.gperf"
      {"UpTee;", "⊥"},
#line 1260 "HTMLCharacterReference.gperf"
      {"jsercy;", "ј"},
      {""},
#line 1115 "HTMLCharacterReference.gperf"
      {"gap;", "⪆"},
#line 1400 "HTMLCharacterReference.gperf"
      {"lscr;", "𝓁"},
#line 829 "HTMLCharacterReference.gperf"
      {"bsime;", "⋍"},
#line 990 "HTMLCharacterReference.gperf"
      {"drcorn;", "⌟"},
      {""},
#line 1286 "HTMLCharacterReference.gperf"
      {"lap;", "⪅"},
#line 1058 "HTMLCharacterReference.gperf"
      {"escr;", "ℯ"},
      {""}, {""}, {""},
#line 1298 "HTMLCharacterReference.gperf"
      {"lat;", "⪫"},
#line 904 "HTMLCharacterReference.gperf"
      {"cscr;", "𝒸"},
      {""}, {""}, {""},
#line 840 "HTMLCharacterReference.gperf"
      {"cap;", "∩"},
#line 713 "HTMLCharacterReference.gperf"
      {"ascr;", "𝒶"},
      {""},
#line 1304 "HTMLCharacterReference.gperf"
      {"lbrace;", "{"},
      {""}, {""},
#line 1259 "HTMLCharacterReference.gperf"
      {"jscr;", "𝒿"},
      {""}, {""}, {""}, {""},
#line 603 "HTMLCharacterReference.gperf"
      {"Uscr;", "𝒰"},
#line 1518 "HTMLCharacterReference.gperf"
      {"ngsim;", "≵"},
      {""}, {""}, {""},
#line 1164 "HTMLCharacterReference.gperf"
      {"gtreqless;", "⋛"},
#line 951 "HTMLCharacterReference.gperf"
      {"ddarr;", "⇊"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1598 "HTMLCharacterReference.gperf"
      {"nsupE;", "⫆̸"},
      {""}, {""}, {""},
#line 992 "HTMLCharacterReference.gperf"
      {"dscr;", "𝒹"},
      {""}, {""}, {""},
#line 1519 "HTMLCharacterReference.gperf"
      {"ngt;", "≯"},
#line 1193 "HTMLCharacterReference.gperf"
      {"hscr;", "𝒽"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1616 "HTMLCharacterReference.gperf"
      {"nvHarr;", "⤄"},
      {""},
#line 30 "HTMLCharacterReference.gperf"
      {"Afr;", "𝔄"},
#line 1156 "HTMLCharacterReference.gperf"
      {"gtcc;", "⪧"},
#line 888 "HTMLCharacterReference.gperf"
      {"comma;", ","},
      {""}, {""}, {""},
#line 1411 "HTMLCharacterReference.gperf"
      {"ltcc;", "⪦"},
      {""}, {""}, {""}, {""},
#line 1520 "HTMLCharacterReference.gperf"
      {"ngtr;", "≯"},
      {""}, {""}, {""},
#line 980 "HTMLCharacterReference.gperf"
      {"doteqdot;", "≑"},
#line 1289 "HTMLCharacterReference.gperf"
      {"larr;", "←"},
#line 796 "HTMLCharacterReference.gperf"
      {"boxVl;", "╢"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 458 "HTMLCharacterReference.gperf"
      {"Qscr;", "𝒬"},
      {""}, {""}, {""}, {""}, {""},
#line 36 "HTMLCharacterReference.gperf"
      {"Aogon;", "Ą"},
      {""},
#line 657 "HTMLCharacterReference.gperf"
      {"ac;", "∾"},
      {""},
#line 1109 "HTMLCharacterReference.gperf"
      {"fscr;", "𝒻"},
      {""}, {""}, {""}, {""},
#line 569 "HTMLCharacterReference.gperf"
      {"Uarr;", "↟"},
      {""}, {""}, {""},
#line 917 "HTMLCharacterReference.gperf"
      {"cupbrcap;", "⩈"},
#line 2199 "HTMLCharacterReference.gperf"
      {"wscr;", "𝓌"},
#line 1595 "HTMLCharacterReference.gperf"
      {"nsucc;", "⊁"},
      {""}, {""}, {""}, {""},
#line 848 "HTMLCharacterReference.gperf"
      {"caron;", "ˇ"},
      {""}, {""}, {""},
#line 2183 "HTMLCharacterReference.gperf"
      {"vscr;", "𝓋"},
      {""}, {""}, {""}, {""},
#line 942 "HTMLCharacterReference.gperf"
      {"darr;", "↓"},
      {""},
#line 1297 "HTMLCharacterReference.gperf"
      {"larrtl;", "↢"},
      {""},
#line 693 "HTMLCharacterReference.gperf"
      {"angmsdag;", "⦮"},
#line 1175 "HTMLCharacterReference.gperf"
      {"harr;", "↔"},
      {""}, {""}, {""}, {""}, {""},
#line 1585 "HTMLCharacterReference.gperf"
      {"nsmid;", "∤"},
      {""}, {""}, {""}, {""},
#line 794 "HTMLCharacterReference.gperf"
      {"boxVR;", "╠"},
      {""},
#line 168 "HTMLCharacterReference.gperf"
      {"Epsilon;", "Ε"},
      {""},
#line 826 "HTMLCharacterReference.gperf"
      {"bscr;", "𝒷"},
#line 595 "HTMLCharacterReference.gperf"
      {"UpTeeArrow;", "↥"},
      {""}, {""}, {""}, {""},
#line 169 "HTMLCharacterReference.gperf"
      {"Equal;", "⩵"},
      {""},
#line 198 "HTMLCharacterReference.gperf"
      {"Gg;", "⋙"},
      {""}, {""}, {""},
#line 1008 "HTMLCharacterReference.gperf"
      {"easter;", "⩮"},
      {""}, {""}, {""},
#line 1303 "HTMLCharacterReference.gperf"
      {"lbbrk;", "❲"},
      {""}, {""}, {""},
#line 608 "HTMLCharacterReference.gperf"
      {"Vbar;", "⫫"},
#line 2212 "HTMLCharacterReference.gperf"
      {"xodot;", "⨀"},
#line 1309 "HTMLCharacterReference.gperf"
      {"lcaron;", "ľ"},
      {""}, {""}, {""}, {""},
#line 1009 "HTMLCharacterReference.gperf"
      {"ecaron;", "ě"},
      {""}, {""}, {""}, {""},
#line 850 "HTMLCharacterReference.gperf"
      {"ccaron;", "č"},
      {""}, {""}, {""}, {""},
#line 1013 "HTMLCharacterReference.gperf"
      {"ecolon;", "≕"},
      {""}, {""}, {""}, {""},
#line 1418 "HTMLCharacterReference.gperf"
      {"ltrPar;", "⦖"},
      {""}, {""},
#line 650 "HTMLCharacterReference.gperf"
      {"Zeta;", "Ζ"},
      {""}, {""}, {""}, {""},
#line 2158 "HTMLCharacterReference.gperf"
      {"varr;", "↕"},
      {""},
#line 918 "HTMLCharacterReference.gperf"
      {"cupcap;", "⩆"},
      {""}, {""}, {""},
#line 1246 "HTMLCharacterReference.gperf"
      {"isins;", "⋴"},
#line 1295 "HTMLCharacterReference.gperf"
      {"larrpl;", "⤹"},
      {""}, {""},
#line 217 "HTMLCharacterReference.gperf"
      {"Hscr;", "ℋ"},
      {""},
#line 2198 "HTMLCharacterReference.gperf"
      {"wreath;", "≀"},
      {""}, {""},
#line 186 "HTMLCharacterReference.gperf"
      {"Fscr;", "ℱ"},
      {""},
#line 947 "HTMLCharacterReference.gperf"
      {"dcaron;", "ď"},
      {""},
#line 211 "HTMLCharacterReference.gperf"
      {"Hat;", "^"},
      {""}, {""}, {""}, {""}, {""},
#line 1242 "HTMLCharacterReference.gperf"
      {"iscr;", "𝒾"},
      {""}, {""}, {""},
#line 1394 "HTMLCharacterReference.gperf"
      {"lrcorner;", "⌟"},
      {""}, {""}, {""}, {""},
#line 626 "HTMLCharacterReference.gperf"
      {"Wfr;", "𝔚"},
      {""}, {""}, {""}, {""},
#line 35 "HTMLCharacterReference.gperf"
      {"And;", "⩓"},
      {""}, {""},
#line 1294 "HTMLCharacterReference.gperf"
      {"larrlp;", "↫"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 862 "HTMLCharacterReference.gperf"
      {"centerdot;", "·"},
      {""}, {""}, {""},
#line 1513 "HTMLCharacterReference.gperf"
      {"nge;", "≱"},
#line 642 "HTMLCharacterReference.gperf"
      {"Yscr;", "𝒴"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 859 "HTMLCharacterReference.gperf"
      {"cemptyv;", "⦲"},
      {""},
#line 893 "HTMLCharacterReference.gperf"
      {"complexes;", "ℂ"},
#line 1375 "HTMLCharacterReference.gperf"
      {"lobrk;", "⟦"},
#line 188 "HTMLCharacterReference.gperf"
      {"GT", ">"},
#line 189 "HTMLCharacterReference.gperf"
      {"GT;", ">"},
      {""},
#line 1582 "HTMLCharacterReference.gperf"
      {"nsim;", "≁"},
      {""},
#line 1052 "HTMLCharacterReference.gperf"
      {"equest;", "≟"},
      {""}, {""},
#line 1566 "HTMLCharacterReference.gperf"
      {"npre;", "⪯̸"},
      {""},
#line 889 "HTMLCharacterReference.gperf"
      {"commat;", "@"},
      {""}, {""},
#line 321 "HTMLCharacterReference.gperf"
      {"Lscr;", "ℒ"},
#line 892 "HTMLCharacterReference.gperf"
      {"complement;", "∁"},
      {""}, {""},
#line 2236 "HTMLCharacterReference.gperf"
      {"yuml", "ÿ"},
#line 2237 "HTMLCharacterReference.gperf"
      {"yuml;", "ÿ"},
      {""}, {""},
#line 1200 "HTMLCharacterReference.gperf"
      {"ic;", "⁣"},
      {""}, {""},
#line 1244 "HTMLCharacterReference.gperf"
      {"isinE;", "⋹"},
      {""}, {""},
#line 554 "HTMLCharacterReference.gperf"
      {"Tfr;", "𝔗"},
#line 2250 "HTMLCharacterReference.gperf"
      {"zwnj;", "‌"},
      {""}, {""},
#line 956 "HTMLCharacterReference.gperf"
      {"demptyv;", "⦱"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1589 "HTMLCharacterReference.gperf"
      {"nsub;", "⊄"},
      {""},
#line 1509 "HTMLCharacterReference.gperf"
      {"nexist;", "∄"},
#line 1001 "HTMLCharacterReference.gperf"
      {"dwangle;", "⦦"},
      {""},
#line 1768 "HTMLCharacterReference.gperf"
      {"qscr;", "𝓆"},
      {""}, {""},
#line 1460 "HTMLCharacterReference.gperf"
      {"mp;", "∓"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 437 "HTMLCharacterReference.gperf"
      {"Pfr;", "𝔓"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1116 "HTMLCharacterReference.gperf"
      {"gbreve;", "ğ"},
      {""},
#line 2244 "HTMLCharacterReference.gperf"
      {"zfr;", "𝔷"},
#line 993 "HTMLCharacterReference.gperf"
      {"dscy;", "ѕ"},
#line 1356 "HTMLCharacterReference.gperf"
      {"lhblk;", "▄"},
      {""}, {""}, {""},
#line 332 "HTMLCharacterReference.gperf"
      {"Mscr;", "ℳ"},
      {""},
#line 1613 "HTMLCharacterReference.gperf"
      {"numero;", "№"},
      {""},
#line 325 "HTMLCharacterReference.gperf"
      {"Map;", "⤅"},
      {""}, {""},
#line 1491 "HTMLCharacterReference.gperf"
      {"nbumpe;", "≏̸"},
      {""}, {""}, {""}, {""},
#line 656 "HTMLCharacterReference.gperf"
      {"abreve;", "ă"},
#line 1463 "HTMLCharacterReference.gperf"
      {"mu;", "μ"},
      {""},
#line 272 "HTMLCharacterReference.gperf"
      {"Larr;", "↞"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 572 "HTMLCharacterReference.gperf"
      {"Ubreve;", "Ŭ"},
      {""}, {""}, {""},
#line 1505 "HTMLCharacterReference.gperf"
      {"nedot;", "≐̸"},
#line 2215 "HTMLCharacterReference.gperf"
      {"xotime;", "⨂"},
      {""}, {""}, {""}, {""},
#line 1230 "HTMLCharacterReference.gperf"
      {"intcal;", "⊺"},
      {""}, {""},
#line 2157 "HTMLCharacterReference.gperf"
      {"varpropto;", "∝"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 351 "HTMLCharacterReference.gperf"
      {"NotCongruent;", "≢"},
      {""}, {""},
#line 1020 "HTMLCharacterReference.gperf"
      {"egrave", "è"},
#line 1021 "HTMLCharacterReference.gperf"
      {"egrave;", "è"},
      {""}, {""},
#line 864 "HTMLCharacterReference.gperf"
      {"chcy;", "ч"},
#line 849 "HTMLCharacterReference.gperf"
      {"ccaps;", "⩍"},
#line 1073 "HTMLCharacterReference.gperf"
      {"female;", "♀"},
#line 739 "HTMLCharacterReference.gperf"
      {"bemptyv;", "⦰"},
      {""}, {""},
#line 669 "HTMLCharacterReference.gperf"
      {"agrave", "à"},
#line 670 "HTMLCharacterReference.gperf"
      {"agrave;", "à"},
      {""}, {""}, {""}, {""},
#line 1173 "HTMLCharacterReference.gperf"
      {"hamilt;", "ℋ"},
      {""}, {""},
#line 173 "HTMLCharacterReference.gperf"
      {"Esim;", "⩳"},
#line 578 "HTMLCharacterReference.gperf"
      {"Ugrave", "Ù"},
#line 579 "HTMLCharacterReference.gperf"
      {"Ugrave;", "Ù"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 147 "HTMLCharacterReference.gperf"
      {"Dscr;", "𝒟"},
#line 2172 "HTMLCharacterReference.gperf"
      {"veeeq;", "≚"},
#line 709 "HTMLCharacterReference.gperf"
      {"approx;", "≈"},
      {""},
#line 620 "HTMLCharacterReference.gperf"
      {"Vfr;", "𝔙"},
#line 1473 "HTMLCharacterReference.gperf"
      {"nLtv;", "≪̸"},
#line 1406 "HTMLCharacterReference.gperf"
      {"lsquo;", "‘"},
#line 1407 "HTMLCharacterReference.gperf"
      {"lsquor;", "‚"},
#line 150 "HTMLCharacterReference.gperf"
      {"ETH", "Ð"},
#line 581 "HTMLCharacterReference.gperf"
      {"UnderBar;", "_"},
      {""}, {""}, {""}, {""},
#line 151 "HTMLCharacterReference.gperf"
      {"ETH;", "Ð"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 273 "HTMLCharacterReference.gperf"
      {"Lcaron;", "Ľ"},
      {""}, {""}, {""}, {""},
#line 255 "HTMLCharacterReference.gperf"
      {"Jsercy;", "Ј"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1514 "HTMLCharacterReference.gperf"
      {"ngeq;", "≱"},
#line 1515 "HTMLCharacterReference.gperf"
      {"ngeqq;", "≧̸"},
      {""}, {""}, {""}, {""},
#line 1583 "HTMLCharacterReference.gperf"
      {"nsime;", "≄"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 254 "HTMLCharacterReference.gperf"
      {"Jscr;", "𝒥"},
      {""}, {""}, {""}, {""},
#line 229 "HTMLCharacterReference.gperf"
      {"Idot;", "İ"},
      {""}, {""}, {""}, {""}, {""},
#line 1240 "HTMLCharacterReference.gperf"
      {"iquest", "¿"},
#line 1241 "HTMLCharacterReference.gperf"
      {"iquest;", "¿"},
      {""}, {""}, {""},
#line 1404 "HTMLCharacterReference.gperf"
      {"lsimg;", "⪏"},
      {""}, {""}, {""},
#line 632 "HTMLCharacterReference.gperf"
      {"Xscr;", "𝒳"},
#line 1311 "HTMLCharacterReference.gperf"
      {"lceil;", "⌈"},
      {""}, {""}, {""},
#line 101 "HTMLCharacterReference.gperf"
      {"Darr;", "↡"},
#line 1591 "HTMLCharacterReference.gperf"
      {"nsube;", "⊈"},
#line 522 "HTMLCharacterReference.gperf"
      {"Square;", "□"},
      {""}, {""},
#line 711 "HTMLCharacterReference.gperf"
      {"aring", "å"},
#line 712 "HTMLCharacterReference.gperf"
      {"aring;", "å"},
#line 1046 "HTMLCharacterReference.gperf"
      {"eqcirc;", "≖"},
      {""}, {""},
#line 662 "HTMLCharacterReference.gperf"
      {"acute", "´"},
#line 663 "HTMLCharacterReference.gperf"
      {"acute;", "´"},
      {""}, {""},
#line 1443 "HTMLCharacterReference.gperf"
      {"mho;", "℧"},
      {""},
#line 602 "HTMLCharacterReference.gperf"
      {"Uring;", "Ů"},
#line 2185 "HTMLCharacterReference.gperf"
      {"vsubne;", "⊊︀"},
      {""},
#line 2231 "HTMLCharacterReference.gperf"
      {"yfr;", "𝔶"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1567 "HTMLCharacterReference.gperf"
      {"nprec;", "⊀"},
      {""}, {""}, {""},
#line 648 "HTMLCharacterReference.gperf"
      {"Zdot;", "Ż"},
#line 715 "HTMLCharacterReference.gperf"
      {"asymp;", "≈"},
      {""}, {""}, {""},
#line 529 "HTMLCharacterReference.gperf"
      {"Sscr;", "𝒮"},
      {""}, {""}, {""}, {""},
#line 285 "HTMLCharacterReference.gperf"
      {"LeftFloor;", "⌊"},
#line 2179 "HTMLCharacterReference.gperf"
      {"vnsup;", "⊃⃒"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1047 "HTMLCharacterReference.gperf"
      {"eqcolon;", "≕"},
      {""},
#line 1235 "HTMLCharacterReference.gperf"
      {"iocy;", "ё"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 841 "HTMLCharacterReference.gperf"
      {"capand;", "⩄"},
      {""}, {""}, {""}, {""},
#line 1584 "HTMLCharacterReference.gperf"
      {"nsimeq;", "≄"},
      {""},
#line 541 "HTMLCharacterReference.gperf"
      {"Superset;", "⊃"},
      {""}, {""}, {""}, {""}, {""},
#line 1619 "HTMLCharacterReference.gperf"
      {"nvge;", "≥⃒"},
#line 1290 "HTMLCharacterReference.gperf"
      {"larrb;", "⇤"},
      {""}, {""}, {""}, {""}, {""},
#line 1462 "HTMLCharacterReference.gperf"
      {"mstpos;", "∾"},
      {""},
#line 1576 "HTMLCharacterReference.gperf"
      {"nsc;", "⊁"},
#line 245 "HTMLCharacterReference.gperf"
      {"Iscr;", "ℐ"},
#line 854 "HTMLCharacterReference.gperf"
      {"ccups;", "⩌"},
#line 103 "HTMLCharacterReference.gperf"
      {"Dcaron;", "Ď"},
      {""}, {""}, {""}, {""}, {""},
#line 1510 "HTMLCharacterReference.gperf"
      {"nexists;", "∄"},
      {""}, {""}, {""}, {""}, {""},
#line 659 "HTMLCharacterReference.gperf"
      {"acd;", "∿"},
      {""},
#line 1209 "HTMLCharacterReference.gperf"
      {"igrave", "ì"},
#line 1210 "HTMLCharacterReference.gperf"
      {"igrave;", "ì"},
      {""}, {""},
#line 1579 "HTMLCharacterReference.gperf"
      {"nscr;", "𝓃"},
      {""}, {""}, {""},
#line 1480 "HTMLCharacterReference.gperf"
      {"nap;", "≉"},
      {""}, {""}, {""}, {""},
#line 690 "HTMLCharacterReference.gperf"
      {"angmsdad;", "⦫"},
      {""}, {""}, {""},
#line 508 "HTMLCharacterReference.gperf"
      {"Sc;", "⪼"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 634 "HTMLCharacterReference.gperf"
      {"YIcy;", "Ї"},
      {""}, {""}, {""}, {""}, {""},
#line 1590 "HTMLCharacterReference.gperf"
      {"nsubE;", "⫅̸"},
      {""}, {""}, {""},
#line 158 "HTMLCharacterReference.gperf"
      {"Edot;", "Ė"},
      {""}, {""}, {""}, {""},
#line 653 "HTMLCharacterReference.gperf"
      {"Zscr;", "𝒵"},
      {""}, {""}, {""}, {""},
#line 635 "HTMLCharacterReference.gperf"
      {"YUcy;", "Ю"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 277 "HTMLCharacterReference.gperf"
      {"LeftArrow;", "←"},
#line 814 "HTMLCharacterReference.gperf"
      {"boxur;", "└"},
      {""}, {""},
#line 1442 "HTMLCharacterReference.gperf"
      {"mfr;", "𝔪"},
#line 196 "HTMLCharacterReference.gperf"
      {"Gdot;", "Ġ"},
      {""},
#line 42 "HTMLCharacterReference.gperf"
      {"Assign;", "≔"},
      {""}, {""}, {""}, {""}, {""},
#line 278 "HTMLCharacterReference.gperf"
      {"LeftArrowBar;", "⇤"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 531 "HTMLCharacterReference.gperf"
      {"Sub;", "⋐"},
#line 614 "HTMLCharacterReference.gperf"
      {"Vert;", "‖"},
      {""}, {""}, {""},
#line 262 "HTMLCharacterReference.gperf"
      {"Kfr;", "𝔎"},
      {""}, {""}, {""}, {""},
#line 687 "HTMLCharacterReference.gperf"
      {"angmsdaa;", "⦨"},
      {""}, {""}, {""}, {""},
#line 93 "HTMLCharacterReference.gperf"
      {"Cup;", "⋓"},
      {""},
#line 824 "HTMLCharacterReference.gperf"
      {"brvbar", "¦"},
#line 825 "HTMLCharacterReference.gperf"
      {"brvbar;", "¦"},
      {""}, {""}, {""},
#line 1049 "HTMLCharacterReference.gperf"
      {"eqslantgtr;", "⪖"},
#line 509 "HTMLCharacterReference.gperf"
      {"Scaron;", "Š"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 782 "HTMLCharacterReference.gperf"
      {"boxH;", "═"},
      {""}, {""},
#line 499 "HTMLCharacterReference.gperf"
      {"RoundImplies;", "⥰"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 501 "HTMLCharacterReference.gperf"
      {"Rscr;", "ℛ"},
      {""}, {""}, {""},
#line 710 "HTMLCharacterReference.gperf"
      {"approxeq;", "≊"},
      {""},
#line 1465 "HTMLCharacterReference.gperf"
      {"mumap;", "⊸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 172 "HTMLCharacterReference.gperf"
      {"Escr;", "ℰ"},
      {""}, {""}, {""},
#line 1141 "HTMLCharacterReference.gperf"
      {"gnE;", "≩"},
      {""},
#line 813 "HTMLCharacterReference.gperf"
      {"boxul;", "┘"},
      {""}, {""},
#line 1366 "HTMLCharacterReference.gperf"
      {"lnE;", "≨"},
#line 2218 "HTMLCharacterReference.gperf"
      {"xscr;", "𝓍"},
      {""}, {""}, {""}, {""},
#line 1516 "HTMLCharacterReference.gperf"
      {"ngeqslant;", "⩾̸"},
      {""}, {""}, {""},
#line 704 "HTMLCharacterReference.gperf"
      {"apE;", "⩰"},
      {""}, {""}, {""}, {""}, {""},
#line 207 "HTMLCharacterReference.gperf"
      {"Gscr;", "𝒢"},
      {""},
#line 1493 "HTMLCharacterReference.gperf"
      {"ncaron;", "ň"},
      {""}, {""},
#line 1492 "HTMLCharacterReference.gperf"
      {"ncap;", "⩃"},
      {""}, {""}, {""},
#line 149 "HTMLCharacterReference.gperf"
      {"ENG;", "Ŋ"},
      {""}, {""}, {""}, {""}, {""},
#line 2222 "HTMLCharacterReference.gperf"
      {"xvee;", "⋁"},
      {""}, {""},
#line 374 "HTMLCharacterReference.gperf"
      {"NotLessSlantEqual;", "⩽̸"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 2243 "HTMLCharacterReference.gperf"
      {"zeta;", "ζ"},
      {""}, {""},
#line 370 "HTMLCharacterReference.gperf"
      {"NotLess;", "≮"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 464 "HTMLCharacterReference.gperf"
      {"Rarr;", "↠"},
      {""},
#line 646 "HTMLCharacterReference.gperf"
      {"Zcaron;", "Ž"},
      {""}, {""}, {""},
#line 812 "HTMLCharacterReference.gperf"
      {"boxuR;", "╘"},
      {""},
#line 2229 "HTMLCharacterReference.gperf"
      {"yen", "¥"},
      {""}, {""}, {""}, {""}, {""},
#line 2230 "HTMLCharacterReference.gperf"
      {"yen;", "¥"},
      {""}, {""},
#line 1192 "HTMLCharacterReference.gperf"
      {"horbar;", "―"},
      {""},
#line 689 "HTMLCharacterReference.gperf"
      {"angmsdac;", "⦪"},
#line 555 "HTMLCharacterReference.gperf"
      {"Therefore;", "∴"},
      {""}, {""}, {""}, {""},
#line 1300 "HTMLCharacterReference.gperf"
      {"late;", "⪭"},
#line 1483 "HTMLCharacterReference.gperf"
      {"napos;", "ŉ"},
      {""}, {""},
#line 612 "HTMLCharacterReference.gperf"
      {"Vee;", "⋁"},
#line 98 "HTMLCharacterReference.gperf"
      {"DScy;", "Ѕ"},
      {""}, {""}, {""},
#line 1189 "HTMLCharacterReference.gperf"
      {"hookleftarrow;", "↩"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 465 "HTMLCharacterReference.gperf"
      {"Rarrtl;", "⤖"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1380 "HTMLCharacterReference.gperf"
      {"looparrowleft;", "↫"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 466 "HTMLCharacterReference.gperf"
      {"Rcaron;", "Ř"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 402 "HTMLCharacterReference.gperf"
      {"Nscr;", "𝒩"},
      {""}, {""},
#line 1160 "HTMLCharacterReference.gperf"
      {"gtquest;", "⩼"},
#line 453 "HTMLCharacterReference.gperf"
      {"Psi;", "Ψ"},
      {""}, {""},
#line 154 "HTMLCharacterReference.gperf"
      {"Ecaron;", "Ě"},
#line 1417 "HTMLCharacterReference.gperf"
      {"ltquest;", "⩻"},
      {""}, {""},
#line 847 "HTMLCharacterReference.gperf"
      {"caret;", "⁁"},
      {""}, {""}, {""},
#line 1204 "HTMLCharacterReference.gperf"
      {"iecy;", "е"},
      {""}, {""}, {""}, {""},
#line 2200 "HTMLCharacterReference.gperf"
      {"xcap;", "⋂"},
#line 717 "HTMLCharacterReference.gperf"
      {"atilde", "ã"},
#line 718 "HTMLCharacterReference.gperf"
      {"atilde;", "ã"},
      {""}, {""}, {""}, {""}, {""},
#line 1600 "HTMLCharacterReference.gperf"
      {"nsupset;", "⊃⃒"},
#line 54 "HTMLCharacterReference.gperf"
      {"Bfr;", "𝔅"},
#line 220 "HTMLCharacterReference.gperf"
      {"HumpEqual;", "≏"},
      {""},
#line 604 "HTMLCharacterReference.gperf"
      {"Utilde;", "Ũ"},
      {""}, {""},
#line 1010 "HTMLCharacterReference.gperf"
      {"ecir;", "≖"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 74 "HTMLCharacterReference.gperf"
      {"Cfr;", "ℭ"},
      {""}, {""}, {""},
#line 855 "HTMLCharacterReference.gperf"
      {"ccupssm;", "⩐"},
      {""},
#line 1312 "HTMLCharacterReference.gperf"
      {"lcub;", "{"},
#line 2178 "HTMLCharacterReference.gperf"
      {"vnsub;", "⊂⃒"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1062 "HTMLCharacterReference.gperf"
      {"eth", "ð"},
      {""}, {""}, {""}, {""}, {""},
#line 1063 "HTMLCharacterReference.gperf"
      {"eth;", "ð"},
#line 1497 "HTMLCharacterReference.gperf"
      {"ncup;", "⩂"},
      {""}, {""},
#line 1137 "HTMLCharacterReference.gperf"
      {"gl;", "≷"},
      {""},
#line 41 "HTMLCharacterReference.gperf"
      {"Ascr;", "𝒜"},
      {""}, {""},
#line 1358 "HTMLCharacterReference.gperf"
      {"ll;", "≪"},
      {""},
#line 99 "HTMLCharacterReference.gperf"
      {"DZcy;", "Џ"},
#line 231 "HTMLCharacterReference.gperf"
      {"Igrave", "Ì"},
#line 232 "HTMLCharacterReference.gperf"
      {"Igrave;", "Ì"},
#line 1024 "HTMLCharacterReference.gperf"
      {"el;", "⪙"},
      {""}, {""}, {""}, {""}, {""},
#line 1027 "HTMLCharacterReference.gperf"
      {"els;", "⪕"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2242 "HTMLCharacterReference.gperf"
      {"zeetrf;", "ℨ"},
      {""},
#line 438 "HTMLCharacterReference.gperf"
      {"Phi;", "Φ"},
#line 1381 "HTMLCharacterReference.gperf"
      {"looparrowright;", "↬"},
#line 358 "HTMLCharacterReference.gperf"
      {"NotGreater;", "≯"},
#line 58 "HTMLCharacterReference.gperf"
      {"Bumpeq;", "≎"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1139 "HTMLCharacterReference.gperf"
      {"gla;", "⪥"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 448 "HTMLCharacterReference.gperf"
      {"Prime;", "″"},
      {""},
#line 1245 "HTMLCharacterReference.gperf"
      {"isindot;", "⋵"},
      {""},
#line 1136 "HTMLCharacterReference.gperf"
      {"gjcy;", "ѓ"},
      {""}, {""}, {""}, {""},
#line 1357 "HTMLCharacterReference.gperf"
      {"ljcy;", "љ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 968 "HTMLCharacterReference.gperf"
      {"div;", "÷"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 961 "HTMLCharacterReference.gperf"
      {"diam;", "⋄"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1596 "HTMLCharacterReference.gperf"
      {"nsucceq;", "⪰̸"},
      {""}, {""},
#line 1301 "HTMLCharacterReference.gperf"
      {"lates;", "⪭︀"},
      {""},
#line 1291 "HTMLCharacterReference.gperf"
      {"larrbfs;", "⤟"},
      {""},
#line 515 "HTMLCharacterReference.gperf"
      {"ShortLeftArrow;", "←"},
#line 778 "HTMLCharacterReference.gperf"
      {"boxDL;", "╗"},
#line 2193 "HTMLCharacterReference.gperf"
      {"weierp;", "℘"},
#line 1484 "HTMLCharacterReference.gperf"
      {"napprox;", "≉"},
#line 1401 "HTMLCharacterReference.gperf"
      {"lsh;", "↰"},
#line 505 "HTMLCharacterReference.gperf"
      {"SHcy;", "Ш"},
      {""},
#line 336 "HTMLCharacterReference.gperf"
      {"Ncaron;", "Ň"},
#line 205 "HTMLCharacterReference.gperf"
      {"GreaterSlantEqual;", "⩾"},
      {""},
#line 973 "HTMLCharacterReference.gperf"
      {"djcy;", "ђ"},
#line 1398 "HTMLCharacterReference.gperf"
      {"lrtri;", "⊿"},
#line 1225 "HTMLCharacterReference.gperf"
      {"incare;", "℅"},
      {""},
#line 868 "HTMLCharacterReference.gperf"
      {"cir;", "○"},
      {""},
#line 967 "HTMLCharacterReference.gperf"
      {"disin;", "⋲"},
      {""}, {""}, {""}, {""},
#line 1421 "HTMLCharacterReference.gperf"
      {"ltrif;", "◂"},
      {""}, {""}, {""}, {""}, {""},
#line 1171 "HTMLCharacterReference.gperf"
      {"hairsp;", " "},
      {""}, {""}, {""},
#line 1359 "HTMLCharacterReference.gperf"
      {"llarr;", "⇇"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 952 "HTMLCharacterReference.gperf"
      {"ddotseq;", "⩷"},
      {""},
#line 1080 "HTMLCharacterReference.gperf"
      {"flat;", "♭"},
      {""}, {""}, {""}, {""},
#line 2202 "HTMLCharacterReference.gperf"
      {"xcup;", "⋃"},
      {""},
#line 192 "HTMLCharacterReference.gperf"
      {"Gbreve;", "Ğ"},
      {""}, {""}, {""},
#line 1113 "HTMLCharacterReference.gperf"
      {"gamma;", "γ"},
      {""}, {""}, {""}, {""},
#line 998 "HTMLCharacterReference.gperf"
      {"dtrif;", "▾"},
#line 1250 "HTMLCharacterReference.gperf"
      {"itilde;", "ĩ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 160 "HTMLCharacterReference.gperf"
      {"Egrave", "È"},
#line 161 "HTMLCharacterReference.gperf"
      {"Egrave;", "È"},
      {""}, {""},
#line 628 "HTMLCharacterReference.gperf"
      {"Wscr;", "𝒲"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2241 "HTMLCharacterReference.gperf"
      {"zdot;", "ż"},
      {""}, {""}, {""}, {""},
#line 1601 "HTMLCharacterReference.gperf"
      {"nsupseteq;", "⊉"},
#line 1602 "HTMLCharacterReference.gperf"
      {"nsupseteqq;", "⫆̸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 644 "HTMLCharacterReference.gperf"
      {"ZHcy;", "Ж"},
#line 734 "HTMLCharacterReference.gperf"
      {"bcong;", "≌"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1211 "HTMLCharacterReference.gperf"
      {"ii;", "ⅈ"},
      {""}, {""}, {""},
#line 86 "HTMLCharacterReference.gperf"
      {"Conint;", "∯"},
      {""},
#line 454 "HTMLCharacterReference.gperf"
      {"QUOT", "\""},
#line 455 "HTMLCharacterReference.gperf"
      {"QUOT;", "\""},
#line 1117 "HTMLCharacterReference.gperf"
      {"gcirc;", "ĝ"},
      {""}, {""}, {""}, {""},
#line 2182 "HTMLCharacterReference.gperf"
      {"vrtri;", "⊳"},
      {""}, {""}, {""},
#line 1011 "HTMLCharacterReference.gperf"
      {"ecirc", "ê"},
#line 1012 "HTMLCharacterReference.gperf"
      {"ecirc;", "ê"},
      {""}, {""}, {""}, {""},
#line 853 "HTMLCharacterReference.gperf"
      {"ccirc;", "ĉ"},
      {""}, {""}, {""},
#line 660 "HTMLCharacterReference.gperf"
      {"acirc", "â"},
#line 661 "HTMLCharacterReference.gperf"
      {"acirc;", "â"},
#line 597 "HTMLCharacterReference.gperf"
      {"Updownarrow;", "⇕"},
      {""}, {""}, {""},
#line 1254 "HTMLCharacterReference.gperf"
      {"jcirc;", "ĵ"},
#line 891 "HTMLCharacterReference.gperf"
      {"compfn;", "∘"},
      {""}, {""},
#line 573 "HTMLCharacterReference.gperf"
      {"Ucirc", "Û"},
#line 574 "HTMLCharacterReference.gperf"
      {"Ucirc;", "Û"},
      {""}, {""}, {""},
#line 565 "HTMLCharacterReference.gperf"
      {"Tscr;", "𝒯"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 787 "HTMLCharacterReference.gperf"
      {"boxUL;", "╝"},
#line 373 "HTMLCharacterReference.gperf"
      {"NotLessLess;", "≪̸"},
      {""},
#line 444 "HTMLCharacterReference.gperf"
      {"Precedes;", "≺"},
#line 598 "HTMLCharacterReference.gperf"
      {"UpperLeftArrow;", "↖"},
      {""},
#line 705 "HTMLCharacterReference.gperf"
      {"apacir;", "⩯"},
      {""}, {""},
#line 708 "HTMLCharacterReference.gperf"
      {"apos;", "'"},
#line 1179 "HTMLCharacterReference.gperf"
      {"hcirc;", "ĥ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1227 "HTMLCharacterReference.gperf"
      {"infintie;", "⧝"},
#line 452 "HTMLCharacterReference.gperf"
      {"Pscr;", "𝒫"},
      {""},
#line 2174 "HTMLCharacterReference.gperf"
      {"verbar;", "|"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2248 "HTMLCharacterReference.gperf"
      {"zscr;", "𝓏"},
      {""},
#line 584 "HTMLCharacterReference.gperf"
      {"UnderParenthesis;", "⏝"},
      {""},
#line 965 "HTMLCharacterReference.gperf"
      {"die;", "¨"},
#line 870 "HTMLCharacterReference.gperf"
      {"circ;", "ˆ"},
      {""}, {""},
#line 309 "HTMLCharacterReference.gperf"
      {"Ll;", "⋘"},
      {""}, {""},
#line 964 "HTMLCharacterReference.gperf"
      {"diams;", "♦"},
#line 798 "HTMLCharacterReference.gperf"
      {"boxbox;", "⧉"},
      {""}, {""},
#line 1314 "HTMLCharacterReference.gperf"
      {"ldca;", "⤶"},
#line 766 "HTMLCharacterReference.gperf"
      {"blank;", "␣"},
      {""}, {""}, {""}, {""},
#line 1395 "HTMLCharacterReference.gperf"
      {"lrhar;", "⇋"},
      {""}, {""}, {""},
#line 894 "HTMLCharacterReference.gperf"
      {"cong;", "≅"},
#line 1482 "HTMLCharacterReference.gperf"
      {"napid;", "≋̸"},
      {""}, {""}, {""},
#line 1321 "HTMLCharacterReference.gperf"
      {"leftarrow;", "←"},
      {""},
#line 1163 "HTMLCharacterReference.gperf"
      {"gtrdot;", "⋗"},
      {""}, {""}, {""},
#line 2189 "HTMLCharacterReference.gperf"
      {"wcirc;", "ŵ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 922 "HTMLCharacterReference.gperf"
      {"cups;", "∪︀"},
#line 802 "HTMLCharacterReference.gperf"
      {"boxdr;", "┌"},
#line 920 "HTMLCharacterReference.gperf"
      {"cupdot;", "⊍"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 26 "HTMLCharacterReference.gperf"
      {"Abreve;", "Ă"},
      {""}, {""},
#line 279 "HTMLCharacterReference.gperf"
      {"LeftArrowRightArrow;", "⇆"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1281 "HTMLCharacterReference.gperf"
      {"lagran;", "ℒ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 989 "HTMLCharacterReference.gperf"
      {"drbkarow;", "⤐"},
      {""}, {""}, {""},
#line 524 "HTMLCharacterReference.gperf"
      {"SquareSubset;", "⊏"},
      {""},
#line 622 "HTMLCharacterReference.gperf"
      {"Vscr;", "𝒱"},
#line 770 "HTMLCharacterReference.gperf"
      {"block;", "█"},
      {""}, {""},
#line 322 "HTMLCharacterReference.gperf"
      {"Lsh;", "↰"},
      {""}, {""},
#line 184 "HTMLCharacterReference.gperf"
      {"ForAll;", "∀"},
      {""}, {""}, {""}, {""},
#line 843 "HTMLCharacterReference.gperf"
      {"capcap;", "⩋"},
      {""}, {""}, {""},
#line 31 "HTMLCharacterReference.gperf"
      {"Agrave", "À"},
#line 32 "HTMLCharacterReference.gperf"
      {"Agrave;", "À"},
      {""}, {""}, {""},
#line 1000 "HTMLCharacterReference.gperf"
      {"duhar;", "⥯"},
#line 1618 "HTMLCharacterReference.gperf"
      {"nvdash;", "⊬"},
      {""}, {""}, {""},
#line 785 "HTMLCharacterReference.gperf"
      {"boxHd;", "╤"},
#line 1114 "HTMLCharacterReference.gperf"
      {"gammad;", "ϝ"},
      {""},
#line 376 "HTMLCharacterReference.gperf"
      {"NotNestedGreaterGreater;", "⪢̸"},
      {""}, {""},
#line 551 "HTMLCharacterReference.gperf"
      {"Tcaron;", "Ť"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1352 "HTMLCharacterReference.gperf"
      {"lgE;", "⪑"},
#line 53 "HTMLCharacterReference.gperf"
      {"Beta;", "Β"},
#line 212 "HTMLCharacterReference.gperf"
      {"Hcirc;", "Ĥ"},
      {""},
#line 377 "HTMLCharacterReference.gperf"
      {"NotNestedLessLess;", "⪡̸"},
#line 550 "HTMLCharacterReference.gperf"
      {"Tau;", "Τ"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 801 "HTMLCharacterReference.gperf"
      {"boxdl;", "┐"},
#line 2171 "HTMLCharacterReference.gperf"
      {"veebar;", "⊻"},
#line 525 "HTMLCharacterReference.gperf"
      {"SquareSubsetEqual;", "⊑"},
      {""},
#line 1201 "HTMLCharacterReference.gperf"
      {"icirc", "î"},
#line 1202 "HTMLCharacterReference.gperf"
      {"icirc;", "î"},
      {""}, {""}, {""}, {""}, {""},
#line 528 "HTMLCharacterReference.gperf"
      {"SquareUnion;", "⊔"},
      {""},
#line 1143 "HTMLCharacterReference.gperf"
      {"gnapprox;", "⪊"},
#line 1578 "HTMLCharacterReference.gperf"
      {"nsce;", "⪰̸"},
      {""}, {""}, {""},
#line 1368 "HTMLCharacterReference.gperf"
      {"lnapprox;", "⪉"},
      {""}, {""},
#line 2239 "HTMLCharacterReference.gperf"
      {"zcaron;", "ž"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1765 "HTMLCharacterReference.gperf"
      {"qint;", "⨌"},
      {""}, {""}, {""}, {""}, {""},
#line 638 "HTMLCharacterReference.gperf"
      {"Ycirc;", "Ŷ"},
      {""}, {""}, {""},
#line 2234 "HTMLCharacterReference.gperf"
      {"yscr;", "𝓎"},
#line 1354 "HTMLCharacterReference.gperf"
      {"lharu;", "↼"},
      {""}, {""}, {""}, {""},
#line 699 "HTMLCharacterReference.gperf"
      {"angst;", "Å"},
#line 1228 "HTMLCharacterReference.gperf"
      {"inodot;", "ı"},
      {""}, {""},
#line 526 "HTMLCharacterReference.gperf"
      {"SquareSuperset;", "⊐"},
#line 903 "HTMLCharacterReference.gperf"
      {"cross;", "✗"},
      {""}, {""}, {""},
#line 527 "HTMLCharacterReference.gperf"
      {"SquareSupersetEqual;", "⊒"},
#line 1082 "HTMLCharacterReference.gperf"
      {"fltns;", "▱"},
      {""},
#line 630 "HTMLCharacterReference.gperf"
      {"Xi;", "Ξ"},
#line 570 "HTMLCharacterReference.gperf"
      {"Uarrocir;", "⥉"},
      {""},
#line 800 "HTMLCharacterReference.gperf"
      {"boxdR;", "╒"},
#line 1355 "HTMLCharacterReference.gperf"
      {"lharul;", "⥪"},
      {""},
#line 842 "HTMLCharacterReference.gperf"
      {"capbrcup;", "⩉"},
      {""}, {""}, {""}, {""}, {""},
#line 39 "HTMLCharacterReference.gperf"
      {"Aring", "Å"},
#line 40 "HTMLCharacterReference.gperf"
      {"Aring;", "Å"},
      {""}, {""}, {""}, {""},
#line 1107 "HTMLCharacterReference.gperf"
      {"frasl;", "⁄"},
      {""}, {""}, {""}, {""},
#line 1315 "HTMLCharacterReference.gperf"
      {"ldquo;", "“"},
#line 1316 "HTMLCharacterReference.gperf"
      {"ldquor;", "„"},
#line 1568 "HTMLCharacterReference.gperf"
      {"npreceq;", "⪯̸"},
      {""},
#line 1340 "HTMLCharacterReference.gperf"
      {"lesg;", "⋚︀"},
#line 827 "HTMLCharacterReference.gperf"
      {"bsemi;", "⁏"},
#line 1633 "HTMLCharacterReference.gperf"
      {"nwnear;", "⤧"},
#line 716 "HTMLCharacterReference.gperf"
      {"asympeq;", "≍"},
      {""}, {""},
#line 817 "HTMLCharacterReference.gperf"
      {"boxvL;", "╡"},
      {""}, {""}, {""}, {""}, {""},
#line 1384 "HTMLCharacterReference.gperf"
      {"loplus;", "⨭"},
      {""}, {""},
#line 680 "HTMLCharacterReference.gperf"
      {"andd;", "⩜"},
      {""},
#line 246 "HTMLCharacterReference.gperf"
      {"Itilde;", "Ĩ"},
      {""}, {""}, {""}, {""},
#line 204 "HTMLCharacterReference.gperf"
      {"GreaterLess;", "≷"},
      {""}, {""},
#line 983 "HTMLCharacterReference.gperf"
      {"dotsquare;", "⊡"},
      {""},
#line 919 "HTMLCharacterReference.gperf"
      {"cupcup;", "⩊"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1604 "HTMLCharacterReference.gperf"
      {"ntilde", "ñ"},
#line 1605 "HTMLCharacterReference.gperf"
      {"ntilde;", "ñ"},
      {""}, {""}, {""}, {""}, {""},
#line 1592 "HTMLCharacterReference.gperf"
      {"nsubset;", "⊂⃒"},
      {""}, {""}, {""},
#line 1396 "HTMLCharacterReference.gperf"
      {"lrhard;", "⥭"},
      {""},
#line 1231 "HTMLCharacterReference.gperf"
      {"integers;", "ℤ"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 556 "HTMLCharacterReference.gperf"
      {"Theta;", "Θ"},
      {""}, {""},
#line 1266 "HTMLCharacterReference.gperf"
      {"kfr;", "𝔨"},
      {""}, {""}, {""},
#line 387 "HTMLCharacterReference.gperf"
      {"NotSquareSuperset;", "⊐̸"},
      {""}, {""}, {""}, {""},
#line 388 "HTMLCharacterReference.gperf"
      {"NotSquareSupersetEqual;", "⋣"},
      {""},
#line 1287 "HTMLCharacterReference.gperf"
      {"laquo", "«"},
#line 1288 "HTMLCharacterReference.gperf"
      {"laquo;", "«"},
#line 871 "HTMLCharacterReference.gperf"
      {"circeq;", "≗"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1524 "HTMLCharacterReference.gperf"
      {"ni;", "∋"},
      {""}, {""}, {""}, {""}, {""},
#line 1525 "HTMLCharacterReference.gperf"
      {"nis;", "⋼"},
#line 300 "HTMLCharacterReference.gperf"
      {"Leftarrow;", "⇐"},
      {""}, {""}, {""},
#line 981 "HTMLCharacterReference.gperf"
      {"dotminus;", "∸"},
#line 1461 "HTMLCharacterReference.gperf"
      {"mscr;", "𝓂"},
      {""}, {""}, {""},
#line 1432 "HTMLCharacterReference.gperf"
      {"map;", "↦"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1325 "HTMLCharacterReference.gperf"
      {"leftleftarrows;", "⇇"},
      {""},
#line 1399 "HTMLCharacterReference.gperf"
      {"lsaquo;", "‹"},
      {""},
#line 1527 "HTMLCharacterReference.gperf"
      {"niv;", "∋"},
      {""}, {""}, {""}, {""}, {""},
#line 264 "HTMLCharacterReference.gperf"
      {"Kscr;", "𝒦"},
      {""}, {""}, {""}, {""},
#line 362 "HTMLCharacterReference.gperf"
      {"NotGreaterLess;", "≹"},
      {""},
#line 119 "HTMLCharacterReference.gperf"
      {"DoubleContourIntegral;", "∯"},
      {""},
#line 75 "HTMLCharacterReference.gperf"
      {"Chi;", "Χ"},
      {""}, {""}, {""}, {""}, {""},
#line 514 "HTMLCharacterReference.gperf"
      {"ShortDownArrow;", "↓"},
#line 736 "HTMLCharacterReference.gperf"
      {"bdquo;", "„"},
      {""}, {""},
#line 808 "HTMLCharacterReference.gperf"
      {"boxminus;", "⊟"},
#line 548 "HTMLCharacterReference.gperf"
      {"TScy;", "Ц"},
#line 1541 "HTMLCharacterReference.gperf"
      {"nlsim;", "≴"},
      {""},
#line 131 "HTMLCharacterReference.gperf"
      {"DoubleUpDownArrow;", "⇕"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1528 "HTMLCharacterReference.gperf"
      {"njcy;", "њ"},
#line 1495 "HTMLCharacterReference.gperf"
      {"ncong;", "≇"},
      {""}, {""}, {""}, {""}, {""},
#line 2190 "HTMLCharacterReference.gperf"
      {"wedbar;", "⩟"},
      {""}, {""},
#line 2245 "HTMLCharacterReference.gperf"
      {"zhcy;", "ж"},
#line 56 "HTMLCharacterReference.gperf"
      {"Breve;", "˘"},
      {""},
#line 1110 "HTMLCharacterReference.gperf"
      {"gE;", "≧"},
#line 1542 "HTMLCharacterReference.gperf"
      {"nlt;", "≮"},
#line 2039 "HTMLCharacterReference.gperf"
      {"tbrk;", "⎴"},
      {""},
#line 729 "HTMLCharacterReference.gperf"
      {"barvee;", "⊽"},
#line 1276 "HTMLCharacterReference.gperf"
      {"lE;", "≦"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1118 "HTMLCharacterReference.gperf"
      {"gcy;", "г"},
      {""},
#line 250 "HTMLCharacterReference.gperf"
      {"Jcirc;", "Ĵ"},
      {""}, {""},
#line 1313 "HTMLCharacterReference.gperf"
      {"lcy;", "л"},
      {""}, {""}, {""}, {""},
#line 1014 "HTMLCharacterReference.gperf"
      {"ecy;", "э"},
      {""}, {""}, {""},
#line 596 "HTMLCharacterReference.gperf"
      {"Uparrow;", "⇑"},
      {""},
#line 931 "HTMLCharacterReference.gperf"
      {"curvearrowleft;", "↶"},
#line 1574 "HTMLCharacterReference.gperf"
      {"nrtri;", "⋫"},
      {""}, {""},
#line 664 "HTMLCharacterReference.gperf"
      {"acy;", "а"},
      {""}, {""}, {""}, {""},
#line 1255 "HTMLCharacterReference.gperf"
      {"jcy;", "й"},
#line 2235 "HTMLCharacterReference.gperf"
      {"yucy;", "ю"},
      {""},
#line 1127 "HTMLCharacterReference.gperf"
      {"gesdot;", "⪀"},
      {""},
#line 575 "HTMLCharacterReference.gperf"
      {"Ucy;", "У"},
      {""},
#line 1056 "HTMLCharacterReference.gperf"
      {"erDot;", "≓"},
#line 1337 "HTMLCharacterReference.gperf"
      {"lesdot;", "⩿"},
      {""}, {""}, {""},
#line 1531 "HTMLCharacterReference.gperf"
      {"nlarr;", "↚"},
      {""}, {""},
#line 2066 "HTMLCharacterReference.gperf"
      {"top;", "⊤"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1177 "HTMLCharacterReference.gperf"
      {"harrw;", "↭"},
      {""},
#line 2207 "HTMLCharacterReference.gperf"
      {"xi;", "ξ"},
#line 948 "HTMLCharacterReference.gperf"
      {"dcy;", "д"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 582 "HTMLCharacterReference.gperf"
      {"UnderBrace;", "⏟"},
#line 1626 "HTMLCharacterReference.gperf"
      {"nvrArr;", "⤃"},
#line 583 "HTMLCharacterReference.gperf"
      {"UnderBracket;", "⎵"},
#line 1339 "HTMLCharacterReference.gperf"
      {"lesdotor;", "⪃"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 511 "HTMLCharacterReference.gperf"
      {"Scirc;", "Ŝ"},
#line 1341 "HTMLCharacterReference.gperf"
      {"lesges;", "⪓"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 750 "HTMLCharacterReference.gperf"
      {"bigoplus;", "⨁"},
      {""}, {""},
#line 1476 "HTMLCharacterReference.gperf"
      {"nVdash;", "⊮"},
#line 1770 "HTMLCharacterReference.gperf"
      {"quatint;", "⨖"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1593 "HTMLCharacterReference.gperf"
      {"nsubseteq;", "⊈"},
#line 1594 "HTMLCharacterReference.gperf"
      {"nsubseteqq;", "⫅̸"},
      {""}, {""}, {""},
#line 71 "HTMLCharacterReference.gperf"
      {"Cdot;", "Ċ"},
      {""}, {""}, {""}, {""}, {""},
#line 913 "HTMLCharacterReference.gperf"
      {"cuesc;", "⋟"},
      {""}, {""},
#line 1072 "HTMLCharacterReference.gperf"
      {"fcy;", "ф"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 761 "HTMLCharacterReference.gperf"
      {"blacksquare;", "▪"},
      {""}, {""},
#line 226 "HTMLCharacterReference.gperf"
      {"Icirc", "Î"},
#line 227 "HTMLCharacterReference.gperf"
      {"Icirc;", "Î"},
      {""}, {""},
#line 810 "HTMLCharacterReference.gperf"
      {"boxtimes;", "⊠"},
      {""},
#line 793 "HTMLCharacterReference.gperf"
      {"boxVL;", "╣"},
      {""}, {""},
#line 2168 "HTMLCharacterReference.gperf"
      {"vcy;", "в"},
#line 2056 "HTMLCharacterReference.gperf"
      {"thorn", "þ"},
#line 2057 "HTMLCharacterReference.gperf"
      {"thorn;", "þ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 502 "HTMLCharacterReference.gperf"
      {"Rsh;", "↱"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 375 "HTMLCharacterReference.gperf"
      {"NotLessTilde;", "≴"},
      {""}, {""}, {""}, {""}, {""},
#line 735 "HTMLCharacterReference.gperf"
      {"bcy;", "б"},
      {""},
#line 403 "HTMLCharacterReference.gperf"
      {"Ntilde", "Ñ"},
#line 404 "HTMLCharacterReference.gperf"
      {"Ntilde;", "Ñ"},
      {""}, {""},
#line 707 "HTMLCharacterReference.gperf"
      {"apid;", "≋"},
      {""},
#line 1507 "HTMLCharacterReference.gperf"
      {"nesear;", "⤨"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1002 "HTMLCharacterReference.gperf"
      {"dzcy;", "џ"},
      {""},
#line 1414 "HTMLCharacterReference.gperf"
      {"lthree;", "⋋"},
      {""}, {""}, {""}, {""}, {""},
#line 22 "HTMLCharacterReference.gperf"
      {"AMP", "&"},
      {""}, {""}, {""},
#line 1437 "HTMLCharacterReference.gperf"
      {"marker;", "▮"},
      {""},
#line 23 "HTMLCharacterReference.gperf"
      {"AMP;", "&"},
      {""},
#line 1771 "HTMLCharacterReference.gperf"
      {"quest;", "?"},
      {""}, {""},
#line 1533 "HTMLCharacterReference.gperf"
      {"nle;", "≰"},
#line 57 "HTMLCharacterReference.gperf"
      {"Bscr;", "ℬ"},
#line 2209 "HTMLCharacterReference.gperf"
      {"xlarr;", "⟵"},
#line 1023 "HTMLCharacterReference.gperf"
      {"egsdot;", "⪘"},
#line 288 "HTMLCharacterReference.gperf"
      {"LeftTee;", "⊣"},
#line 1277 "HTMLCharacterReference.gperf"
      {"lEg;", "⪋"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 962 "HTMLCharacterReference.gperf"
      {"diamond;", "⋄"},
#line 2045 "HTMLCharacterReference.gperf"
      {"tfr;", "𝔱"},
#line 92 "HTMLCharacterReference.gperf"
      {"Cscr;", "𝒞"},
      {""}, {""}, {""},
#line 63 "HTMLCharacterReference.gperf"
      {"Cap;", "⋒"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 2221 "HTMLCharacterReference.gperf"
      {"xutri;", "△"},
      {""}, {""},
#line 179 "HTMLCharacterReference.gperf"
      {"Fcy;", "Ф"},
#line 1181 "HTMLCharacterReference.gperf"
      {"heartsuit;", "♥"},
      {""}, {""}, {""}, {""}, {""},
#line 43 "HTMLCharacterReference.gperf"
      {"Atilde", "Ã"},
#line 44 "HTMLCharacterReference.gperf"
      {"Atilde;", "Ã"},
      {""},
#line 1203 "HTMLCharacterReference.gperf"
      {"icy;", "и"},
      {""}, {""},
#line 1196 "HTMLCharacterReference.gperf"
      {"hybull;", "⁃"},
      {""}, {""}, {""},
#line 1555 "HTMLCharacterReference.gperf"
      {"notni;", "∌"},
      {""}, {""}, {""},
#line 879 "HTMLCharacterReference.gperf"
      {"cire;", "≗"},
#line 190 "HTMLCharacterReference.gperf"
      {"Gamma;", "Γ"},
      {""}, {""}, {""},
#line 2071 "HTMLCharacterReference.gperf"
      {"tosa;", "⤩"},
#line 1275 "HTMLCharacterReference.gperf"
      {"lBarr;", "⤎"},
      {""}, {""},
#line 1496 "HTMLCharacterReference.gperf"
      {"ncongdot;", "⩭̸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 639 "HTMLCharacterReference.gperf"
      {"Ycy;", "Ы"},
      {""},
#line 1004 "HTMLCharacterReference.gperf"
      {"eDDot;", "⩷"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 966 "HTMLCharacterReference.gperf"
      {"digamma;", "ϝ"},
      {""}, {""},
#line 1272 "HTMLCharacterReference.gperf"
      {"lAarr;", "⇚"},
      {""}, {""}, {""},
#line 155 "HTMLCharacterReference.gperf"
      {"Ecirc", "Ê"},
#line 156 "HTMLCharacterReference.gperf"
      {"Ecirc;", "Ê"},
      {""}, {""}, {""},
#line 1970 "HTMLCharacterReference.gperf"
      {"star;", "☆"},
#line 1017 "HTMLCharacterReference.gperf"
      {"efDot;", "≒"},
      {""}, {""}, {""},
#line 1273 "HTMLCharacterReference.gperf"
      {"lArr;", "⇐"},
#line 2201 "HTMLCharacterReference.gperf"
      {"xcirc;", "◯"},
      {""}, {""}, {""}, {""},
#line 1135 "HTMLCharacterReference.gperf"
      {"gimel;", "ℷ"},
      {""}, {""}, {""},
#line 1948 "HTMLCharacterReference.gperf"
      {"spar;", "∥"},
#line 516 "HTMLCharacterReference.gperf"
      {"ShortRightArrow;", "→"},
      {""}, {""},
#line 275 "HTMLCharacterReference.gperf"
      {"Lcy;", "Л"},
      {""},
#line 194 "HTMLCharacterReference.gperf"
      {"Gcirc;", "Ĝ"},
      {""}, {""},
#line 2001 "HTMLCharacterReference.gperf"
      {"sum;", "∑"},
#line 2148 "HTMLCharacterReference.gperf"
      {"vBar;", "⫨"},
      {""},
#line 963 "HTMLCharacterReference.gperf"
      {"diamondsuit;", "♦"},
#line 239 "HTMLCharacterReference.gperf"
      {"Intersection;", "⋂"},
      {""}, {""},
#line 2149 "HTMLCharacterReference.gperf"
      {"vBarv;", "⫩"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1614 "HTMLCharacterReference.gperf"
      {"numsp;", " "},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 938 "HTMLCharacterReference.gperf"
      {"dArr;", "⇓"},
      {""},
#line 1458 "HTMLCharacterReference.gperf"
      {"models;", "⊧"},
#line 1128 "HTMLCharacterReference.gperf"
      {"gesdoto;", "⪂"},
#line 2009 "HTMLCharacterReference.gperf"
      {"sup;", "⊃"},
#line 1170 "HTMLCharacterReference.gperf"
      {"hArr;", "⇔"},
      {""},
#line 1769 "HTMLCharacterReference.gperf"
      {"quaternions;", "ℍ"},
#line 1338 "HTMLCharacterReference.gperf"
      {"lesdoto;", "⪁"},
#line 1512 "HTMLCharacterReference.gperf"
      {"ngE;", "≧̸"},
      {""}, {""}, {""}, {""},
#line 2003 "HTMLCharacterReference.gperf"
      {"sup1", "¹"},
#line 2004 "HTMLCharacterReference.gperf"
      {"sup1;", "¹"},
#line 1965 "HTMLCharacterReference.gperf"
      {"srarr;", "→"},
      {""}, {""},
#line 2005 "HTMLCharacterReference.gperf"
      {"sup2", "²"},
#line 2006 "HTMLCharacterReference.gperf"
      {"sup2;", "²"},
      {""}, {""}, {""},
#line 2007 "HTMLCharacterReference.gperf"
      {"sup3", "³"},
#line 2008 "HTMLCharacterReference.gperf"
      {"sup3;", "³"},
      {""}, {""}, {""},
#line 326 "HTMLCharacterReference.gperf"
      {"Mcy;", "М"},
      {""},
#line 2032 "HTMLCharacterReference.gperf"
      {"swarr;", "↙"},
      {""}, {""},
#line 130 "HTMLCharacterReference.gperf"
      {"DoubleUpArrow;", "⇑"},
      {""}, {""},
#line 66 "HTMLCharacterReference.gperf"
      {"Ccaron;", "Č"},
#line 1563 "HTMLCharacterReference.gperf"
      {"npolint;", "⨔"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1386 "HTMLCharacterReference.gperf"
      {"lowast;", "∗"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 385 "HTMLCharacterReference.gperf"
      {"NotSquareSubset;", "⊏̸"},
      {""}, {""}, {""},
#line 2147 "HTMLCharacterReference.gperf"
      {"vArr;", "⇕"},
#line 386 "HTMLCharacterReference.gperf"
      {"NotSquareSubsetEqual;", "⋢"},
      {""}, {""}, {""},
#line 1536 "HTMLCharacterReference.gperf"
      {"nleq;", "≰"},
#line 1537 "HTMLCharacterReference.gperf"
      {"nleqq;", "≦̸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 206 "HTMLCharacterReference.gperf"
      {"GreaterTilde;", "≳"},
#line 542 "HTMLCharacterReference.gperf"
      {"SupersetEqual;", "⊇"},
      {""},
#line 883 "HTMLCharacterReference.gperf"
      {"clubs;", "♣"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 104 "HTMLCharacterReference.gperf"
      {"Dcy;", "Д"},
      {""}, {""}, {""},
#line 72 "HTMLCharacterReference.gperf"
      {"Cedilla;", "¸"},
#line 658 "HTMLCharacterReference.gperf"
      {"acE;", "∾̳"},
      {""}, {""},
#line 623 "HTMLCharacterReference.gperf"
      {"Vvdash;", "⊪"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 534 "HTMLCharacterReference.gperf"
      {"Succeeds;", "≻"},
#line 1532 "HTMLCharacterReference.gperf"
      {"nldr;", "‥"},
      {""}, {""}, {""}, {""},
#line 257 "HTMLCharacterReference.gperf"
      {"KHcy;", "Х"},
#line 599 "HTMLCharacterReference.gperf"
      {"UpperRightArrow;", "↗"},
      {""}, {""}, {""}, {""},
#line 1213 "HTMLCharacterReference.gperf"
      {"iiint;", "∭"},
      {""},
#line 422 "HTMLCharacterReference.gperf"
      {"Or;", "⩔"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 210 "HTMLCharacterReference.gperf"
      {"Hacek;", "ˇ"},
      {""}, {""},
#line 251 "HTMLCharacterReference.gperf"
      {"Jcy;", "Й"},
#line 1083 "HTMLCharacterReference.gperf"
      {"fnof;", "ƒ"},
      {""},
#line 191 "HTMLCharacterReference.gperf"
      {"Gammad;", "Ϝ"},
      {""}, {""},
#line 2065 "HTMLCharacterReference.gperf"
      {"toea;", "⤨"},
      {""}, {""}, {""},
#line 1961 "HTMLCharacterReference.gperf"
      {"squ;", "□"},
#line 27 "HTMLCharacterReference.gperf"
      {"Acirc", "Â"},
#line 28 "HTMLCharacterReference.gperf"
      {"Acirc;", "Â"},
#line 2220 "HTMLCharacterReference.gperf"
      {"xuplus;", "⨄"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 94 "HTMLCharacterReference.gperf"
      {"CupCap;", "≍"},
      {""}, {""}, {""}, {""},
#line 2046 "HTMLCharacterReference.gperf"
      {"there4;", "∴"},
      {""}, {""},
#line 1344 "HTMLCharacterReference.gperf"
      {"lesseqgtr;", "⋚"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 439 "HTMLCharacterReference.gperf"
      {"Pi;", "Π"},
      {""}, {""}, {""}, {""}, {""},
#line 1422 "HTMLCharacterReference.gperf"
      {"lurdshar;", "⥊"},
#line 1148 "HTMLCharacterReference.gperf"
      {"gopf;", "𝕘"},
      {""}, {""}, {""},
#line 512 "HTMLCharacterReference.gperf"
      {"Scy;", "С"},
#line 1383 "HTMLCharacterReference.gperf"
      {"lopf;", "𝕝"},
      {""},
#line 975 "HTMLCharacterReference.gperf"
      {"dlcrop;", "⌍"},
      {""}, {""},
#line 1039 "HTMLCharacterReference.gperf"
      {"eopf;", "𝕖"},
      {""}, {""}, {""}, {""},
#line 897 "HTMLCharacterReference.gperf"
      {"copf;", "𝕔"},
      {""}, {""}, {""}, {""},
#line 702 "HTMLCharacterReference.gperf"
      {"aopf;", "𝕒"},
#line 1911 "HTMLCharacterReference.gperf"
      {"sharp;", "♯"},
#line 759 "HTMLCharacterReference.gperf"
      {"bkarow;", "⤍"},
      {""}, {""},
#line 1258 "HTMLCharacterReference.gperf"
      {"jopf;", "𝕛"},
      {""}, {""},
#line 361 "HTMLCharacterReference.gperf"
      {"NotGreaterGreater;", "≫̸"},
#line 1909 "HTMLCharacterReference.gperf"
      {"sfr;", "𝔰"},
#line 588 "HTMLCharacterReference.gperf"
      {"Uopf;", "𝕌"},
      {""},
#line 974 "HTMLCharacterReference.gperf"
      {"dlcorn;", "⌞"},
#line 113 "HTMLCharacterReference.gperf"
      {"Diamond;", "⋄"},
      {""}, {""},
#line 271 "HTMLCharacterReference.gperf"
      {"Laplacetrf;", "ℒ"},
      {""}, {""},
#line 429 "HTMLCharacterReference.gperf"
      {"Ouml", "Ö"},
#line 430 "HTMLCharacterReference.gperf"
      {"Ouml;", "Ö"},
      {""},
#line 2214 "HTMLCharacterReference.gperf"
      {"xoplus;", "⨁"},
      {""}, {""}, {""},
#line 1485 "HTMLCharacterReference.gperf"
      {"natur;", "♮"},
#line 543 "HTMLCharacterReference.gperf"
      {"Supset;", "⋑"},
      {""},
#line 228 "HTMLCharacterReference.gperf"
      {"Icy;", "И"},
#line 977 "HTMLCharacterReference.gperf"
      {"dopf;", "𝕕"},
      {""},
#line 2219 "HTMLCharacterReference.gperf"
      {"xsqcup;", "⨆"},
      {""},
#line 1055 "HTMLCharacterReference.gperf"
      {"eqvparsl;", "⧥"},
#line 1191 "HTMLCharacterReference.gperf"
      {"hopf;", "𝕙"},
      {""}, {""}, {""}, {""}, {""},
#line 298 "HTMLCharacterReference.gperf"
      {"LeftVector;", "↼"},
#line 1967 "HTMLCharacterReference.gperf"
      {"ssetmn;", "∖"},
#line 1632 "HTMLCharacterReference.gperf"
      {"nwarrow;", "↖"},
      {""}, {""},
#line 359 "HTMLCharacterReference.gperf"
      {"NotGreaterEqual;", "≱"},
      {""}, {""},
#line 1498 "HTMLCharacterReference.gperf"
      {"ncy;", "н"},
      {""},
#line 2020 "HTMLCharacterReference.gperf"
      {"supne;", "⊋"},
      {""}, {""}, {""}, {""},
#line 2191 "HTMLCharacterReference.gperf"
      {"wedge;", "∧"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1212 "HTMLCharacterReference.gperf"
      {"iiiint;", "⨌"},
      {""}, {""},
#line 457 "HTMLCharacterReference.gperf"
      {"Qopf;", "ℚ"},
      {""}, {""}, {""}, {""}, {""},
#line 1974 "HTMLCharacterReference.gperf"
      {"strns;", "¯"},
#line 2072 "HTMLCharacterReference.gperf"
      {"tprime;", "‴"},
      {""}, {""},
#line 1084 "HTMLCharacterReference.gperf"
      {"fopf;", "𝕗"},
      {""}, {""}, {""},
#line 647 "HTMLCharacterReference.gperf"
      {"Zcy;", "З"},
      {""}, {""}, {""}, {""}, {""},
#line 2195 "HTMLCharacterReference.gperf"
      {"wopf;", "𝕨"},
      {""}, {""},
#line 895 "HTMLCharacterReference.gperf"
      {"congdot;", "⩭"},
      {""}, {""},
#line 624 "HTMLCharacterReference.gperf"
      {"Wcirc;", "Ŵ"},
      {""}, {""}, {""},
#line 2180 "HTMLCharacterReference.gperf"
      {"vopf;", "𝕧"},
#line 784 "HTMLCharacterReference.gperf"
      {"boxHU;", "╩"},
#line 1575 "HTMLCharacterReference.gperf"
      {"nrtrie;", "⋭"},
#line 749 "HTMLCharacterReference.gperf"
      {"bigodot;", "⨀"},
      {""}, {""},
#line 185 "HTMLCharacterReference.gperf"
      {"Fouriertrf;", "ℱ"},
      {""}, {""}, {""}, {""},
#line 1215 "HTMLCharacterReference.gperf"
      {"iiota;", "℩"},
      {""}, {""},
#line 1280 "HTMLCharacterReference.gperf"
      {"laemptyv;", "⦴"},
#line 1283 "HTMLCharacterReference.gperf"
      {"lang;", "⟨"},
      {""},
#line 698 "HTMLCharacterReference.gperf"
      {"angsph;", "∢"},
      {""}, {""},
#line 133 "HTMLCharacterReference.gperf"
      {"DownArrow;", "↓"},
#line 969 "HTMLCharacterReference.gperf"
      {"divide", "÷"},
#line 970 "HTMLCharacterReference.gperf"
      {"divide;", "÷"},
      {""},
#line 724 "HTMLCharacterReference.gperf"
      {"backcong;", "≌"},
#line 774 "HTMLCharacterReference.gperf"
      {"bopf;", "𝕓"},
#line 450 "HTMLCharacterReference.gperf"
      {"Proportion;", "∷"},
      {""}, {""},
#line 1088 "HTMLCharacterReference.gperf"
      {"fpartint;", "⨍"},
#line 1538 "HTMLCharacterReference.gperf"
      {"nleqslant;", "⩽̸"},
      {""},
#line 1292 "HTMLCharacterReference.gperf"
      {"larrfs;", "⤝"},
#line 134 "HTMLCharacterReference.gperf"
      {"DownArrowBar;", "⤓"},
      {""},
#line 1035 "HTMLCharacterReference.gperf"
      {"emsp;", " "},
      {""}, {""},
#line 676 "HTMLCharacterReference.gperf"
      {"amp", "&"},
      {""}, {""}, {""}, {""}, {""},
#line 677 "HTMLCharacterReference.gperf"
      {"amp;", "&"},
      {""}, {""}, {""}, {""}, {""},
#line 59 "HTMLCharacterReference.gperf"
      {"CHcy;", "Ч"},
      {""},
#line 1034 "HTMLCharacterReference.gperf"
      {"emsp14;", " "},
      {""}, {""},
#line 2047 "HTMLCharacterReference.gperf"
      {"therefore;", "∴"},
      {""},
#line 2192 "HTMLCharacterReference.gperf"
      {"wedgeq;", "≙"},
#line 1552 "HTMLCharacterReference.gperf"
      {"notinva;", "∉"},
      {""},
#line 1067 "HTMLCharacterReference.gperf"
      {"excl;", "!"},
      {""}, {""}, {""}, {""},
#line 1908 "HTMLCharacterReference.gperf"
      {"sext;", "✶"},
      {""},
#line 504 "HTMLCharacterReference.gperf"
      {"SHCHcy;", "Щ"},
      {""},
#line 468 "HTMLCharacterReference.gperf"
      {"Rcy;", "Р"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 2019 "HTMLCharacterReference.gperf"
      {"supnE;", "⫌"},
      {""}, {""},
#line 157 "HTMLCharacterReference.gperf"
      {"Ecy;", "Э"},
#line 215 "HTMLCharacterReference.gperf"
      {"Hopf;", "ℍ"},
      {""}, {""}, {""}, {""},
#line 183 "HTMLCharacterReference.gperf"
      {"Fopf;", "𝔽"},
      {""}, {""}, {""}, {""},
#line 1904 "HTMLCharacterReference.gperf"
      {"semi;", ";"},
      {""},
#line 1033 "HTMLCharacterReference.gperf"
      {"emsp13;", " "},
      {""}, {""},
#line 1237 "HTMLCharacterReference.gperf"
      {"iopf;", "𝕚"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 195 "HTMLCharacterReference.gperf"
      {"Gcy;", "Г"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1271 "HTMLCharacterReference.gperf"
      {"kscr;", "𝓀"},
#line 811 "HTMLCharacterReference.gperf"
      {"boxuL;", "╛"},
      {""},
#line 1861 "HTMLCharacterReference.gperf"
      {"rpar;", ")"},
#line 1360 "HTMLCharacterReference.gperf"
      {"llcorner;", "⌞"},
#line 641 "HTMLCharacterReference.gperf"
      {"Yopf;", "𝕐"},
      {""},
#line 1188 "HTMLCharacterReference.gperf"
      {"homtht;", "∻"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 846 "HTMLCharacterReference.gperf"
      {"caps;", "∩︀"},
#line 2156 "HTMLCharacterReference.gperf"
      {"varpi;", "ϖ"},
#line 845 "HTMLCharacterReference.gperf"
      {"capdot;", "⩀"},
      {""}, {""}, {""}, {""},
#line 613 "HTMLCharacterReference.gperf"
      {"Verbar;", "‖"},
      {""},
#line 413 "HTMLCharacterReference.gperf"
      {"Ofr;", "𝔒"},
      {""}, {""}, {""}, {""},
#line 681 "HTMLCharacterReference.gperf"
      {"andslope;", "⩘"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1569 "HTMLCharacterReference.gperf"
      {"nrArr;", "⇏"},
      {""}, {""}, {""}, {""},
#line 1900 "HTMLCharacterReference.gperf"
      {"searr;", "↘"},
#line 1408 "HTMLCharacterReference.gperf"
      {"lstrok;", "ł"},
#line 2021 "HTMLCharacterReference.gperf"
      {"supplus;", "⫀"},
      {""},
#line 318 "HTMLCharacterReference.gperf"
      {"Lopf;", "𝕃"},
#line 1907 "HTMLCharacterReference.gperf"
      {"setmn;", "∖"},
      {""}, {""}, {""}, {""},
#line 1629 "HTMLCharacterReference.gperf"
      {"nwArr;", "⇖"},
      {""},
#line 1873 "HTMLCharacterReference.gperf"
      {"rtri;", "▹"},
#line 1864 "HTMLCharacterReference.gperf"
      {"rrarr;", "⇉"},
#line 48 "HTMLCharacterReference.gperf"
      {"Barv;", "⫧"},
#line 885 "HTMLCharacterReference.gperf"
      {"colon;", ":"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 549 "HTMLCharacterReference.gperf"
      {"Tab;", "\t"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1299 "HTMLCharacterReference.gperf"
      {"latail;", "⤙"},
      {""},
#line 1805 "HTMLCharacterReference.gperf"
      {"rbarr;", "⤍"},
#line 1766 "HTMLCharacterReference.gperf"
      {"qopf;", "𝕢"},
      {""}, {""},
#line 162 "HTMLCharacterReference.gperf"
      {"Element;", "∈"},
      {""}, {""}, {""},
#line 1969 "HTMLCharacterReference.gperf"
      {"sstarf;", "⋆"},
      {""}, {""}, {""}, {""},
#line 995 "HTMLCharacterReference.gperf"
      {"dstrok;", "đ"},
      {""}, {""}, {""}, {""},
#line 1195 "HTMLCharacterReference.gperf"
      {"hstrok;", "ħ"},
      {""}, {""},
#line 833 "HTMLCharacterReference.gperf"
      {"bull;", "•"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 331 "HTMLCharacterReference.gperf"
      {"Mopf;", "𝕄"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1349 "HTMLCharacterReference.gperf"
      {"lfloor;", "⌊"},
      {""},
#line 338 "HTMLCharacterReference.gperf"
      {"Ncy;", "Н"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 2067 "HTMLCharacterReference.gperf"
      {"topbot;", "⌶"},
      {""}, {""}, {""},
#line 1029 "HTMLCharacterReference.gperf"
      {"emacr;", "ē"},
#line 1050 "HTMLCharacterReference.gperf"
      {"eqslantless;", "⪕"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 290 "HTMLCharacterReference.gperf"
      {"LeftTeeVector;", "⥚"},
#line 803 "HTMLCharacterReference.gperf"
      {"boxh;", "─"},
#line 674 "HTMLCharacterReference.gperf"
      {"amacr;", "ā"},
#line 1835 "HTMLCharacterReference.gperf"
      {"rho;", "ρ"},
      {""}, {""}, {""}, {""}, {""},
#line 1070 "HTMLCharacterReference.gperf"
      {"exponentiale;", "ⅇ"},
      {""}, {""},
#line 580 "HTMLCharacterReference.gperf"
      {"Umacr;", "Ū"},
      {""}, {""},
#line 2153 "HTMLCharacterReference.gperf"
      {"varkappa;", "ϰ"},
      {""}, {""},
#line 1416 "HTMLCharacterReference.gperf"
      {"ltlarr;", "⥶"},
      {""}, {""}, {""}, {""},
#line 1197 "HTMLCharacterReference.gperf"
      {"hyphen;", "‐"},
      {""}, {""}, {""},
#line 2227 "HTMLCharacterReference.gperf"
      {"ycirc;", "ŷ"},
#line 1282 "HTMLCharacterReference.gperf"
      {"lambda;", "λ"},
      {""}, {""}, {""}, {""},
#line 940 "HTMLCharacterReference.gperf"
      {"dagger;", "†"},
      {""}, {""},
#line 270 "HTMLCharacterReference.gperf"
      {"Lang;", "⟪"},
#line 459 "HTMLCharacterReference.gperf"
      {"RBarr;", "⤐"},
      {""}, {""},
#line 832 "HTMLCharacterReference.gperf"
      {"bsolhsub;", "⟈"},
#line 223 "HTMLCharacterReference.gperf"
      {"IOcy;", "Ё"},
#line 2216 "HTMLCharacterReference.gperf"
      {"xrArr;", "⟹"},
#line 746 "HTMLCharacterReference.gperf"
      {"bigcap;", "⋂"},
      {""},
#line 29 "HTMLCharacterReference.gperf"
      {"Acy;", "А"},
#line 115 "HTMLCharacterReference.gperf"
      {"Dopf;", "𝔻"},
      {""}, {""}, {""},
#line 1855 "HTMLCharacterReference.gperf"
      {"roarr;", "⇾"},
      {""}, {""}, {""}, {""},
#line 1318 "HTMLCharacterReference.gperf"
      {"ldrushar;", "⥋"},
#line 89 "HTMLCharacterReference.gperf"
      {"Coproduct;", "∐"},
#line 1345 "HTMLCharacterReference.gperf"
      {"lesseqqgtr;", "⪋"},
#line 1963 "HTMLCharacterReference.gperf"
      {"squarf;", "▪"},
      {""}, {""}, {""},
#line 557 "HTMLCharacterReference.gperf"
      {"ThickSpace;", "  "},
#line 1247 "HTMLCharacterReference.gperf"
      {"isinsv;", "⋳"},
      {""},
#line 447 "HTMLCharacterReference.gperf"
      {"PrecedesTilde;", "≾"},
      {""}, {""},
#line 972 "HTMLCharacterReference.gperf"
      {"divonx;", "⋇"},
      {""},
#line 2129 "HTMLCharacterReference.gperf"
      {"upsi;", "υ"},
#line 2043 "HTMLCharacterReference.gperf"
      {"tdot;", "⃛"},
      {""}, {""}, {""}, {""}, {""},
#line 1521 "HTMLCharacterReference.gperf"
      {"nhArr;", "⇎"},
#line 844 "HTMLCharacterReference.gperf"
      {"capcup;", "⩇"},
      {""}, {""},
#line 1169 "HTMLCharacterReference.gperf"
      {"gvnE;", "≩︀"},
      {""},
#line 914 "HTMLCharacterReference.gperf"
      {"cularr;", "↶"},
      {""}, {""},
#line 1425 "HTMLCharacterReference.gperf"
      {"lvnE;", "≨︀"},
      {""},
#line 378 "HTMLCharacterReference.gperf"
      {"NotPrecedes;", "⊀"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1232 "HTMLCharacterReference.gperf"
      {"intercal;", "⊺"},
#line 253 "HTMLCharacterReference.gperf"
      {"Jopf;", "𝕁"},
      {""}, {""}, {""},
#line 1427 "HTMLCharacterReference.gperf"
      {"macr", "¯"},
#line 1428 "HTMLCharacterReference.gperf"
      {"macr;", "¯"},
      {""},
#line 218 "HTMLCharacterReference.gperf"
      {"Hstrok;", "Ħ"},
      {""},
#line 1874 "HTMLCharacterReference.gperf"
      {"rtrie;", "⊵"},
#line 1517 "HTMLCharacterReference.gperf"
      {"nges;", "⩾̸"},
      {""}, {""},
#line 460 "HTMLCharacterReference.gperf"
      {"REG", "®"},
      {""}, {""}, {""}, {""}, {""},
#line 461 "HTMLCharacterReference.gperf"
      {"REG;", "®"},
#line 631 "HTMLCharacterReference.gperf"
      {"Xopf;", "𝕏"},
      {""}, {""}, {""},
#line 1671 "HTMLCharacterReference.gperf"
      {"or;", "∨"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1809 "HTMLCharacterReference.gperf"
      {"rbrke;", "⦌"},
#line 1190 "HTMLCharacterReference.gperf"
      {"hookrightarrow;", "↪"},
      {""},
#line 2186 "HTMLCharacterReference.gperf"
      {"vsupnE;", "⫌︀"},
      {""}, {""}, {""},
#line 934 "HTMLCharacterReference.gperf"
      {"cuwed;", "⋏"},
      {""},
#line 1343 "HTMLCharacterReference.gperf"
      {"lessdot;", "⋖"},
#line 445 "HTMLCharacterReference.gperf"
      {"PrecedesEqual;", "⪯"},
      {""}, {""},
#line 2151 "HTMLCharacterReference.gperf"
      {"vangrt;", "⦜"},
      {""}, {""}, {""}, {""},
#line 1831 "HTMLCharacterReference.gperf"
      {"rfr;", "𝔯"},
      {""}, {""},
#line 1683 "HTMLCharacterReference.gperf"
      {"orv;", "⩛"},
      {""},
#line 757 "HTMLCharacterReference.gperf"
      {"bigvee;", "⋁"},
#line 1504 "HTMLCharacterReference.gperf"
      {"nearrow;", "↗"},
#line 2141 "HTMLCharacterReference.gperf"
      {"utri;", "▵"},
      {""}, {""}, {""},
#line 2144 "HTMLCharacterReference.gperf"
      {"uuml", "ü"},
#line 2145 "HTMLCharacterReference.gperf"
      {"uuml;", "ü"},
#line 520 "HTMLCharacterReference.gperf"
      {"Sopf;", "𝕊"},
#line 1681 "HTMLCharacterReference.gperf"
      {"oror;", "⩖"},
#line 1565 "HTMLCharacterReference.gperf"
      {"nprcue;", "⋠"},
      {""}, {""}, {""}, {""}, {""},
#line 1779 "HTMLCharacterReference.gperf"
      {"rHar;", "⥤"},
      {""}, {""}, {""},
#line 873 "HTMLCharacterReference.gperf"
      {"circlearrowright;", "↻"},
      {""}, {""},
#line 2088 "HTMLCharacterReference.gperf"
      {"tscr;", "𝓉"},
      {""}, {""}, {""},
#line 875 "HTMLCharacterReference.gperf"
      {"circledS;", "Ⓢ"},
#line 1390 "HTMLCharacterReference.gperf"
      {"lozf;", "⧫"},
#line 1668 "HTMLCharacterReference.gperf"
      {"opar;", "⦷"},
      {""}, {""}, {""}, {""}, {""},
#line 323 "HTMLCharacterReference.gperf"
      {"Lstrok;", "Ł"},
#line 1772 "HTMLCharacterReference.gperf"
      {"questeq;", "≟"},
      {""}, {""}, {""}, {""}, {""},
#line 1025 "HTMLCharacterReference.gperf"
      {"elinters;", "⏧"},
#line 2211 "HTMLCharacterReference.gperf"
      {"xnis;", "⋻"},
#line 1378 "HTMLCharacterReference.gperf"
      {"longmapsto;", "⟼"},
      {""}, {""}, {""},
#line 2143 "HTMLCharacterReference.gperf"
      {"uuarr;", "⇈"},
#line 1217 "HTMLCharacterReference.gperf"
      {"imacr;", "ī"},
      {""},
#line 296 "HTMLCharacterReference.gperf"
      {"LeftUpVector;", "↿"},
      {""},
#line 243 "HTMLCharacterReference.gperf"
      {"Iopf;", "𝕀"},
#line 259 "HTMLCharacterReference.gperf"
      {"Kappa;", "Κ"},
      {""}, {""},
#line 1121 "HTMLCharacterReference.gperf"
      {"gel;", "⋛"},
      {""}, {""}, {""}, {""}, {""},
#line 2013 "HTMLCharacterReference.gperf"
      {"supe;", "⊇"},
      {""}, {""}, {""}, {""}, {""},
#line 2205 "HTMLCharacterReference.gperf"
      {"xhArr;", "⟺"},
#line 1968 "HTMLCharacterReference.gperf"
      {"ssmile;", "⌣"},
      {""}, {""},
#line 1546 "HTMLCharacterReference.gperf"
      {"nopf;", "𝕟"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1672 "HTMLCharacterReference.gperf"
      {"orarr;", "↻"},
      {""}, {""},
#line 1692 "HTMLCharacterReference.gperf"
      {"ouml", "ö"},
#line 1693 "HTMLCharacterReference.gperf"
      {"ouml;", "ö"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1268 "HTMLCharacterReference.gperf"
      {"khcy;", "х"},
      {""}, {""}, {""}, {""}, {""},
#line 518 "HTMLCharacterReference.gperf"
      {"Sigma;", "Σ"},
      {""},
#line 2084 "HTMLCharacterReference.gperf"
      {"triplus;", "⨹"},
#line 1467 "HTMLCharacterReference.gperf"
      {"nGt;", "≫⃒"},
#line 2226 "HTMLCharacterReference.gperf"
      {"yacy;", "я"},
      {""},
#line 2053 "HTMLCharacterReference.gperf"
      {"thinsp;", " "},
      {""}, {""},
#line 652 "HTMLCharacterReference.gperf"
      {"Zopf;", "ℤ"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1069 "HTMLCharacterReference.gperf"
      {"expectation;", "ℰ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1687 "HTMLCharacterReference.gperf"
      {"osol;", "⊘"},
      {""}, {""}, {""},
#line 985 "HTMLCharacterReference.gperf"
      {"downarrow;", "↓"},
      {""}, {""}, {""},
#line 553 "HTMLCharacterReference.gperf"
      {"Tcy;", "Т"},
      {""},
#line 946 "HTMLCharacterReference.gperf"
      {"dblac;", "˝"},
#line 1108 "HTMLCharacterReference.gperf"
      {"frown;", "⌢"},
      {""}, {""}, {""}, {""}, {""},
#line 233 "HTMLCharacterReference.gperf"
      {"Im;", "ℑ"},
#line 1857 "HTMLCharacterReference.gperf"
      {"ropar;", "⦆"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 269 "HTMLCharacterReference.gperf"
      {"Lambda;", "Λ"},
      {""},
#line 436 "HTMLCharacterReference.gperf"
      {"Pcy;", "П"},
      {""},
#line 1342 "HTMLCharacterReference.gperf"
      {"lessapprox;", "⪅"},
      {""}, {""},
#line 1634 "HTMLCharacterReference.gperf"
      {"oS;", "Ⓢ"},
      {""}, {""}, {""}, {""},
#line 2240 "HTMLCharacterReference.gperf"
      {"zcy;", "з"},
#line 1655 "HTMLCharacterReference.gperf"
      {"ohm;", "Ω"},
#line 819 "HTMLCharacterReference.gperf"
      {"boxvh;", "┼"},
#line 148 "HTMLCharacterReference.gperf"
      {"Dstrok;", "Đ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2113 "HTMLCharacterReference.gperf"
      {"uharr;", "↾"},
      {""},
#line 2040 "HTMLCharacterReference.gperf"
      {"tcaron;", "ť"},
      {""},
#line 884 "HTMLCharacterReference.gperf"
      {"clubsuit;", "♣"},
      {""}, {""},
#line 1305 "HTMLCharacterReference.gperf"
      {"lbrack;", "["},
      {""}, {""},
#line 498 "HTMLCharacterReference.gperf"
      {"Ropf;", "ℝ"},
      {""},
#line 1457 "HTMLCharacterReference.gperf"
      {"mnplus;", "∓"},
      {""},
#line 2038 "HTMLCharacterReference.gperf"
      {"tau;", "τ"},
      {""}, {""},
#line 576 "HTMLCharacterReference.gperf"
      {"Udblac;", "Ű"},
      {""}, {""},
#line 1479 "HTMLCharacterReference.gperf"
      {"nang;", "∠⃒"},
      {""}, {""}, {""}, {""},
#line 167 "HTMLCharacterReference.gperf"
      {"Eopf;", "𝔼"},
#line 2203 "HTMLCharacterReference.gperf"
      {"xdtri;", "▽"},
      {""}, {""},
#line 60 "HTMLCharacterReference.gperf"
      {"COPY", "©"},
#line 61 "HTMLCharacterReference.gperf"
      {"COPY;", "©"},
      {""},
#line 834 "HTMLCharacterReference.gperf"
      {"bullet;", "•"},
      {""}, {""},
#line 2213 "HTMLCharacterReference.gperf"
      {"xopf;", "𝕩"},
      {""},
#line 1267 "HTMLCharacterReference.gperf"
      {"kgreen;", "ĸ"},
#line 1822 "HTMLCharacterReference.gperf"
      {"real;", "ℜ"},
      {""}, {""}, {""}, {""}, {""},
#line 1138 "HTMLCharacterReference.gperf"
      {"glE;", "⪒"},
      {""}, {""}, {""}, {""},
#line 1915 "HTMLCharacterReference.gperf"
      {"shortparallel;", "∥"},
#line 199 "HTMLCharacterReference.gperf"
      {"Gopf;", "𝔾"},
#line 1501 "HTMLCharacterReference.gperf"
      {"neArr;", "⇗"},
      {""},
#line 2109 "HTMLCharacterReference.gperf"
      {"ufr;", "𝔲"},
#line 1466 "HTMLCharacterReference.gperf"
      {"nGg;", "⋙̸"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1284 "HTMLCharacterReference.gperf"
      {"langd;", "⦑"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 743 "HTMLCharacterReference.gperf"
      {"beth;", "ℶ"},
      {""}, {""}, {""},
#line 2096 "HTMLCharacterReference.gperf"
      {"uHar;", "⥣"},
      {""}, {""},
#line 100 "HTMLCharacterReference.gperf"
      {"Dagger;", "‡"},
#line 915 "HTMLCharacterReference.gperf"
      {"cularrp;", "⤽"},
#line 609 "HTMLCharacterReference.gperf"
      {"Vcy;", "В"},
      {""}, {""}, {""}, {""}, {""},
#line 2122 "HTMLCharacterReference.gperf"
      {"uogon;", "ų"},
      {""}, {""}, {""}, {""},
#line 2112 "HTMLCharacterReference.gperf"
      {"uharl;", "↿"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1446 "HTMLCharacterReference.gperf"
      {"mid;", "∣"},
#line 1895 "HTMLCharacterReference.gperf"
      {"sdot;", "⋅"},
#line 69 "HTMLCharacterReference.gperf"
      {"Ccirc;", "Ĉ"},
      {""}, {""}, {""},
#line 544 "HTMLCharacterReference.gperf"
      {"THORN", "Þ"},
#line 545 "HTMLCharacterReference.gperf"
      {"THORN;", "Þ"},
#line 1438 "HTMLCharacterReference.gperf"
      {"mcomma;", "⨩"},
      {""}, {""},
#line 558 "HTMLCharacterReference.gperf"
      {"ThinSpace;", " "},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 571 "HTMLCharacterReference.gperf"
      {"Ubrcy;", "Ў"},
      {""}, {""},
#line 1649 "HTMLCharacterReference.gperf"
      {"ofr;", "𝔬"},
      {""},
#line 1550 "HTMLCharacterReference.gperf"
      {"notinE;", "⋹̸"},
      {""}, {""},
#line 2089 "HTMLCharacterReference.gperf"
      {"tscy;", "ц"},
      {""}, {""}, {""}, {""},
#line 463 "HTMLCharacterReference.gperf"
      {"Rang;", "⟫"},
      {""}, {""}, {""}, {""}, {""},
#line 1827 "HTMLCharacterReference.gperf"
      {"reg", "®"},
      {""}, {""}, {""},
#line 1456 "HTMLCharacterReference.gperf"
      {"mldr;", "…"},
      {""},
#line 1828 "HTMLCharacterReference.gperf"
      {"reg;", "®"},
      {""}, {""}, {""},
#line 87 "HTMLCharacterReference.gperf"
      {"ContourIntegral;", "∮"},
      {""}, {""}, {""}, {""},
#line 2048 "HTMLCharacterReference.gperf"
      {"theta;", "θ"},
      {""}, {""},
#line 1853 "HTMLCharacterReference.gperf"
      {"rnmid;", "⫮"},
      {""},
#line 799 "HTMLCharacterReference.gperf"
      {"boxdL;", "╕"},
#line 1669 "HTMLCharacterReference.gperf"
      {"operp;", "⦹"},
#line 696 "HTMLCharacterReference.gperf"
      {"angrtvb;", "⊾"},
#line 2228 "HTMLCharacterReference.gperf"
      {"ycy;", "ы"},
      {""},
#line 1477 "HTMLCharacterReference.gperf"
      {"nabla;", "∇"},
      {""}, {""}, {""}, {""}, {""},
#line 1962 "HTMLCharacterReference.gperf"
      {"square;", "□"},
      {""}, {""},
#line 349 "HTMLCharacterReference.gperf"
      {"Nopf;", "ℕ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1346 "HTMLCharacterReference.gperf"
      {"lessgtr;", "≶"},
      {""}, {""}, {""},
#line 1387 "HTMLCharacterReference.gperf"
      {"lowbar;", "_"},
#line 1389 "HTMLCharacterReference.gperf"
      {"lozenge;", "◊"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1624 "HTMLCharacterReference.gperf"
      {"nvlt;", "<⃒"},
      {""},
#line 1285 "HTMLCharacterReference.gperf"
      {"langle;", "⟨"},
      {""}, {""},
#line 2210 "HTMLCharacterReference.gperf"
      {"xmap;", "⟼"},
      {""}, {""},
#line 1868 "HTMLCharacterReference.gperf"
      {"rsqb;", "]"},
      {""},
#line 1966 "HTMLCharacterReference.gperf"
      {"sscr;", "𝓈"},
      {""},
#line 937 "HTMLCharacterReference.gperf"
      {"cylcty;", "⌭"},
      {""},
#line 1376 "HTMLCharacterReference.gperf"
      {"longleftarrow;", "⟵"},
      {""}, {""}, {""}, {""}, {""},
#line 2139 "HTMLCharacterReference.gperf"
      {"utdot;", "⋰"},
      {""},
#line 886 "HTMLCharacterReference.gperf"
      {"colone;", "≔"},
      {""}, {""},
#line 2082 "HTMLCharacterReference.gperf"
      {"trie;", "≜"},
      {""}, {""}, {""}, {""}, {""},
#line 234 "HTMLCharacterReference.gperf"
      {"Imacr;", "Ī"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 971 "HTMLCharacterReference.gperf"
      {"divideontimes;", "⋇"},
      {""},
#line 786 "HTMLCharacterReference.gperf"
      {"boxHu;", "╧"},
      {""},
#line 517 "HTMLCharacterReference.gperf"
      {"ShortUpArrow;", "↑"},
      {""},
#line 37 "HTMLCharacterReference.gperf"
      {"Aopf;", "𝔸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1825 "HTMLCharacterReference.gperf"
      {"reals;", "ℝ"},
      {""}, {""},
#line 1174 "HTMLCharacterReference.gperf"
      {"hardcy;", "ъ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 428 "HTMLCharacterReference.gperf"
      {"Otimes;", "⨷"},
#line 1836 "HTMLCharacterReference.gperf"
      {"rhov;", "ϱ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 795 "HTMLCharacterReference.gperf"
      {"boxVh;", "╫"},
      {""},
#line 291 "HTMLCharacterReference.gperf"
      {"LeftTriangle;", "⊲"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1673 "HTMLCharacterReference.gperf"
      {"ord;", "⩝"},
#line 675 "HTMLCharacterReference.gperf"
      {"amalg;", "⨿"},
      {""},
#line 1881 "HTMLCharacterReference.gperf"
      {"sc;", "≻"},
      {""},
#line 1678 "HTMLCharacterReference.gperf"
      {"ordm", "º"},
#line 1679 "HTMLCharacterReference.gperf"
      {"ordm;", "º"},
      {""}, {""}, {""}, {""},
#line 1897 "HTMLCharacterReference.gperf"
      {"sdote;", "⩦"},
      {""}, {""},
#line 105 "HTMLCharacterReference.gperf"
      {"Del;", "∇"},
      {""}, {""}, {""}, {""},
#line 1439 "HTMLCharacterReference.gperf"
      {"mcy;", "м"},
      {""},
#line 806 "HTMLCharacterReference.gperf"
      {"boxhd;", "┬"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 146 "HTMLCharacterReference.gperf"
      {"Downarrow;", "⇓"},
      {""}, {""}, {""}, {""}, {""},
#line 1893 "HTMLCharacterReference.gperf"
      {"scsim;", "≿"},
      {""}, {""},
#line 261 "HTMLCharacterReference.gperf"
      {"Kcy;", "К"},
#line 1993 "HTMLCharacterReference.gperf"
      {"succ;", "≻"},
      {""}, {""},
#line 293 "HTMLCharacterReference.gperf"
      {"LeftTriangleEqual;", "⊴"},
      {""}, {""},
#line 363 "HTMLCharacterReference.gperf"
      {"NotGreaterSlantEqual;", "⩾̸"},
      {""}, {""}, {""}, {""},
#line 1811 "HTMLCharacterReference.gperf"
      {"rbrkslu;", "⦐"},
#line 209 "HTMLCharacterReference.gperf"
      {"HARDcy;", "Ъ"},
      {""},
#line 1832 "HTMLCharacterReference.gperf"
      {"rhard;", "⇁"},
      {""}, {""},
#line 737 "HTMLCharacterReference.gperf"
      {"becaus;", "∵"},
      {""},
#line 1975 "HTMLCharacterReference.gperf"
      {"sub;", "⊂"},
      {""}, {""},
#line 2131 "HTMLCharacterReference.gperf"
      {"upsilon;", "υ"},
      {""}, {""}, {""},
#line 91 "HTMLCharacterReference.gperf"
      {"Cross;", "⨯"},
      {""}, {""},
#line 2249 "HTMLCharacterReference.gperf"
      {"zwj;", "‍"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1362 "HTMLCharacterReference.gperf"
      {"lltri;", "◺"},
      {""}, {""}, {""},
#line 360 "HTMLCharacterReference.gperf"
      {"NotGreaterFullEqual;", "≧̸"},
      {""},
#line 1884 "HTMLCharacterReference.gperf"
      {"scaron;", "š"},
      {""},
#line 874 "HTMLCharacterReference.gperf"
      {"circledR;", "®"},
#line 1883 "HTMLCharacterReference.gperf"
      {"scap;", "⪸"},
#line 625 "HTMLCharacterReference.gperf"
      {"Wedge;", "⋀"},
      {""}, {""},
#line 756 "HTMLCharacterReference.gperf"
      {"biguplus;", "⨄"},
      {""}, {""},
#line 2184 "HTMLCharacterReference.gperf"
      {"vsubnE;", "⫋︀"},
      {""},
#line 1682 "HTMLCharacterReference.gperf"
      {"orslope;", "⩗"},
      {""},
#line 163 "HTMLCharacterReference.gperf"
      {"Emacr;", "Ē"},
      {""}, {""},
#line 238 "HTMLCharacterReference.gperf"
      {"Integral;", "∫"},
#line 47 "HTMLCharacterReference.gperf"
      {"Backslash;", "∖"},
      {""}, {""}, {""}, {""},
#line 125 "HTMLCharacterReference.gperf"
      {"DoubleLongLeftArrow;", "⟸"},
      {""}, {""}, {""},
#line 1471 "HTMLCharacterReference.gperf"
      {"nLl;", "⋘̸"},
#line 126 "HTMLCharacterReference.gperf"
      {"DoubleLongLeftRightArrow;", "⟺"},
#line 122 "HTMLCharacterReference.gperf"
      {"DoubleLeftArrow;", "⇐"},
      {""}, {""},
#line 1124 "HTMLCharacterReference.gperf"
      {"geqslant;", "⩾"},
      {""}, {""},
#line 1112 "HTMLCharacterReference.gperf"
      {"gacute;", "ǵ"},
      {""},
#line 1334 "HTMLCharacterReference.gperf"
      {"leqslant;", "⩽"},
#line 627 "HTMLCharacterReference.gperf"
      {"Wopf;", "𝕎"},
      {""},
#line 1279 "HTMLCharacterReference.gperf"
      {"lacute;", "ĺ"},
      {""}, {""}, {""},
#line 1006 "HTMLCharacterReference.gperf"
      {"eacute", "é"},
#line 1007 "HTMLCharacterReference.gperf"
      {"eacute;", "é"},
      {""}, {""}, {""}, {""},
#line 839 "HTMLCharacterReference.gperf"
      {"cacute;", "ć"},
      {""}, {""}, {""},
#line 654 "HTMLCharacterReference.gperf"
      {"aacute", "á"},
#line 655 "HTMLCharacterReference.gperf"
      {"aacute;", "á"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 567 "HTMLCharacterReference.gperf"
      {"Uacute", "Ú"},
#line 568 "HTMLCharacterReference.gperf"
      {"Uacute;", "Ú"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 423 "HTMLCharacterReference.gperf"
      {"Oscr;", "𝒪"},
      {""}, {""}, {""},
#line 935 "HTMLCharacterReference.gperf"
      {"cwconint;", "∲"},
      {""}, {""}, {""}, {""},
#line 721 "HTMLCharacterReference.gperf"
      {"awconint;", "∳"},
#line 344 "HTMLCharacterReference.gperf"
      {"NestedLessLess;", "≪"},
#line 768 "HTMLCharacterReference.gperf"
      {"blk14;", "░"},
      {""}, {""},
#line 397 "HTMLCharacterReference.gperf"
      {"NotTilde;", "≁"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 769 "HTMLCharacterReference.gperf"
      {"blk34;", "▓"},
      {""}, {""}, {""},
#line 1653 "HTMLCharacterReference.gperf"
      {"ogt;", "⧁"},
      {""}, {""}, {""}, {""}, {""},
#line 2177 "HTMLCharacterReference.gperf"
      {"vltri;", "⊲"},
      {""}, {""}, {""},
#line 563 "HTMLCharacterReference.gperf"
      {"Topf;", "𝕋"},
#line 1949 "HTMLCharacterReference.gperf"
      {"sqcap;", "⊓"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1735 "HTMLCharacterReference.gperf"
      {"pr;", "≺"},
      {""}, {""}, {""}, {""},
#line 315 "HTMLCharacterReference.gperf"
      {"Longleftarrow;", "⟸"},
      {""},
#line 767 "HTMLCharacterReference.gperf"
      {"blk12;", "▒"},
#line 1028 "HTMLCharacterReference.gperf"
      {"elsdot;", "⪗"},
      {""}, {""},
#line 1319 "HTMLCharacterReference.gperf"
      {"ldsh;", "↲"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 887 "HTMLCharacterReference.gperf"
      {"coloneq;", "≔"},
      {""},
#line 442 "HTMLCharacterReference.gperf"
      {"Popf;", "ℙ"},
      {""}, {""}, {""}, {""},
#line 1468 "HTMLCharacterReference.gperf"
      {"nGtv;", "≫̸"},
      {""}, {""},
#line 910 "HTMLCharacterReference.gperf"
      {"cudarrl;", "⤸"},
      {""},
#line 2247 "HTMLCharacterReference.gperf"
      {"zopf;", "𝕫"},
      {""},
#line 2223 "HTMLCharacterReference.gperf"
      {"xwedge;", "⋀"},
      {""}, {""},
#line 1872 "HTMLCharacterReference.gperf"
      {"rtimes;", "⋊"},
#line 1650 "HTMLCharacterReference.gperf"
      {"ogon;", "˛"},
#line 1759 "HTMLCharacterReference.gperf"
      {"prsim;", "≾"},
      {""}, {""},
#line 1269 "HTMLCharacterReference.gperf"
      {"kjcy;", "ќ"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1950 "HTMLCharacterReference.gperf"
      {"sqcaps;", "⊓︀"},
      {""},
#line 1886 "HTMLCharacterReference.gperf"
      {"sce;", "⪰"},
#line 85 "HTMLCharacterReference.gperf"
      {"Congruent;", "≡"},
#line 1426 "HTMLCharacterReference.gperf"
      {"mDDot;", "∺"},
      {""},
#line 760 "HTMLCharacterReference.gperf"
      {"blacklozenge;", "⧫"},
#line 1854 "HTMLCharacterReference.gperf"
      {"roang;", "⟭"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1075 "HTMLCharacterReference.gperf"
      {"fflig;", "ﬀ"},
      {""},
#line 1554 "HTMLCharacterReference.gperf"
      {"notinvc;", "⋶"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1862 "HTMLCharacterReference.gperf"
      {"rpargt;", "⦔"},
#line 1757 "HTMLCharacterReference.gperf"
      {"prop;", "∝"},
      {""}, {""},
#line 50 "HTMLCharacterReference.gperf"
      {"Bcy;", "Б"},
      {""},
#line 1880 "HTMLCharacterReference.gperf"
      {"sbquo;", "‚"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1737 "HTMLCharacterReference.gperf"
      {"prap;", "⪷"},
      {""}, {""}, {""},
#line 633 "HTMLCharacterReference.gperf"
      {"YAcy;", "Я"},
      {""},
#line 2159 "HTMLCharacterReference.gperf"
      {"varrho;", "ϱ"},
      {""},
#line 1902 "HTMLCharacterReference.gperf"
      {"sect", "§"},
#line 1903 "HTMLCharacterReference.gperf"
      {"sect;", "§"},
      {""}, {""},
#line 144 "HTMLCharacterReference.gperf"
      {"DownTee;", "⊤"},
      {""},
#line 1913 "HTMLCharacterReference.gperf"
      {"shcy;", "ш"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1627 "HTMLCharacterReference.gperf"
      {"nvrtrie;", "⊵⃒"},
#line 537 "HTMLCharacterReference.gperf"
      {"SucceedsTilde;", "≿"},
      {""},
#line 1896 "HTMLCharacterReference.gperf"
      {"sdotb;", "⊡"},
#line 1941 "HTMLCharacterReference.gperf"
      {"softcy;", "ь"},
      {""}, {""}, {""},
#line 34 "HTMLCharacterReference.gperf"
      {"Amacr;", "Ā"},
#line 532 "HTMLCharacterReference.gperf"
      {"Subset;", "⋐"},
      {""}, {""},
#line 621 "HTMLCharacterReference.gperf"
      {"Vopf;", "𝕍"},
#line 1860 "HTMLCharacterReference.gperf"
      {"rotimes;", "⨵"},
      {""}, {""}, {""}, {""},
#line 1810 "HTMLCharacterReference.gperf"
      {"rbrksld;", "⦎"},
#line 1996 "HTMLCharacterReference.gperf"
      {"succeq;", "⪰"},
#line 2017 "HTMLCharacterReference.gperf"
      {"suplarr;", "⥻"},
#line 441 "HTMLCharacterReference.gperf"
      {"Poincareplane;", "ℌ"},
#line 943 "HTMLCharacterReference.gperf"
      {"dash;", "‐"},
#line 1198 "HTMLCharacterReference.gperf"
      {"iacute", "í"},
#line 1199 "HTMLCharacterReference.gperf"
      {"iacute;", "í"},
      {""}, {""}, {""},
#line 944 "HTMLCharacterReference.gperf"
      {"dashv;", "⊣"},
      {""}, {""},
#line 733 "HTMLCharacterReference.gperf"
      {"bbrktbrk;", "⎶"},
      {""},
#line 1982 "HTMLCharacterReference.gperf"
      {"subne;", "⊊"},
#line 881 "HTMLCharacterReference.gperf"
      {"cirmid;", "⫯"},
      {""}, {""}, {""}, {""}, {""},
#line 1758 "HTMLCharacterReference.gperf"
      {"propto;", "∝"},
      {""}, {""}, {""}, {""}, {""},
#line 1530 "HTMLCharacterReference.gperf"
      {"nlE;", "≦̸"},
      {""},
#line 932 "HTMLCharacterReference.gperf"
      {"curvearrowright;", "↷"},
      {""}, {""}, {""}, {""},
#line 636 "HTMLCharacterReference.gperf"
      {"Yacute", "Ý"},
#line 637 "HTMLCharacterReference.gperf"
      {"Yacute;", "Ý"},
      {""},
#line 1607 "HTMLCharacterReference.gperf"
      {"ntriangleleft;", "⋪"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1957 "HTMLCharacterReference.gperf"
      {"sqsup;", "⊐"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 535 "HTMLCharacterReference.gperf"
      {"SucceedsEqual;", "⪰"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 268 "HTMLCharacterReference.gperf"
      {"Lacute;", "Ĺ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2233 "HTMLCharacterReference.gperf"
      {"yopf;", "𝕪"},
      {""},
#line 1159 "HTMLCharacterReference.gperf"
      {"gtlPar;", "⦕"},
#line 1916 "HTMLCharacterReference.gperf"
      {"shy", "­"},
      {""}, {""},
#line 1262 "HTMLCharacterReference.gperf"
      {"kappa;", "κ"},
      {""},
#line 1866 "HTMLCharacterReference.gperf"
      {"rscr;", "𝓇"},
#line 1917 "HTMLCharacterReference.gperf"
      {"shy;", "­"},
#line 1326 "HTMLCharacterReference.gperf"
      {"leftrightarrow;", "↔"},
#line 1327 "HTMLCharacterReference.gperf"
      {"leftrightarrows;", "⇆"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 665 "HTMLCharacterReference.gperf"
      {"aelig", "æ"},
#line 666 "HTMLCharacterReference.gperf"
      {"aelig;", "æ"},
      {""}, {""}, {""},
#line 1807 "HTMLCharacterReference.gperf"
      {"rbrace;", "}"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1167 "HTMLCharacterReference.gperf"
      {"gtrsim;", "≳"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 976 "HTMLCharacterReference.gperf"
      {"dollar;", "$"},
      {""},
#line 1803 "HTMLCharacterReference.gperf"
      {"ratio;", "∶"},
#line 1739 "HTMLCharacterReference.gperf"
      {"pre;", "⪯"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 472 "HTMLCharacterReference.gperf"
      {"ReverseUpEquilibrium;", "⥯"},
      {""},
#line 1553 "HTMLCharacterReference.gperf"
      {"notinvb;", "⋷"},
      {""},
#line 221 "HTMLCharacterReference.gperf"
      {"IEcy;", "Е"},
#line 1981 "HTMLCharacterReference.gperf"
      {"subnE;", "⫋"},
#line 566 "HTMLCharacterReference.gperf"
      {"Tstrok;", "Ŧ"},
      {""}, {""},
#line 1444 "HTMLCharacterReference.gperf"
      {"micro", "µ"},
#line 1445 "HTMLCharacterReference.gperf"
      {"micro;", "µ"},
      {""}, {""}, {""},
#line 1609 "HTMLCharacterReference.gperf"
      {"ntriangleright;", "⋫"},
      {""},
#line 1610 "HTMLCharacterReference.gperf"
      {"ntrianglerighteq;", "⋭"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1890 "HTMLCharacterReference.gperf"
      {"scnap;", "⪺"},
      {""},
#line 1790 "HTMLCharacterReference.gperf"
      {"rarr;", "→"},
      {""}, {""}, {""},
#line 1447 "HTMLCharacterReference.gperf"
      {"midast;", "*"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1455 "HTMLCharacterReference.gperf"
      {"mlcp;", "⫛"},
      {""},
#line 1194 "HTMLCharacterReference.gperf"
      {"hslash;", "ℏ"},
      {""}, {""}, {""},
#line 367 "HTMLCharacterReference.gperf"
      {"NotLeftTriangle;", "⋪"},
      {""}, {""},
#line 368 "HTMLCharacterReference.gperf"
      {"NotLeftTriangleBar;", "⧏̸"},
      {""},
#line 369 "HTMLCharacterReference.gperf"
      {"NotLeftTriangleEqual;", "⋬"},
#line 1361 "HTMLCharacterReference.gperf"
      {"llhard;", "⥫"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 299 "HTMLCharacterReference.gperf"
      {"LeftVectorBar;", "⥒"},
      {""}, {""}, {""}, {""}, {""},
#line 1800 "HTMLCharacterReference.gperf"
      {"rarrtl;", "↣"},
#line 414 "HTMLCharacterReference.gperf"
      {"Ograve", "Ò"},
#line 415 "HTMLCharacterReference.gperf"
      {"Ograve;", "Ò"},
#line 1328 "HTMLCharacterReference.gperf"
      {"leftrightharpoons;", "⇋"},
      {""},
#line 1708 "HTMLCharacterReference.gperf"
      {"pfr;", "𝔭"},
#line 876 "HTMLCharacterReference.gperf"
      {"circledast;", "⊛"},
#line 1615 "HTMLCharacterReference.gperf"
      {"nvDash;", "⊭"},
      {""}, {""},
#line 1459 "HTMLCharacterReference.gperf"
      {"mopf;", "𝕞"},
      {""},
#line 327 "HTMLCharacterReference.gperf"
      {"MediumSpace;", " "},
#line 1690 "HTMLCharacterReference.gperf"
      {"otimes;", "⊗"},
      {""},
#line 1526 "HTMLCharacterReference.gperf"
      {"nisd;", "⋺"},
      {""}, {""},
#line 1983 "HTMLCharacterReference.gperf"
      {"subplus;", "⪿"},
#line 1322 "HTMLCharacterReference.gperf"
      {"leftarrowtail;", "↢"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1806 "HTMLCharacterReference.gperf"
      {"rbbrk;", "❳"},
#line 263 "HTMLCharacterReference.gperf"
      {"Kopf;", "𝕂"},
      {""}, {""}, {""}, {""},
#line 1812 "HTMLCharacterReference.gperf"
      {"rcaron;", "ř"},
#line 170 "HTMLCharacterReference.gperf"
      {"EqualTilde;", "≂"},
      {""},
#line 738 "HTMLCharacterReference.gperf"
      {"because;", "∵"},
      {""}, {""},
#line 1608 "HTMLCharacterReference.gperf"
      {"ntrianglelefteq;", "⋬"},
      {""}, {""}, {""}, {""},
#line 2135 "HTMLCharacterReference.gperf"
      {"urcrop;", "⌎"},
#line 1910 "HTMLCharacterReference.gperf"
      {"sfrown;", "⌢"},
      {""}, {""}, {""},
#line 1165 "HTMLCharacterReference.gperf"
      {"gtreqqless;", "⪌"},
      {""}, {""}, {""}, {""}, {""},
#line 380 "HTMLCharacterReference.gperf"
      {"NotPrecedesSlantEqual;", "⋠"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 2014 "HTMLCharacterReference.gperf"
      {"supedot;", "⫄"},
      {""},
#line 1798 "HTMLCharacterReference.gperf"
      {"rarrpl;", "⥅"},
#line 2133 "HTMLCharacterReference.gperf"
      {"urcorn;", "⌝"},
      {""}, {""}, {""},
#line 2232 "HTMLCharacterReference.gperf"
      {"yicy;", "ї"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1645 "HTMLCharacterReference.gperf"
      {"odot;", "⊙"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2064 "HTMLCharacterReference.gperf"
      {"tint;", "∭"},
#line 1740 "HTMLCharacterReference.gperf"
      {"prec;", "≺"},
      {""}, {""}, {""},
#line 2105 "HTMLCharacterReference.gperf"
      {"udarr;", "⇅"},
      {""},
#line 1577 "HTMLCharacterReference.gperf"
      {"nsccue;", "⋡"},
#line 1296 "HTMLCharacterReference.gperf"
      {"larrsim;", "⥳"},
      {""},
#line 1797 "HTMLCharacterReference.gperf"
      {"rarrlp;", "↬"},
      {""}, {""},
#line 925 "HTMLCharacterReference.gperf"
      {"curlyeqprec;", "⋞"},
      {""}, {""}, {""},
#line 507 "HTMLCharacterReference.gperf"
      {"Sacute;", "Ś"},
#line 1760 "HTMLCharacterReference.gperf"
      {"prurel;", "⊰"},
#line 2138 "HTMLCharacterReference.gperf"
      {"uscr;", "𝓊"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 347 "HTMLCharacterReference.gperf"
      {"NoBreak;", "⁠"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1543 "HTMLCharacterReference.gperf"
      {"nltri;", "⋪"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1959 "HTMLCharacterReference.gperf"
      {"sqsupset;", "⊐"},
#line 1691 "HTMLCharacterReference.gperf"
      {"otimesas;", "⨶"},
#line 1960 "HTMLCharacterReference.gperf"
      {"sqsupseteq;", "⊒"},
#line 1712 "HTMLCharacterReference.gperf"
      {"phone;", "☎"},
      {""},
#line 1856 "HTMLCharacterReference.gperf"
      {"robrk;", "⟧"},
      {""},
#line 1637 "HTMLCharacterReference.gperf"
      {"oast;", "⊛"},
      {""}, {""}, {""}, {""}, {""},
#line 1751 "HTMLCharacterReference.gperf"
      {"prnap;", "⪹"},
      {""}, {""}, {""},
#line 224 "HTMLCharacterReference.gperf"
      {"Iacute", "Í"},
#line 225 "HTMLCharacterReference.gperf"
      {"Iacute;", "Í"},
      {""}, {""},
#line 301 "HTMLCharacterReference.gperf"
      {"Leftrightarrow;", "⇔"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 748 "HTMLCharacterReference.gperf"
      {"bigcup;", "⋃"},
      {""},
#line 1129 "HTMLCharacterReference.gperf"
      {"gesdotol;", "⪄"},
      {""},
#line 102 "HTMLCharacterReference.gperf"
      {"Dashv;", "⫤"},
      {""},
#line 2246 "HTMLCharacterReference.gperf"
      {"zigrarr;", "⇝"},
      {""},
#line 1623 "HTMLCharacterReference.gperf"
      {"nvle;", "≤⃒"},
#line 2073 "HTMLCharacterReference.gperf"
      {"trade;", "™"},
#line 1478 "HTMLCharacterReference.gperf"
      {"nacute;", "ń"},
      {""},
#line 697 "HTMLCharacterReference.gperf"
      {"angrtvbd;", "⦝"},
      {""}, {""},
#line 725 "HTMLCharacterReference.gperf"
      {"backepsilon;", "϶"},
      {""},
#line 124 "HTMLCharacterReference.gperf"
      {"DoubleLeftTee;", "⫤"},
      {""}, {""}, {""}, {""},
#line 2099 "HTMLCharacterReference.gperf"
      {"uarr;", "↑"},
      {""},
#line 1684 "HTMLCharacterReference.gperf"
      {"oscr;", "ℴ"},
      {""},
#line 926 "HTMLCharacterReference.gperf"
      {"curlyeqsucc;", "⋟"},
      {""}, {""}, {""}, {""}, {""},
#line 1794 "HTMLCharacterReference.gperf"
      {"rarrc;", "⤳"},
#line 372 "HTMLCharacterReference.gperf"
      {"NotLessGreater;", "≸"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 878 "HTMLCharacterReference.gperf"
      {"circleddash;", "⊝"},
      {""}, {""}, {""}, {""},
#line 645 "HTMLCharacterReference.gperf"
      {"Zacute;", "Ź"},
      {""}, {""}, {""},
#line 955 "HTMLCharacterReference.gperf"
      {"delta;", "δ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 295 "HTMLCharacterReference.gperf"
      {"LeftUpTeeVector;", "⥠"},
      {""}, {""}, {""}, {""},
#line 1953 "HTMLCharacterReference.gperf"
      {"sqsub;", "⊏"},
      {""}, {""}, {""}, {""},
#line 297 "HTMLCharacterReference.gperf"
      {"LeftUpVectorBar;", "⥘"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 319 "HTMLCharacterReference.gperf"
      {"LowerLeftArrow;", "↙"},
      {""}, {""}, {""},
#line 1998 "HTMLCharacterReference.gperf"
      {"succneqq;", "⪶"},
#line 586 "HTMLCharacterReference.gperf"
      {"UnionPlus;", "⊎"},
      {""}, {""},
#line 1826 "HTMLCharacterReference.gperf"
      {"rect;", "▭"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 120 "HTMLCharacterReference.gperf"
      {"DoubleDot;", "¨"},
#line 1223 "HTMLCharacterReference.gperf"
      {"imped;", "Ƶ"},
      {""},
#line 1176 "HTMLCharacterReference.gperf"
      {"harrcir;", "⥈"},
      {""}, {""}, {""}, {""},
#line 1763 "HTMLCharacterReference.gperf"
      {"puncsp;", " "},
      {""},
#line 55 "HTMLCharacterReference.gperf"
      {"Bopf;", "𝔹"},
      {""}, {""},
#line 950 "HTMLCharacterReference.gperf"
      {"ddagger;", "‡"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 265 "HTMLCharacterReference.gperf"
      {"LJcy;", "Љ"},
      {""}, {""}, {""}, {""},
#line 88 "HTMLCharacterReference.gperf"
      {"Copf;", "ℂ"},
#line 1079 "HTMLCharacterReference.gperf"
      {"fjlig;", "fj"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1185 "HTMLCharacterReference.gperf"
      {"hksearow;", "⤥"},
#line 352 "HTMLCharacterReference.gperf"
      {"NotCupCap;", "≭"},
      {""},
#line 462 "HTMLCharacterReference.gperf"
      {"Racute;", "Ŕ"},
      {""}, {""}, {""},
#line 1706 "HTMLCharacterReference.gperf"
      {"perp;", "⊥"},
      {""}, {""},
#line 1869 "HTMLCharacterReference.gperf"
      {"rsquo;", "’"},
#line 1870 "HTMLCharacterReference.gperf"
      {"rsquor;", "’"},
      {""}, {""}, {""}, {""},
#line 286 "HTMLCharacterReference.gperf"
      {"LeftRightArrow;", "↔"},
#line 152 "HTMLCharacterReference.gperf"
      {"Eacute", "É"},
#line 153 "HTMLCharacterReference.gperf"
      {"Eacute;", "É"},
      {""}, {""}, {""}, {""},
#line 1674 "HTMLCharacterReference.gperf"
      {"order;", "ℴ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1978 "HTMLCharacterReference.gperf"
      {"sube;", "⊆"},
      {""},
#line 1433 "HTMLCharacterReference.gperf"
      {"mapsto;", "↦"},
      {""}, {""}, {""}, {""}, {""},
#line 2134 "HTMLCharacterReference.gperf"
      {"urcorner;", "⌝"},
#line 1265 "HTMLCharacterReference.gperf"
      {"kcy;", "к"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1042 "HTMLCharacterReference.gperf"
      {"eplus;", "⩱"},
      {""}, {""},
#line 1921 "HTMLCharacterReference.gperf"
      {"sim;", "∼"},
      {""},
#line 740 "HTMLCharacterReference.gperf"
      {"bepsi;", "϶"},
      {""}, {""}, {""}, {""}, {""},
#line 2068 "HTMLCharacterReference.gperf"
      {"topcir;", "⫱"},
      {""},
#line 1377 "HTMLCharacterReference.gperf"
      {"longleftrightarrow;", "⟷"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 700 "HTMLCharacterReference.gperf"
      {"angzarr;", "⍼"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 945 "HTMLCharacterReference.gperf"
      {"dbkarow;", "⤏"},
#line 1814 "HTMLCharacterReference.gperf"
      {"rceil;", "⌉"},
      {""}, {""},
#line 1475 "HTMLCharacterReference.gperf"
      {"nVDash;", "⊯"},
      {""}, {""}, {""},
#line 1971 "HTMLCharacterReference.gperf"
      {"starf;", "★"},
      {""}, {""}, {""}, {""}, {""},
#line 2146 "HTMLCharacterReference.gperf"
      {"uwangle;", "⦧"},
      {""}, {""}, {""},
#line 783 "HTMLCharacterReference.gperf"
      {"boxHD;", "╦"},
      {""},
#line 1743 "HTMLCharacterReference.gperf"
      {"preceq;", "⪯"},
      {""},
#line 1927 "HTMLCharacterReference.gperf"
      {"siml;", "⪝"},
      {""},
#line 1648 "HTMLCharacterReference.gperf"
      {"ofcir;", "⦿"},
      {""},
#line 446 "HTMLCharacterReference.gperf"
      {"PrecedesSlantEqual;", "≼"},
#line 80 "HTMLCharacterReference.gperf"
      {"ClockwiseContourIntegral;", "∲"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 96 "HTMLCharacterReference.gperf"
      {"DDotrahd;", "⤑"},
#line 1481 "HTMLCharacterReference.gperf"
      {"napE;", "⩰̸"},
#line 1216 "HTMLCharacterReference.gperf"
      {"ijlig;", "ĳ"},
      {""}, {""}, {""},
#line 1580 "HTMLCharacterReference.gperf"
      {"nshortmid;", "∤"},
      {""},
#line 877 "HTMLCharacterReference.gperf"
      {"circledcirc;", "⊚"},
      {""}, {""}, {""},
#line 805 "HTMLCharacterReference.gperf"
      {"boxhU;", "╨"},
      {""}, {""}, {""},
#line 97 "HTMLCharacterReference.gperf"
      {"DJcy;", "Ђ"},
#line 121 "HTMLCharacterReference.gperf"
      {"DoubleDownArrow;", "⇓"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 2152 "HTMLCharacterReference.gperf"
      {"varepsilon;", "ϵ"},
      {""}, {""}, {""}, {""}, {""},
#line 2167 "HTMLCharacterReference.gperf"
      {"vartriangleright;", "⊳"},
      {""},
#line 1792 "HTMLCharacterReference.gperf"
      {"rarrb;", "⇥"},
      {""},
#line 1932 "HTMLCharacterReference.gperf"
      {"slarr;", "←"},
      {""},
#line 1054 "HTMLCharacterReference.gperf"
      {"equivDD;", "⩸"},
      {""},
#line 2114 "HTMLCharacterReference.gperf"
      {"uhblk;", "▀"},
#line 2101 "HTMLCharacterReference.gperf"
      {"ubreve;", "ŭ"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1646 "HTMLCharacterReference.gperf"
      {"odsold;", "⦼"},
      {""}, {""}, {""},
#line 335 "HTMLCharacterReference.gperf"
      {"Nacute;", "Ń"},
      {""},
#line 1914 "HTMLCharacterReference.gperf"
      {"shortmid;", "∣"},
      {""}, {""}, {""}, {""},
#line 1140 "HTMLCharacterReference.gperf"
      {"glj;", "⪤"},
#line 203 "HTMLCharacterReference.gperf"
      {"GreaterGreater;", "⪢"},
#line 546 "HTMLCharacterReference.gperf"
      {"TRADE;", "™"},
#line 1958 "HTMLCharacterReference.gperf"
      {"sqsupe;", "⊒"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 2110 "HTMLCharacterReference.gperf"
      {"ugrave", "ù"},
#line 2111 "HTMLCharacterReference.gperf"
      {"ugrave;", "ù"},
      {""}, {""}, {""}, {""},
#line 2054 "HTMLCharacterReference.gperf"
      {"thkap;", "≈"},
      {""}, {""}, {""},
#line 1539 "HTMLCharacterReference.gperf"
      {"nles;", "⩽̸"},
#line 426 "HTMLCharacterReference.gperf"
      {"Otilde", "Õ"},
#line 427 "HTMLCharacterReference.gperf"
      {"Otilde;", "Õ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1704 "HTMLCharacterReference.gperf"
      {"period;", "."},
      {""},
#line 2132 "HTMLCharacterReference.gperf"
      {"upuparrows;", "⇈"},
      {""}, {""}, {""}, {""}, {""},
#line 24 "HTMLCharacterReference.gperf"
      {"Aacute", "Á"},
#line 25 "HTMLCharacterReference.gperf"
      {"Aacute;", "Á"},
      {""},
#line 1955 "HTMLCharacterReference.gperf"
      {"sqsubset;", "⊏"},
#line 2002 "HTMLCharacterReference.gperf"
      {"sung;", "♪"},
#line 1956 "HTMLCharacterReference.gperf"
      {"sqsubseteq;", "⊑"},
      {""}, {""}, {""},
#line 1329 "HTMLCharacterReference.gperf"
      {"leftrightsquigarrow;", "↭"},
#line 1888 "HTMLCharacterReference.gperf"
      {"scirc;", "ŝ"},
#line 928 "HTMLCharacterReference.gperf"
      {"curlywedge;", "⋏"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1364 "HTMLCharacterReference.gperf"
      {"lmoust;", "⎰"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1436 "HTMLCharacterReference.gperf"
      {"mapstoup;", "↥"},
      {""}, {""}, {""}, {""}, {""},
#line 1762 "HTMLCharacterReference.gperf"
      {"psi;", "ψ"},
#line 83 "HTMLCharacterReference.gperf"
      {"Colon;", "∷"},
#line 302 "HTMLCharacterReference.gperf"
      {"LessEqualGreater;", "⋚"},
      {""}, {""}, {""}, {""},
#line 1651 "HTMLCharacterReference.gperf"
      {"ograve", "ò"},
#line 1652 "HTMLCharacterReference.gperf"
      {"ograve;", "ò"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 364 "HTMLCharacterReference.gperf"
      {"NotGreaterTilde;", "≵"},
      {""}, {""}, {""}, {""}, {""},
#line 777 "HTMLCharacterReference.gperf"
      {"bowtie;", "⋈"},
      {""},
#line 1782 "HTMLCharacterReference.gperf"
      {"radic;", "√"},
      {""},
#line 106 "HTMLCharacterReference.gperf"
      {"Delta;", "Δ"},
      {""}, {""}, {""}, {""}, {""},
#line 1274 "HTMLCharacterReference.gperf"
      {"lAtail;", "⤛"},
      {""}, {""},
#line 2136 "HTMLCharacterReference.gperf"
      {"uring;", "ů"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1946 "HTMLCharacterReference.gperf"
      {"spades;", "♠"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1929 "HTMLCharacterReference.gperf"
      {"simne;", "≆"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 316 "HTMLCharacterReference.gperf"
      {"Longleftrightarrow;", "⟺"},
      {""}, {""},
#line 1074 "HTMLCharacterReference.gperf"
      {"ffilig;", "ﬃ"},
      {""}, {""}, {""}, {""},
#line 2051 "HTMLCharacterReference.gperf"
      {"thickapprox;", "≈"},
      {""},
#line 2042 "HTMLCharacterReference.gperf"
      {"tcy;", "т"},
      {""},
#line 292 "HTMLCharacterReference.gperf"
      {"LeftTriangleBar;", "⧏"},
      {""}, {""}, {""}, {""}, {""},
#line 2011 "HTMLCharacterReference.gperf"
      {"supdot;", "⪾"},
      {""}, {""},
#line 1709 "HTMLCharacterReference.gperf"
      {"phi;", "φ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 395 "HTMLCharacterReference.gperf"
      {"NotSuperset;", "⊃⃒"},
      {""}, {""}, {""}, {""},
#line 1748 "HTMLCharacterReference.gperf"
      {"prime;", "′"},
#line 341 "HTMLCharacterReference.gperf"
      {"NegativeThinSpace;", "​"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1451 "HTMLCharacterReference.gperf"
      {"minus;", "−"},
      {""}, {""}, {""}, {""},
#line 1994 "HTMLCharacterReference.gperf"
      {"succapprox;", "⪸"},
      {""}, {""},
#line 752 "HTMLCharacterReference.gperf"
      {"bigsqcup;", "⨆"},
      {""}, {""}, {""},
#line 1703 "HTMLCharacterReference.gperf"
      {"percnt;", "%"},
      {""}, {""},
#line 1928 "HTMLCharacterReference.gperf"
      {"simlE;", "⪟"},
      {""}, {""}, {""}, {""},
#line 1068 "HTMLCharacterReference.gperf"
      {"exist;", "∃"},
      {""}, {""}, {""},
#line 73 "HTMLCharacterReference.gperf"
      {"CenterDot;", "·"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 547 "HTMLCharacterReference.gperf"
      {"TSHcy;", "Ћ"},
      {""}, {""}, {""},
#line 1540 "HTMLCharacterReference.gperf"
      {"nless;", "≮"},
      {""},
#line 1749 "HTMLCharacterReference.gperf"
      {"primes;", "ℙ"},
      {""}, {""}, {""},
#line 1824 "HTMLCharacterReference.gperf"
      {"realpart;", "ℜ"},
      {""}, {""}, {""}, {""}, {""},
#line 65 "HTMLCharacterReference.gperf"
      {"Cayleys;", "ℭ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1733 "HTMLCharacterReference.gperf"
      {"pound", "£"},
#line 1734 "HTMLCharacterReference.gperf"
      {"pound;", "£"},
      {""}, {""},
#line 187 "HTMLCharacterReference.gperf"
      {"GJcy;", "Ѓ"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1654 "HTMLCharacterReference.gperf"
      {"ohbar;", "⦵"},
      {""},
#line 1026 "HTMLCharacterReference.gperf"
      {"ell;", "ℓ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1544 "HTMLCharacterReference.gperf"
      {"nltrie;", "⋬"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 409 "HTMLCharacterReference.gperf"
      {"Ocirc", "Ô"},
#line 410 "HTMLCharacterReference.gperf"
      {"Ocirc;", "Ô"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 305 "HTMLCharacterReference.gperf"
      {"LessLess;", "⪡"},
      {""},
#line 672 "HTMLCharacterReference.gperf"
      {"aleph;", "ℵ"},
#line 1694 "HTMLCharacterReference.gperf"
      {"ovbar;", "⌽"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 451 "HTMLCharacterReference.gperf"
      {"Proportional;", "∝"},
      {""}, {""},
#line 872 "HTMLCharacterReference.gperf"
      {"circlearrowleft;", "↺"},
#line 2238 "HTMLCharacterReference.gperf"
      {"zacute;", "ź"},
#line 1815 "HTMLCharacterReference.gperf"
      {"rcub;", "}"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 470 "HTMLCharacterReference.gperf"
      {"ReverseElement;", "∋"},
      {""}, {""},
#line 730 "HTMLCharacterReference.gperf"
      {"barwed;", "⌅"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 342 "HTMLCharacterReference.gperf"
      {"NegativeVeryThinSpace;", "​"},
      {""}, {""}, {""}, {""}, {""},
#line 1930 "HTMLCharacterReference.gperf"
      {"simplus;", "⨤"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1745 "HTMLCharacterReference.gperf"
      {"precneqq;", "⪵"},
      {""}, {""}, {""}, {""},
#line 869 "HTMLCharacterReference.gperf"
      {"cirE;", "⧃"},
      {""}, {""}, {""}, {""},
#line 1560 "HTMLCharacterReference.gperf"
      {"nparallel;", "∦"},
      {""},
#line 1850 "HTMLCharacterReference.gperf"
      {"rlm;", "‏"},
#line 70 "HTMLCharacterReference.gperf"
      {"Cconint;", "∰"},
      {""}, {""}, {""}, {""},
#line 1556 "HTMLCharacterReference.gperf"
      {"notniva;", "∌"},
      {""},
#line 1270 "HTMLCharacterReference.gperf"
      {"kopf;", "𝕜"},
      {""},
#line 1954 "HTMLCharacterReference.gperf"
      {"sqsube;", "⊑"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 289 "HTMLCharacterReference.gperf"
      {"LeftTeeArrow;", "↤"},
      {""},
#line 334 "HTMLCharacterReference.gperf"
      {"NJcy;", "Њ"},
      {""}, {""}, {""},
#line 744 "HTMLCharacterReference.gperf"
      {"between;", "≬"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1761 "HTMLCharacterReference.gperf"
      {"pscr;", "𝓅"},
      {""}, {""}, {""},
#line 129 "HTMLCharacterReference.gperf"
      {"DoubleRightTee;", "⊨"},
      {""}, {""},
#line 589 "HTMLCharacterReference.gperf"
      {"UpArrow;", "↑"},
      {""},
#line 564 "HTMLCharacterReference.gperf"
      {"TripleDot;", "⃛"},
      {""}, {""}, {""}, {""}, {""},
#line 1793 "HTMLCharacterReference.gperf"
      {"rarrbfs;", "⤠"},
      {""}, {""}, {""}, {""}, {""},
#line 1867 "HTMLCharacterReference.gperf"
      {"rsh;", "↱"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1529 "HTMLCharacterReference.gperf"
      {"nlArr;", "⇍"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1875 "HTMLCharacterReference.gperf"
      {"rtrif;", "▸"},
      {""}, {""},
#line 1453 "HTMLCharacterReference.gperf"
      {"minusd;", "∸"},
      {""}, {""}, {""},
#line 354 "HTMLCharacterReference.gperf"
      {"NotElement;", "∉"},
      {""}, {""},
#line 1848 "HTMLCharacterReference.gperf"
      {"rlarr;", "⇄"},
      {""}, {""}, {""}, {""}, {""},
#line 1695 "HTMLCharacterReference.gperf"
      {"par;", "∥"},
#line 1701 "HTMLCharacterReference.gperf"
      {"part;", "∂"},
      {""}, {""}, {""}, {""},
#line 1951 "HTMLCharacterReference.gperf"
      {"sqcup;", "⊔"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1434 "HTMLCharacterReference.gperf"
      {"mapstodown;", "↧"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 2224 "HTMLCharacterReference.gperf"
      {"yacute", "ý"},
#line 2225 "HTMLCharacterReference.gperf"
      {"yacute;", "ý"},
      {""}, {""}, {""},
#line 1469 "HTMLCharacterReference.gperf"
      {"nLeftarrow;", "⇍"},
      {""}, {""}, {""},
#line 2059 "HTMLCharacterReference.gperf"
      {"times", "×"},
#line 2060 "HTMLCharacterReference.gperf"
      {"times;", "×"},
#line 2081 "HTMLCharacterReference.gperf"
      {"tridot;", "◬"},
      {""},
#line 619 "HTMLCharacterReference.gperf"
      {"VeryThinSpace;", " "},
#line 38 "HTMLCharacterReference.gperf"
      {"ApplyFunction;", "⁡"},
      {""}, {""}, {""}, {""}, {""},
#line 2140 "HTMLCharacterReference.gperf"
      {"utilde;", "ũ"},
      {""},
#line 2033 "HTMLCharacterReference.gperf"
      {"swarrow;", "↙"},
      {""}, {""}, {""},
#line 1952 "HTMLCharacterReference.gperf"
      {"sqcups;", "⊔︀"},
#line 2018 "HTMLCharacterReference.gperf"
      {"supmult;", "⫂"},
#line 1894 "HTMLCharacterReference.gperf"
      {"scy;", "с"},
#line 984 "HTMLCharacterReference.gperf"
      {"doublebarwedge;", "⌆"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1310 "HTMLCharacterReference.gperf"
      {"lcedil;", "ļ"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 400 "HTMLCharacterReference.gperf"
      {"NotTildeTilde;", "≉"},
      {""},
#line 851 "HTMLCharacterReference.gperf"
      {"ccedil", "ç"},
#line 852 "HTMLCharacterReference.gperf"
      {"ccedil;", "ç"},
#line 1984 "HTMLCharacterReference.gperf"
      {"subrarr;", "⥹"},
      {""}, {""},
#line 1878 "HTMLCharacterReference.gperf"
      {"rx;", "℞"},
      {""}, {""}, {""}, {""}, {""},
#line 171 "HTMLCharacterReference.gperf"
      {"Equilibrium;", "⇌"},
      {""}, {""}, {""},
#line 2150 "HTMLCharacterReference.gperf"
      {"vDash;", "⊨"},
      {""}, {""}, {""}, {""}, {""},
#line 2022 "HTMLCharacterReference.gperf"
      {"supset;", "⊃"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 2208 "HTMLCharacterReference.gperf"
      {"xlArr;", "⟸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1688 "HTMLCharacterReference.gperf"
      {"otilde", "õ"},
#line 1689 "HTMLCharacterReference.gperf"
      {"otilde;", "õ"},
      {""},
#line 1791 "HTMLCharacterReference.gperf"
      {"rarrap;", "⥵"},
#line 343 "HTMLCharacterReference.gperf"
      {"NestedGreaterGreater;", "≫"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1638 "HTMLCharacterReference.gperf"
      {"ocir;", "⊚"},
      {""}, {""},
#line 398 "HTMLCharacterReference.gperf"
      {"NotTildeEqual;", "≄"},
      {""}, {""}, {""},
#line 1588 "HTMLCharacterReference.gperf"
      {"nsqsupe;", "⋣"},
      {""},
#line 986 "HTMLCharacterReference.gperf"
      {"downdownarrows;", "⇊"},
      {""}, {""}, {""}, {""}, {""},
#line 320 "HTMLCharacterReference.gperf"
      {"LowerRightArrow;", "↘"},
      {""},
#line 1817 "HTMLCharacterReference.gperf"
      {"rdca;", "⤷"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 2083 "HTMLCharacterReference.gperf"
      {"triminus;", "⨺"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1003 "HTMLCharacterReference.gperf"
      {"dzigrarr;", "⟿"},
#line 1923 "HTMLCharacterReference.gperf"
      {"sime;", "≃"},
#line 1924 "HTMLCharacterReference.gperf"
      {"simeq;", "≃"},
      {""}, {""}, {""},
#line 306 "HTMLCharacterReference.gperf"
      {"LessSlantEqual;", "⩽"},
#line 2154 "HTMLCharacterReference.gperf"
      {"varnothing;", "∅"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2137 "HTMLCharacterReference.gperf"
      {"urtri;", "◹"},
#line 1470 "HTMLCharacterReference.gperf"
      {"nLeftrightarrow;", "⇎"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2142 "HTMLCharacterReference.gperf"
      {"utrif;", "▴"},
      {""}, {""}, {""}, {""},
#line 2069 "HTMLCharacterReference.gperf"
      {"topf;", "𝕥"},
      {""}, {""}, {""},
#line 1324 "HTMLCharacterReference.gperf"
      {"leftharpoonup;", "↼"},
      {""},
#line 1218 "HTMLCharacterReference.gperf"
      {"image;", "ℑ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1696 "HTMLCharacterReference.gperf"
      {"para", "¶"},
#line 1697 "HTMLCharacterReference.gperf"
      {"para;", "¶"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 84 "HTMLCharacterReference.gperf"
      {"Colone;", "⩴"},
      {""}, {""},
#line 1661 "HTMLCharacterReference.gperf"
      {"olt;", "⧀"},
      {""}, {""},
#line 1711 "HTMLCharacterReference.gperf"
      {"phmmat;", "ℳ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1997 "HTMLCharacterReference.gperf"
      {"succnapprox;", "⪺"},
      {""}, {""},
#line 1330 "HTMLCharacterReference.gperf"
      {"leftthreetimes;", "⋋"},
      {""}, {""}, {""},
#line 755 "HTMLCharacterReference.gperf"
      {"bigtriangleup;", "△"},
#line 139 "HTMLCharacterReference.gperf"
      {"DownLeftVector;", "↽"},
      {""}, {""},
#line 140 "HTMLCharacterReference.gperf"
      {"DownLeftVectorBar;", "⥖"},
#line 1906 "HTMLCharacterReference.gperf"
      {"setminus;", "∖"},
#line 1379 "HTMLCharacterReference.gperf"
      {"longrightarrow;", "⟶"},
      {""}, {""}, {""}, {""}, {""},
#line 2030 "HTMLCharacterReference.gperf"
      {"swArr;", "⇙"},
      {""}, {""}, {""}, {""}, {""},
#line 2063 "HTMLCharacterReference.gperf"
      {"timesd;", "⨰"},
      {""}, {""}, {""},
#line 807 "HTMLCharacterReference.gperf"
      {"boxhu;", "┴"},
      {""}, {""},
#line 411 "HTMLCharacterReference.gperf"
      {"Ocy;", "О"},
      {""},
#line 1710 "HTMLCharacterReference.gperf"
      {"phiv;", "ϕ"},
      {""},
#line 727 "HTMLCharacterReference.gperf"
      {"backsim;", "∽"},
      {""}, {""}, {""},
#line 1657 "HTMLCharacterReference.gperf"
      {"olarr;", "↺"},
#line 219 "HTMLCharacterReference.gperf"
      {"HumpDownHump;", "≎"},
#line 536 "HTMLCharacterReference.gperf"
      {"SucceedsSlantEqual;", "≽"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 2125 "HTMLCharacterReference.gperf"
      {"updownarrow;", "↕"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2102 "HTMLCharacterReference.gperf"
      {"ucirc", "û"},
#line 2103 "HTMLCharacterReference.gperf"
      {"ucirc;", "û"},
#line 1656 "HTMLCharacterReference.gperf"
      {"oint;", "∮"},
      {""}, {""},
#line 1675 "HTMLCharacterReference.gperf"
      {"orderof;", "ℴ"},
      {""}, {""}, {""}, {""},
#line 1833 "HTMLCharacterReference.gperf"
      {"rharu;", "⇀"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1741 "HTMLCharacterReference.gperf"
      {"precapprox;", "⪷"},
      {""}, {""},
#line 1430 "HTMLCharacterReference.gperf"
      {"malt;", "✠"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1834 "HTMLCharacterReference.gperf"
      {"rharul;", "⥬"},
      {""},
#line 274 "HTMLCharacterReference.gperf"
      {"Lcedil;", "Ļ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 751 "HTMLCharacterReference.gperf"
      {"bigotimes;", "⨂"},
      {""}, {""}, {""},
#line 1819 "HTMLCharacterReference.gperf"
      {"rdquo;", "”"},
#line 1820 "HTMLCharacterReference.gperf"
      {"rdquor;", "”"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 312 "HTMLCharacterReference.gperf"
      {"LongLeftArrow;", "⟵"},
      {""}, {""}, {""}, {""}, {""},
#line 1859 "HTMLCharacterReference.gperf"
      {"roplus;", "⨮"},
      {""}, {""},
#line 1979 "HTMLCharacterReference.gperf"
      {"subedot;", "⫃"},
      {""}, {""}, {""}, {""},
#line 747 "HTMLCharacterReference.gperf"
      {"bigcirc;", "◯"},
      {""},
#line 1606 "HTMLCharacterReference.gperf"
      {"ntlg;", "≸"},
#line 1639 "HTMLCharacterReference.gperf"
      {"ocirc", "ô"},
#line 1640 "HTMLCharacterReference.gperf"
      {"ocirc;", "ô"},
      {""},
#line 2023 "HTMLCharacterReference.gperf"
      {"supseteq;", "⊇"},
#line 2024 "HTMLCharacterReference.gperf"
      {"supseteqq;", "⫆"},
      {""},
#line 1182 "HTMLCharacterReference.gperf"
      {"hellip;", "…"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1622 "HTMLCharacterReference.gperf"
      {"nvlArr;", "⤂"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1876 "HTMLCharacterReference.gperf"
      {"rtriltri;", "⧎"},
#line 1788 "HTMLCharacterReference.gperf"
      {"raquo", "»"},
#line 1789 "HTMLCharacterReference.gperf"
      {"raquo;", "»"},
      {""}, {""},
#line 62 "HTMLCharacterReference.gperf"
      {"Cacute;", "Ć"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1111 "HTMLCharacterReference.gperf"
      {"gEl;", "⪌"},
      {""}, {""},
#line 1261 "HTMLCharacterReference.gperf"
      {"jukcy;", "є"},
      {""}, {""}, {""}, {""},
#line 2173 "HTMLCharacterReference.gperf"
      {"vellip;", "⋮"},
      {""},
#line 1882 "HTMLCharacterReference.gperf"
      {"scE;", "⪴"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1454 "HTMLCharacterReference.gperf"
      {"minusdu;", "⨪"},
      {""}, {""}, {""},
#line 941 "HTMLCharacterReference.gperf"
      {"daleth;", "ℸ"},
      {""},
#line 118 "HTMLCharacterReference.gperf"
      {"DotEqual;", "≐"},
#line 1865 "HTMLCharacterReference.gperf"
      {"rsaquo;", "›"},
#line 127 "HTMLCharacterReference.gperf"
      {"DoubleLongRightArrow;", "⟹"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 136 "HTMLCharacterReference.gperf"
      {"DownBreve;", "̑"},
      {""},
#line 2091 "HTMLCharacterReference.gperf"
      {"tstrok;", "ŧ"},
      {""}, {""}, {""}, {""}, {""},
#line 2086 "HTMLCharacterReference.gperf"
      {"tritime;", "⨻"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 728 "HTMLCharacterReference.gperf"
      {"backsimeq;", "⋍"},
      {""}, {""},
#line 1901 "HTMLCharacterReference.gperf"
      {"searrow;", "↘"},
      {""}, {""},
#line 1644 "HTMLCharacterReference.gperf"
      {"odiv;", "⨸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2085 "HTMLCharacterReference.gperf"
      {"trisb;", "⧍"},
#line 177 "HTMLCharacterReference.gperf"
      {"Exists;", "∃"},
#line 281 "HTMLCharacterReference.gperf"
      {"LeftDoubleBracket;", "⟦"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1801 "HTMLCharacterReference.gperf"
      {"rarrw;", "↝"},
#line 731 "HTMLCharacterReference.gperf"
      {"barwedge;", "⌅"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1816 "HTMLCharacterReference.gperf"
      {"rcy;", "р"},
      {""}, {""},
#line 317 "HTMLCharacterReference.gperf"
      {"Longrightarrow;", "⟹"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1945 "HTMLCharacterReference.gperf"
      {"sopf;", "𝕤"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 510 "HTMLCharacterReference.gperf"
      {"Scedil;", "Ş"},
      {""}, {""}, {""}, {""},
#line 294 "HTMLCharacterReference.gperf"
      {"LeftUpDownVector;", "⥑"},
      {""}, {""},
#line 258 "HTMLCharacterReference.gperf"
      {"KJcy;", "Ќ"},
      {""},
#line 379 "HTMLCharacterReference.gperf"
      {"NotPrecedesEqual;", "⪯̸"},
      {""},
#line 2165 "HTMLCharacterReference.gperf"
      {"vartheta;", "ϑ"},
      {""}, {""}, {""}, {""},
#line 538 "HTMLCharacterReference.gperf"
      {"SuchThat;", "∋"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 772 "HTMLCharacterReference.gperf"
      {"bnequiv;", "≡⃥"},
      {""}, {""}, {""},
#line 1251 "HTMLCharacterReference.gperf"
      {"iukcy;", "і"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1964 "HTMLCharacterReference.gperf"
      {"squf;", "▪"},
      {""},
#line 1494 "HTMLCharacterReference.gperf"
      {"ncedil;", "ņ"},
      {""}, {""},
#line 1736 "HTMLCharacterReference.gperf"
      {"prE;", "⪳"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 135 "HTMLCharacterReference.gperf"
      {"DownArrowUpArrow;", "⇵"},
      {""}, {""}, {""},
#line 1918 "HTMLCharacterReference.gperf"
      {"sigma;", "σ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1078 "HTMLCharacterReference.gperf"
      {"filig;", "ﬁ"},
      {""}, {""}, {""},
#line 314 "HTMLCharacterReference.gperf"
      {"LongRightArrow;", "⟶"},
      {""},
#line 1448 "HTMLCharacterReference.gperf"
      {"midcir;", "⫰"},
      {""}, {""},
#line 1871 "HTMLCharacterReference.gperf"
      {"rthree;", "⋌"},
      {""},
#line 2037 "HTMLCharacterReference.gperf"
      {"target;", "⌖"},
      {""}, {""}, {""}, {""}, {""},
#line 1423 "HTMLCharacterReference.gperf"
      {"luruhar;", "⥦"},
      {""}, {""},
#line 1081 "HTMLCharacterReference.gperf"
      {"fllig;", "ﬂ"},
      {""}, {""}, {""}, {""},
#line 1449 "HTMLCharacterReference.gperf"
      {"middot", "·"},
#line 1450 "HTMLCharacterReference.gperf"
      {"middot;", "·"},
      {""}, {""}, {""}, {""}, {""},
#line 1347 "HTMLCharacterReference.gperf"
      {"lesssim;", "≲"},
      {""}, {""}, {""}, {""},
#line 753 "HTMLCharacterReference.gperf"
      {"bigstar;", "★"},
#line 1938 "HTMLCharacterReference.gperf"
      {"smt;", "⪪"},
#line 2025 "HTMLCharacterReference.gperf"
      {"supsetneq;", "⊋"},
#line 2026 "HTMLCharacterReference.gperf"
      {"supsetneqq;", "⫌"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1898 "HTMLCharacterReference.gperf"
      {"seArr;", "⇘"},
      {""},
#line 51 "HTMLCharacterReference.gperf"
      {"Because;", "∵"},
      {""}, {""}, {""}, {""},
#line 449 "HTMLCharacterReference.gperf"
      {"Product;", "∏"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 328 "HTMLCharacterReference.gperf"
      {"Mellintrf;", "ℳ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 2124 "HTMLCharacterReference.gperf"
      {"uparrow;", "↑"},
      {""}, {""},
#line 389 "HTMLCharacterReference.gperf"
      {"NotSubset;", "⊂⃒"},
      {""},
#line 2050 "HTMLCharacterReference.gperf"
      {"thetav;", "ϑ"},
      {""},
#line 1778 "HTMLCharacterReference.gperf"
      {"rBarr;", "⤏"},
      {""},
#line 287 "HTMLCharacterReference.gperf"
      {"LeftRightVector;", "⥎"},
#line 467 "HTMLCharacterReference.gperf"
      {"Rcedil;", "Ŗ"},
      {""}, {""}, {""}, {""}, {""},
#line 2104 "HTMLCharacterReference.gperf"
      {"ucy;", "у"},
      {""},
#line 2010 "HTMLCharacterReference.gperf"
      {"supE;", "⫆"},
      {""}, {""},
#line 132 "HTMLCharacterReference.gperf"
      {"DoubleVerticalBar;", "∥"},
#line 523 "HTMLCharacterReference.gperf"
      {"SquareIntersection;", "⊓"},
      {""}, {""}, {""},
#line 1780 "HTMLCharacterReference.gperf"
      {"race;", "∽̱"},
      {""}, {""}, {""}, {""}, {""},
#line 1775 "HTMLCharacterReference.gperf"
      {"rAarr;", "⇛"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1363 "HTMLCharacterReference.gperf"
      {"lmidot;", "ŀ"},
      {""}, {""}, {""}, {""}, {""},
#line 1776 "HTMLCharacterReference.gperf"
      {"rArr;", "⇒"},
      {""}, {""}, {""},
#line 193 "HTMLCharacterReference.gperf"
      {"Gcedil;", "Ģ"},
      {""},
#line 1942 "HTMLCharacterReference.gperf"
      {"sol;", "/"},
#line 419 "HTMLCharacterReference.gperf"
      {"Oopf;", "𝕆"},
      {""}, {""}, {""}, {""}, {""},
#line 123 "HTMLCharacterReference.gperf"
      {"DoubleLeftRightArrow;", "⇔"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 476 "HTMLCharacterReference.gperf"
      {"RightArrow;", "→"},
      {""}, {""}, {""}, {""}, {""},
#line 1452 "HTMLCharacterReference.gperf"
      {"minusb;", "⊟"},
      {""}, {""}, {""}, {""}, {""},
#line 1744 "HTMLCharacterReference.gperf"
      {"precnapprox;", "⪹"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1641 "HTMLCharacterReference.gperf"
      {"ocy;", "о"},
#line 1257 "HTMLCharacterReference.gperf"
      {"jmath;", "ȷ"},
#line 1919 "HTMLCharacterReference.gperf"
      {"sigmaf;", "ς"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1713 "HTMLCharacterReference.gperf"
      {"pi;", "π"},
      {""}, {""}, {""},
#line 2163 "HTMLCharacterReference.gperf"
      {"varsupsetneq;", "⊋︀"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 671 "HTMLCharacterReference.gperf"
      {"alefsym;", "ℵ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1753 "HTMLCharacterReference.gperf"
      {"prod;", "∏"},
      {""},
#line 1931 "HTMLCharacterReference.gperf"
      {"simrarr;", "⥲"},
      {""},
#line 1715 "HTMLCharacterReference.gperf"
      {"piv;", "ϖ"},
      {""},
#line 256 "HTMLCharacterReference.gperf"
      {"Jukcy;", "Є"},
      {""}, {""}, {""}, {""}, {""},
#line 371 "HTMLCharacterReference.gperf"
      {"NotLessEqual;", "≰"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 399 "HTMLCharacterReference.gperf"
      {"NotTildeFullEqual;", "≇"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 355 "HTMLCharacterReference.gperf"
      {"NotEqual;", "≠"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1365 "HTMLCharacterReference.gperf"
      {"lmoustache;", "⎰"},
#line 337 "HTMLCharacterReference.gperf"
      {"Ncedil;", "Ņ"},
      {""},
#line 1233 "HTMLCharacterReference.gperf"
      {"intlarhk;", "⨗"},
      {""}, {""},
#line 506 "HTMLCharacterReference.gperf"
      {"SOFTcy;", "Ь"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1977 "HTMLCharacterReference.gperf"
      {"subdot;", "⪽"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 726 "HTMLCharacterReference.gperf"
      {"backprime;", "‵"},
      {""}, {""}, {""}, {""}, {""},
#line 673 "HTMLCharacterReference.gperf"
      {"alpha;", "α"},
      {""},
#line 178 "HTMLCharacterReference.gperf"
      {"ExponentialE;", "ⅇ"},
      {""}, {""},
#line 2169 "HTMLCharacterReference.gperf"
      {"vdash;", "⊢"},
      {""}, {""}, {""}, {""},
#line 1823 "HTMLCharacterReference.gperf"
      {"realine;", "ℛ"},
      {""}, {""}, {""}, {""}, {""},
#line 1348 "HTMLCharacterReference.gperf"
      {"lfisht;", "⥼"},
#line 1558 "HTMLCharacterReference.gperf"
      {"notnivc;", "⋽"},
      {""}, {""}, {""},
#line 202 "HTMLCharacterReference.gperf"
      {"GreaterFullEqual;", "≧"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 247 "HTMLCharacterReference.gperf"
      {"Iukcy;", "І"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 137 "HTMLCharacterReference.gperf"
      {"DownLeftRightVector;", "⥐"},
      {""}, {""}, {""}, {""},
#line 1222 "HTMLCharacterReference.gperf"
      {"imof;", "⊷"},
#line 1323 "HTMLCharacterReference.gperf"
      {"leftharpoondown;", "↽"},
#line 957 "HTMLCharacterReference.gperf"
      {"dfisht;", "⥿"},
      {""}, {""}, {""}, {""}, {""},
#line 345 "HTMLCharacterReference.gperf"
      {"NewLine;", "\n"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 2095 "HTMLCharacterReference.gperf"
      {"uArr;", "⇑"},
      {""},
#line 1221 "HTMLCharacterReference.gperf"
      {"imath;", "ı"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 649 "HTMLCharacterReference.gperf"
      {"ZeroWidthSpace;", "​"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 754 "HTMLCharacterReference.gperf"
      {"bigtriangledown;", "▽"},
      {""},
#line 1858 "HTMLCharacterReference.gperf"
      {"ropf;", "𝕣"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 311 "HTMLCharacterReference.gperf"
      {"Lmidot;", "Ŀ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1431 "HTMLCharacterReference.gperf"
      {"maltese;", "✠"},
      {""}, {""}, {""},
#line 533 "HTMLCharacterReference.gperf"
      {"SubsetEqual;", "⊆"},
#line 2126 "HTMLCharacterReference.gperf"
      {"upharpoonleft;", "↿"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 353 "HTMLCharacterReference.gperf"
      {"NotDoubleVerticalBar;", "∦"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1429 "HTMLCharacterReference.gperf"
      {"male;", "♂"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 2164 "HTMLCharacterReference.gperf"
      {"varsupsetneqq;", "⫌︀"},
      {""}, {""},
#line 2061 "HTMLCharacterReference.gperf"
      {"timesb;", "⊠"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2130 "HTMLCharacterReference.gperf"
      {"upsih;", "ϒ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1545 "HTMLCharacterReference.gperf"
      {"nmid;", "∤"},
      {""}, {""},
#line 200 "HTMLCharacterReference.gperf"
      {"GreaterEqual;", "≥"},
      {""}, {""},
#line 416 "HTMLCharacterReference.gperf"
      {"Omacr;", "Ō"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1557 "HTMLCharacterReference.gperf"
      {"notnivb;", "⋾"},
#line 2062 "HTMLCharacterReference.gperf"
      {"timesbar;", "⨱"},
      {""}, {""}, {""},
#line 1804 "HTMLCharacterReference.gperf"
      {"rationals;", "ℚ"},
      {""}, {""}, {""}, {""},
#line 1680 "HTMLCharacterReference.gperf"
      {"origof;", "⊶"},
#line 340 "HTMLCharacterReference.gperf"
      {"NegativeThickSpace;", "​"},
      {""}, {""},
#line 1783 "HTMLCharacterReference.gperf"
      {"raemptyv;", "⦳"},
#line 1784 "HTMLCharacterReference.gperf"
      {"rang;", "⟩"},
      {""}, {""}, {""},
#line 615 "HTMLCharacterReference.gperf"
      {"VerticalBar;", "∣"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 2127 "HTMLCharacterReference.gperf"
      {"upharpoonright;", "↾"},
      {""}, {""}, {""}, {""}, {""},
#line 1795 "HTMLCharacterReference.gperf"
      {"rarrfs;", "⤞"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1863 "HTMLCharacterReference.gperf"
      {"rppolint;", "⨒"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 552 "HTMLCharacterReference.gperf"
      {"Tcedil;", "Ţ"},
      {""}, {""}, {""},
#line 804 "HTMLCharacterReference.gperf"
      {"boxhD;", "╥"},
      {""},
#line 1587 "HTMLCharacterReference.gperf"
      {"nsqsube;", "⋢"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2117 "HTMLCharacterReference.gperf"
      {"ulcrop;", "⌏"},
      {""}, {""}, {""}, {""},
#line 2166 "HTMLCharacterReference.gperf"
      {"vartriangleleft;", "⊲"},
#line 1660 "HTMLCharacterReference.gperf"
      {"oline;", "‾"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 607 "HTMLCharacterReference.gperf"
      {"VDash;", "⊫"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2115 "HTMLCharacterReference.gperf"
      {"ulcorn;", "⌜"},
      {""},
#line 2012 "HTMLCharacterReference.gperf"
      {"supdsub;", "⫘"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 2123 "HTMLCharacterReference.gperf"
      {"uopf;", "𝕦"},
      {""}, {""},
#line 1263 "HTMLCharacterReference.gperf"
      {"kappav;", "ϰ"},
#line 1980 "HTMLCharacterReference.gperf"
      {"submult;", "⫁"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 478 "HTMLCharacterReference.gperf"
      {"RightArrowLeftArrow;", "⇄"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1506 "HTMLCharacterReference.gperf"
      {"nequiv;", "≢"},
      {""}, {""},
#line 927 "HTMLCharacterReference.gperf"
      {"curlyvee;", "⋎"},
#line 1885 "HTMLCharacterReference.gperf"
      {"sccue;", "≽"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1985 "HTMLCharacterReference.gperf"
      {"subset;", "⊂"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1658 "HTMLCharacterReference.gperf"
      {"olcir;", "⦾"},
#line 236 "HTMLCharacterReference.gperf"
      {"Implies;", "⇒"},
      {""},
#line 283 "HTMLCharacterReference.gperf"
      {"LeftDownVector;", "⇃"},
      {""}, {""},
#line 284 "HTMLCharacterReference.gperf"
      {"LeftDownVectorBar;", "⥙"},
#line 765 "HTMLCharacterReference.gperf"
      {"blacktriangleright;", "▸"},
#line 1802 "HTMLCharacterReference.gperf"
      {"ratail;", "⤚"},
      {""},
#line 1995 "HTMLCharacterReference.gperf"
      {"succcurlyeq;", "≽"},
      {""}, {""}, {""},
#line 1667 "HTMLCharacterReference.gperf"
      {"oopf;", "𝕠"},
#line 280 "HTMLCharacterReference.gperf"
      {"LeftCeiling;", "⌈"},
      {""},
#line 1786 "HTMLCharacterReference.gperf"
      {"range;", "⦥"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1830 "HTMLCharacterReference.gperf"
      {"rfloor;", "⌋"},
      {""}, {""},
#line 616 "HTMLCharacterReference.gperf"
      {"VerticalLine;", "|"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1943 "HTMLCharacterReference.gperf"
      {"solb;", "⧄"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1499 "HTMLCharacterReference.gperf"
      {"ndash;", "–"},
#line 1573 "HTMLCharacterReference.gperf"
      {"nrightarrow;", "↛"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1630 "HTMLCharacterReference.gperf"
      {"nwarhk;", "⤣"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 401 "HTMLCharacterReference.gperf"
      {"NotVerticalBar;", "∤"},
#line 310 "HTMLCharacterReference.gperf"
      {"Lleftarrow;", "⇚"},
      {""}, {""}, {""}, {""}, {""},
#line 2044 "HTMLCharacterReference.gperf"
      {"telrec;", "⌕"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2107 "HTMLCharacterReference.gperf"
      {"udhar;", "⥮"},
      {""}, {""},
#line 1705 "HTMLCharacterReference.gperf"
      {"permil;", "‰"},
      {""}, {""}, {""},
#line 412 "HTMLCharacterReference.gperf"
      {"Odblac;", "Ő"},
      {""}, {""}, {""}, {""}, {""},
#line 49 "HTMLCharacterReference.gperf"
      {"Barwed;", "⌆"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1933 "HTMLCharacterReference.gperf"
      {"smallsetminus;", "∖"},
#line 1939 "HTMLCharacterReference.gperf"
      {"smte;", "⪬"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 391 "HTMLCharacterReference.gperf"
      {"NotSucceeds;", "⊁"},
      {""}, {""}, {""}, {""},
#line 1030 "HTMLCharacterReference.gperf"
      {"empty;", "∅"},
      {""}, {""}, {""}, {""}, {""},
#line 214 "HTMLCharacterReference.gperf"
      {"HilbertSpace;", "ℋ"},
#line 1220 "HTMLCharacterReference.gperf"
      {"imagpart;", "ℑ"},
      {""}, {""},
#line 1738 "HTMLCharacterReference.gperf"
      {"prcue;", "≼"},
#line 2116 "HTMLCharacterReference.gperf"
      {"ulcorner;", "⌜"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1702 "HTMLCharacterReference.gperf"
      {"pcy;", "п"},
      {""}, {""},
#line 145 "HTMLCharacterReference.gperf"
      {"DownTeeArrow;", "↧"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1925 "HTMLCharacterReference.gperf"
      {"simg;", "⪞"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 500 "HTMLCharacterReference.gperf"
      {"Rrightarrow;", "⇛"},
      {""},
#line 1464 "HTMLCharacterReference.gperf"
      {"multimap;", "⊸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 471 "HTMLCharacterReference.gperf"
      {"ReverseEquilibrium;", "⇋"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 758 "HTMLCharacterReference.gperf"
      {"bigwedge;", "⋀"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 260 "HTMLCharacterReference.gperf"
      {"Kcedil;", "Ķ"},
#line 1621 "HTMLCharacterReference.gperf"
      {"nvinfin;", "⧞"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 762 "HTMLCharacterReference.gperf"
      {"blacktriangle;", "▴"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1986 "HTMLCharacterReference.gperf"
      {"subseteq;", "⊆"},
#line 1987 "HTMLCharacterReference.gperf"
      {"subseteqq;", "⫅"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1659 "HTMLCharacterReference.gperf"
      {"olcross;", "⦻"},
      {""}, {""},
#line 201 "HTMLCharacterReference.gperf"
      {"GreaterEqualLess;", "⋛"},
      {""}, {""},
#line 2119 "HTMLCharacterReference.gperf"
      {"umacr;", "ū"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1879 "HTMLCharacterReference.gperf"
      {"sacute;", "ś"},
      {""}, {""},
#line 1676 "HTMLCharacterReference.gperf"
      {"ordf", "ª"},
#line 1677 "HTMLCharacterReference.gperf"
      {"ordf;", "ª"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 128 "HTMLCharacterReference.gperf"
      {"DoubleRightArrow;", "⇒"},
      {""}, {""}, {""},
#line 1937 "HTMLCharacterReference.gperf"
      {"smile;", "⌣"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1940 "HTMLCharacterReference.gperf"
      {"smtes;", "⪬︀"},
#line 1662 "HTMLCharacterReference.gperf"
      {"omacr;", "ō"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1922 "HTMLCharacterReference.gperf"
      {"simdot;", "⩪"},
      {""}, {""}, {""},
#line 78 "HTMLCharacterReference.gperf"
      {"CirclePlus;", "⊕"},
      {""}, {""}, {""},
#line 1808 "HTMLCharacterReference.gperf"
      {"rbrack;", "]"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1534 "HTMLCharacterReference.gperf"
      {"nleftarrow;", "↚"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 79 "HTMLCharacterReference.gperf"
      {"CircleTimes;", "⊗"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 592 "HTMLCharacterReference.gperf"
      {"UpDownArrow;", "↕"},
      {""},
#line 1785 "HTMLCharacterReference.gperf"
      {"rangd;", "⦒"},
      {""}, {""}, {""}, {""}, {""},
#line 1031 "HTMLCharacterReference.gperf"
      {"emptyset;", "∅"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 67 "HTMLCharacterReference.gperf"
      {"Ccedil", "Ç"},
#line 68 "HTMLCharacterReference.gperf"
      {"Ccedil;", "Ç"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1926 "HTMLCharacterReference.gperf"
      {"simgE;", "⪠"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 33 "HTMLCharacterReference.gperf"
      {"Alpha;", "Α"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 407 "HTMLCharacterReference.gperf"
      {"Oacute", "Ó"},
#line 408 "HTMLCharacterReference.gperf"
      {"Oacute;", "Ó"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1502 "HTMLCharacterReference.gperf"
      {"nearhk;", "⤤"},
      {""},
#line 141 "HTMLCharacterReference.gperf"
      {"DownRightTeeVector;", "⥟"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1707 "HTMLCharacterReference.gperf"
      {"pertenk;", "‱"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 2161 "HTMLCharacterReference.gperf"
      {"varsubsetneq;", "⊊︀"},
      {""}, {""},
#line 1535 "HTMLCharacterReference.gperf"
      {"nleftrightarrow;", "↮"},
#line 2155 "HTMLCharacterReference.gperf"
      {"varphi;", "ϕ"},
#line 1742 "HTMLCharacterReference.gperf"
      {"preccurlyeq;", "≼"},
      {""},
#line 1787 "HTMLCharacterReference.gperf"
      {"rangle;", "⟩"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1988 "HTMLCharacterReference.gperf"
      {"subsetneq;", "⊊"},
#line 1989 "HTMLCharacterReference.gperf"
      {"subsetneqq;", "⫋"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2106 "HTMLCharacterReference.gperf"
      {"udblac;", "ű"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 485 "HTMLCharacterReference.gperf"
      {"RightTee;", "⊢"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 216 "HTMLCharacterReference.gperf"
      {"HorizontalLine;", "─"},
#line 2027 "HTMLCharacterReference.gperf"
      {"supsim;", "⫈"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 487 "HTMLCharacterReference.gperf"
      {"RightTeeVector;", "⥛"},
      {""}, {""}, {""}, {""},
#line 1947 "HTMLCharacterReference.gperf"
      {"spadesuit;", "♠"},
      {""}, {""}, {""}, {""},
#line 1172 "HTMLCharacterReference.gperf"
      {"half;", "½"},
      {""}, {""}, {""},
#line 1892 "HTMLCharacterReference.gperf"
      {"scpolint;", "⨓"},
      {""}, {""}, {""}, {""}, {""},
#line 1976 "HTMLCharacterReference.gperf"
      {"subE;", "⫅"},
      {""},
#line 1214 "HTMLCharacterReference.gperf"
      {"iinfin;", "⧜"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1643 "HTMLCharacterReference.gperf"
      {"odblac;", "ő"},
      {""}, {""}, {""},
#line 988 "HTMLCharacterReference.gperf"
      {"downharpoonright;", "⇂"},
      {""}, {""}, {""},
#line 1732 "HTMLCharacterReference.gperf"
      {"popf;", "𝕡"},
      {""}, {""}, {""}, {""}, {""},
#line 2029 "HTMLCharacterReference.gperf"
      {"supsup;", "⫖"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 432 "HTMLCharacterReference.gperf"
      {"OverBrace;", "⏞"},
      {""}, {""}, {""}, {""}, {""},
#line 2100 "HTMLCharacterReference.gperf"
      {"ubrcy;", "ў"},
#line 1293 "HTMLCharacterReference.gperf"
      {"larrhk;", "↩"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 497 "HTMLCharacterReference.gperf"
      {"Rightarrow;", "⇒"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 1889 "HTMLCharacterReference.gperf"
      {"scnE;", "⪶"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 138 "HTMLCharacterReference.gperf"
      {"DownLeftTeeVector;", "⥞"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 76 "HTMLCharacterReference.gperf"
      {"CircleDot;", "⊙"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 610 "HTMLCharacterReference.gperf"
      {"Vdash;", "⊩"},
      {""}, {""},
#line 1730 "HTMLCharacterReference.gperf"
      {"pm;", "±"},
      {""}, {""},
#line 1474 "HTMLCharacterReference.gperf"
      {"nRightarrow;", "⇏"},
      {""}, {""}, {""}, {""}, {""},
#line 1846 "HTMLCharacterReference.gperf"
      {"ring;", "˚"},
      {""},
#line 1781 "HTMLCharacterReference.gperf"
      {"racute;", "ŕ"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 611 "HTMLCharacterReference.gperf"
      {"Vdashl;", "⫦"},
#line 1625 "HTMLCharacterReference.gperf"
      {"nvltrie;", "⊴⃒"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 493 "HTMLCharacterReference.gperf"
      {"RightUpVector;", "↾"},
#line 20 "HTMLCharacterReference.gperf"
      {"AElig", "Æ"},
#line 21 "HTMLCharacterReference.gperf"
      {"AElig;", "Æ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2162 "HTMLCharacterReference.gperf"
      {"varsubsetneqq;", "⫋︀"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2092 "HTMLCharacterReference.gperf"
      {"twixt;", "≬"},
#line 2000 "HTMLCharacterReference.gperf"
      {"succsim;", "≿"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 424 "HTMLCharacterReference.gperf"
      {"Oslash", "Ø"},
#line 425 "HTMLCharacterReference.gperf"
      {"Oslash;", "Ø"},
      {""}, {""}, {""}, {""}, {""},
#line 1821 "HTMLCharacterReference.gperf"
      {"rdsh;", "↳"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 519 "HTMLCharacterReference.gperf"
      {"SmallCircle;", "∘"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 477 "HTMLCharacterReference.gperf"
      {"RightArrowBar;", "⇥"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 52 "HTMLCharacterReference.gperf"
      {"Bernoullis;", "ℬ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 618 "HTMLCharacterReference.gperf"
      {"VerticalTilde;", "≀"},
      {""}, {""}, {""}, {""},
#line 356 "HTMLCharacterReference.gperf"
      {"NotEqualTilde;", "≂̸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 396 "HTMLCharacterReference.gperf"
      {"NotSupersetEqual;", "⊉"},
      {""}, {""}, {""},
#line 1750 "HTMLCharacterReference.gperf"
      {"prnE;", "⪵"},
      {""}, {""},
#line 1849 "HTMLCharacterReference.gperf"
      {"rlhar;", "⇌"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1076 "HTMLCharacterReference.gperf"
      {"ffllig;", "ﬄ"},
#line 2034 "HTMLCharacterReference.gperf"
      {"swnwar;", "⤪"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 2052 "HTMLCharacterReference.gperf"
      {"thicksim;", "∼"},
      {""}, {""}, {""},
#line 2094 "HTMLCharacterReference.gperf"
      {"twoheadrightarrow;", "↠"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 2118 "HTMLCharacterReference.gperf"
      {"ultri;", "◸"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 433 "HTMLCharacterReference.gperf"
      {"OverBracket;", "⎴"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1317 "HTMLCharacterReference.gperf"
      {"ldrdhar;", "⥧"},
      {""}, {""},
#line 1440 "HTMLCharacterReference.gperf"
      {"mdash;", "—"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1186 "HTMLCharacterReference.gperf"
      {"hkswarow;", "⤦"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 142 "HTMLCharacterReference.gperf"
      {"DownRightVector;", "⇁"},
      {""},
#line 418 "HTMLCharacterReference.gperf"
      {"Omicron;", "Ο"},
#line 143 "HTMLCharacterReference.gperf"
      {"DownRightVectorBar;", "⥗"},
#line 2097 "HTMLCharacterReference.gperf"
      {"uacute", "ú"},
#line 2098 "HTMLCharacterReference.gperf"
      {"uacute;", "ú"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 486 "HTMLCharacterReference.gperf"
      {"RightTeeArrow;", "↦"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 431 "HTMLCharacterReference.gperf"
      {"OverBar;", "‾"},
#line 1487 "HTMLCharacterReference.gperf"
      {"naturals;", "ℕ"},
      {""}, {""},
#line 1264 "HTMLCharacterReference.gperf"
      {"kcedil;", "ķ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 492 "HTMLCharacterReference.gperf"
      {"RightUpTeeVector;", "⥜"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 763 "HTMLCharacterReference.gperf"
      {"blacktriangledown;", "▾"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 357 "HTMLCharacterReference.gperf"
      {"NotExists;", "∄"},
#line 222 "HTMLCharacterReference.gperf"
      {"IJlig;", "Ĳ"},
#line 304 "HTMLCharacterReference.gperf"
      {"LessGreater;", "≶"},
      {""}, {""}, {""}, {""},
#line 1635 "HTMLCharacterReference.gperf"
      {"oacute", "ó"},
#line 1636 "HTMLCharacterReference.gperf"
      {"oacute;", "ó"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1756 "HTMLCharacterReference.gperf"
      {"profsurf;", "⌓"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1700 "HTMLCharacterReference.gperf"
      {"parsl;", "⫽"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 276 "HTMLCharacterReference.gperf"
      {"LeftAngleBracket;", "⟨"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1944 "HTMLCharacterReference.gperf"
      {"solbar;", "⌿"},
      {""}, {""}, {""}, {""},
#line 77 "HTMLCharacterReference.gperf"
      {"CircleMinus;", "⊖"},
      {""}, {""}, {""}, {""},
#line 90 "HTMLCharacterReference.gperf"
      {"CounterClockwiseContourIntegral;", "∳"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2016 "HTMLCharacterReference.gperf"
      {"suphsub;", "⫗"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 882 "HTMLCharacterReference.gperf"
      {"cirscir;", "⧂"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1486 "HTMLCharacterReference.gperf"
      {"natural;", "♮"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 109 "HTMLCharacterReference.gperf"
      {"DiacriticalDot;", "˙"},
      {""}, {""}, {""}, {""}, {""},
#line 235 "HTMLCharacterReference.gperf"
      {"ImaginaryI;", "ⅈ"},
      {""}, {""}, {""}, {""},
#line 590 "HTMLCharacterReference.gperf"
      {"UpArrowBar;", "⤒"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 393 "HTMLCharacterReference.gperf"
      {"NotSucceedsSlantEqual;", "⋡"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1905 "HTMLCharacterReference.gperf"
      {"seswar;", "⤩"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 421 "HTMLCharacterReference.gperf"
      {"OpenCurlyQuote;", "‘"},
      {""}, {""},
#line 2070 "HTMLCharacterReference.gperf"
      {"topfork;", "⫚"},
      {""}, {""},
#line 434 "HTMLCharacterReference.gperf"
      {"OverParenthesis;", "⏜"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 110 "HTMLCharacterReference.gperf"
      {"DiacriticalDoubleAcute;", "˝"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1799 "HTMLCharacterReference.gperf"
      {"rarrsim;", "⥴"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 2041 "HTMLCharacterReference.gperf"
      {"tcedil;", "ţ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1847 "HTMLCharacterReference.gperf"
      {"risingdotseq;", "≓"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1647 "HTMLCharacterReference.gperf"
      {"oelig;", "œ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 330 "HTMLCharacterReference.gperf"
      {"MinusPlus;", "∓"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1912 "HTMLCharacterReference.gperf"
      {"shchcy;", "щ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1698 "HTMLCharacterReference.gperf"
      {"parallel;", "∥"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 475 "HTMLCharacterReference.gperf"
      {"RightAngleBracket;", "⟩"},
      {""}, {""}, {""},
#line 1685 "HTMLCharacterReference.gperf"
      {"oslash", "ø"},
#line 1686 "HTMLCharacterReference.gperf"
      {"oslash;", "ø"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1747 "HTMLCharacterReference.gperf"
      {"precsim;", "≾"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1581 "HTMLCharacterReference.gperf"
      {"nshortparallel;", "∦"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 591 "HTMLCharacterReference.gperf"
      {"UpArrowDownArrow;", "⇅"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1999 "HTMLCharacterReference.gperf"
      {"succnsim;", "⋩"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1664 "HTMLCharacterReference.gperf"
      {"omicron;", "ο"},
      {""}, {""}, {""}, {""},
#line 880 "HTMLCharacterReference.gperf"
      {"cirfnint;", "⨐"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 390 "HTMLCharacterReference.gperf"
      {"NotSubsetEqual;", "⊈"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 2035 "HTMLCharacterReference.gperf"
      {"szlig", "ß"},
#line 2036 "HTMLCharacterReference.gperf"
      {"szlig;", "ß"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1920 "HTMLCharacterReference.gperf"
      {"sigmav;", "ς"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1754 "HTMLCharacterReference.gperf"
      {"profalar;", "⌮"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1731 "HTMLCharacterReference.gperf"
      {"pointint;", "⨕"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1887 "HTMLCharacterReference.gperf"
      {"scedil;", "ş"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 313 "HTMLCharacterReference.gperf"
      {"LongLeftRightArrow;", "⟷"},
      {""}, {""}, {""}, {""}, {""},
#line 1851 "HTMLCharacterReference.gperf"
      {"rmoust;", "⎱"},
      {""},
#line 2090 "HTMLCharacterReference.gperf"
      {"tshcy;", "ћ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2128 "HTMLCharacterReference.gperf"
      {"uplus;", "⊎"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1777 "HTMLCharacterReference.gperf"
      {"rAtail;", "⤜"},
      {""},
#line 2028 "HTMLCharacterReference.gperf"
      {"supsub;", "⫔"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1071 "HTMLCharacterReference.gperf"
      {"fallingdotseq;", "≒"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 64 "HTMLCharacterReference.gperf"
      {"CapitalDifferentialD;", "ⅅ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1670 "HTMLCharacterReference.gperf"
      {"oplus;", "⊕"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 282 "HTMLCharacterReference.gperf"
      {"LeftDownTeeVector;", "⥡"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1990 "HTMLCharacterReference.gperf"
      {"subsim;", "⫇"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 559 "HTMLCharacterReference.gperf"
      {"Tilde;", "∼"},
      {""},
#line 617 "HTMLCharacterReference.gperf"
      {"VerticalSeparator;", "❘"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1435 "HTMLCharacterReference.gperf"
      {"mapstoleft;", "↤"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 491 "HTMLCharacterReference.gperf"
      {"RightUpDownVector;", "⥏"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 1992 "HTMLCharacterReference.gperf"
      {"subsup;", "⫓"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1891 "HTMLCharacterReference.gperf"
      {"scnsim;", "⋩"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1718 "HTMLCharacterReference.gperf"
      {"plankv;", "ℏ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 2049 "HTMLCharacterReference.gperf"
      {"thetasym;", "ϑ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 114 "HTMLCharacterReference.gperf"
      {"DifferentialD;", "ⅆ"},
      {""},
#line 365 "HTMLCharacterReference.gperf"
      {"NotHumpDownHump;", "≎̸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2160 "HTMLCharacterReference.gperf"
      {"varsigma;", "ς"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1719 "HTMLCharacterReference.gperf"
      {"plus;", "+"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1726 "HTMLCharacterReference.gperf"
      {"plusmn", "±"},
#line 1727 "HTMLCharacterReference.gperf"
      {"plusmn;", "±"},
      {""},
#line 1746 "HTMLCharacterReference.gperf"
      {"precnsim;", "⋨"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 764 "HTMLCharacterReference.gperf"
      {"blacktriangleleft;", "◂"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1752 "HTMLCharacterReference.gperf"
      {"prnsim;", "⋨"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1813 "HTMLCharacterReference.gperf"
      {"rcedil;", "ŗ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1936 "HTMLCharacterReference.gperf"
      {"smid;", "∣"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 481 "HTMLCharacterReference.gperf"
      {"RightDownTeeVector;", "⥝"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1725 "HTMLCharacterReference.gperf"
      {"pluse;", "⩲"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1729 "HTMLCharacterReference.gperf"
      {"plustwo;", "⨧"},
      {""}, {""}, {""}, {""},
#line 1844 "HTMLCharacterReference.gperf"
      {"rightsquigarrow;", "↝"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 435 "HTMLCharacterReference.gperf"
      {"PartialD;", "∂"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 82 "HTMLCharacterReference.gperf"
      {"CloseCurlyQuote;", "’"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 987 "HTMLCharacterReference.gperf"
      {"downharpoonleft;", "⇃"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 2055 "HTMLCharacterReference.gperf"
      {"thksim;", "∼"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1032 "HTMLCharacterReference.gperf"
      {"emptyv;", "∅"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 2031 "HTMLCharacterReference.gperf"
      {"swarhk;", "⤦"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 348 "HTMLCharacterReference.gperf"
      {"NonBreakingSpace;", " "},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 81 "HTMLCharacterReference.gperf"
      {"CloseCurlyDoubleQuote;", "”"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1721 "HTMLCharacterReference.gperf"
      {"plusb;", "⊞"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1219 "HTMLCharacterReference.gperf"
      {"imagline;", "ℐ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 382 "HTMLCharacterReference.gperf"
      {"NotRightTriangle;", "⋫"},
      {""}, {""},
#line 383 "HTMLCharacterReference.gperf"
      {"NotRightTriangleBar;", "⧐̸"},
      {""},
#line 384 "HTMLCharacterReference.gperf"
      {"NotRightTriangleEqual;", "⋭"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 2074 "HTMLCharacterReference.gperf"
      {"triangle;", "▵"},
#line 2078 "HTMLCharacterReference.gperf"
      {"triangleq;", "≜"},
      {""}, {""},
#line 2076 "HTMLCharacterReference.gperf"
      {"triangleleft;", "◃"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 2075 "HTMLCharacterReference.gperf"
      {"triangledown;", "▿"},
#line 1841 "HTMLCharacterReference.gperf"
      {"rightleftarrows;", "⇄"},
      {""}, {""},
#line 495 "HTMLCharacterReference.gperf"
      {"RightVector;", "⇀"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2077 "HTMLCharacterReference.gperf"
      {"trianglelefteq;", "⊴"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 482 "HTMLCharacterReference.gperf"
      {"RightDownVector;", "⇂"},
      {""}, {""},
#line 483 "HTMLCharacterReference.gperf"
      {"RightDownVectorBar;", "⥕"},
      {""}, {""}, {""},
#line 1724 "HTMLCharacterReference.gperf"
      {"plusdu;", "⨥"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 392 "HTMLCharacterReference.gperf"
      {"NotSucceedsEqual;", "⪰̸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1723 "HTMLCharacterReference.gperf"
      {"plusdo;", "∔"},
      {""}, {""}, {""}, {""},
#line 381 "HTMLCharacterReference.gperf"
      {"NotReverseElement;", "∌"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 394 "HTMLCharacterReference.gperf"
      {"NotSucceedsTilde;", "≿̸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1842 "HTMLCharacterReference.gperf"
      {"rightleftharpoons;", "⇌"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1935 "HTMLCharacterReference.gperf"
      {"smeparsl;", "⧤"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 420 "HTMLCharacterReference.gperf"
      {"OpenCurlyDoubleQuote;", "“"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 1973 "HTMLCharacterReference.gperf"
      {"straightphi;", "ϕ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1899 "HTMLCharacterReference.gperf"
      {"searhk;", "⤥"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1840 "HTMLCharacterReference.gperf"
      {"rightharpoonup;", "⇀"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1665 "HTMLCharacterReference.gperf"
      {"omid;", "⦶"},
      {""}, {""}, {""}, {""},
#line 417 "HTMLCharacterReference.gperf"
      {"Omega;", "Ω"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1991 "HTMLCharacterReference.gperf"
      {"subsub;", "⫕"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 366 "HTMLCharacterReference.gperf"
      {"NotHumpEqual;", "≏̸"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 1852 "HTMLCharacterReference.gperf"
      {"rmoustache;", "⎱"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 1714 "HTMLCharacterReference.gperf"
      {"pitchfork;", "⋔"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2079 "HTMLCharacterReference.gperf"
      {"triangleright;", "▹"},
      {""}, {""}, {""}, {""}, {""},
#line 1829 "HTMLCharacterReference.gperf"
      {"rfisht;", "⥽"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1839 "HTMLCharacterReference.gperf"
      {"rightharpoondown;", "⇁"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 406 "HTMLCharacterReference.gperf"
      {"OElig;", "Œ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 303 "HTMLCharacterReference.gperf"
      {"LessFullEqual;", "≦"},
      {""},
#line 484 "HTMLCharacterReference.gperf"
      {"RightFloor;", "⌋"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""},
#line 1699 "HTMLCharacterReference.gperf"
      {"parsim;", "⫳"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 1642 "HTMLCharacterReference.gperf"
      {"odash;", "⊝"},
      {""}, {""}, {""}, {""}, {""},
#line 1716 "HTMLCharacterReference.gperf"
      {"planck;", "ℏ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2080 "HTMLCharacterReference.gperf"
      {"trianglerighteq;", "⊵"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 2108 "HTMLCharacterReference.gperf"
      {"ufisht;", "⥾"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 2015 "HTMLCharacterReference.gperf"
      {"suphsol;", "⟉"},
      {""}, {""}, {""}, {""}, {""},
#line 1441 "HTMLCharacterReference.gperf"
      {"measuredangle;", "∡"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 2058 "HTMLCharacterReference.gperf"
      {"tilde;", "˜"},
      {""},
#line 1666 "HTMLCharacterReference.gperf"
      {"ominus;", "⊖"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""},
#line 2120 "HTMLCharacterReference.gperf"
      {"uml", "¨"},
      {""}, {""}, {""}, {""}, {""},
#line 2121 "HTMLCharacterReference.gperf"
      {"uml;", "¨"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 164 "HTMLCharacterReference.gperf"
      {"EmptySmallSquare;", "◻"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 494 "HTMLCharacterReference.gperf"
      {"RightUpVectorBar;", "⥔"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 181 "HTMLCharacterReference.gperf"
      {"FilledSmallSquare;", "◼"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1837 "HTMLCharacterReference.gperf"
      {"rightarrow;", "→"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1663 "HTMLCharacterReference.gperf"
      {"omega;", "ω"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
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
#line 2188 "HTMLCharacterReference.gperf"
      {"vzigzag;", "⦚"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1843 "HTMLCharacterReference.gperf"
      {"rightrightarrows;", "⇉"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
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
#line 241 "HTMLCharacterReference.gperf"
      {"InvisibleTimes;", "⁢"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""},
#line 339 "HTMLCharacterReference.gperf"
      {"NegativeMediumSpace;", "​"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 440 "HTMLCharacterReference.gperf"
      {"PlusMinus;", "±"},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 112 "HTMLCharacterReference.gperf"
      {"DiacriticalTilde;", "˜"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
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
#line 108 "HTMLCharacterReference.gperf"
      {"DiacriticalAcute;", "´"},
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
#line 307 "HTMLCharacterReference.gperf"
      {"LessTilde;", "≲"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
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
#line 1796 "HTMLCharacterReference.gperf"
      {"rarrhk;", "↪"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""},
#line 2093 "HTMLCharacterReference.gperf"
      {"twoheadleftarrow;", "↞"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1755 "HTMLCharacterReference.gperf"
      {"profline;", "⌒"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1720 "HTMLCharacterReference.gperf"
      {"plusacir;", "⨣"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
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
#line 488 "HTMLCharacterReference.gperf"
      {"RightTriangle;", "⊳"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1934 "HTMLCharacterReference.gperf"
      {"smashp;", "⨳"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 240 "HTMLCharacterReference.gperf"
      {"InvisibleComma;", "⁣"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1972 "HTMLCharacterReference.gperf"
      {"straightepsilon;", "ϵ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 479 "HTMLCharacterReference.gperf"
      {"RightCeiling;", "⌉"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
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
#line 560 "HTMLCharacterReference.gperf"
      {"TildeEqual;", "≃"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 496 "HTMLCharacterReference.gperf"
      {"RightVectorBar;", "⥓"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 562 "HTMLCharacterReference.gperf"
      {"TildeTilde;", "≈"},
      {""}, {""},
#line 480 "HTMLCharacterReference.gperf"
      {"RightDoubleBracket;", "⟧"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 111 "HTMLCharacterReference.gperf"
      {"DiacriticalGrave;", "`"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
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
#line 182 "HTMLCharacterReference.gperf"
      {"FilledVerySmallSquare;", "▪"},
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
#line 1717 "HTMLCharacterReference.gperf"
      {"planckh;", "ℎ"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""},
#line 1728 "HTMLCharacterReference.gperf"
      {"plussim;", "⨦"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1722 "HTMLCharacterReference.gperf"
      {"pluscir;", "⨢"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
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
#line 165 "HTMLCharacterReference.gperf"
      {"EmptyVerySmallSquare;", "▫"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
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
#line 1877 "HTMLCharacterReference.gperf"
      {"ruluhar;", "⥨"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 593 "HTMLCharacterReference.gperf"
      {"UpEquilibrium;", "⥮"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
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
#line 503 "HTMLCharacterReference.gperf"
      {"RuleDelayed;", "⧴"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
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
#line 561 "HTMLCharacterReference.gperf"
      {"TildeFullEqual;", "≅"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
#line 1838 "HTMLCharacterReference.gperf"
      {"rightarrowtail;", "↣"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
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
#line 490 "HTMLCharacterReference.gperf"
      {"RightTriangleEqual;", "⊵"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""},
#line 1845 "HTMLCharacterReference.gperf"
      {"rightthreetimes;", "⋌"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
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
#line 489 "HTMLCharacterReference.gperf"
      {"RightTriangleBar;", "⧐"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
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
#line 2087 "HTMLCharacterReference.gperf"
      {"trpezium;", "⏢"},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
      {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""}, {""},
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
#line 1818 "HTMLCharacterReference.gperf"
      {"rdldhar;", "⥩"}
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
#line 2251 "HTMLCharacterReference.gperf"

