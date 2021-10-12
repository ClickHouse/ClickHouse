#include <iostream>
#include <string>
#include <vector>

#include <libprotobuf-mutator/src/libfuzzer/libfuzzer_macro.h>

#include "out.pb.h"

void GenerateWord(const Word&, std::string&, int);

void GenerateSentence(const Sentence& stc, std::string &s, int depth) {
    for (int i = 0; i < stc.words_size(); i++ ) {
        GenerateWord(stc.words(i), s, ++depth);
    }
}
void GenerateWord(const Word& word, std::string &s, int depth) {
    if (depth > 5) return;

    switch (word.value()) {
        case 0: {
            s += " ";
            break;
        }
        case 1: {
            s += " ";
            break;
        }
        case 2: {
            s += " ";
            break;
        }
        case 3: {
            s += ";";
            break;
        }
        case 4: {
            s += "(";
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += ")";
            break;
        }
        case 5: {
            s += "(";
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += ", ";
            if (word.inner().words_size() > 1)                GenerateWord(word.inner().words(1), s, ++depth);
            s += ")";
            break;
        }
        case 6: {
            s += "(";
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += ", ";
            if (word.inner().words_size() > 1)                GenerateWord(word.inner().words(1), s, ++depth);
            s += ", ";
            if (word.inner().words_size() > 2)                GenerateWord(word.inner().words(2), s, ++depth);
            s += ")";
            break;
        }
        case 7: {
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += ", ";
            if (word.inner().words_size() > 1)                GenerateWord(word.inner().words(1), s, ++depth);
            break;
        }
        case 8: {
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += ", ";
            if (word.inner().words_size() > 1)                GenerateWord(word.inner().words(1), s, ++depth);
            s += ", ";
            if (word.inner().words_size() > 2)                GenerateWord(word.inner().words(2), s, ++depth);
            break;
        }
        case 9: {
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += ", ";
            if (word.inner().words_size() > 1)                GenerateWord(word.inner().words(1), s, ++depth);
            s += ", ";
            if (word.inner().words_size() > 2)                GenerateWord(word.inner().words(2), s, ++depth);
            s += ", ";
            if (word.inner().words_size() > 3)                GenerateWord(word.inner().words(3), s, ++depth);
            break;
        }
        case 10: {
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += ", ";
            if (word.inner().words_size() > 1)                GenerateWord(word.inner().words(1), s, ++depth);
            s += ", ";
            if (word.inner().words_size() > 2)                GenerateWord(word.inner().words(2), s, ++depth);
            s += ", ";
            if (word.inner().words_size() > 3)                GenerateWord(word.inner().words(3), s, ++depth);
            s += ", ";
            if (word.inner().words_size() > 4)                GenerateWord(word.inner().words(4), s, ++depth);
            break;
        }
        case 11: {
            s += "[";
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += ", ";
            if (word.inner().words_size() > 1)                GenerateWord(word.inner().words(1), s, ++depth);
            s += "]";
            break;
        }
        case 12: {
            s += "[";
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += ", ";
            if (word.inner().words_size() > 1)                GenerateWord(word.inner().words(1), s, ++depth);
            s += ", ";
            if (word.inner().words_size() > 2)                GenerateWord(word.inner().words(2), s, ++depth);
            s += "]";
            break;
        }
        case 13: {
            s += "[";
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += ", ";
            if (word.inner().words_size() > 1)                GenerateWord(word.inner().words(1), s, ++depth);
            s += ", ";
            if (word.inner().words_size() > 2)                GenerateWord(word.inner().words(2), s, ++depth);
            s += ", ";
            if (word.inner().words_size() > 3)                GenerateWord(word.inner().words(3), s, ++depth);
            s += "]";
            break;
        }
        case 14: {
            s += "[";
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += ", ";
            if (word.inner().words_size() > 1)                GenerateWord(word.inner().words(1), s, ++depth);
            s += ", ";
            if (word.inner().words_size() > 2)                GenerateWord(word.inner().words(2), s, ++depth);
            s += ", ";
            if (word.inner().words_size() > 3)                GenerateWord(word.inner().words(3), s, ++depth);
            s += ", ";
            if (word.inner().words_size() > 4)                GenerateWord(word.inner().words(4), s, ++depth);
            s += "]";
            break;
        }
        case 15: {
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += "(";
            if (word.inner().words_size() > 1)                GenerateWord(word.inner().words(1), s, ++depth);
            s += ")";
            break;
        }
        case 16: {
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += "(";
            if (word.inner().words_size() > 1)                GenerateWord(word.inner().words(1), s, ++depth);
            s += ", ";
            if (word.inner().words_size() > 2)                GenerateWord(word.inner().words(2), s, ++depth);
            s += ")";
            break;
        }
        case 17: {
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += "(";
            if (word.inner().words_size() > 1)                GenerateWord(word.inner().words(1), s, ++depth);
            s += ", ";
            if (word.inner().words_size() > 2)                GenerateWord(word.inner().words(2), s, ++depth);
            s += ", ";
            if (word.inner().words_size() > 3)                GenerateWord(word.inner().words(3), s, ++depth);
            s += ")";
            break;
        }
        case 18: {
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += " as ";
            if (word.inner().words_size() > 1)                GenerateWord(word.inner().words(1), s, ++depth);
            break;
        }
        case 19: {
            s += "SELECT ";
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += " FROM ";
            if (word.inner().words_size() > 1)                GenerateWord(word.inner().words(1), s, ++depth);
            s += " WHERE ";
            if (word.inner().words_size() > 2)                GenerateWord(word.inner().words(2), s, ++depth);
            break;
        }
        case 20: {
            s += "SELECT ";
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += " FROM ";
            if (word.inner().words_size() > 1)                GenerateWord(word.inner().words(1), s, ++depth);
            s += " GROUP BY ";
            if (word.inner().words_size() > 2)                GenerateWord(word.inner().words(2), s, ++depth);
            break;
        }
        case 21: {
            s += "SELECT ";
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += " FROM ";
            if (word.inner().words_size() > 1)                GenerateWord(word.inner().words(1), s, ++depth);
            s += " SORT BY ";
            if (word.inner().words_size() > 2)                GenerateWord(word.inner().words(2), s, ++depth);
            break;
        }
        case 22: {
            s += "SELECT ";
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += " FROM ";
            if (word.inner().words_size() > 1)                GenerateWord(word.inner().words(1), s, ++depth);
            s += " LIMIT ";
            if (word.inner().words_size() > 2)                GenerateWord(word.inner().words(2), s, ++depth);
            break;
        }
        case 23: {
            s += "SELECT ";
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += " FROM ";
            if (word.inner().words_size() > 1)                GenerateWord(word.inner().words(1), s, ++depth);
            s += " JOIN ";
            if (word.inner().words_size() > 2)                GenerateWord(word.inner().words(2), s, ++depth);
            break;
        }
        case 24: {
            s += "SELECT ";
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += " FROM ";
            if (word.inner().words_size() > 1)                GenerateWord(word.inner().words(1), s, ++depth);
            s += " ARRAY JOIN ";
            if (word.inner().words_size() > 2)                GenerateWord(word.inner().words(2), s, ++depth);
            break;
        }
        case 25: {
            s += "SELECT ";
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += " FROM ";
            if (word.inner().words_size() > 1)                GenerateWord(word.inner().words(1), s, ++depth);
            s += " JOIN ";
            if (word.inner().words_size() > 2)                GenerateWord(word.inner().words(2), s, ++depth);
            s += " ON ";
            if (word.inner().words_size() > 3)                GenerateWord(word.inner().words(3), s, ++depth);
            break;
        }
        case 26: {
            s += "SELECT ";
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += " FROM ";
            if (word.inner().words_size() > 1)                GenerateWord(word.inner().words(1), s, ++depth);
            s += " JOIN ";
            if (word.inner().words_size() > 2)                GenerateWord(word.inner().words(2), s, ++depth);
            s += " USING ";
            if (word.inner().words_size() > 3)                GenerateWord(word.inner().words(3), s, ++depth);
            break;
        }
        case 27: {
            s += "SELECT ";
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += " INTO OUTFILE ";
            if (word.inner().words_size() > 1)                GenerateWord(word.inner().words(1), s, ++depth);
            break;
        }
        case 28: {
            s += "WITH ";
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += " AS ";
            if (word.inner().words_size() > 1)                GenerateWord(word.inner().words(1), s, ++depth);
            break;
        }
        case 29: {
            s += "{";
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += ":";
            if (word.inner().words_size() > 1)                GenerateWord(word.inner().words(1), s, ++depth);
            s += "}";
            break;
        }
        case 30: {
            s += "[";
            if (word.inner().words_size() > 0)                GenerateWord(word.inner().words(0), s, ++depth);
            s += ",";
            if (word.inner().words_size() > 1)                GenerateWord(word.inner().words(1), s, ++depth);
            s += "]";
            break;
        }
        case 31: {
            s += "[]";
            break;
        }
        case 32: {
            s += " x ";
            break;
        }
        case 33: {
            s += "x";
            break;
        }
        case 34: {
            s += " `x` ";
            break;
        }
        case 35: {
            s += "`x`";
            break;
        }
        case 36: {
            s += " \"value\" ";
            break;
        }
        case 37: {
            s += "\"value\"";
            break;
        }
        case 38: {
            s += " 0 ";
            break;
        }
        case 39: {
            s += "0";
            break;
        }
        case 40: {
            s += "1";
            break;
        }
        case 41: {
            s += "2";
            break;
        }
        case 42: {
            s += "123123123123123123";
            break;
        }
        case 43: {
            s += "182374019873401982734091873420923123123123123123";
            break;
        }
        case 44: {
            s += "1e-1";
            break;
        }
        case 45: {
            s += "1.1";
            break;
        }
        case 46: {
            s += "\"\"";
            break;
        }
        case 47: {
            s += " '../../../../../../../../../etc/passwd' ";
            break;
        }
        case 48: {
            s += "/";
            break;
        }
        case 49: {
            s += "=";
            break;
        }
        case 50: {
            s += "==";
            break;
        }
        case 51: {
            s += "!=";
            break;
        }
        case 52: {
            s += "<>";
            break;
        }
        case 53: {
            s += "<";
            break;
        }
        case 54: {
            s += "<=";
            break;
        }
        case 55: {
            s += ">";
            break;
        }
        case 56: {
            s += ">=";
            break;
        }
        case 57: {
            s += "<<";
            break;
        }
        case 58: {
            s += "|<<";
            break;
        }
        case 59: {
            s += "&";
            break;
        }
        case 60: {
            s += "|";
            break;
        }
        case 61: {
            s += "||";
            break;
        }
        case 62: {
            s += "<|";
            break;
        }
        case 63: {
            s += "|>";
            break;
        }
        case 64: {
            s += "+";
            break;
        }
        case 65: {
            s += "-";
            break;
        }
        case 66: {
            s += "~";
            break;
        }
        case 67: {
            s += "*";
            break;
        }
        case 68: {
            s += "/";
            break;
        }
        case 69: {
            s += "\\";
            break;
        }
        case 70: {
            s += "%";
            break;
        }
        case 71: {
            s += "";
            break;
        }
        case 72: {
            s += ".";
            break;
        }
        case 73: {
            s += ",";
            break;
        }
        case 74: {
            s += ",";
            break;
        }
        case 75: {
            s += ",";
            break;
        }
        case 76: {
            s += ",";
            break;
        }
        case 77: {
            s += ",";
            break;
        }
        case 78: {
            s += ",";
            break;
        }
        case 79: {
            s += "(";
            break;
        }
        case 80: {
            s += ")";
            break;
        }
        case 81: {
            s += "(";
            break;
        }
        case 82: {
            s += ")";
            break;
        }
        case 83: {
            s += "(";
            break;
        }
        case 84: {
            s += ")";
            break;
        }
        case 85: {
            s += "(";
            break;
        }
        case 86: {
            s += ")";
            break;
        }
        case 87: {
            s += "(";
            break;
        }
        case 88: {
            s += ")";
            break;
        }
        case 89: {
            s += "(";
            break;
        }
        case 90: {
            s += ")";
            break;
        }
        case 91: {
            s += "?";
            break;
        }
        case 92: {
            s += ":";
            break;
        }
        case 93: {
            s += "@";
            break;
        }
        case 94: {
            s += "@@";
            break;
        }
        case 95: {
            s += "$";
            break;
        }
        case 96: {
            s += "\"";
            break;
        }
        case 97: {
            s += "`";
            break;
        }
        case 98: {
            s += "{";
            break;
        }
        case 99: {
            s += "}";
            break;
        }
        case 100: {
            s += "^";
            break;
        }
        case 101: {
            s += "::";
            break;
        }
        case 102: {
            s += "->";
            break;
        }
        case 103: {
            s += "]";
            break;
        }
        case 104: {
            s += "[";
            break;
        }
        case 105: {
            s += " ADD ";
            break;
        }
        case 106: {
            s += " ADD COLUMN ";
            break;
        }
        case 107: {
            s += " ADD CONSTRAINT ";
            break;
        }
        case 108: {
            s += " ADD INDEX ";
            break;
        }
        case 109: {
            s += " AFTER ";
            break;
        }
        case 110: {
            s += " AggregateFunction ";
            break;
        }
        case 111: {
            s += " aggThrow ";
            break;
        }
        case 112: {
            s += " ALL ";
            break;
        }
        case 113: {
            s += " ALTER LIVE VIEW ";
            break;
        }
        case 114: {
            s += " ALTER TABLE ";
            break;
        }
        case 115: {
            s += " and ";
            break;
        }
        case 116: {
            s += " ANTI ";
            break;
        }
        case 117: {
            s += " any ";
            break;
        }
        case 118: {
            s += " anyHeavy ";
            break;
        }
        case 119: {
            s += " anyLast ";
            break;
        }
        case 120: {
            s += " argMax ";
            break;
        }
        case 121: {
            s += " argMin ";
            break;
        }
        case 122: {
            s += " array ";
            break;
        }
        case 123: {
            s += " Array ";
            break;
        }
        case 124: {
            s += " arrayAll ";
            break;
        }
        case 125: {
            s += " arrayAUC ";
            break;
        }
        case 126: {
            s += " arrayCompact ";
            break;
        }
        case 127: {
            s += " arrayConcat ";
            break;
        }
        case 128: {
            s += " arrayCount ";
            break;
        }
        case 129: {
            s += " arrayCumSum ";
            break;
        }
        case 130: {
            s += " arrayCumSumNonNegative ";
            break;
        }
        case 131: {
            s += " arrayDifference ";
            break;
        }
        case 132: {
            s += " arrayDistinct ";
            break;
        }
        case 133: {
            s += " arrayElement ";
            break;
        }
        case 134: {
            s += " arrayEnumerate ";
            break;
        }
        case 135: {
            s += " arrayEnumerateDense ";
            break;
        }
        case 136: {
            s += " arrayEnumerateDenseRanked ";
            break;
        }
        case 137: {
            s += " arrayEnumerateUniq ";
            break;
        }
        case 138: {
            s += " arrayEnumerateUniqRanked ";
            break;
        }
        case 139: {
            s += " arrayExists ";
            break;
        }
        case 140: {
            s += " arrayFill ";
            break;
        }
        case 141: {
            s += " arrayFilter ";
            break;
        }
        case 142: {
            s += " arrayFirst ";
            break;
        }
        case 143: {
            s += " arrayFirstIndex ";
            break;
        }
        case 144: {
            s += " arrayFlatten ";
            break;
        }
        case 145: {
            s += " arrayIntersect ";
            break;
        }
        case 146: {
            s += " arrayJoin ";
            break;
        }
        case 147: {
            s += " ARRAY JOIN ";
            break;
        }
        case 148: {
            s += " arrayMap ";
            break;
        }
        case 149: {
            s += " arrayPopBack ";
            break;
        }
        case 150: {
            s += " arrayPopFront ";
            break;
        }
        case 151: {
            s += " arrayPushBack ";
            break;
        }
        case 152: {
            s += " arrayPushFront ";
            break;
        }
        case 153: {
            s += " arrayReduce ";
            break;
        }
        case 154: {
            s += " arrayReduceInRanges ";
            break;
        }
        case 155: {
            s += " arrayResize ";
            break;
        }
        case 156: {
            s += " arrayReverse ";
            break;
        }
        case 157: {
            s += " arrayReverseFill ";
            break;
        }
        case 158: {
            s += " arrayReverseSort ";
            break;
        }
        case 159: {
            s += " arrayReverseSplit ";
            break;
        }
        case 160: {
            s += " arraySlice ";
            break;
        }
        case 161: {
            s += " arraySort ";
            break;
        }
        case 162: {
            s += " arraySplit ";
            break;
        }
        case 163: {
            s += " arraySum ";
            break;
        }
        case 164: {
            s += " arrayUniq ";
            break;
        }
        case 165: {
            s += " arrayWithConstant ";
            break;
        }
        case 166: {
            s += " arrayZip ";
            break;
        }
        case 167: {
            s += " AS ";
            break;
        }
        case 168: {
            s += " ASC ";
            break;
        }
        case 169: {
            s += " ASCENDING ";
            break;
        }
        case 170: {
            s += " ASOF ";
            break;
        }
        case 171: {
            s += " AST ";
            break;
        }
        case 172: {
            s += " ATTACH ";
            break;
        }
        case 173: {
            s += " ATTACH PART ";
            break;
        }
        case 174: {
            s += " ATTACH PARTITION ";
            break;
        }
        case 175: {
            s += " avg ";
            break;
        }
        case 176: {
            s += " avgWeighted ";
            break;
        }
        case 177: {
            s += " basename ";
            break;
        }
        case 178: {
            s += " BETWEEN ";
            break;
        }
        case 179: {
            s += " BOTH ";
            break;
        }
        case 180: {
            s += " boundingRatio ";
            break;
        }
        case 181: {
            s += " BY ";
            break;
        }
        case 182: {
            s += " CAST ";
            break;
        }
        case 183: {
            s += " categoricalInformationValue ";
            break;
        }
        case 184: {
            s += " CHECK ";
            break;
        }
        case 185: {
            s += " CHECK TABLE ";
            break;
        }
        case 186: {
            s += " CLEAR COLUMN ";
            break;
        }
        case 187: {
            s += " CLEAR INDEX ";
            break;
        }
        case 188: {
            s += " COLLATE ";
            break;
        }
        case 189: {
            s += " COLUMNS ";
            break;
        }
        case 190: {
            s += " COMMENT COLUMN ";
            break;
        }
        case 191: {
            s += " CONSTRAINT ";
            break;
        }
        case 192: {
            s += " corr ";
            break;
        }
        case 193: {
            s += " corrStable ";
            break;
        }
        case 194: {
            s += " count ";
            break;
        }
        case 195: {
            s += " countEqual ";
            break;
        }
        case 196: {
            s += " covarPop ";
            break;
        }
        case 197: {
            s += " covarPopStable ";
            break;
        }
        case 198: {
            s += " covarSamp ";
            break;
        }
        case 199: {
            s += " covarSampStable ";
            break;
        }
        case 200: {
            s += " CREATE ";
            break;
        }
        case 201: {
            s += " CROSS ";
            break;
        }
        case 202: {
            s += " CUBE ";
            break;
        }
        case 203: {
            s += " cutFragment ";
            break;
        }
        case 204: {
            s += " cutQueryString ";
            break;
        }
        case 205: {
            s += " cutQueryStringAndFragment ";
            break;
        }
        case 206: {
            s += " cutToFirstSignificantSubdomain ";
            break;
        }
        case 207: {
            s += " cutURLParameter ";
            break;
        }
        case 208: {
            s += " cutWWW ";
            break;
        }
        case 209: {
            s += " D ";
            break;
        }
        case 210: {
            s += " DATABASE ";
            break;
        }
        case 211: {
            s += " DATABASES ";
            break;
        }
        case 212: {
            s += " Date ";
            break;
        }
        case 213: {
            s += " DATE ";
            break;
        }
        case 214: {
            s += " DATE_ADD ";
            break;
        }
        case 215: {
            s += " DATEADD ";
            break;
        }
        case 216: {
            s += " DATE_DIFF ";
            break;
        }
        case 217: {
            s += " DATEDIFF ";
            break;
        }
        case 218: {
            s += " DATE_SUB ";
            break;
        }
        case 219: {
            s += " DATESUB ";
            break;
        }
        case 220: {
            s += " DateTime ";
            break;
        }
        case 221: {
            s += " DateTime64 ";
            break;
        }
        case 222: {
            s += " DAY ";
            break;
        }
        case 223: {
            s += " DD ";
            break;
        }
        case 224: {
            s += " Decimal ";
            break;
        }
        case 225: {
            s += " Decimal128 ";
            break;
        }
        case 226: {
            s += " Decimal32 ";
            break;
        }
        case 227: {
            s += " Decimal64 ";
            break;
        }
        case 228: {
            s += " decodeURLComponent ";
            break;
        }
        case 229: {
            s += " DEDUPLICATE ";
            break;
        }
        case 230: {
            s += " DELETE ";
            break;
        }
        case 231: {
            s += " DELETE WHERE ";
            break;
        }
        case 232: {
            s += " DESC ";
            break;
        }
        case 233: {
            s += " DESCENDING ";
            break;
        }
        case 234: {
            s += " DESCRIBE ";
            break;
        }
        case 235: {
            s += " DETACH ";
            break;
        }
        case 236: {
            s += " DETACH PARTITION ";
            break;
        }
        case 237: {
            s += " DICTIONARIES ";
            break;
        }
        case 238: {
            s += " DICTIONARY ";
            break;
        }
        case 239: {
            s += " DISTINCT ";
            break;
        }
        case 240: {
            s += " domain ";
            break;
        }
        case 241: {
            s += " domainWithoutWWW ";
            break;
        }
        case 242: {
            s += " DROP ";
            break;
        }
        case 243: {
            s += " DROP COLUMN ";
            break;
        }
        case 244: {
            s += " DROP CONSTRAINT ";
            break;
        }
        case 245: {
            s += " DROP DETACHED PART ";
            break;
        }
        case 246: {
            s += " DROP DETACHED PARTITION ";
            break;
        }
        case 247: {
            s += " DROP INDEX ";
            break;
        }
        case 248: {
            s += " DROP PARTITION ";
            break;
        }
        case 249: {
            s += " emptyArrayToSingle ";
            break;
        }
        case 250: {
            s += " ENGINE ";
            break;
        }
        case 251: {
            s += " entropy ";
            break;
        }
        case 252: {
            s += " Enum ";
            break;
        }
        case 253: {
            s += " Enum16 ";
            break;
        }
        case 254: {
            s += " Enum8 ";
            break;
        }
        case 255: {
            s += " EVENTS ";
            break;
        }
        case 256: {
            s += " EXCHANGE TABLES ";
            break;
        }
        case 257: {
            s += " EXISTS ";
            break;
        }
        case 258: {
            s += " EXTRACT ";
            break;
        }
        case 259: {
            s += " extractURLParameter ";
            break;
        }
        case 260: {
            s += " extractURLParameterNames ";
            break;
        }
        case 261: {
            s += " extractURLParameters ";
            break;
        }
        case 262: {
            s += " FETCH PARTITION ";
            break;
        }
        case 263: {
            s += " FETCH PART ";
            break;
        }
        case 264: {
            s += " FINAL ";
            break;
        }
        case 265: {
            s += " FIRST ";
            break;
        }
        case 266: {
            s += " firstSignificantSubdomain ";
            break;
        }
        case 267: {
            s += " FixedString ";
            break;
        }
        case 268: {
            s += " Float32 ";
            break;
        }
        case 269: {
            s += " Float64 ";
            break;
        }
        case 270: {
            s += " FOR ";
            break;
        }
        case 271: {
            s += " ForEach ";
            break;
        }
        case 272: {
            s += " FORMAT ";
            break;
        }
        case 273: {
            s += " fragment ";
            break;
        }
        case 274: {
            s += " FREEZE ";
            break;
        }
        case 275: {
            s += " FROM ";
            break;
        }
        case 276: {
            s += " FULL ";
            break;
        }
        case 277: {
            s += " FUNCTION ";
            break;
        }
        case 278: {
            s += " __getScalar ";
            break;
        }
        case 279: {
            s += " GLOBAL ";
            break;
        }
        case 280: {
            s += " GRANULARITY ";
            break;
        }
        case 281: {
            s += " groupArray ";
            break;
        }
        case 282: {
            s += " groupArrayInsertAt ";
            break;
        }
        case 283: {
            s += " groupArrayMovingAvg ";
            break;
        }
        case 284: {
            s += " groupArrayMovingSum ";
            break;
        }
        case 285: {
            s += " groupArraySample ";
            break;
        }
        case 286: {
            s += " groupBitAnd ";
            break;
        }
        case 287: {
            s += " groupBitmap ";
            break;
        }
        case 288: {
            s += " groupBitmapAnd ";
            break;
        }
        case 289: {
            s += " groupBitmapOr ";
            break;
        }
        case 290: {
            s += " groupBitmapXor ";
            break;
        }
        case 291: {
            s += " groupBitOr ";
            break;
        }
        case 292: {
            s += " groupBitXor ";
            break;
        }
        case 293: {
            s += " GROUP BY ";
            break;
        }
        case 294: {
            s += " groupUniqArray ";
            break;
        }
        case 295: {
            s += " has ";
            break;
        }
        case 296: {
            s += " hasAll ";
            break;
        }
        case 297: {
            s += " hasAny ";
            break;
        }
        case 298: {
            s += " HAVING ";
            break;
        }
        case 299: {
            s += " HH ";
            break;
        }
        case 300: {
            s += " histogram ";
            break;
        }
        case 301: {
            s += " HOUR ";
            break;
        }
        case 302: {
            s += " ID ";
            break;
        }
        case 303: {
            s += " if ";
            break;
        }
        case 304: {
            s += " IF EXISTS ";
            break;
        }
        case 305: {
            s += " IF NOT EXISTS ";
            break;
        }
        case 306: {
            s += " IN ";
            break;
        }
        case 307: {
            s += " INDEX ";
            break;
        }
        case 308: {
            s += " indexOf ";
            break;
        }
        case 309: {
            s += " INNER ";
            break;
        }
        case 310: {
            s += " IN PARTITION ";
            break;
        }
        case 311: {
            s += " INSERT INTO ";
            break;
        }
        case 312: {
            s += " Int16 ";
            break;
        }
        case 313: {
            s += " Int32 ";
            break;
        }
        case 314: {
            s += " Int64 ";
            break;
        }
        case 315: {
            s += " Int8 ";
            break;
        }
        case 316: {
            s += " INTERVAL ";
            break;
        }
        case 317: {
            s += " IntervalDay ";
            break;
        }
        case 318: {
            s += " IntervalHour ";
            break;
        }
        case 319: {
            s += " IntervalMinute ";
            break;
        }
        case 320: {
            s += " IntervalMonth ";
            break;
        }
        case 321: {
            s += " IntervalQuarter ";
            break;
        }
        case 322: {
            s += " IntervalSecond ";
            break;
        }
        case 323: {
            s += " IntervalWeek ";
            break;
        }
        case 324: {
            s += " IntervalYear ";
            break;
        }
        case 325: {
            s += " INTO OUTFILE ";
            break;
        }
        case 326: {
            s += " JOIN ";
            break;
        }
        case 327: {
            s += " kurtPop ";
            break;
        }
        case 328: {
            s += " kurtSamp ";
            break;
        }
        case 329: {
            s += " LAST ";
            break;
        }
        case 330: {
            s += " LAYOUT ";
            break;
        }
        case 331: {
            s += " LEADING ";
            break;
        }
        case 332: {
            s += " LEFT ";
            break;
        }
        case 333: {
            s += " LEFT ARRAY JOIN ";
            break;
        }
        case 334: {
            s += " length ";
            break;
        }
        case 335: {
            s += " LIFETIME ";
            break;
        }
        case 336: {
            s += " LIKE ";
            break;
        }
        case 337: {
            s += " LIMIT ";
            break;
        }
        case 338: {
            s += " LIVE ";
            break;
        }
        case 339: {
            s += " LOCAL ";
            break;
        }
        case 340: {
            s += " LowCardinality ";
            break;
        }
        case 341: {
            s += " LTRIM ";
            break;
        }
        case 342: {
            s += " M ";
            break;
        }
        case 343: {
            s += " MATERIALIZED ";
            break;
        }
        case 344: {
            s += " MATERIALIZE INDEX ";
            break;
        }
        case 345: {
            s += " MATERIALIZE TTL ";
            break;
        }
        case 346: {
            s += " max ";
            break;
        }
        case 347: {
            s += " maxIntersections ";
            break;
        }
        case 348: {
            s += " maxIntersectionsPosition ";
            break;
        }
        case 349: {
            s += " Merge ";
            break;
        }
        case 350: {
            s += " MI ";
            break;
        }
        case 351: {
            s += " min ";
            break;
        }
        case 352: {
            s += " MINUTE ";
            break;
        }
        case 353: {
            s += " MM ";
            break;
        }
        case 354: {
            s += " MODIFY ";
            break;
        }
        case 355: {
            s += " MODIFY COLUMN ";
            break;
        }
        case 356: {
            s += " MODIFY ORDER BY ";
            break;
        }
        case 357: {
            s += " MODIFY QUERY ";
            break;
        }
        case 358: {
            s += " MODIFY SETTING ";
            break;
        }
        case 359: {
            s += " MODIFY TTL ";
            break;
        }
        case 360: {
            s += " MONTH ";
            break;
        }
        case 361: {
            s += " MOVE PART ";
            break;
        }
        case 362: {
            s += " MOVE PARTITION ";
            break;
        }
        case 363: {
            s += " movingXXX ";
            break;
        }
        case 364: {
            s += " N ";
            break;
        }
        case 365: {
            s += " NAME ";
            break;
        }
        case 366: {
            s += " Nested ";
            break;
        }
        case 367: {
            s += " NO DELAY ";
            break;
        }
        case 368: {
            s += " NONE ";
            break;
        }
        case 369: {
            s += " not ";
            break;
        }
        case 370: {
            s += " nothing ";
            break;
        }
        case 371: {
            s += " Nothing ";
            break;
        }
        case 372: {
            s += " Null ";
            break;
        }
        case 373: {
            s += " Nullable ";
            break;
        }
        case 374: {
            s += " NULLS ";
            break;
        }
        case 375: {
            s += " OFFSET ";
            break;
        }
        case 376: {
            s += " ON ";
            break;
        }
        case 377: {
            s += " ONLY ";
            break;
        }
        case 378: {
            s += " OPTIMIZE TABLE ";
            break;
        }
        case 379: {
            s += " ORDER BY ";
            break;
        }
        case 380: {
            s += " OR REPLACE ";
            break;
        }
        case 381: {
            s += " OUTER ";
            break;
        }
        case 382: {
            s += " PARTITION ";
            break;
        }
        case 383: {
            s += " PARTITION BY ";
            break;
        }
        case 384: {
            s += " path ";
            break;
        }
        case 385: {
            s += " pathFull ";
            break;
        }
        case 386: {
            s += " POPULATE ";
            break;
        }
        case 387: {
            s += " PREWHERE ";
            break;
        }
        case 388: {
            s += " PRIMARY KEY ";
            break;
        }
        case 389: {
            s += " protocol ";
            break;
        }
        case 390: {
            s += " Q ";
            break;
        }
        case 391: {
            s += " QQ ";
            break;
        }
        case 392: {
            s += " QUARTER ";
            break;
        }
        case 393: {
            s += " queryString ";
            break;
        }
        case 394: {
            s += " queryStringAndFragment ";
            break;
        }
        case 395: {
            s += " range ";
            break;
        }
        case 396: {
            s += " REFRESH ";
            break;
        }
        case 397: {
            s += " RENAME COLUMN ";
            break;
        }
        case 398: {
            s += " RENAME TABLE ";
            break;
        }
        case 399: {
            s += " REPLACE PARTITION ";
            break;
        }
        case 400: {
            s += " Resample ";
            break;
        }
        case 401: {
            s += " RESUME ";
            break;
        }
        case 402: {
            s += " retention ";
            break;
        }
        case 403: {
            s += " RIGHT ";
            break;
        }
        case 404: {
            s += " ROLLUP ";
            break;
        }
        case 405: {
            s += " RTRIM ";
            break;
        }
        case 406: {
            s += " S ";
            break;
        }
        case 407: {
            s += " SAMPLE ";
            break;
        }
        case 408: {
            s += " SAMPLE BY ";
            break;
        }
        case 409: {
            s += " SECOND ";
            break;
        }
        case 410: {
            s += " SELECT ";
            break;
        }
        case 411: {
            s += " SEMI ";
            break;
        }
        case 412: {
            s += " sequenceCount ";
            break;
        }
        case 413: {
            s += " sequenceMatch ";
            break;
        }
        case 414: {
            s += " SET ";
            break;
        }
        case 415: {
            s += " SETTINGS ";
            break;
        }
        case 416: {
            s += " SHOW ";
            break;
        }
        case 417: {
            s += " SHOW PROCESSLIST ";
            break;
        }
        case 418: {
            s += " simpleLinearRegression ";
            break;
        }
        case 419: {
            s += " skewPop ";
            break;
        }
        case 420: {
            s += " skewSamp ";
            break;
        }
        case 421: {
            s += " SOURCE ";
            break;
        }
        case 422: {
            s += " SQL_TSI_DAY ";
            break;
        }
        case 423: {
            s += " SQL_TSI_HOUR ";
            break;
        }
        case 424: {
            s += " SQL_TSI_MINUTE ";
            break;
        }
        case 425: {
            s += " SQL_TSI_MONTH ";
            break;
        }
        case 426: {
            s += " SQL_TSI_QUARTER ";
            break;
        }
        case 427: {
            s += " SQL_TSI_SECOND ";
            break;
        }
        case 428: {
            s += " SQL_TSI_WEEK ";
            break;
        }
        case 429: {
            s += " SQL_TSI_YEAR ";
            break;
        }
        case 430: {
            s += " SS ";
            break;
        }
        case 431: {
            s += " State ";
            break;
        }
        case 432: {
            s += " stddevPop ";
            break;
        }
        case 433: {
            s += " stddevPopStable ";
            break;
        }
        case 434: {
            s += " stddevSamp ";
            break;
        }
        case 435: {
            s += " stddevSampStable ";
            break;
        }
        case 436: {
            s += " STEP ";
            break;
        }
        case 437: {
            s += " stochasticLinearRegression ";
            break;
        }
        case 438: {
            s += " stochasticLogisticRegression ";
            break;
        }
        case 439: {
            s += " String ";
            break;
        }
        case 440: {
            s += " SUBSTRING ";
            break;
        }
        case 441: {
            s += " sum ";
            break;
        }
        case 442: {
            s += " sumKahan ";
            break;
        }
        case 443: {
            s += " sumMap ";
            break;
        }
        case 444: {
            s += " sumMapFiltered ";
            break;
        }
        case 445: {
            s += " sumMapFilteredWithOverflow ";
            break;
        }
        case 446: {
            s += " sumMapWithOverflow ";
            break;
        }
        case 447: {
            s += " sumWithOverflow ";
            break;
        }
        case 448: {
            s += " SUSPEND ";
            break;
        }
        case 449: {
            s += " TABLE ";
            break;
        }
        case 450: {
            s += " TABLES ";
            break;
        }
        case 451: {
            s += " TEMPORARY ";
            break;
        }
        case 452: {
            s += " TIMESTAMP ";
            break;
        }
        case 453: {
            s += " TIMESTAMP_ADD ";
            break;
        }
        case 454: {
            s += " TIMESTAMPADD ";
            break;
        }
        case 455: {
            s += " TIMESTAMP_DIFF ";
            break;
        }
        case 456: {
            s += " TIMESTAMPDIFF ";
            break;
        }
        case 457: {
            s += " TIMESTAMP_SUB ";
            break;
        }
        case 458: {
            s += " TIMESTAMPSUB ";
            break;
        }
        case 459: {
            s += " TO ";
            break;
        }
        case 460: {
            s += " TO DISK ";
            break;
        }
        case 461: {
            s += " TOP ";
            break;
        }
        case 462: {
            s += " topK ";
            break;
        }
        case 463: {
            s += " topKWeighted ";
            break;
        }
        case 464: {
            s += " topLevelDomain ";
            break;
        }
        case 465: {
            s += " TO TABLE ";
            break;
        }
        case 466: {
            s += " TOTALS ";
            break;
        }
        case 467: {
            s += " TO VOLUME ";
            break;
        }
        case 468: {
            s += " TRAILING ";
            break;
        }
        case 469: {
            s += " TRIM ";
            break;
        }
        case 470: {
            s += " TRUNCATE ";
            break;
        }
        case 471: {
            s += " TTL ";
            break;
        }
        case 472: {
            s += " Tuple ";
            break;
        }
        case 473: {
            s += " TYPE ";
            break;
        }
        case 474: {
            s += " UInt16 ";
            break;
        }
        case 475: {
            s += " UInt32 ";
            break;
        }
        case 476: {
            s += " UInt64 ";
            break;
        }
        case 477: {
            s += " UInt8 ";
            break;
        }
        case 478: {
            s += " uniq ";
            break;
        }
        case 479: {
            s += " uniqCombined ";
            break;
        }
        case 480: {
            s += " uniqCombined64 ";
            break;
        }
        case 481: {
            s += " uniqExact ";
            break;
        }
        case 482: {
            s += " uniqHLL12 ";
            break;
        }
        case 483: {
            s += " uniqUpTo ";
            break;
        }
        case 484: {
            s += " UPDATE ";
            break;
        }
        case 485: {
            s += " URLHierarchy ";
            break;
        }
        case 486: {
            s += " URLPathHierarchy ";
            break;
        }
        case 487: {
            s += " USE ";
            break;
        }
        case 488: {
            s += " USING ";
            break;
        }
        case 489: {
            s += " UUID ";
            break;
        }
        case 490: {
            s += " VALUES ";
            break;
        }
        case 491: {
            s += " varPop ";
            break;
        }
        case 492: {
            s += " varPopStable ";
            break;
        }
        case 493: {
            s += " varSamp ";
            break;
        }
        case 494: {
            s += " varSampStable ";
            break;
        }
        case 495: {
            s += " VIEW ";
            break;
        }
        case 496: {
            s += " WATCH ";
            break;
        }
        case 497: {
            s += " WEEK ";
            break;
        }
        case 498: {
            s += " WHERE ";
            break;
        }
        case 499: {
            s += " windowFunnel ";
            break;
        }
        case 500: {
            s += " WITH ";
            break;
        }
        case 501: {
            s += " WITH FILL ";
            break;
        }
        case 502: {
            s += " WITH TIES ";
            break;
        }
        case 503: {
            s += " WK ";
            break;
        }
        case 504: {
            s += " WW ";
            break;
        }
        case 505: {
            s += " YEAR ";
            break;
        }
        case 506: {
            s += " YY ";
            break;
        }
        case 507: {
            s += " YYYY ";
            break;
        }
        default: break;
    }
}
