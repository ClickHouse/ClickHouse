#include <algorithm>
#include <cctype>
#include <cstdint>
#include <unordered_map>

#include <fmt/format.h>

#include <Client/BuzzHouse/AST/SQLProtoStr.h>
#include <base/wide_integer.h>
#include <base/wide_integer_to_string.h>

#define CONV_FN(TYPE, VAR_NAME) static void TYPE##ToString(String & ret, const TYPE &(VAR_NAME))
#define CONV_FN_QUOTE(TYPE, VAR_NAME) static void TYPE##ToString(String & ret, const uint32_t quote, const TYPE &(VAR_NAME))

namespace BuzzHouse
{

static constexpr char hexDigits[] = "0123456789ABCDEF";

/// Append a single-quoted SQL string literal to ret, doubling any embedded single quotes.
static void appendSQLStringLiteral(String & ret, const String & s)
{
    ret += '\'';
    for (const char c : s)
    {
        if (c == '\'')
            ret += '\'';
        ret += c;
    }
    ret += '\'';
}

CONV_FN(Expr, expr);
CONV_FN(Select, select);

/// Shortest round-trip form, with `.0` added to integral values so the literal stays a `Float64`
static String doubleToSQLString(const double v)
{
    String res = fmt::format("{}", v);

    if (res.find_first_not_of("-0123456789") == String::npos)
    {
        res += ".0";
    }
    return res;
}

/// Inside backquotes the lexer treats both '`' and '\' as escape chars, each nesting level doubles them
CONV_FN_QUOTE(Column, col)
{
    for (const auto & c : col.column())
    {
        if (c == '`' || c == '\\')
        {
            ret.append(static_cast<size_t>(1) << quote, c);
        }
        else
        {
            ret += c;
        }
    }
}

CONV_FN_QUOTE(ColumnPath, ic)
{
    const uint32_t nquote = quote + 1;
    const uint32_t quotes = (1 << quote);

    for (uint32_t i = 0; i < quotes; i++)
    {
        ret += "`";
    }
    ColumnToString(ret, nquote, ic.col());
    for (int i = 0; i < ic.sub_cols_size(); i++)
    {
        ret += ".";
        ColumnToString(ret, nquote, ic.sub_cols(i));
    }
    for (uint32_t i = 0; i < quotes; i++)
    {
        ret += "`";
    }
}

/// Append name backtick-quoted, escaping any embedded backtick or backslash by doubling it.
CONV_FN(SQLIdentifier, ident)
{
    ret += "`";
    for (const char c : ident.value())
    {
        if (c == '`' || c == '\\')
            ret += c;
        ret += c;
    }
    ret += "`";
}

void AggregateParamToString(String & ret, const AggregateParam & p)
{
    if (p.has_float_param())
        ret += doubleToSQLString(p.float_param());
    else
        ret += std::to_string(p.int_param());
}

static void ClusterToString(String & ret, const bool clause, const Cluster & cl)
{
    if (cl.has_cluster())
    {
        if (clause)
        {
            ret += " ON CLUSTER ";
        }
        appendSQLStringLiteral(ret, cl.cluster());
        if (!clause)
        {
            ret += ", ";
        }
    }
}

CONV_FN(Window, win)
{
    ret += win.window();
}

CONV_FN(Storage, store)
{
    ret += Storage_DataStorage_Name(store.storage());
    ret += " ";
    appendSQLStringLiteral(ret, store.storage_name());
}

CONV_FN(ExprColAlias, eca)
{
    if (eca.use_parenthesis())
    {
        ret += "(";
    }
    ExprToString(ret, eca.expr());
    if (eca.has_col_alias())
    {
        ret += " AS `";
        ColumnToString(ret, 1, eca.col_alias());
        ret += "`";
    }
    if (eca.use_parenthesis())
    {
        ret += ")";
    }
}

static void ConvertToSQLString(String & ret, const String & s)
{
    for (size_t i = 0; i < s.length(); i++)
    {
        const char & c = s[i];

        switch (c)
        {
            case '\'': ret += "''"; break;
            case '\\': ret += "\\\\"; break;
            case '\b': ret += "\\b"; break;
            case '\f': ret += "\\f"; break;
            case '\r': ret += "\\r"; break;
            case '\n': ret += "\\n"; break;
            case '\t': ret += "\\t"; break;
            case '\0': ret += "\\0"; break;
            case '\a': ret += "\\a"; break;
            case '\v': ret += "\\v"; break;
            default: {
                if (c > static_cast<char>(31) && c < static_cast<char>(127))
                {
                    ret += c;
                }
                else
                {
                    const uint8_t & x = static_cast<uint8_t>(c);

                    ret += "\\x";
                    ret += hexDigits[(x & 0xF0) >> 4];
                    ret += hexDigits[x & 0x0F];
                }
            }
        }
    }
}

/// Single-quoted literal escaping both `'` and `\`, unlike `appendSQLStringLiteral`
static void appendEscapedSQLString(String & ret, const String & s)
{
    ret += "'";
    ConvertToSQLString(ret, s);
    ret += "'";
}

CONV_FN(ExprSchemaTable, st)
{
    if (st.has_database())
    {
        SQLIdentifierToString(ret, st.database());
        ret += ".";
    }
    SQLIdentifierToString(ret, st.table());
}

CONV_FN_QUOTE(TypeName, top);
CONV_FN_QUOTE(TopTypeName, top);

CONV_FN(JSONColumn, jcol)
{
    ret += ".";
    if (jcol.has_jcol())
    {
        ret += "^";
    }
    else if (jcol.has_jcombined())
    {
        ret += "@";
    }
    ret += "`";
    ColumnToString(ret, 1, jcol.col());
    ret += "`";
    if (jcol.has_jarray())
    {
        const uint32_t limit = (jcol.jarray() % 4) + 1;

        for (uint32_t j = 0; j < limit; j++)
        {
            ret += "[]";
        }
    }
}

CONV_FN(JSONColumns, jcols)
{
    JSONColumnToString(ret, jcols.jcol());
    for (int i = 0; i < jcols.other_jcols_size(); i++)
    {
        JSONColumnToString(ret, jcols.other_jcols(i));
    }
    if (jcols.has_jcast())
    {
        ret += "::";
        TypeNameToString(ret, 0, jcols.jcast());
    }
    else if (jcols.has_jreinterpret())
    {
        ret += ".:`";
        TypeNameToString(ret, 1, jcols.jreinterpret());
        ret += "`";
    }
}

CONV_FN(FieldAccess, fa)
{
    using FieldType = FieldAccess::NestedOneofCase;
    switch (fa.nested_oneof_case())
    {
        case FieldType::kArrayIndex:
            ret += "[";
            ret += fa.array_index() < 0 ? "-" : "";
            ret += std::to_string(std::abs(fa.array_index()) % 10);
            ret += "]";
            break;
        case FieldType::kArrayExpr:
            ret += "[";
            ExprToString(ret, fa.array_expr());
            ret += "]";
            break;
        case FieldType::kArrayKey:
            ret += "[`";
            ColumnToString(ret, 1, fa.array_key());
            ret += "`]";
            break;
        case FieldType::kTupleIndex:
            ret += ".";
            ret += fa.tuple_index() < 0 ? "-" : "";
            ret += std::to_string((std::abs(fa.tuple_index()) % 5) + 1);
            break;
        default: ret += "[1]";
    }
}

CONV_FN(ExprColumn, ec)
{
    ColumnPathToString(ret, 0, ec.path());
    if (ec.has_subcols())
    {
        JSONColumnsToString(ret, ec.subcols());
    }
    if (ec.has_dynamic_subtype())
    {
        ret += ".`";
        TypeNameToString(ret, 1, ec.dynamic_subtype());
        ret += "`";
    }
}

CONV_FN(ExprSchemaTableColumn, stc)
{
    if (stc.has_database())
    {
        SQLIdentifierToString(ret, stc.database());
        ret += ".";
    }
    if (stc.has_table())
    {
        SQLIdentifierToString(ret, stc.table());
        ret += ".";
    }
    ExprColumnToString(ret, stc.col());
}

CONV_FN(ExprList, me)
{
    ExprToString(ret, me.expr());
    for (int i = 0; i < me.extra_exprs_size(); i++)
    {
        ret += ", ";
        ExprToString(ret, me.extra_exprs(i));
    }
}

/// Limbs are given least significant first; the middle limbs make it 256-bit
CONV_FN(HugeIntLiteral, huge)
{
    const ::UInt64 upper = static_cast<::UInt64>(huge.upper());

    if (huge.has_mid_upper() || huge.has_mid_lower())
    {
        ret += wide::to_string(::Int256{huge.lower(), huge.mid_lower(), huge.mid_upper(), upper});
    }
    else
    {
        ret += wide::to_string(::Int128{huge.lower(), upper});
    }
}

CONV_FN(UHugeIntLiteral, uhuge)
{
    if (uhuge.has_mid_upper() || uhuge.has_mid_lower())
    {
        ret += wide::to_string(::UInt256{uhuge.lower(), uhuge.mid_lower(), uhuge.mid_upper(), uhuge.upper()});
    }
    else
    {
        ret += wide::to_string(::UInt128{uhuge.lower(), uhuge.upper()});
    }
}

CONV_FN(IntLiteral, int_val)
{
    if (int_val.has_int_lit())
    {
        ret += std::to_string(int_val.int_lit());
    }
    else if (int_val.has_uint_lit())
    {
        ret += std::to_string(int_val.uint_lit());
    }
    else if (int_val.has_huge_lit())
    {
        HugeIntLiteralToString(ret, int_val.huge_lit());
    }
    else if (int_val.has_uhuge_lit())
    {
        UHugeIntLiteralToString(ret, int_val.uhuge_lit());
    }
    else
    {
        ret += "0";
    }
    if (int_val.has_integers())
    {
        ret += "::";
        ret += Integers_Name(int_val.integers());
    }
}

CONV_FN(SpecialVal, val)
{
    /// The literal, followed by its type cast when parenthesized
    const auto typed = [&](const String & lit, const char * tp)
    {
        ret += lit;
        if (val.paren())
        {
            ret += "::";
            ret += tp;
        }
    };

    ret += val.paren() ? "(" : "";
    switch (val.val())
    {
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_NULL: ret += "NULL"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_TRUE: ret += "TRUE"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_FALSE: ret += "FALSE"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_ZERO: ret += "0"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_ONE: ret += "1"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_MINUS_ONE: ret += "-1"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_EMPTY_STRING: ret += "''"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_EMPTY_ARRAY: ret += "[]"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_EMPTY_TUPLE: ret += "()"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_EMPTY_MAP:
            if (val.paren())
            {
                ret += "map()";
            }
            else
            {
                ret += "'{}'";
            }
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_EMPTY_JSON: typed("'{}'", "JSON"); break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_MINUS_ZERO_FP: ret += "-0.0"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_PLUS_ZERO_FP: ret += "+0.0"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_ZERO_FP: ret += "0.0"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_INF: ret += "inf"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_PLUS_INF: ret += "+inf"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_MINUS_INF: ret += "-inf"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_NAN: ret += "nan"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_PLUS_NAN: ret += "+nan"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_MINUS_NAN: ret += "-nan"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_HAPPY: ret += "'😂'"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_TEN_HAPPY: ret += "'😂😂😂😂😂😂😂😂😂😂'"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MIN_INT32:
            typed(std::to_string(std::numeric_limits<int32_t>::min()), "Int32");
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_INT32:
            typed(std::to_string(std::numeric_limits<int32_t>::max()), "Int32");
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MIN_INT64:
            typed(std::to_string(std::numeric_limits<int64_t>::min()), "Int64");
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_INT64:
            typed(std::to_string(std::numeric_limits<int64_t>::max()), "Int64");
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MIN_INT128:
            typed("-170141183460469231731687303715884105728", "Int128");
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_INT128:
            typed("170141183460469231731687303715884105727", "Int128");
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MIN_INT256:
            typed("-57896044618658097711785492504343953926634992332820282019728792003956564819968", "Int256");
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_INT256:
            typed("57896044618658097711785492504343953926634992332820282019728792003956564819967", "Int256");
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_UINT32:
            typed(std::to_string(std::numeric_limits<uint32_t>::max()), "UInt32");
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_UINT64:
            typed(std::to_string(std::numeric_limits<uint64_t>::max()), "UInt64");
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_UINT128:
            typed("340282366920938463463374607431768211455", "UInt128");
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_UINT256:
            typed("115792089237316195423570985008687907853269984665640564039457584007913129639935", "UInt256");
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MIN_DATE: typed("'1970-01-01'", "Date"); break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_DATE: typed("'2149-06-06'", "Date"); break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MIN_DATE32: typed("'0000-01-01'", "Date32"); break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_DATE32: typed("'9999-12-31'", "Date32"); break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MIN_TIME: typed("'-999:59:59'", "Time"); break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_TIME: typed("'999:59:59'", "Time"); break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MIN_TIME64: typed("'-999:59:59.999999999'", "Time64"); break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_TIME64: typed("'999:59:59.999999999'", "Time64"); break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MIN_DATETIME: typed("'1970-01-01 00:00:00'", "DateTime"); break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_DATETIME: typed("'2106-02-07 06:28:15'", "DateTime"); break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MIN_DATETIME64: typed("'0001-01-01 00:00:00'", "DateTime64"); break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_DATETIME64:
            typed("'9999-12-31 23:59:59.999999999'", "DateTime64");
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MIN_DOUBLE:
            typed(doubleToSQLString(std::numeric_limits<double>::min()), "Float64");
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_DOUBLE:
            typed(std::to_string(std::numeric_limits<double>::max()), "Float64");
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_DENORM_MIN_DOUBLE:
            typed(doubleToSQLString(std::numeric_limits<double>::denorm_min()), "Float64");
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_LOWEST_DOUBLE:
            typed(std::to_string(std::numeric_limits<double>::lowest()), "Float64");
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_NULL_CHAR: ret += "'\\0'"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_DEFAULT: ret += "DEFAULT"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_STAR: ret += "*"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_BAD_UTF8_OVERLONG: ret += R"('\xC0\xAF')"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_BAD_UTF8_CONTINUATION: ret += R"('\x80\xBF\xC3\x28')"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_BAD_UTF8_TRUNCATED: ret += R"('\xE2\x82')"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_BAD_UTF8_SURROGATE: ret += R"('\xED\xA0\x80')"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_BAD_UTF8_BEYOND_RANGE: ret += R"('\xF4\x90\x80\x80')"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_BAD_UTF8_INVALID_FE_FF: ret += R"('\xFE\xFF')"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_UTF8_BOM: ret += R"('\xEF\xBB\xBF')"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_BAD_UTF8_MIXED:
            ret += R"('hello\xC0\xAFworld\xED\xA0\x80\xF4\x90\x80\x80')";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_CONTROL_CHARS:
            ret += R"('\x01\x02\x03\x04\x05\x06\x07\x08\x0E\x0F\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1A\x1C\x1D\x1E\x1F')";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_ANSI_ESCAPE: ret += R"('\x1B[31mRED\x1B[0m')"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_ZERO_WIDTH_SPACE: ret += R"('a\xE2\x80\x8Bb')"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_ZERO_WIDTH_JOINER:
            // Split so \x8D and b are in separate literals (style check)
            ret += R"('a\xE2\x80\x8D)"
                   "b'";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_BIDI_OVERRIDE: ret += R"('\xE2\x80\xAEabc\xE2\x80\xAC')"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_MAX_CODEPOINT: ret += R"('\xF4\x8F\xBF\xBF')"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_ARRAY_NULL: ret += "[NULL]"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_TUPLE_NULL: ret += "(NULL)"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_NESTED_EMPTY_ARRAY: ret += "[[]]"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_ARRAY_EMPTY_MAP: ret += "[map()]"; break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_LEAP_DAY: typed("'2024-02-29'", "Date32"); break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_NON_LEAP_DAY: typed("'2023-02-29'", "Date32"); break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_BEYOND_INT256:
            ret += "115792089237316195423570985008687907853269984665640564039457584007913129639936";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_FLOAT64_MAX_SCIENTIFIC: ret += "1e308"; break;
    }
    ret += val.paren() ? ")" : "";
}

CONV_FN(LiteralValue, lit_val)
{
    using LitValType = LiteralValue::LitValOneofCase;
    switch (lit_val.lit_val_oneof_case())
    {
        case LitValType::kIntLit: IntLiteralToString(ret, lit_val.int_lit()); break;
        case LitValType::kNoQuoteStr: ret += lit_val.no_quote_str(); break;
        case LitValType::kStringLit:
            ret += "'";
            ConvertToSQLString(ret, lit_val.string_lit());
            ret += "'";
            break;
        case LitValType::kSpecialVal: SpecialValToString(ret, lit_val.special_val()); break;
        default: ret += "1";
    }
}

CONV_FN(UnaryExpr, uexpr)
{
    ret += uexpr.paren() ? "(" : "";
    switch (uexpr.unary_op())
    {
        case UNOP_MINUS: ret += "-"; break;
        case UNOP_PLUS: ret += "+"; break;
        case UNOP_NOT: ret += "NOT "; break;
    }
    const size_t operand_start = ret.size();

    ExprToString(ret, uexpr.expr());
    /// `--` starts a comment, so separate a minus from a negative operand
    if (uexpr.unary_op() == UNOP_MINUS && operand_start < ret.size() && ret[operand_start] == '-')
    {
        ret.insert(operand_start, " ");
    }
    ret += uexpr.paren() ? ")" : "";
}

CONV_FN(BinaryOperator, bop)
{
    switch (bop)
    {
        case BINOP_LE: ret += " < "; break;
        case BINOP_LEQ: ret += " <= "; break;
        case BINOP_GR: ret += " > "; break;
        case BINOP_GREQ: ret += " >= "; break;
        case BINOP_EQ: ret += " = "; break;
        case BINOP_EQEQ: ret += " == "; break;
        case BINOP_NOTEQ: ret += " != "; break;
        case BINOP_LEGR: ret += " <> "; break;
        case BINOP_LEEQGR: ret += " <=> "; break;
        case BINOP_IS_DISTINCT_FROM: ret += " IS DISTINCT FROM "; break;
        case BINOP_IS_NOT_DISTINCT_FROM: ret += " IS NOT DISTINCT FROM "; break;
        case BINOP_AND: ret += " AND "; break;
        case BINOP_OR: ret += " OR "; break;
        case BINOP_CONCAT: ret += " || "; break;
        case BINOP_STAR: ret += " * "; break;
        case BINOP_SLASH: ret += " / "; break;
        case BINOP_PERCENT: ret += " % "; break;
        case BINOP_PLUS: ret += " + "; break;
        case BINOP_MINUS: ret += " - "; break;
        case BINOP_DIV: ret += " DIV "; break;
        case BINOP_MOD: ret += " MOD "; break;
    }
}

CONV_FN(BinaryExpr, bexpr)
{
    ExprToString(ret, bexpr.lhs());
    BinaryOperatorToString(ret, bexpr.op());
    ExprToString(ret, bexpr.rhs());
}

CONV_FN_QUOTE(ColumnPathList, cols)
{
    ColumnPathToString(ret, quote, cols.col());
    for (int i = 0; i < cols.other_cols_size(); i++)
    {
        ret += ", ";
        ColumnPathToString(ret, quote, cols.other_cols(i));
    }
}

CONV_FN(EnumDefValue, edf)
{
    ret += edf.enumv();
    ret += " = ";
    ret += std::to_string(edf.number());
}

CONV_FN(EnumDef, edef)
{
    ret += "(";
    EnumDefValueToString(ret, edef.first_value());
    for (int i = 0; i < edef.other_values_size(); i++)
    {
        ret += ", ";
        EnumDefValueToString(ret, edef.other_values(i));
    }
    ret += ")";
}

static void BottomTypeNameToString(String & ret, const uint32_t quote, const bool lcard, const BottomTypeName & btn)
{
    using BottomTypeNameType = BottomTypeName::BottomOneOfCase;
    switch (btn.bottom_one_of_case())
    {
        case BottomTypeNameType::kIntegers: ret += Integers_Name(btn.integers()); break;
        case BottomTypeNameType::kFloats: ret += FloatingPoints_Name(btn.floats()); break;
        case BottomTypeNameType::kStandardString: ret += "String"; break;
        case BottomTypeNameType::kFixedString:
            ret += "FixedString(";
            ret += std::to_string(std::max<uint32_t>(1, btn.fixed_string()));
            ret += ")";
            break;
        case BottomTypeNameType::kDates: ret += Dates_Name(btn.dates()); break;
        case BottomTypeNameType::kTimes: {
            const TimeTp & tt = btn.times();

            ret += Times_Name(tt.type());
            if (tt.has_precision())
            {
                ret += "(";
                ret += std::to_string(tt.precision() % UINT32_C(10));
                ret += ")";
            }
        }
        break;
        case BottomTypeNameType::kDatetimes: {
            const DateTimeTp & dt = btn.datetimes();
            const bool has_precision = dt.has_precision();

            ret += DateTimes_Name(dt.type());
            if (has_precision || dt.has_timezone())
            {
                ret += "(";
                if (has_precision)
                {
                    ret += std::to_string(dt.precision() % UINT32_C(10));
                }
                if (dt.has_timezone())
                {
                    if (has_precision)
                    {
                        ret += ",";
                    }
                    appendSQLStringLiteral(ret, dt.timezone());
                }
                ret += ")";
            }
        }
        break;
        case BottomTypeNameType::kBoolean: ret += "Bool"; break;
        case BottomTypeNameType::kUuid: ret += "UUID"; break;
        case BottomTypeNameType::kIPv4: ret += "IPv4"; break;
        case BottomTypeNameType::kIPv6: ret += "IPv6"; break;
        case BottomTypeNameType::kDecimal: {
            const Decimal & dec = btn.decimal();

            ret += "Decimal";
            if (dec.has_decimaln())
            {
                uint32_t precision = 0;
                const DecimalN & dn = dec.decimaln();

                switch (dn.precision())
                {
                    case DecimalN_DecimalPrecision::DecimalN_DecimalPrecision_D32: precision = 9; break;
                    case DecimalN_DecimalPrecision::DecimalN_DecimalPrecision_D64: precision = 18; break;
                    case DecimalN_DecimalPrecision::DecimalN_DecimalPrecision_D128: precision = 38; break;
                    case DecimalN_DecimalPrecision::DecimalN_DecimalPrecision_D256: precision = 76; break;
                }
                ret += DecimalN_DecimalPrecision_Name(dn.precision()).substr(1);
                ret += "(";
                ret += std::to_string(dn.scale() % (precision + 1));
                ret += ")";
            }
            else if (dec.has_decimal_simple())
            {
                const DecimalSimple & ds = dec.decimal_simple();

                if (ds.has_precision())
                {
                    const uint32_t precision = std::max<uint32_t>(1, ds.precision() % 77);

                    ret += "(";
                    ret += std::to_string(precision);
                    if (ds.has_scale())
                    {
                        ret += ",";
                        ret += std::to_string(ds.scale() % (precision + 1));
                    }
                    ret += ")";
                }
            }
        }
        break;
        case BottomTypeNameType::kQbit: {
            const QBit & qq = btn.qbit();

            ret += "QBit(";
            ret += qq.has_int8() ? "Int8" : FloatingPoints_Name(qq.floats());
            ret += ", ";
            ret += std::to_string(qq.dimension());
            if (qq.has_stride())
            {
                ret += ", ";
                ret += std::to_string(qq.stride());
            }
            ret += ")";
        }
        break;
        case BottomTypeNameType::kGeo: ret += GeoTypes_Name(btn.geo()); break;
        case BottomTypeNameType::kAggr: {
            const AggregateFunction & af = btn.aggr();

            ret += af.simple() ? "Simple" : "";
            ret += "AggregateFunction(";
            ret += af.aggr();
            if (af.params_size() > 0)
            {
                ret += "(";
                for (int i = 0; i < af.params_size(); i++)
                {
                    if (i != 0)
                        ret += ", ";
                    AggregateParamToString(ret, af.params(i));
                }
                ret += ")";
            }
            for (int i = 0; i < af.types_size(); i++)
            {
                ret += ", ";
                TopTypeNameToString(ret, quote, af.types(i));
            }
            ret += ")";
        }
        break;
        default: {
            if (lcard)
            {
                ret += "Int";
            }
            else
            {
                switch (btn.bottom_one_of_case())
                {
                    case BottomTypeNameType::kJdef: {
                        const JSONDef & jdef = btn.jdef();

                        ret += "JSON";
                        if (jdef.spec_size() > 0)
                        {
                            ret += "(";
                            for (int i = 0; i < jdef.spec_size(); i++)
                            {
                                const JSONDefItem & jspec = jdef.spec(i);

                                if (i != 0)
                                {
                                    ret += ", ";
                                }
                                if (jspec.has_max_dynamic_types())
                                {
                                    ret += "max_dynamic_types=";
                                    ret += std::to_string(jspec.max_dynamic_types() % 33);
                                }
                                else if (jspec.has_max_dynamic_paths())
                                {
                                    ret += "max_dynamic_paths=";
                                    ret += std::to_string(jspec.max_dynamic_paths() % 1025);
                                }
                                else if (jspec.has_path_type())
                                {
                                    const JSONPathType & jpt = jspec.path_type();

                                    if (jpt.shared())
                                    {
                                        ret += "SHARED ";
                                    }
                                    ColumnPathToString(ret, quote, jpt.col());
                                    ret += " ";
                                    TopTypeNameToString(ret, quote, jpt.type());
                                }
                                else
                                {
                                    ret += "max_dynamic_types=8";
                                }
                            }
                            ret += ")";
                        }
                    }
                    break;
                    case BottomTypeNameType::kDynamic:
                        ret += "Dynamic";
                        if (btn.dynamic().has_ntypes())
                        {
                            ret += "(max_types=";
                            ret += std::to_string(std::max<uint32_t>(1, btn.dynamic().ntypes() % 256));
                            ret += ")";
                        }
                        break;
                    case BottomTypeNameType::kEnumDef: {
                        const EnumDef & edef = btn.enum_def();

                        ret += "Enum";
                        if (edef.has_bits())
                        {
                            ret += edef.bits() ? "16" : "8";
                        }
                        EnumDefToString(ret, edef);
                    }
                    break;
                    default: ret += "Int";
                }
            }
        }
    }
}

CONV_FN_QUOTE(TypeColumnDef, col_def)
{
    const uint32_t quotes = (1 << quote);

    for (uint32_t i = 0; i < quotes; i++)
    {
        ret += "`";
    }
    ColumnToString(ret, quote + 1, col_def.col());
    for (uint32_t i = 0; i < quotes; i++)
    {
        ret += "`";
    }
    ret += " ";
    TopTypeNameToString(ret, quote, col_def.type_name());
}

CONV_FN_QUOTE(TupleWithColumnNames, twcn)
{
    ret += "(";
    for (int i = 0; i < twcn.values_size(); i++)
    {
        if (i != 0)
        {
            ret += ",";
        }
        TypeColumnDefToString(ret, quote, twcn.values(i));
    }
    ret += ")";
}

CONV_FN_QUOTE(TupleWithOutColumnNames, twcn)
{
    ret += "(";
    for (int i = 0; i < twcn.values_size(); i++)
    {
        if (i != 0)
        {
            ret += ",";
        }
        TopTypeNameToString(ret, quote, twcn.values(i));
    }
    ret += ")";
}

CONV_FN_QUOTE(TopTypeName, ttn)
{
    using TopTypeNameType = TopTypeName::TypeOneofCase;
    switch (ttn.type_oneof_case())
    {
        case TopTypeNameType::kNonNullable: BottomTypeNameToString(ret, quote, false, ttn.non_nullable()); break;
        case TopTypeNameType::kNullable:
            ret += "Nullable(";
            BottomTypeNameToString(ret, quote, false, ttn.nullable());
            ret += ")";
            break;
        case TopTypeNameType::kNonNullableLcard:
            ret += "LowCardinality(";
            BottomTypeNameToString(ret, quote, true, ttn.non_nullable_lcard());
            ret += ")";
            break;
        case TopTypeNameType::kNullableLcard:
            ret += "LowCardinality(Nullable(";
            BottomTypeNameToString(ret, quote, true, ttn.nullable_lcard());
            ret += "))";
            break;
        case TopTypeNameType::kArray:
            ret += "Array(";
            TopTypeNameToString(ret, quote, ttn.array());
            ret += ")";
            break;
        case TopTypeNameType::kMap:
            ret += "Map(";
            TopTypeNameToString(ret, quote, ttn.map().key());
            ret += ",";
            TopTypeNameToString(ret, quote, ttn.map().value());
            ret += ")";
            break;
        case TopTypeNameType::kTuple: {
            const TupleTypeDef & tt = ttn.tuple();

            if (tt.is_nullable())
            {
                ret += "Nullable(";
            }
            ret += "Tuple";
            if (tt.has_with_names())
            {
                TupleWithColumnNamesToString(ret, quote, tt.with_names());
            }
            else if (tt.has_no_names())
            {
                TupleWithOutColumnNamesToString(ret, quote, tt.no_names());
            }
            else
            {
                ret += "()";
            }
            if (tt.is_nullable())
            {
                ret += ")";
            }
        }
        break;
        case TopTypeNameType::kNested:
            ret += "Nested(";
            TypeColumnDefToString(ret, quote, ttn.nested().type1());
            for (int i = 0; i < ttn.nested().others_size(); i++)
            {
                ret += ", ";
                TypeColumnDefToString(ret, quote, ttn.nested().others(i));
            }
            ret += ")";
            break;
        case TopTypeNameType::kVariant:
            ret += "Variant";
            TupleWithOutColumnNamesToString(ret, quote, ttn.variant());
            break;
        default: ret += "Int";
    }
}

CONV_FN_QUOTE(TypeName, tp)
{
    TopTypeNameToString(ret, quote, tp.type());
}

CONV_FN(CastExpr, cexpr)
{
    if (cexpr.simple())
    {
        ExprToString(ret, cexpr.expr());
        ret += "::`";
        TypeNameToString(ret, 1, cexpr.type_name());
        ret += "`";
    }
    else
    {
        ret += "CAST(";
        ExprToString(ret, cexpr.expr());
        ret += " AS ";
        TypeNameToString(ret, 0, cexpr.type_name());
        ret += ")";
    }
}

CONV_FN(ExprLike, elike)
{
    const ExprLike_PossibleKeywords kw = elike.keyword();
    const bool is_regexp = kw == ExprLike_PossibleKeywords::ExprLike_PossibleKeywords_REGEXP
        || kw == ExprLike_PossibleKeywords::ExprLike_PossibleKeywords_TILDE
        || kw == ExprLike_PossibleKeywords::ExprLike_PossibleKeywords_TILDE_ASTERISK;

    ExprToString(ret, elike.expr1());
    ret += " ";
    switch (kw)
    {
        /// `NOT REGEXP` is a syntax error, `!~` is its negated form
        case ExprLike_PossibleKeywords::ExprLike_PossibleKeywords_REGEXP: ret += elike.not_() ? "!~" : "REGEXP"; break;
        case ExprLike_PossibleKeywords::ExprLike_PossibleKeywords_TILDE: ret += elike.not_() ? "!~" : "~"; break;
        case ExprLike_PossibleKeywords::ExprLike_PossibleKeywords_TILDE_ASTERISK: ret += elike.not_() ? "!~*" : "~*"; break;
        case ExprLike_PossibleKeywords::ExprLike_PossibleKeywords_SIMILAR_TO: ret += elike.not_() ? "NOT SIMILAR TO" : "SIMILAR TO"; break;
        default:
            ret += elike.not_() ? "NOT " : "";
            ret += ExprLike_PossibleKeywords_Name(kw);
    }
    ret += " ";
    ExprToString(ret, elike.expr2());
    if (elike.has_escape() && !is_regexp)
    {
        ret += " ESCAPE ";
        ret += elike.escape();
    }
}

CONV_FN(CondExpr, econd)
{
    ret += econd.paren() ? "(" : "";
    ExprToString(ret, econd.expr1());
    ret += " ? ";
    ExprToString(ret, econd.expr2());
    ret += " : ";
    ExprToString(ret, econd.expr3());
    ret += econd.paren() ? ")" : "";
}

CONV_FN(ExprTruthTests, ent)
{
    ExprToString(ret, ent.expr());
    ret += " IS";
    ret += ent.not_() ? " NOT" : "";
    switch (ent.truth_value())
    {
        case ExprTruthTests::IS_TRUE: ret += " TRUE"; break;
        case ExprTruthTests::IS_FALSE: ret += " FALSE"; break;
        case ExprTruthTests::IS_UNKNOWN: ret += " UNKNOWN"; break;
        default: ret += " NULL"; break;
    }
}

CONV_FN(ExprBetween, ebetween)
{
    ret += ebetween.paren() ? "(" : "";
    ExprToString(ret, ebetween.expr1());
    ret += " ";
    if (ebetween.not_())
        ret += "NOT ";
    ret += "BETWEEN ";
    ExprToString(ret, ebetween.expr2());
    ret += " AND ";
    ExprToString(ret, ebetween.expr3());
    ret += ebetween.paren() ? ")" : "";
}

CONV_FN(ExplainQuery, explain);

CONV_FN(ExprInType, ein)
{
    using InType = ExprInType::InOneofCase;
    switch (ein.in_oneof_case())
    {
        case InType::kSingleExpr: ExprToString(ret, ein.single_expr()); break;
        case InType::kArray:
            ret += "[";
            ExprListToString(ret, ein.array());
            ret += "]";
            break;
        case InType::kTuple:
            ret += "(";
            ExprListToString(ret, ein.tuple());
            ret += ")";
            break;
        case InType::kSel:
            ret += "(";
            ExplainQueryToString(ret, ein.sel());
            ret += ")";
            break;
        case InType::kEmptyList: ret += ein.empty_list() ? "[]" : "()"; break;
        case InType::kTbl: ExprSchemaTableToString(ret, ein.tbl()); break;
        default: ret += "1";
    }
}

CONV_FN(ExprIn, ein)
{
    const ExprList & elist = ein.expr();

    if (elist.extra_exprs_size() == 0)
    {
        ExprToString(ret, elist.expr());
    }
    else
    {
        ret += "(";
        ExprListToString(ret, ein.expr());
        ret += ")";
    }
    ret += " ";
    if (ein.global())
        ret += "GLOBAL ";
    if (ein.not_())
        ret += "NOT ";
    ret += "IN ";
    ExprInTypeToString(ret, ein.in_type());
}

CONV_FN(ExprAny, eany)
{
    const int op = static_cast<int>(eany.op());

    ExprToString(ret, eany.expr());
    /// Only `<`, `<=`, `>`, `>=`, `=`, `==`, `!=` and `<>` parse before ANY/ALL/SOME, fold anything else into them
    BinaryOperatorToString(ret, op <= BINOP_LEGR ? eany.op() : static_cast<BinaryOperator>(((op - 1) % BINOP_LEGR) + 1));
    ret += ExprAny_AnyAllSome_Name(eany.anyall()).substr(4);
    ret += "(";
    ExprInTypeToString(ret, eany.in_type());
    ret += ")";
}

CONV_FN(ExprExists, exists)
{
    if (exists.not_())
        ret += "NOT ";
    ret += "EXISTS (";
    ExplainQueryToString(ret, exists.select());
    ret += ")";
}

CONV_FN(ExprWhenThen, ewt)
{
    ret += "WHEN ";
    ExprToString(ret, ewt.when_expr());
    ret += " THEN ";
    ExprToString(ret, ewt.then_expr());
}

CONV_FN(ExprCase, ecase)
{
    ret += "CASE ";
    if (ecase.has_expr())
    {
        ExprToString(ret, ecase.expr());
        ret += " ";
    }
    ExprWhenThenToString(ret, ecase.when_then());
    ret += " ";
    for (int i = 0; i < ecase.extra_when_thens_size(); i++)
    {
        ExprWhenThenToString(ret, ecase.extra_when_thens(i));
        ret += " ";
    }
    if (ecase.has_else_expr())
    {
        ret += "ELSE ";
        ExprToString(ret, ecase.else_expr());
        ret += " ";
    }
    ret += "END";
}

CONV_FN(LambdaExpr, lambda)
{
    ret += lambda.paren() ? "(" : "";
    for (int i = 0; i < lambda.args_size(); i++)
    {
        if (i != 0)
        {
            ret += ", ";
        }
        ret += "`";
        ColumnToString(ret, 1, lambda.args(i));
        ret += "`";
    }
    ret += lambda.paren() ? ")" : "";
    ret += " -> ";
    ExprToString(ret, lambda.expr());
}

CONV_FN(FuncComposition, comp)
{
    for (int i = 0; i < comp.stages_size(); i++)
    {
        const ComposeStage & stage = comp.stages(i);

        if (i != 0)
        {
            ret += " | ";
        }
        if (stage.has_lambda())
        {
            /// Without parentheses the lambda body would take the rest of the chain
            ret += "(";
            LambdaExprToString(ret, stage.lambda());
            ret += ")";
        }
        else if (stage.has_call())
        {
            ret += stage.call().func();
            ret += "(";
            for (int j = 0; j < stage.call().args_size(); j++)
            {
                if (j != 0)
                {
                    ret += ", ";
                }
                ret += stage.call().args(j);
            }
            ret += ")";
        }
        else if (stage.has_func_name())
        {
            ret += stage.func_name();
        }
        else
        {
            ret += "_";
        }
    }
}

CONV_FN(SQLFuncName, sfn)
{
    if (sfn.has_catalog_func())
    {
        ret += sfn.catalog_func();
    }
    else if (sfn.has_function())
    {
        SQLIdentifierToString(ret, sfn.function());
    }
    else
    {
        ret += "count";
    }
}

/// Comma-separated arguments shared by scalar and table function calls
static void SQLFuncArgsToString(String & ret, const google::protobuf::RepeatedPtrField<SQLFuncArg> & args)
{
    for (int i = 0; i < args.size(); i++)
    {
        const SQLFuncArg & sfa = args.Get(i);

        if (i != 0)
        {
            ret += ", ";
        }
        if (sfa.has_lambda())
        {
            LambdaExprToString(ret, sfa.lambda());
        }
        else if (sfa.has_func_name())
        {
            ret += sfa.func_name();
        }
        else if (sfa.has_composition())
        {
            FuncCompositionToString(ret, sfa.composition());
        }
        else if (sfa.has_expr())
        {
            ExprToString(ret, sfa.expr());
        }
        else
        {
            ret += "1";
        }
    }
}

CONV_FN(SQLFuncCall, sfc)
{
    SQLFuncNameToString(ret, sfc.func());
    for (int i = 0; i < sfc.combinators_size(); i++)
    {
        ret += SQLFuncCall_AggregateCombinator_Name(sfc.combinators(i));
    }
    if (sfc.params_size())
    {
        ret += '(';
        for (int i = 0; i < sfc.params_size(); i++)
        {
            if (i != 0)
            {
                ret += ", ";
            }
            ExprToString(ret, sfc.params(i));
        }
        ret += ')';
    }
    ret += '(';
    if (sfc.args_size() > 0 && sfc.distinct())
    {
        ret += "DISTINCT ";
    }
    SQLFuncArgsToString(ret, sfc.args());
    ret += ')';
    if (sfc.has_fnulls())
    {
        ret += " ";
        ret += FuncNulls_Name(sfc.fnulls()).substr(1);
        ret += " NULLS";
    }
}

CONV_FN(ExprOrderingWithFill, eowf)
{
    ret += "WITH FILL";
    if (eowf.has_from_expr())
    {
        ret += " FROM ";
        ExprToString(ret, eowf.from_expr());
    }
    if (eowf.has_to_expr())
    {
        ret += " TO ";
        ExprToString(ret, eowf.to_expr());
    }
    if (eowf.has_step_expr())
    {
        ret += " STEP ";
        ExprToString(ret, eowf.step_expr());
    }
    if (eowf.has_staleness_expr())
    {
        ret += " STALENESS ";
        ExprToString(ret, eowf.staleness_expr());
    }
}

CONV_FN(ExprOrderingTerm, eot)
{
    ExprToString(ret, eot.expr());
    if (eot.has_asc_desc())
    {
        ret += " ";
        ret += AscDesc_Name(eot.asc_desc());
    }
    if (eot.has_nulls_order())
    {
        ret += " NULLS ";
        ret += ExprOrderingTerm_NullsOrder_Name(eot.nulls_order());
    }
    if (eot.has_collation())
    {
        ret += " COLLATE ";
        appendSQLStringLiteral(ret, eot.collation());
    }
    if (eot.has_fill())
    {
        ret += " ";
        ExprOrderingWithFillToString(ret, eot.fill());
    }
}

CONV_FN(OrderByList, ol)
{
    bool has_fill = false;

    ExprOrderingTermToString(ret, ol.ord_term());
    has_fill |= ol.ord_term().has_fill();
    for (int i = 0; i < ol.extra_ord_terms_size(); i++)
    {
        const ExprOrderingTerm & eot = ol.extra_ord_terms(i);

        ret += ", ";
        has_fill |= eot.has_fill();
        ExprOrderingTermToString(ret, eot);
    }
    if (has_fill && ol.interpolate_size())
    {
        ret += " INTERPOLATE(";
        for (int i = 0; i < ol.interpolate_size(); i++)
        {
            const InterpolateExpr & ie = ol.interpolate(i);

            if (i != 0)
            {
                ret += ", ";
            }
            ret += "`";
            ColumnToString(ret, 1, ie.col());
            ret += "` AS ";
            ExprToString(ret, ie.expr());
        }
        ret += ")";
    }
}

CONV_FN(OrderByStatement, obs)
{
    ret += "ORDER BY ";
    if (obs.has_olist())
    {
        OrderByListToString(ret, obs.olist());
    }
    else
    {
        ret += "ALL";
    }
}

CONV_FN(SQLWindowCall, wc)
{
    ret += WindowFuncs_Name(wc.func()).substr(3);
    ret += '(';
    for (int i = 0; i < wc.args_size(); i++)
    {
        if (i != 0)
        {
            ret += ", ";
        }
        ExprToString(ret, wc.args(i));
    }
    ret += ')';
    if (wc.has_fnulls())
    {
        ret += " ";
        ret += FuncNulls_Name(wc.fnulls()).substr(1);
        ret += " NULLS";
    }
}

CONV_FN(FrameSpecSubLeftExpr, fssle)
{
    String next = FrameSpecSubLeftExpr_Which_Name(fssle.which());

    if (fssle.which() > FrameSpecSubLeftExpr_Which_UNBOUNDED_PRECEDING && fssle.has_expr())
    {
        ExprToString(ret, fssle.expr());
        ret += " ";
    }
    std::replace(next.begin(), next.end(), '_', ' ');
    ret += next;
}

CONV_FN(FrameSpecSubRightExpr, fsslr)
{
    String next = FrameSpecSubRightExpr_Which_Name(fsslr.which());

    if (fsslr.which() > FrameSpecSubRightExpr_Which_UNBOUNDED_FOLLOWING && fsslr.has_expr())
    {
        ExprToString(ret, fsslr.expr());
        ret += " ";
    }
    std::replace(next.begin(), next.end(), '_', ' ');
    ret += next;
}

CONV_FN(ExprFrameSpec, efs)
{
    const bool has_right = efs.has_right_expr();

    ret += ExprFrameSpec_RangeRows_Name(efs.range_rows());
    if (has_right)
    {
        ret += " BETWEEN";
    }
    ret += " ";
    FrameSpecSubLeftExprToString(ret, efs.left_expr());
    if (has_right)
    {
        ret += " AND ";
        FrameSpecSubRightExprToString(ret, efs.right_expr());
    }
}

CONV_FN(WindowDefn, wd)
{
    if (wd.partition_exprs_size())
    {
        ret += "PARTITION BY ";
        for (int i = 0; i < wd.partition_exprs_size(); i++)
        {
            if (i != 0)
            {
                ret += ", ";
            }
            ExprToString(ret, wd.partition_exprs(i));
        }
    }
    if (wd.has_order_by())
    {
        if (wd.partition_exprs_size())
        {
            ret += " ";
        }
        OrderByStatementToString(ret, wd.order_by());
    }
    if (wd.has_frame_spec())
    {
        if ((wd.partition_exprs_size() && !wd.has_order_by()) || wd.has_order_by())
        {
            ret += " ";
        }
        ExprFrameSpecToString(ret, wd.frame_spec());
    }
}

CONV_FN(WindowFuncCall, wc)
{
    if (wc.has_win_func())
    {
        SQLWindowCallToString(ret, wc.win_func());
    }
    else if (wc.has_agg_func())
    {
        SQLFuncCallToString(ret, wc.agg_func());
    }
    else
    {
        ret += "rank()";
    }
    ret += " OVER (";
    if (wc.has_win_defn())
    {
        WindowDefnToString(ret, wc.win_defn());
    }
    else if (wc.has_window())
    {
        WindowToString(ret, wc.window());
    }
    ret += ")";
}

CONV_FN(WindowDef, wdef)
{
    WindowToString(ret, wdef.window());
    ret += " AS (";
    WindowDefnToString(ret, wdef.win_defn());
    ret += ")";
}

CONV_FN(IntervalExpr, ie)
{
    if (!ie.use_extract() && ie.interval() <= IntervalExpr::YEAR)
    {
        ret += "INTERVAL (";
        ExprToString(ret, ie.expr());
        ret += ") ";
        ret += IntervalExpr_Interval_Name(ie.interval());
    }
    else
    {
        ret += "EXTRACT(";
        ret += IntervalExpr_Interval_Name(ie.interval());
        ret += " FROM ";
        ExprToString(ret, ie.expr());
        ret += ")";
    }
}

CONV_FN(ComplicatedExpr, expr)
{
    using ExprType = ComplicatedExpr::ComplicatedExprOneofCase;
    switch (expr.complicated_expr_oneof_case())
    {
        case ExprType::kExprStc: ExprSchemaTableColumnToString(ret, expr.expr_stc()); break;
        case ExprType::kUnaryExpr: UnaryExprToString(ret, expr.unary_expr()); break;
        case ExprType::kBinaryExpr: BinaryExprToString(ret, expr.binary_expr()); break;
        case ExprType::kAliasExpr: ExprColAliasToString(ret, expr.alias_expr()); break;
        case ExprType::kCastExpr: CastExprToString(ret, expr.cast_expr()); break;
        case ExprType::kExprBetween: ExprBetweenToString(ret, expr.expr_between()); break;
        case ExprType::kExprIn: ExprInToString(ret, expr.expr_in()); break;
        case ExprType::kExprAny: ExprAnyToString(ret, expr.expr_any()); break;
        case ExprType::kExprTruthTests: ExprTruthTestsToString(ret, expr.expr_truth_tests()); break;
        case ExprType::kExprCase: ExprCaseToString(ret, expr.expr_case()); break;
        case ExprType::kExprExists: ExprExistsToString(ret, expr.expr_exists()); break;
        case ExprType::kExprLike: ExprLikeToString(ret, expr.expr_like()); break;
        case ExprType::kExprCond: CondExprToString(ret, expr.expr_cond()); break;
        case ExprType::kSubquery:
            ret += "(";
            ExplainQueryToString(ret, expr.subquery());
            ret += ")";
            break;
        case ExprType::kFuncCall: SQLFuncCallToString(ret, expr.func_call()); break;
        case ExprType::kWindowCall: WindowFuncCallToString(ret, expr.window_call()); break;
        case ExprType::kInterval: IntervalExprToString(ret, expr.interval()); break;
        case ExprType::kColumns:
            ret += "COLUMNS('";
            ret += expr.columns();
            ret += "')";
            break;
        case ExprType::kArray:
            ret += "[";
            ExprListToString(ret, expr.array());
            ret += "]";
            break;
        case ExprType::kTuple:
            ret += "(";
            ExprListToString(ret, expr.tuple());
            ret += ")";
            break;
        case ExprType::kTable:
            SQLIdentifierToString(ret, expr.table());
            ret += ".*";
            break;
        case ExprType::kLambda: LambdaExprToString(ret, expr.lambda()); break;
        default: ret += "1";
    }
}

CONV_FN(Expr, expr)
{
    if (expr.has_lit_val())
    {
        LiteralValueToString(ret, expr.lit_val());
    }
    else if (expr.has_comp_expr())
    {
        ComplicatedExprToString(ret, expr.comp_expr());
    }
    else
    {
        ret += "1";
    }
    for (int i = 0; i < expr.fields_size(); i++)
    {
        FieldAccessToString(ret, expr.fields(i));
    }
}

CONV_FN(ResultColumn, rc)
{
    if (rc.has_etc())
    {
        ExprSchemaTableColumnToString(ret, rc.etc());
    }
    else if (rc.has_eca())
    {
        ExprColAliasToString(ret, rc.eca());
    }
    else
    {
        ret += "*";
    }
}

CONV_FN(TableOrSubquery, tos);

CONV_FN(JoinConstraint, jc)
{
    if (jc.has_on_expr())
    {
        ret += " ON ";
        ExprToString(ret, jc.on_expr());
    }
    else if (jc.has_using_expr())
    {
        const UsingExpr & uexpr = jc.using_expr();

        ret += " USING (";
        ExprColumnToString(ret, uexpr.column());
        for (int i = 0; i < uexpr.other_columns_size(); i++)
        {
            ret += ", ";
            ExprColumnToString(ret, uexpr.other_columns(i));
        }
        ret += ")";
    }
    else
    {
        ret += " ON TRUE";
    }
}

CONV_FN(JoinCore, jcc)
{
    const bool no_clause = (jcc.join_op() > JoinType::J_FULL) || (jcc.has_join_const() && jcc.join_const() == JoinConst::J_NATURAL);
    const bool has_join_const = jcc.join_op() <= JoinType::J_FULL && jcc.has_join_const();

    if (jcc.join_op() == JoinType::J_COMMA)
    {
        /// A comma join has no modifiers, `LATERAL` nor constraint
        ret += ", ";
        TableOrSubqueryToString(ret, jcc.tos());
        return;
    }
    if (jcc.global())
    {
        ret += " GLOBAL";
    }
    if (has_join_const && !jcc.const_on_right())
    {
        ret += " ";
        ret += JoinConst_Name(jcc.join_const()).substr(2);
    }
    if (jcc.has_join_op())
    {
        ret += " ";
        ret += JoinType_Name(jcc.join_op()).substr(2);
    }
    if (has_join_const && jcc.const_on_right())
    {
        ret += " ";
        ret += JoinConst_Name(jcc.join_const()).substr(2);
    }
    ret += " JOIN ";
    /// `LATERAL` is only parsed before a subquery (`(VALUES ...)` included) and never with `CROSS JOIN`
    if (jcc.lateral() && jcc.join_op() != JoinType::J_CROSS && jcc.tos().has_joined_table()
        && (jcc.tos().joined_table().tof().has_select() || jcc.tos().joined_table().tof().has_values_standard()))
    {
        ret += "LATERAL ";
    }
    TableOrSubqueryToString(ret, jcc.tos());
    if (!no_clause)
    {
        JoinConstraintToString(ret, jcc.join_constraint());
    }
}

CONV_FN(ArrayJoin, aj)
{
    if (aj.left())
    {
        ret += " LEFT";
    }
    ret += " ARRAY JOIN ";
    ExprColAliasToString(ret, aj.constraint());
    for (int i = 0; i < aj.other_constraints_size(); i++)
    {
        ret += ", ";
        ExprColAliasToString(ret, aj.other_constraints(i));
    }
}

CONV_FN(JoinClauseCore, jcc)
{
    if (jcc.has_core())
    {
        JoinCoreToString(ret, jcc.core());
    }
    else if (jcc.has_arr())
    {
        ArrayJoinToString(ret, jcc.arr());
    }
}

CONV_FN(JoinClause, jc)
{
    TableOrSubqueryToString(ret, jc.tos());
    for (int i = 0; i < jc.clauses_size(); i++)
    {
        JoinClauseCoreToString(ret, jc.clauses(i));
    }
}

CONV_FN(KeyValuePair, kvp)
{
    ret += kvp.key();
    ret += " = ";
    appendSQLStringLiteral(ret, kvp.value());
}

CONV_FN(HTTPHeader, hdr)
{
    appendSQLStringLiteral(ret, hdr.name());
    ret += "=";
    appendSQLStringLiteral(ret, hdr.value());
}

template <typename T>
static void appendHTTPHeaders(String & ret, const T & msg)
{
    if (msg.http_headers_size() > 0)
    {
        ret += ", headers(";
        for (int i = 0; i < msg.http_headers_size(); i++)
        {
            if (i > 0)
            {
                ret += ", ";
            }
            HTTPHeaderToString(ret, msg.http_headers(i));
        }
        ret += ")";
    }
}

CONV_FN(SetValue, setv)
{
    ret += setv.property();
    ret += " = ";
    ret += setv.value();
}

CONV_FN(SettingValues, setv)
{
    SetValueToString(ret, setv.set_value());
    for (int i = 0; i < setv.other_values_size(); i++)
    {
        ret += ", ";
        SetValueToString(ret, setv.other_values(i));
    }
}

template <typename T>
static void appendSettings(String & ret, const T & msg)
{
    if (msg.has_setting_values())
    {
        ret += " SETTINGS ";
        SettingValuesToString(ret, msg.setting_values());
    }
}

CONV_FN(FileFunc, ff)
{
    ret += FileFunc_FName_Name(ff.fname());
    ret += "(";
    if (ff.fname() == FileFunc_FName_fileCluster)
    {
        ClusterToString(ret, false, ff.cluster());
    }
    appendSQLStringLiteral(ret, ff.path());
    /// Arguments are positional, so a skipped one followed by a later one is filled with 'auto'
    if (ff.has_format() || ff.has_structure() || ff.has_fcomp())
    {
        ret += ", ";
        appendSQLStringLiteral(ret, ff.has_format() ? ff.format() : "auto");
    }
    if (ff.has_structure())
    {
        ret += ", ";
        ExprToString(ret, ff.structure());
    }
    else if (ff.has_fcomp())
    {
        ret += ", 'auto'";
    }
    if (ff.has_fcomp())
    {
        ret += ", ";
        appendSQLStringLiteral(ret, ff.fcomp());
    }
}

CONV_FN(NumbersFunc, gsf)
{
    ret += NumbersFunc_NumbersName_Name(gsf.fname());
    ret += "(";
    ExprToString(ret, gsf.expr1());
    if (gsf.has_expr2())
    {
        ret += ", ";
        ExprToString(ret, gsf.expr2());
    }
    if (gsf.has_expr3())
    {
        ret += ", ";
        ExprToString(ret, gsf.expr3());
    }
}

static void FlatExprSchemaTableToString(String & ret, const ExprSchemaTable & est, const String & separator)
{
    ret += "'";
    ConvertToSQLString(ret, est.has_database() ? est.database().value() : "default");
    ret += separator;
    ConvertToSQLString(ret, est.table().value());
    ret += "'";
}

CONV_FN(TableFunction, tf);

static void ValuesStatementToString(String & ret, const bool tudf, const ValuesStatement & values)
{
    ret += "VALUES ";
    ret += tudf ? "(" : "";
    ret += "(";
    ExprListToString(ret, values.expr_list());
    ret += ")";
    for (int i = 0; i < values.extra_expr_lists_size(); i++)
    {
        ret += ", (";
        ExprListToString(ret, values.extra_expr_lists(i));
        ret += ")";
    }
}

static void TableOrFunctionToString(String & ret, const bool tudf, const TableOrFunction & tof)
{
    using TableOrFunctionType = TableOrFunction::JtfOneofCase;
    switch (tof.jtf_oneof_case())
    {
        case TableOrFunctionType::kEst:
            if (tudf)
            {
                FlatExprSchemaTableToString(ret, tof.est(), "', '");
            }
            else
            {
                ExprSchemaTableToString(ret, tof.est());
            }
            break;
        case TableOrFunctionType::kTfunc: TableFunctionToString(ret, tof.tfunc()); break;
        case TableOrFunctionType::kSelect:
            ret += tudf ? "view" : "";
            ret += "(";
            ExplainQueryToString(ret, tof.select());
            ret += ")";
            break;
        case TableOrFunctionType::kValuesStandard:
            ret += "(";
            ValuesStatementToString(ret, false, tof.values_standard());
            ret += ")";
            break;
        default: ret += "numbers(10)";
    }
}

CONV_FN(RemoteFunc, rfunc)
{
    const TableOrFunction & tof = rfunc.tof();
    bool has_arg = false;
    auto sep = [&]() -> String { return std::exchange(has_arg, true) ? ", " : ""; };

    ret += RemoteFunc_RName_Name(rfunc.rname());
    ret += "(";
    if (rfunc.has_named_collection())
    {
        ret += sep();
        ret += rfunc.named_collection();
    }
    if (rfunc.has_address())
    {
        ret += sep();
        appendSQLStringLiteral(ret, rfunc.address());
    }
    ret += sep();
    if (rfunc.has_named_collection() && tof.has_est())
    {
        const ExprSchemaTable & est = tof.est();
        if (est.has_database())
        {
            ret += "database=";
            appendEscapedSQLString(ret, est.database().value());
            ret += ", ";
        }
        ret += "table=";
        appendEscapedSQLString(ret, est.table().value());
    }
    else
    {
        TableOrFunctionToString(ret, true, tof);
    }
    if (rfunc.has_user())
    {
        ret += sep();
        appendSQLStringLiteral(ret, rfunc.user());
    }
    if (rfunc.has_password())
    {
        ret += sep();
        appendSQLStringLiteral(ret, rfunc.password());
    }
    if (rfunc.has_sharding_key())
    {
        ret += sep();
        if (rfunc.has_named_collection())
        {
            ret += "sharding_key=";
        }
        ExprToString(ret, rfunc.sharding_key());
    }
}

/// One `[key=]'value'` argument of a table function that may take a named collection
static void appendNamedArg(String & ret, bool & has_arg, const bool kv, const bool has, const char * key, const String & value)
{
    if (has)
    {
        ret += std::exchange(has_arg, true) ? ", " : "";
        if (kv)
        {
            ret += key;
        }
        appendSQLStringLiteral(ret, value);
    }
}

CONV_FN(MySQLFunc, mfunc)
{
    const bool kv = mfunc.has_named_collection();
    bool has_arg = kv;

    ret += "mysql(";
    if (kv)
    {
        ret += mfunc.named_collection();
    }
    appendNamedArg(ret, has_arg, kv, mfunc.has_address(), "host=", mfunc.address());
    appendNamedArg(ret, has_arg, kv, mfunc.has_rdatabase(), "database=", mfunc.rdatabase());
    appendNamedArg(ret, has_arg, kv, mfunc.has_rtable(), "table=", mfunc.rtable());
    appendNamedArg(ret, has_arg, kv, mfunc.has_user(), "user=", mfunc.user());
    appendNamedArg(ret, has_arg, kv, mfunc.has_password(), "password=", mfunc.password());
}

CONV_FN(PostgreSQLFunc, pfunc)
{
    const bool kv = pfunc.has_named_collection();
    bool has_arg = kv;

    ret += "postgresql(";
    if (kv)
    {
        ret += pfunc.named_collection();
    }
    appendNamedArg(ret, has_arg, kv, pfunc.has_address(), "host=", pfunc.address());
    appendNamedArg(ret, has_arg, kv, pfunc.has_rdatabase(), "database=", pfunc.rdatabase());
    appendNamedArg(ret, has_arg, kv, pfunc.has_rtable(), "table=", pfunc.rtable());
    appendNamedArg(ret, has_arg, kv, pfunc.has_user(), "user=", pfunc.user());
    appendNamedArg(ret, has_arg, kv, pfunc.has_password(), "password=", pfunc.password());
    appendNamedArg(ret, has_arg, kv, pfunc.has_rschema(), "schema=", pfunc.rschema());
}

CONV_FN(SQLiteFunc, sfunc)
{
    ret += "sqlite(";
    appendSQLStringLiteral(ret, sfunc.rdatabase());
    ret += ", ";
    appendSQLStringLiteral(ret, sfunc.rtable());
}

CONV_FN(RedisFunc, rfunc)
{
    ret += "redis(";
    appendSQLStringLiteral(ret, rfunc.address());
    ret += ", ";
    ExprToString(ret, rfunc.key());
    /// The structure is positional with no placeholder, so the later arguments need it
    if (!rfunc.has_structure())
    {
        return;
    }
    ret += ", ";
    ExprToString(ret, rfunc.structure());
    /// Unset `db_index` and `password` read as their defaults 0 and '' when a later argument follows
    if (rfunc.has_db_index() || rfunc.has_password() || rfunc.has_pool_size())
    {
        ret += ", ";
        ret += std::to_string(rfunc.db_index());
    }
    if (rfunc.has_password() || rfunc.has_pool_size())
    {
        ret += ", ";
        appendSQLStringLiteral(ret, rfunc.password());
    }
    if (rfunc.has_pool_size())
    {
        ret += ", ";
        ret += std::to_string(rfunc.pool_size());
    }
}

CONV_FN(MongoDBFunc, mfunc)
{
    const bool kv = mfunc.has_named_collection();
    bool has_arg = kv;

    ret += "mongodb(";
    if (kv)
    {
        ret += mfunc.named_collection();
    }
    appendNamedArg(ret, has_arg, kv, mfunc.has_address(), "host=", mfunc.address());
    appendNamedArg(ret, has_arg, kv, mfunc.has_database(), "database=", mfunc.database());
    appendNamedArg(ret, has_arg, kv, mfunc.has_collection(), "collection=", mfunc.collection());
    appendNamedArg(ret, has_arg, kv, mfunc.has_user(), "user=", mfunc.user());
    appendNamedArg(ret, has_arg, kv, mfunc.has_password(), "password=", mfunc.password());
    if (mfunc.has_structure())
    {
        ret += has_arg ? ", " : "";
        ret += "structure=";
        ExprToString(ret, mfunc.structure());
    }
}

CONV_FN(ObjectStoreFunc, ofunc)
{
    ret += ObjectStoreFunc_FName_Name(ofunc.fname());
    if (ofunc.cluster_func())
    {
        ret += "Cluster";
    }
    ret += "(";
    if (ofunc.cluster_func())
    {
        ClusterToString(ret, false, ofunc.cluster());
    }
    ret += ofunc.credential();
    for (int i = 0; i < ofunc.params_size(); i++)
    {
        ret += ", ";
        KeyValuePairToString(ret, ofunc.params(i));
    }
}

CONV_FN(URLFunc, url)
{
    ret += URLFunc_FName_Name(url.fname());
    ret += "(";
    if (url.fname() == URLFunc_FName_urlCluster)
    {
        ClusterToString(ret, false, url.cluster());
    }
    appendSQLStringLiteral(ret, url.uurl());
    if (url.has_format())
    {
        ret += ", ";
        appendSQLStringLiteral(ret, url.format());
    }
    if (url.has_structure())
    {
        ret += ", ";
        ExprToString(ret, url.structure());
    }
}

CONV_FN(SQLTableFuncCall, sfc)
{
    ret += SQLTableFunc_Name(sfc.func()).substr(2);
    ret += '(';
    SQLFuncArgsToString(ret, sfc.args());
}

CONV_FN(MergeFunc, mfunc)
{
    ret += "merge(";
    if (mfunc.has_mdatabase())
    {
        ret += "REGEXP('";
        ret += mfunc.mdatabase();
        ret += "'), ";
    }
    appendSQLStringLiteral(ret, mfunc.mtable());
}

CONV_FN(ClusterFunc, cluster)
{
    const TableOrFunction & tof = cluster.tof();

    ret += "cluster";
    if (cluster.all_replicas())
    {
        ret += "AllReplicas";
    }
    ret += "(";
    ClusterToString(ret, false, cluster.cluster());
    TableOrFunctionToString(ret, true, tof);
    if (cluster.has_sharding_key())
    {
        ret += ", ";
        ExprToString(ret, cluster.sharding_key());
    }
}

CONV_FN(MergeTreeIndexFunc, mfunc)
{
    ret += "mergeTreeIndex(";
    FlatExprSchemaTableToString(ret, mfunc.est(), "', '");
    if (mfunc.has_with_marks())
    {
        ret += ", with_marks = ";
        ret += mfunc.with_marks() ? "true" : "false";
    }
    if (mfunc.has_with_minmax())
    {
        ret += ", with_minmax = ";
        ret += mfunc.with_minmax() ? "true" : "false";
    }
}

CONV_FN(MergeTreeProjectionFunc, mfunc)
{
    ret += "mergeTreeProjection(";
    FlatExprSchemaTableToString(ret, mfunc.est(), "', '");
    ret += ", ";
    appendEscapedSQLString(ret, mfunc.proj().value());
}

CONV_FN(MergeTreeTextIndexFunc, mttxtidx)
{
    ret += "mergeTreeTextIndex(";
    FlatExprSchemaTableToString(ret, mttxtidx.est(), "', '");
    ret += ", ";
    appendEscapedSQLString(ret, mttxtidx.idx().value());
}

CONV_FN(MergeTreeAnalyzeIndexesFunc, mfunc)
{
    ret += "mergeTreeAnalyzeIndexes(";
    FlatExprSchemaTableToString(ret, mfunc.est(), "', '");
    if (mfunc.has_pred())
    {
        ret += ", ";
        ExprToString(ret, mfunc.pred());
    }
    if (mfunc.has_plist())
    {
        ret += ", [";
        for (int i = 0; i < mfunc.plist().parts_size(); i++)
        {
            if (i != 0)
            {
                ret += ", ";
            }
            appendSQLStringLiteral(ret, mfunc.plist().parts(i));
        }
        ret += "]";
    }
}

CONV_FN(GenerateRandomFunc, grfunc)
{
    ret += "generateRandom(";
    ExprToString(ret, grfunc.structure());
    if (grfunc.has_random_seed())
    {
        ret += ", ";
        ret += std::to_string(grfunc.random_seed());
    }
    if (grfunc.has_max_string_length())
    {
        ret += ", ";
        ret += std::to_string(grfunc.max_string_length());
    }
    if (grfunc.has_max_array_length())
    {
        ret += ", ";
        ret += std::to_string(grfunc.max_array_length());
    }
}

CONV_FN(ArrowFlightFunc, afunc)
{
    ret += "arrowFlight(";
    appendSQLStringLiteral(ret, afunc.address());
    ret += ", ";
    appendSQLStringLiteral(ret, afunc.dataset());
}

CONV_FN(TableFunction, tf)
{
    using TableFunctionType = TableFunction::JtfOneofCase;
    switch (tf.jtf_oneof_case())
    {
        case TableFunctionType::kFile: FileFuncToString(ret, tf.file()); break;
        case TableFunctionType::kNumbers: NumbersFuncToString(ret, tf.numbers()); break;
        case TableFunctionType::kRemote: RemoteFuncToString(ret, tf.remote()); break;
        case TableFunctionType::kMysql: MySQLFuncToString(ret, tf.mysql()); break;
        case TableFunctionType::kPostgresql: PostgreSQLFuncToString(ret, tf.postgresql()); break;
        case TableFunctionType::kSqite: SQLiteFuncToString(ret, tf.sqite()); break;
        case TableFunctionType::kObjectFunc: ObjectStoreFuncToString(ret, tf.object_func()); break;
        case TableFunctionType::kMerge: MergeFuncToString(ret, tf.merge()); break;
        case TableFunctionType::kCluster: ClusterFuncToString(ret, tf.cluster()); break;
        case TableFunctionType::kLoop:
            ret += "loop(";
            TableOrFunctionToString(ret, true, tf.loop());
            break;
        case TableFunctionType::kGrandom: GenerateRandomFuncToString(ret, tf.grandom()); break;
        case TableFunctionType::kValues: ValuesStatementToString(ret, true, tf.values()); break;
        case TableFunctionType::kDictionary:
            ret += "dictionary(";
            FlatExprSchemaTableToString(ret, tf.dictionary(), ".");
            break;
        case TableFunctionType::kUrl: URLFuncToString(ret, tf.url()); break;
        case TableFunctionType::kRedis: RedisFuncToString(ret, tf.redis()); break;
        case TableFunctionType::kMongodb: MongoDBFuncToString(ret, tf.mongodb()); break;
        case TableFunctionType::kNullf:
            ret += "`null`(";
            ExprToString(ret, tf.nullf());
            break;
        case TableFunctionType::kFlight: ArrowFlightFuncToString(ret, tf.flight()); break;
        case TableFunctionType::kMtindex: MergeTreeIndexFuncToString(ret, tf.mtindex()); break;
        case TableFunctionType::kMtproj: MergeTreeProjectionFuncToString(ret, tf.mtproj()); break;
        case TableFunctionType::kMttxtidx: MergeTreeTextIndexFuncToString(ret, tf.mttxtidx()); break;
        case TableFunctionType::kMtcodecblocks:
            ret += "mergeTreeCodecBlockCounts(";
            FlatExprSchemaTableToString(ret, tf.mtcodecblocks(), "', '");
            break;
        case TableFunctionType::kMtanindex: MergeTreeAnalyzeIndexesFuncToString(ret, tf.mtanindex()); break;
        case TableFunctionType::kFunc: SQLTableFuncCallToString(ret, tf.func()); break;
        case TableFunctionType::kObfuscate:
            ret += "obfuscate(";
            SelectToString(ret, tf.obfuscate());
            break;
        default: ret += "numbers(10";
    }
    appendHTTPHeaders(ret, tf);
    if (tf.has_setting_values())
    {
        ret += ", SETTINGS ";
        SettingValuesToString(ret, tf.setting_values());
    }
    ret += ")";
}

CONV_FN(SampleClause, sc)
{
    ret += " SAMPLE ";
    if (sc.has_ratio())
    {
        /// Format ratio as decimal (e.g., 0.1)
        ret += fmt::format("{:.6f}", sc.ratio());
    }
    else if (sc.has_num_rows())
    {
        /// Format as number of rows
        ret += std::to_string(sc.num_rows());
    }
    /// Add OFFSET if present
    if (sc.has_offset_ratio() || sc.has_offset_rows())
    {
        ret += " OFFSET ";
        if (sc.has_offset_ratio())
        {
            ret += fmt::format("{:.6f}", sc.offset_ratio());
        }
        else
        {
            ret += std::to_string(sc.offset_rows());
        }
    }
}

CONV_FN(JoinedTableOrFunction, jtf)
{
    const TableOrFunction & tof = jtf.tof();

    TableOrFunctionToString(ret, false, tof);
    if (jtf.has_table_alias())
    {
        ret += " AS ";
        SQLIdentifierToString(ret, jtf.table_alias());
    }
    if (jtf.col_aliases_size())
    {
        ret += "(";
        for (int i = 0; i < jtf.col_aliases_size(); i++)
        {
            if (i != 0)
            {
                ret += ", ";
            }
            ret += "`";
            ColumnToString(ret, 1, jtf.col_aliases(i));
            ret += "`";
        }
        ret += ")";
    }
    if (jtf.final())
    {
        ret += " FINAL";
    }
    if (jtf.has_sample())
    {
        SampleClauseToString(ret, jtf.sample());
    }
}

CONV_FN(TableOrSubquery, tos)
{
    if (tos.has_joined_table())
    {
        JoinedTableOrFunctionToString(ret, tos.joined_table());
    }
    else
    {
        ret += "(SELECT 1 c0) t0";
    }
}

CONV_FN(FromStatement, fs)
{
    ret += "FROM ";
    JoinClauseToString(ret, fs.tos().join_clause());
}

CONV_FN(ExprComparisonHighProbability, echp)
{
    if (echp.has_expr())
    {
        ExprToString(ret, echp.expr());
    }
}

CONV_FN(WhereStatement, ws)
{
    ExprComparisonHighProbabilityToString(ret, ws.expr());
}

CONV_FN(OptionalExprList, oel)
{
    for (int i = 0; i < oel.exprs_size(); i++)
    {
        if (i != 0)
        {
            ret += ", ";
        }
        ExprToString(ret, oel.exprs(i));
    }
}

CONV_FN(GroupingSets, gs)
{
    ret += "(";
    OptionalExprListToString(ret, gs.exprs());
    ret += ")";
    for (int i = 0; i < gs.other_exprs_size(); i++)
    {
        ret += ", (";
        OptionalExprListToString(ret, gs.other_exprs(i));
        ret += ")";
    }
}

CONV_FN(GroupByList, gbl)
{
    using GroupByListType = GroupByList::GroupByListOneofCase;
    switch (gbl.group_by_list_oneof_case())
    {
        case GroupByListType::kExprs: ExprListToString(ret, gbl.exprs()); break;
        case GroupByListType::kRollup:
            ret += "ROLLUP(";
            ExprListToString(ret, gbl.rollup());
            ret += ")";
            break;
        case GroupByListType::kCube:
            ret += "CUBE(";
            ExprListToString(ret, gbl.cube());
            ret += ")";
            break;
        case GroupByListType::kSets:
            ret += "GROUPING SETS(";
            GroupingSetsToString(ret, gbl.sets());
            ret += ")";
            break;
        default: ret += "GROUPING SETS(())";
    }
    if (gbl.has_gsm())
    {
        ret += " WITH ";
        ret += GroupByList_GroupingSetsModifier_Name(gbl.gsm());
    }
    if (gbl.with_totals())
    {
        ret += " WITH TOTALS";
    }
}


CONV_FN(GroupByStatement, gbs)
{
    ret += "GROUP BY ";
    if (gbs.has_glist())
    {
        GroupByListToString(ret, gbs.glist());
    }
    else
    {
        ret += "ALL";
    }
    if (gbs.has_having_expr())
    {
        ret += " HAVING ";
        WhereStatementToString(ret, gbs.having_expr());
    }
}

CONV_FN(LimitByStatement, ls)
{
    ret += "LIMIT ";
    ExprToString(ret, ls.limit());
    if (ls.has_offset())
    {
        ret += ls.comma() ? "," : " OFFSET";
        ret += " ";
        ExprToString(ret, ls.offset());
    }
    ret += " BY ";
    if (ls.has_by_expr())
    {
        ExprToString(ret, ls.by_expr());
    }
    else
    {
        ret += "ALL";
    }
}

CONV_FN(FetchStatement, fet)
{
    ret += "FETCH ";
    ret += fet.first() ? "FIRST" : "NEXT";
    ret += " ";
    ExprToString(ret, fet.row_count());
    ret += " ";
    ret += RowsKeyword_Name(fet.rows()).substr(4);
}

static void LimitStatementToString(String & ret, const bool has_offset, const LimitStatement & lim)
{
    ret += "LIMIT";
    if (lim.has_limit())
    {
        ret += " ";
        ExprToString(ret, lim.limit());
    }
    if (!has_offset && lim.with_ties())
    {
        ret += " WITH TIES";
    }
    if (lim.has_limit_after())
    {
        ret += " AFTER ";
        ExprToString(ret, lim.limit_after());
        if (lim.after_all())
        {
            ret += " ALL";
        }
    }
    if (lim.has_limit_until())
    {
        ret += " UNTIL ";
        ExprToString(ret, lim.limit_until());
    }
}

static void OffsetStatementToString(String & ret, const bool has_limit, const OffsetStatement & off)
{
    ret += (has_limit && off.comma()) ? "," : "OFFSET";
    ret += " ";
    ExprToString(ret, off.row_count());
    if ((!has_limit || !off.comma()) && (off.has_rows() || off.has_fetch()))
    {
        ret += " ";
        ret += off.has_rows() ? RowsKeyword_Name(off.rows()).substr(4) : "ROWS";
    }
    if (off.has_fetch())
    {
        ret += " ";
        FetchStatementToString(ret, off.fetch());
        ret += " ";
        ret += off.with_ties() ? "WITH TIES" : "ONLY";
    }
    else if (has_limit && off.comma() && off.with_ties())
    {
        ret += " WITH TIES";
    }
}

CONV_FN(SelectStatementCore, ssc)
{
    const bool from_first = ssc.from_first() && ssc.has_from() && ssc.from().tos().join_clause().clauses_size() == 0;

    if (from_first)
    {
        FromStatementToString(ret, ssc.from());
        ret += " ";
    }
    ret += "SELECT ";
    if (ssc.has_s_or_d())
    {
        ret += AllOrDistinct_Name(ssc.s_or_d());
        ret += " ";
    }
    if (ssc.result_columns_size() == 0)
    {
        ret += "*";
    }
    else
    {
        for (int i = 0; i < ssc.result_columns_size(); i++)
        {
            if (i != 0)
            {
                ret += ", ";
            }
            ResultColumnToString(ret, ssc.result_columns(i));
        }
    }
    if (!from_first && ssc.has_from())
    {
        ret += " ";
        FromStatementToString(ret, ssc.from());
    }
    if (ssc.has_pre_where())
    {
        ret += " PREWHERE ";
        WhereStatementToString(ret, ssc.pre_where());
    }
    if (ssc.has_where())
    {
        ret += " WHERE ";
        WhereStatementToString(ret, ssc.where());
    }
    if (ssc.has_groupby())
    {
        ret += " ";
        GroupByStatementToString(ret, ssc.groupby());
    }
    if (ssc.window_defs_size() > 0)
    {
        ret += " WINDOW ";
        for (int i = 0; i < ssc.window_defs_size(); i++)
        {
            if (i != 0)
            {
                ret += ", ";
            }
            WindowDefToString(ret, ssc.window_defs(i));
        }
    }
    if (ssc.has_qualify_expr())
    {
        ret += " QUALIFY ";
        WhereStatementToString(ret, ssc.qualify_expr());
    }
    if (ssc.has_orderby())
    {
        ret += " ";
        OrderByStatementToString(ret, ssc.orderby());
    }
    else if (ssc.shuffle())
    {
        ret += " SHUFFLE";
    }
    if (ssc.has_limit_by())
    {
        ret += " ";
        LimitByStatementToString(ret, ssc.limit_by());
    }
    if (ssc.has_limit())
    {
        ret += " ";
        LimitStatementToString(ret, ssc.has_offset(), ssc.limit());
    }
    if (ssc.has_offset() && (ssc.has_limit() || !ssc.has_limit_by()))
    {
        ret += " ";
        OffsetStatementToString(ret, ssc.has_limit(), ssc.offset());
    }
}

CONV_FN(SetQuery, setq)
{
    ret += setq.paren1() ? "(" : "";
    ExplainQueryToString(ret, setq.sel1());
    ret += setq.paren1() ? ")" : "";
    ret += " ";
    ret += SetQuery_SetOp_Name(setq.set_op());
    if (setq.has_s_or_d())
    {
        ret += " ";
        ret += AllOrDistinct_Name(setq.s_or_d());
    }
    ret += " ";
    ret += setq.paren2() ? "(" : "";
    ExplainQueryToString(ret, setq.sel2());
    ret += setq.paren2() ? ")" : "";
}

CONV_FN(CTEquery, cteq)
{
    SQLIdentifierToString(ret, cteq.table());
    ret += " AS ";
    if (cteq.is_materialized())
    {
        ret += "MATERIALIZED ";
    }
    ret += "(";
    SelectToString(ret, cteq.query());
    ret += ")";
}

CONV_FN(CTEexpr, cte_expr)
{
    ExprToString(ret, cte_expr.expr());
    ret += " AS `";
    ColumnToString(ret, 1, cte_expr.col_alias());
    ret += "`";
}

CONV_FN(SingleCTE, scte)
{
    if (scte.has_cte_query())
    {
        CTEqueryToString(ret, scte.cte_query());
    }
    else if (scte.has_cte_expr())
    {
        CTEexprToString(ret, scte.cte_expr());
    }
    else
    {
        ret += "1 AS c0";
    }
}

CONV_FN(CTEs, cteq)
{
    /// RECURSIVE applies to the whole WITH clause, it is a syntax error before a later CTE
    bool recursive = cteq.cte().cte_query().recursive();

    for (int i = 0; i < cteq.other_ctes_size(); i++)
    {
        recursive |= cteq.other_ctes(i).cte_query().recursive();
    }
    ret += "WITH ";
    ret += recursive ? "RECURSIVE " : "";
    SingleCTEToString(ret, cteq.cte());
    for (int i = 0; i < cteq.other_ctes_size(); i++)
    {
        ret += ", ";
        SingleCTEToString(ret, cteq.other_ctes(i));
    }
    ret += " ";
}

CONV_FN(Select, select)
{
    if (select.has_ctes())
    {
        CTEsToString(ret, select.ctes());
    }
    if (select.has_select_core())
    {
        SelectStatementCoreToString(ret, select.select_core());
    }
    else if (select.has_set_query())
    {
        SetQueryToString(ret, select.set_query());
    }
    else
    {
        ret += "1";
    }
    appendSettings(ret, select);
}

CONV_FN(ColumnStatistics, cst)
{
    ret += ColumnStat_Name(cst.stat()).substr(5);
    for (int i = 0; i < cst.other_stats_size(); i++)
    {
        ret += ", ";
        ret += ColumnStat_Name(cst.other_stats(i)).substr(5);
    }
}

CONV_FN(IndexParam, ip)
{
    using IndexParamType = IndexParam::IndexParamOneofCase;
    switch (ip.index_param_oneof_case())
    {
        case IndexParamType::kIval: ret += std::to_string(ip.ival()); break;
        case IndexParamType::kDval: ret += doubleToSQLString(ip.dval()); break;
        case IndexParamType::kSval: appendSQLStringLiteral(ret, ip.sval()); break;
        case IndexParamType::kUnescapedSval: ret += ip.unescaped_sval(); break;
        case IndexParamType::kKval:
            ret += ip.kval().key();
            ret += " = ";
            ExprToString(ret, ip.kval().value());
            break;
        default: ret += "0";
    }
}

CONV_FN(BackupParam, bp)
{
    using BackupParamType = BackupParam::BackupParamOneofCase;
    switch (bp.backup_param_oneof_case())
    {
        case BackupParamType::kSvalue: appendSQLStringLiteral(ret, bp.svalue()); break;
        case BackupParamType::kRvalue: ret += bp.rvalue(); break;
        default: ret += "backup.data";
    }
}

void BackupOutToString(String & ret, const BackupOut & bout)
{
    const BackupOut_BackupOutput & output = bout.out();

    ret += BackupOut_BackupOutput_Name(output);
    if (output != BackupOut_BackupOutput_Null)
    {
        ret += "(";
        for (int i = 0; i < bout.out_params_size(); i++)
        {
            if (i != 0)
            {
                ret += ", ";
            }
            BackupParamToString(ret, bout.out_params(i));
        }
        ret += ")";
    }
}

CONV_FN(DatabaseEngineParam, dep)
{
    using DatabaseEngineParamType = DatabaseEngineParam::DatabaseEngineParamOneofCase;
    switch (dep.database_engine_param_oneof_case())
    {
        case DatabaseEngineParamType::kSvalue: appendSQLStringLiteral(ret, dep.svalue()); break;
        case DatabaseEngineParamType::kExpr: ExprToString(ret, dep.expr()); break;
        case DatabaseEngineParamType::kBackupOut: BackupOutToString(ret, dep.backup_out()); break;
        default: ret += "d0";
    }
}

CONV_FN(DatabaseEngine, deng)
{
    ret += DatabaseEngineValues_Name(deng.engine()).substr(1);
    if (deng.params_size())
    {
        ret += "(";
        for (int i = 0; i < deng.params_size(); i++)
        {
            if (i != 0)
            {
                ret += ", ";
            }
            DatabaseEngineParamToString(ret, deng.params(i));
        }
        ret += ")";
    }
    appendSettings(ret, deng);
}

void CreateDatabaseToString(String & ret, const CreateDatabase & create_database)
{
    ret += "CREATE DATABASE ";
    if (create_database.if_not_exists())
    {
        ret += "IF NOT EXISTS ";
    }
    SQLIdentifierToString(ret, create_database.database());
    if (create_database.has_uuid())
    {
        ret += " UUID '";
        ret += create_database.uuid();
        ret += "'";
    }
    ClusterToString(ret, true, create_database.cluster());
    ret += " ENGINE = ";
    DatabaseEngineToString(ret, create_database.dengine());
    if (create_database.has_comment())
    {
        ret += " COMMENT ";
        ret += create_database.comment();
    }
}

static void CreateOrReplaceToString(String & ret, const CreateReplaceOption & cro)
{
    switch (cro)
    {
        case CreateReplaceOption::Create: ret += "CREATE"; break;
        case CreateReplaceOption::Replace: ret += "REPLACE"; break;
        case CreateReplaceOption::CreateOrReplace: ret += "CREATE OR REPLACE"; break;
    }
}

CONV_FN(CreateFunction, create_function)
{
    CreateOrReplaceToString(ret, create_function.create_opt());
    ret += " FUNCTION ";
    SQLIdentifierToString(ret, create_function.function());
    ClusterToString(ret, true, create_function.cluster());
    ret += " AS ";
    LambdaExprToString(ret, create_function.lexpr());
}

CONV_FN(DefaultModifier, def_mod)
{
    ret += DModifier_Name(def_mod.dvalue()).substr(4);
    if (def_mod.has_expr())
    {
        ret += " ";
        ExprToString(ret, def_mod.expr());
    }
}

CONV_FN(ColumnDef, cdf)
{
    ColumnPathToString(ret, 0, cdf.col());
    ret += " ";
    TypeNameToString(ret, 0, cdf.type());
    if (cdf.has_nullable())
    {
        ret += cdf.nullable() ? "" : " NOT";
        ret += " NULL";
    }
    if (cdf.has_defaultv())
    {
        ret += " ";
        DefaultModifierToString(ret, cdf.defaultv());
    }
    if (cdf.has_comment())
    {
        ret += " COMMENT ";
        ret += cdf.comment();
    }
    if (cdf.has_codecs())
    {
        ret += " CODEC(";
        ret += cdf.codecs();
        ret += ")";
    }
    if (cdf.has_stats())
    {
        ret += " STATISTICS(";
        ColumnStatisticsToString(ret, cdf.stats());
        ret += ")";
    }
    if (cdf.has_ttl_expr())
    {
        ret += " TTL ";
        ExprToString(ret, cdf.ttl_expr());
    }
    if ((!cdf.has_defaultv() || cdf.defaultv().dvalue() != DModifier::DEF_EPHEMERAL) && cdf.is_pkey())
    {
        ret += " PRIMARY KEY";
    }
    if (cdf.has_setting_values())
    {
        ret += " SETTINGS(";
        SettingValuesToString(ret, cdf.setting_values());
        ret += ")";
    }
}

/// The `TYPE type(params) [GRANULARITY n]` tail shared by table indexes and hypothetical indexes
static void IndexDefTypeToString(String & ret, const IndexDef & idef)
{
    ret += " TYPE ";
    ret += IndexType_Name(idef.type()).substr(4);
    if (idef.params_size())
    {
        ret += "(";
        for (int i = 0; i < idef.params_size(); i++)
        {
            if (i != 0)
            {
                ret += ", ";
            }
            IndexParamToString(ret, idef.params(i));
        }
        ret += ")";
    }
    if (idef.has_granularity())
    {
        ret += " GRANULARITY ";
        ret += std::to_string(idef.granularity());
    }
}

CONV_FN(IndexDef, idef)
{
    ret += "INDEX ";
    if (idef.has_idx())
    {
        SQLIdentifierToString(ret, idef.idx());
        ret += " ";
    }
    ExprToString(ret, idef.expr());
    IndexDefTypeToString(ret, idef);
}

/// Everything after the projection name, shared by table projections and hypothetical projections
static void ProjectionDefBodyToString(String & ret, const ProjectionDef & proj_def)
{
    using ProjectionDefType = ProjectionDef::ProjectionOneofCase;
    switch (proj_def.projection_oneof_case())
    {
        case ProjectionDefType::kSelDef:
            ret += "(";
            SelectToString(ret, proj_def.sel_def());
            ret += ")";
            break;
        case ProjectionDefType::kIdxDef: IndexDefToString(ret, proj_def.idx_def()); break;
        default: ret += "(SELECT c0 ORDER BY c0)";
    }
    if (proj_def.has_setting_values())
    {
        ret += " WITH SETTINGS (";
        SettingValuesToString(ret, proj_def.setting_values());
        ret += ")";
    }
}

CONV_FN(ProjectionDef, proj_def)
{
    ret += "PROJECTION ";
    SQLIdentifierToString(ret, proj_def.proj());
    ret += " ";
    ProjectionDefBodyToString(ret, proj_def);
}

CONV_FN(ConstraintDef, const_def)
{
    ret += "CONSTRAINT ";
    SQLIdentifierToString(ret, const_def.constr());
    ret += " ";
    ret += ConstraintDef_ConstraintType_Name(const_def.ctype());
    ret += " (";
    ExprToString(ret, const_def.expr());
    ret += ")";
}

CONV_FN(TableDefItem, tdef)
{
    using CreateDefType = TableDefItem::CreatedefOneofCase;
    switch (tdef.createdef_oneof_case())
    {
        case CreateDefType::kColDef: ColumnDefToString(ret, tdef.col_def()); break;
        case CreateDefType::kIdxDef: IndexDefToString(ret, tdef.idx_def()); break;
        case CreateDefType::kProjDef: ProjectionDefToString(ret, tdef.proj_def()); break;
        case CreateDefType::kConstDef: ConstraintDefToString(ret, tdef.const_def()); break;
        case CreateDefType::kCodecDecl:
            ret += "CODEC ";
            ColumnPathToString(ret, 0, tdef.codec_decl().col());
            ret += " (";
            ret += tdef.codec_decl().codecs();
            ret += ")";
            break;
        default: ret += "c0 Int";
    }
}

CONV_FN(TableDef, tdef)
{
    if (tdef.table_defs_size() > 0)
    {
        ret += " (";
        for (int i = 0; i < tdef.table_defs_size(); i++)
        {
            if (i != 0)
            {
                ret += ", ";
            }
            TableDefItemToString(ret, tdef.table_defs(i));
        }
        ret += ")";
    }
}

CONV_FN(TableKeyExpr, tke)
{
    ExprToString(ret, tke.expr());
    if (tke.has_asc_desc())
    {
        ret += " ";
        ret += AscDesc_Name(tke.asc_desc());
    }
}

CONV_FN(TableKey, to)
{
    if (to.exprs_size() == 0)
    {
        ret += "tuple()";
    }
    else
    {
        ret += "(";
        for (int i = 0; i < to.exprs_size(); i++)
        {
            if (i != 0)
            {
                ret += ", ";
            }
            TableKeyExprToString(ret, to.exprs(i));
        }
        ret += ")";
    }
}

CONV_FN(TableEngineParam, tep)
{
    using TableEngineParamType = TableEngineParam::TableEngineParamOneofCase;
    switch (tep.table_engine_param_oneof_case())
    {
        case TableEngineParamType::kCols: ColumnPathToString(ret, 0, tep.cols()); break;
        case TableEngineParamType::kOut: ret += tep.out(); break;
        case TableEngineParamType::kInOut: ret += tep.in_out(); break;
        case TableEngineParamType::kJoinOp: ret += JoinType_Name(tep.join_op()).substr(2); break;
        case TableEngineParamType::kJoinConst: ret += JoinConst_Name(tep.join_const()).substr(2); break;
        case TableEngineParamType::kDatabase: SQLIdentifierToString(ret, tep.database()); break;
        case TableEngineParamType::kTable: SQLIdentifierToString(ret, tep.table()); break;
        case TableEngineParamType::kNum: ret += std::to_string(tep.num()); break;
        case TableEngineParamType::kSvalue: appendSQLStringLiteral(ret, tep.svalue()); break;
        case TableEngineParamType::kColList:
            ret += "(";
            ColumnPathListToString(ret, 0, tep.col_list());
            ret += ")";
            break;
        case TableEngineParamType::kRegexp:
            ret += "REGEXP('";
            ret += tep.regexp();
            ret += "')";
            break;
        case TableEngineParamType::kEst: FlatExprSchemaTableToString(ret, tep.est(), "."); break;
        case TableEngineParamType::kExpr: ExprToString(ret, tep.expr()); break;
        case TableEngineParamType::kKvalue: KeyValuePairToString(ret, tep.kvalue()); break;
        case TableEngineParamType::kRvalue: ret += tep.rvalue(); break;
        default: ret += "c0";
    }
}

CONV_FN(TTLDelete, del)
{
    ret += "DELETE";
    if (del.has_where())
    {
        ret += " WHERE ";
        WhereStatementToString(ret, del.where());
    }
}

CONV_FN(TTLUpdate, upt)
{
    using TTLUpdateType = TTLUpdate::TtlupdateOneofCase;
    switch (upt.ttlupdate_oneof_case())
    {
        case TTLUpdateType::kDel: TTLDeleteToString(ret, upt.del()); break;
        case TTLUpdateType::kCodecs:
            ret += "RECOMPRESS CODEC(";
            ret += upt.codecs();
            ret += ")";
            break;
        case TTLUpdateType::kStorage:
            ret += "TO ";
            StorageToString(ret, upt.storage());
            break;
        default: ret += "DELETE";
    }
}

CONV_FN(TTLSet, ttl_set)
{
    ColumnPathToString(ret, 0, ttl_set.col());
    ret += " = ";
    ExprToString(ret, ttl_set.expr());
}

CONV_FN(TTLGroupBy, ttl_groupby)
{
    ret += "GROUP BY ";
    ExprListToString(ret, ttl_groupby.expr_list());
    ret += " SET ";
    TTLSetToString(ret, ttl_groupby.ttl_set());
    for (int i = 0; i < ttl_groupby.other_ttl_set_size(); i++)
    {
        ret += ", ";
        TTLSetToString(ret, ttl_groupby.other_ttl_set(i));
    }
}

CONV_FN(TTLEntry, ttl_entry)
{
    ExprToString(ret, ttl_entry.time_expr());
    if (ttl_entry.has_update())
    {
        ret += " ";
        TTLUpdateToString(ret, ttl_entry.update());
    }
    else if (ttl_entry.has_group_by())
    {
        ret += " ";
        TTLGroupByToString(ret, ttl_entry.group_by());
    }
}

CONV_FN(TTLExpr, ttl_expr)
{
    ret += "TTL ";
    TTLEntryToString(ret, ttl_expr.ttl_expr());
    for (int i = 0; i < ttl_expr.other_ttl_size(); i++)
    {
        ret += ", ";
        TTLEntryToString(ret, ttl_expr.other_ttl(i));
    }
}

CONV_FN(TableEngine, te)
{
    if (te.has_engine())
    {
        const TableEngineValues & teng = te.engine();

        ret += " ENGINE = ";
        if (te.has_toption()
            && ((teng >= TableEngineValues::MergeTree && teng <= TableEngineValues::GraphiteMergeTree) || teng == TableEngineValues::Set
                || teng == TableEngineValues::Join))
        {
            ret += TableEngineOption_Name(te.toption()).substr(1);
        }
        ret += TableEngineValues_Name(teng);
        ret += "(";
        for (int i = 0; i < te.params_size(); i++)
        {
            if (i != 0)
            {
                ret += ", ";
            }
            TableEngineParamToString(ret, te.params(i));
        }
        ret += ")";
    }
    if (te.has_order())
    {
        ret += " ORDER BY ";
        TableKeyToString(ret, te.order());
    }
    if (te.has_partition_by())
    {
        ret += " PARTITION BY ";
        TableKeyToString(ret, te.partition_by());
    }
    if (te.has_primary_key())
    {
        ret += " PRIMARY KEY ";
        TableKeyToString(ret, te.primary_key());
    }
    if (te.has_unique_key())
    {
        ret += " UNIQUE KEY ";
        TableKeyToString(ret, te.unique_key());
    }
    if (te.has_sample_by())
    {
        ret += " SAMPLE BY ";
        TableKeyToString(ret, te.sample_by());
    }
    if (te.has_ttl_expr())
    {
        ret += " ";
        TTLExprToString(ret, te.ttl_expr());
    }
    appendSettings(ret, te);
    for (int i = 0; i < te.ts_inner_tables_size(); i++)
    {
        const TimeSeriesInnerTable & tit = te.ts_inner_tables(i);
        /// TS_RECENT_SAMPLES -> RECENT SAMPLES
        String target = TimeSeriesTarget_Name(tit.target()).substr(3);
        const auto add_clause = [&](const String & clause) { ret += " " + target + clause; };

        std::replace(target.begin(), target.end(), '_', ' ');
        if (tit.has_target_table())
        {
            add_clause(" ");
            ExprSchemaTableToString(ret, tit.target_table());
        }
        if (tit.has_inner_uuid())
        {
            add_clause(" INNER UUID '" + tit.inner_uuid() + "'");
        }
        if (tit.has_columns())
        {
            add_clause(" INNER COLUMNS");
            TableDefToString(ret, tit.columns());
        }
        if (tit.has_engine())
        {
            add_clause(tit.skip_inner() ? " ENGINE = " : " INNER ENGINE = ");
            if (tit.has_toption())
            {
                ret += TableEngineOption_Name(tit.toption()).substr(1);
            }
            ret += TableEngineValues_Name(tit.engine());
        }
    }
}

CONV_FN(CreateTableAs, create_table)
{
    ret += " ";
    if (create_table.clone())
    {
        ret += "CLONE ";
    }
    ret += "AS ";
    ExprSchemaTableToString(ret, create_table.est());
}

CONV_FN(CreateTableSelect, create_table)
{
    if (create_table.empty())
    {
        ret += " EMPTY";
    }
    ret += " AS ";
    ret += create_table.paren() ? "(" : "";
    SelectToString(ret, create_table.select());
    ret += create_table.paren() ? ")" : "";
}

void CreateTableToString(String & ret, const CreateTable & create_table)
{
    CreateOrReplaceToString(ret, create_table.create_opt());
    ret += " ";
    if (create_table.is_temp())
    {
        ret += "TEMPORARY ";
    }
    ret += "TABLE ";
    if (create_table.if_not_exists())
    {
        ret += "IF NOT EXISTS ";
    }
    ExprSchemaTableToString(ret, create_table.est());
    if (create_table.has_uuid())
    {
        ret += " UUID '";
        ret += create_table.uuid();
        ret += "'";
    }
    ClusterToString(ret, true, create_table.cluster());
    if (create_table.has_table_def())
    {
        TableDefToString(ret, create_table.table_def());
    }
    else if (create_table.has_table_as())
    {
        CreateTableAsToString(ret, create_table.table_as());
    }
    TableEngineToString(ret, create_table.engine());
    if (create_table.has_table_def() && create_table.has_as_select_stmt())
    {
        CreateTableSelectToString(ret, create_table.as_select_stmt());
    }
    if (create_table.has_comment())
    {
        ret += " COMMENT ";
        ret += create_table.comment();
    }
}

CONV_FN(SQLObjectName, son)
{
    using SQLObjectNameType = SQLObjectName::ObjnOneofCase;
    switch (son.objn_oneof_case())
    {
        case SQLObjectNameType::kEst: ExprSchemaTableToString(ret, son.est()); break;
        case SQLObjectNameType::kDatabase: SQLIdentifierToString(ret, son.database()); break;
        case SQLObjectNameType::kFunction: SQLIdentifierToString(ret, son.function()); break;
        case SQLObjectNameType::kPolicy: SQLIdentifierToString(ret, son.policy()); break;
        case SQLObjectNameType::kIndex: SQLIdentifierToString(ret, son.index()); break;
        case SQLObjectNameType::kProjection: SQLIdentifierToString(ret, son.projection()); break;
        default: ret += "t0";
    }
}

static String SQLObjectToString(const SQLObject obj)
{
    if (obj == SQLObject::ROW_POLICY)
        return "ROW POLICY";
    if (obj == SQLObject::MASKING_POLICY)
        return "MASKING POLICY";
    if (obj == SQLObject::HYPOTHETICAL_INDEX)
        return "HYPOTHETICAL INDEX";
    if (obj == SQLObject::HYPOTHETICAL_PROJECTION)
        return "HYPOTHETICAL PROJECTION";
    return SQLObject_Name(obj);
}

CONV_FN(Drop, dt)
{
    const bool is_table = dt.sobject() == SQLObject::TABLE;

    ret += "DROP ";
    if (dt.all())
    {
        if (dt.sobject() == SQLObject::HYPOTHETICAL_INDEX)
        {
            ret += "ALL HYPOTHETICAL INDEXES";
            return;
        }
        if (dt.sobject() == SQLObject::HYPOTHETICAL_PROJECTION)
        {
            ret += "ALL HYPOTHETICAL PROJECTIONS";
            return;
        }
    }
    if ((is_table || dt.sobject() == SQLObject::VIEW) && dt.is_temp())
    {
        ret += "TEMPORARY ";
    }
    ret += SQLObjectToString(dt.sobject());
    if (dt.if_exists())
    {
        ret += " IF EXISTS";
    }
    if (is_table && dt.if_empty())
    {
        ret += " IF EMPTY";
    }
    ret += " ";
    SQLObjectNameToString(ret, dt.object());
    for (const auto & other : dt.other_objects())
    {
        ret += ", ";
        SQLObjectNameToString(ret, other);
    }
    ClusterToString(ret, true, dt.cluster());
    if ((dt.sobject() == SQLObject::ROW_POLICY || dt.sobject() == SQLObject::MASKING_POLICY
         || dt.sobject() == SQLObject::HYPOTHETICAL_INDEX || dt.sobject() == SQLObject::HYPOTHETICAL_PROJECTION)
        && dt.has_target())
    {
        ret += " ON ";
        ExprSchemaTableToString(ret, dt.target());
    }
    if (dt.sync())
    {
        ret += " SYNC";
    }
    appendSettings(ret, dt);
}

CONV_FN(SelectParen, sel)
{
    ret += sel.paren() ? "(" : "";
    SelectToString(ret, sel.select());
    ret += sel.paren() ? ")" : "";
}

CONV_FN(Insert, insert)
{
    if (insert.has_ctes())
    {
        CTEsToString(ret, insert.ctes());
    }
    ret += "INSERT INTO TABLE ";
    if (insert.tof().has_tfunc())
    {
        ret += "FUNCTION ";
    }
    TableOrFunctionToString(ret, false, insert.tof());
    if (insert.cols_size())
    {
        ret += " (";
        for (int i = 0; i < insert.cols_size(); i++)
        {
            if (i != 0)
            {
                ret += ", ";
            }
            ColumnPathToString(ret, 0, insert.cols(i));
        }
        ret += ")";
    }
    if (insert.has_returning())
    {
        /// Goes before the data, everything after VALUES or FORMAT is parsed as data
        ret += " RETURNING (";
        SelectToString(ret, insert.returning());
        ret += ")";
    }
    ret += " ";
    if (!insert.has_insert_file() && insert.has_setting_values())
    {
        ret += "SETTINGS ";
        SettingValuesToString(ret, insert.setting_values());
        ret += " ";
    }
    if (insert.has_values())
    {
        ValuesStatementToString(ret, false, insert.values());
    }
    else if (insert.has_select())
    {
        SelectParenToString(ret, insert.select());
    }
    else if (insert.has_insert_file())
    {
        const InsertFromFile & insert_file = insert.insert_file();

        ret += "FROM INFILE ";
        appendSQLStringLiteral(ret, insert_file.path());
        if (insert_file.has_fcomp())
        {
            ret += " COMPRESSION ";
            appendSQLStringLiteral(ret, insert_file.fcomp());
        }
        appendSettings(ret, insert);
        ret += " FORMAT ";
        ret += insert_file.format();
    }
    else if (insert.has_query())
    {
        ret += "VALUES ";
        ret += insert.query();
    }
    else
    {
        ret += "VALUES (0)";
    }
}

CONV_FN(PartitionExpr, pexpr)
{
    using PartitionType = PartitionExpr::PartitionOneofCase;
    switch (pexpr.partition_oneof_case())
    {
        case PartitionType::kPart: appendSQLStringLiteral(ret, pexpr.part()); break;
        case PartitionType::kPartition:
            ret += pexpr.partition();
            break;
        case PartitionType::kPartitionId:
            ret += "ID ";
            appendSQLStringLiteral(ret, pexpr.partition_id());
            break;
        case PartitionType::kAll: ret += "ALL"; break;
        default: ret += "tuple()";
    }
}

CONV_FN(SinglePartitionExpr, spexpr)
{
    const PartitionExpr & pexpr = spexpr.partition();

    ret += "PART";
    if (!pexpr.has_part())
    {
        ret += "ITION";
    }
    ret += " ";
    PartitionExprToString(ret, pexpr);
}

/// ` IN PARTITION ...`, followed by the extra partitions when the message has them
template <typename T>
static void appendInPartition(String & ret, const T & msg)
{
    if (msg.has_single_partition())
    {
        ret += " IN ";
        SinglePartitionExprToString(ret, msg.single_partition());
        if constexpr (requires { msg.other_partitions_size(); })
        {
            for (int i = 0; i < msg.other_partitions_size(); i++)
            {
                ret += ", ";
                PartitionExprToString(ret, msg.other_partitions(i));
            }
        }
    }
}

CONV_FN(Delete, del)
{
    appendInPartition(ret, del);
    ret += " WHERE ";
    WhereStatementToString(ret, del.where());
}

CONV_FN(LightDelete, del)
{
    ret += "DELETE FROM ";
    ExprSchemaTableToString(ret, del.est());
    ClusterToString(ret, true, del.cluster());
    DeleteToString(ret, del.del());
    appendSettings(ret, del);
}

CONV_FN(UpdateSet, upt)
{
    ColumnPathToString(ret, 0, upt.col());
    ret += " = ";
    ExprToString(ret, upt.expr());
}

CONV_FN(Update, upt)
{
    UpdateSetToString(ret, upt.update());
    for (int i = 0; i < upt.other_updates_size(); i++)
    {
        ret += ", ";
        UpdateSetToString(ret, upt.other_updates(i));
    }
    appendInPartition(ret, upt);
    ret += " WHERE ";
    WhereStatementToString(ret, upt.where());
}

CONV_FN(LightUpdate, upt)
{
    ret += "UPDATE ";
    ExprSchemaTableToString(ret, upt.est());
    ClusterToString(ret, true, upt.cluster());
    ret += " SET ";
    UpdateToString(ret, upt.upt());
    appendSettings(ret, upt);
}

CONV_FN(Truncate, trunc)
{
    ret += "TRUNCATE ";
    using TruncateType = Truncate::TruncateOneofCase;
    switch (trunc.truncate_oneof_case())
    {
        case TruncateType::kEst: ExprSchemaTableToString(ret, trunc.est()); break;
        case TruncateType::kAllTables:
            ret += "ALL TABLES FROM ";
            SQLIdentifierToString(ret, trunc.all_tables());
            break;
        case TruncateType::kDatabase:
            ret += "DATABASE ";
            SQLIdentifierToString(ret, trunc.database());
            break;
        default: ret += "t0";
    }
    ClusterToString(ret, true, trunc.cluster());
    if (trunc.sync())
    {
        ret += " SYNC";
    }
    appendSettings(ret, trunc);
}

CONV_FN(CheckTable, ct)
{
    ret += "CHECK ";
    ret += SQLObjectToString(ct.sobject());
    ret += " ";
    SQLObjectNameToString(ret, ct.object());
    if (ct.has_single_partition())
    {
        ret += " ";
        SinglePartitionExprToString(ret, ct.single_partition());
    }
    appendSettings(ret, ct);
    if (ct.has_format())
    {
        ret += " FORMAT ";
        ret += ct.format();
    }
}

CONV_FN(DescribeStatement, ds)
{
    ret += "DESCRIBE ";
    using DescType = DescribeStatement::DescOneofCase;
    /// `TEMPORARY TABLE` qualifier is only meaningful when the target is a bare table name.
    if (ds.temporary() && ds.desc_oneof_case() == DescType::kTof)
        ret += "TEMPORARY TABLE ";
    switch (ds.desc_oneof_case())
    {
        case DescType::kTof: TableOrFunctionToString(ret, false, ds.tof()); break;
        case DescType::kSel:
            ret += ds.paren() ? "(" : "";
            ExplainQueryToString(ret, ds.sel());
            ret += ds.paren() ? ")" : "";
            break;
        case DescType::kStf:
            SQLTableFuncCallToString(ret, ds.stf());
            ret += ")";
            break;
        default: ret += "t0";
    }
    appendSettings(ret, ds);
    if (ds.has_format())
    {
        ret += " FORMAT ";
        ret += ds.format();
    }
}

CONV_FN(DeduplicateExpr, de)
{
    if (de.has_col_list())
    {
        ret += " BY ";
        ColumnPathListToString(ret, 0, de.col_list());
    }
    else if (de.has_ded_star())
    {
        ret += " BY *";
    }
    else if (de.has_ded_star_except())
    {
        ret += " BY * EXCEPT (";
        ColumnPathListToString(ret, 0, de.ded_star_except());
        ret += ")";
    }
}

CONV_FN(OptimizeTable, ot)
{
    ret += "OPTIMIZE TABLE ";
    ExprSchemaTableToString(ret, ot.est());
    ClusterToString(ret, true, ot.cluster());
    if (ot.has_single_partition())
    {
        ret += " ";
        SinglePartitionExprToString(ret, ot.single_partition());
    }
    if (ot.dry_run_parts_size() > 0)
    {
        ret += " DRY RUN PARTS ";
        for (int i = 0; i < ot.dry_run_parts_size(); i++)
        {
            if (i != 0)
            {
                ret += ", ";
            }
            PartitionExprToString(ret, ot.dry_run_parts(i));
        }
    }
    if (ot.final())
    {
        ret += " ";
        ret += ot.use_force() ? "FORCE" : "FINAL";
    }
    if (ot.has_dedup())
    {
        ret += " DEDUPLICATE";
        if (ot.cleanup())
        {
            ret += " CLEANUP";
        }
        DeduplicateExprToString(ret, ot.dedup());
    }
    else if (ot.cleanup())
    {
        ret += " CLEANUP";
    }
    if (ot.manifest())
    {
        ret += " MANIFEST";
    }
    appendSettings(ret, ot);
    if (ot.has_format())
    {
        ret += " FORMAT ";
        ret += ot.format();
    }
}

CONV_FN(Exchange, et)
{
    ret += "EXCHANGE ";
    switch (et.sobject())
    {
        case SQLObject::TABLE:
        case SQLObject::VIEW:
            /// EXCHANGE VIEWS is a syntax error
            ret += "TABLES";
            break;
        case SQLObject::DICTIONARY: ret += "DICTIONARIES"; break;
        case SQLObject::DATABASE: ret += "DATABASES"; break;
        case SQLObject::FUNCTION: ret += "FUNCTIONS"; break;
        case SQLObject::ROW_POLICY: ret += "ROW POLICIES"; break;
        case SQLObject::MASKING_POLICY: ret += "MASKING POLICIES"; break;
        case SQLObject::HYPOTHETICAL_INDEX: ret += "HYPOTHETICAL INDEXES"; break;
        case SQLObject::HYPOTHETICAL_PROJECTION: ret += "HYPOTHETICAL PROJECTIONS"; break;
    }
    ret += " ";
    SQLObjectNameToString(ret, et.object1());
    ret += " AND ";
    SQLObjectNameToString(ret, et.object2());
    ClusterToString(ret, true, et.cluster());
    appendSettings(ret, et);
}

CONV_FN(RefreshInterval, ri)
{
    ret += std::to_string(ri.interval());
    ret += " ";
    ret += RefreshInterval_RefreshUnit_Name(ri.unit());
}

CONV_FN(RefreshableView, rv)
{
    ret += "REFRESH ";
    ret += RefreshableView_RefreshPolicy_Name(rv.policy());
    ret += " ";
    RefreshIntervalToString(ret, rv.interval());
    if (rv.policy() == RefreshableView_RefreshPolicy::RefreshableView_RefreshPolicy_EVERY && rv.has_offset())
    {
        ret += " OFFSET ";
        RefreshIntervalToString(ret, rv.offset());
    }
    if (rv.has_randomize())
    {
        ret += " RANDOMIZE FOR ";
        RefreshIntervalToString(ret, rv.randomize());
    }
    if (rv.has_depends())
    {
        ret += " DEPENDS ON ";
        ExprSchemaTableToString(ret, rv.depends().est());
    }
    appendSettings(ret, rv);
    if (rv.append())
    {
        ret += " APPEND";
        if (rv.incremental())
        {
            ret += " INCREMENTAL";
        }
    }
}

CONV_FN(CreateView, create_view)
{
    const bool materialized = create_view.materialized();
    const bool refreshable = create_view.has_refresh();

    CreateOrReplaceToString(ret, create_view.create_opt());
    ret += " ";
    if (create_view.is_temp())
    {
        ret += "TEMPORARY ";
    }
    if (materialized)
    {
        ret += "MATERIALIZED ";
    }
    ret += "VIEW ";
    if (create_view.if_not_exists())
    {
        ret += "IF NOT EXISTS ";
    }
    ExprSchemaTableToString(ret, create_view.est());
    if (create_view.has_uuid())
    {
        ret += " UUID '";
        ret += create_view.uuid();
        ret += "'";
    }
    ClusterToString(ret, true, create_view.cluster());
    if (materialized)
    {
        if (refreshable)
        {
            ret += " ";
            RefreshableViewToString(ret, create_view.refresh());
        }
        if (create_view.has_to())
        {
            const CreateMatViewTo & cmvt = create_view.to();

            ret += " TO ";
            ExprSchemaTableToString(ret, cmvt.est());
            if (cmvt.col_list_size())
            {
                ret += " (";
                for (int i = 0; i < cmvt.col_list_size(); i++)
                {
                    if (i != 0)
                    {
                        ret += ",";
                    }
                    ColumnDefToString(ret, cmvt.col_list(i));
                }
                ret += ")";
            }
        }
        if (create_view.has_engine())
        {
            TableEngineToString(ret, create_view.engine());
        }
        if (!refreshable && create_view.populate())
        {
            ret += " POPULATE";
        }
        if (refreshable && create_view.empty())
        {
            ret += " EMPTY";
        }
    }
    ret += " AS ";
    SelectParenToString(ret, create_view.select());
    if (create_view.has_comment())
    {
        ret += " COMMENT ";
        ret += create_view.comment();
    }
}

CONV_FN(DictionaryColumn, dc)
{
    ret += "`";
    ColumnToString(ret, 1, dc.col());
    ret += "` ";
    TypeNameToString(ret, 0, dc.type());
    ret += " DEFAULT ";
    ExprToString(ret, dc.default_val());
    if (dc.has_expression())
    {
        ret += " EXPRESSION ";
        ExprToString(ret, dc.expression());
    }
    if (dc.has_hierarchical())
    {
        ret += " ";
        ret += dc.hierarchical() ? "HIERARCHICAL" : "INJECTIVE";
    }
    if (dc.is_object_id())
    {
        ret += " IS_OBJECT_ID";
    }
}

CONV_FN(DictionarySourceDetails, dsd)
{
    bool has_something = false;
    /// Space-separated `KEYWORD value` pairs
    const auto key = [&](const char * kw)
    {
        ret += std::exchange(has_something, true) ? " " : "";
        ret += kw;
    };
    const auto literal = [&](const bool has, const char * kw, const String & value)
    {
        if (has)
        {
            key(kw);
            appendSQLStringLiteral(ret, value);
        }
    };

    ret += DictionarySourceDetails_DictionarySourceType_Name(dsd.source());
    ret += "(";
    literal(dsd.has_named_collection(), "NAME ", dsd.named_collection());
    if (dsd.has_est())
    {
        key("DB ");
        FlatExprSchemaTableToString(ret, dsd.est(), dsd.source() == DictionarySourceDetails::MONGODB ? "' COLLECTION '" : "' TABLE '");
    }
    if (dsd.has_query())
    {
        String query;

        SelectToString(query, dsd.query());
        key("QUERY ");
        appendEscapedSQLString(ret, query);
    }
    if (dsd.has_where())
    {
        String where;

        ExprToString(where, dsd.where());
        key("WHERE ");
        appendEscapedSQLString(ret, where);
    }
    if (dsd.has_secure())
    {
        key("SECURE ");
        ret += dsd.secure() ? "1" : "0";
    }
    literal(dsd.has_host(), "HOST ", dsd.host());
    literal(dsd.has_port(), "PORT ", dsd.port());
    literal(dsd.has_user(), "USER ", dsd.user());
    literal(dsd.has_password(), "PASSWORD ", dsd.password());
    literal(
        dsd.source() == DictionarySourceDetails::REDIS && dsd.has_redis_storage(),
        "STORAGE_TYPE ",
        DictionarySourceDetails_RedisStorageType_Name(dsd.redis_storage()));
    literal(dsd.has_url(), "URL ", dsd.url());
    literal(dsd.has_path(), "PATH ", dsd.path());
    literal(dsd.has_format(), "FORMAT ", dsd.format());
    literal(dsd.has_invalidate_query(), "INVALIDATE_QUERY ", dsd.invalidate_query());
    if (dsd.has_update_field())
    {
        const auto & cp = dsd.update_field();

        key("UPDATE_FIELD '");
        ConvertToSQLString(ret, cp.col().column());
        for (int i = 0; i < cp.sub_cols_size(); i++)
        {
            ret += ".";
            ConvertToSQLString(ret, cp.sub_cols(i).column());
        }
        ret += "'";
        if (dsd.has_update_lag())
        {
            ret += " UPDATE_LAG ";
            ret += std::to_string(dsd.update_lag());
        }
    }
    ret += ")";
}

CONV_FN(DictionarySource, ds)
{
    ret += " SOURCE(";
    using DictionarySourceType = DictionarySource::DictionarySourceOneofCase;
    switch (ds.dictionary_source_oneof_case())
    {
        case DictionarySourceType::kSource: DictionarySourceDetailsToString(ret, ds.source()); break;
        default: ret += "NULL()";
    }
    ret += ")";
}

CONV_FN(DictionaryLayout, dl)
{
    ret += " LAYOUT(";
    ret += DictionaryLayouts_Name(dl.layout());
    ret += "(";
    if (dl.has_setting_values())
    {
        const SettingValues & sv = dl.setting_values();
        const SetValue & first = sv.set_value();

        ret += first.property();
        ret += " ";
        ret += first.value();
        for (int i = 0; i < sv.other_values_size(); i++)
        {
            const SetValue & next = sv.other_values(i);

            ret += " ";
            ret += next.property();
            ret += " ";
            ret += next.value();
        }
    }
    ret += "))";
}

CONV_FN(DictionaryRange, dr)
{
    ret += " RANGE(MIN ";
    ExprToString(ret, dr.min());
    ret += " MAX ";
    ExprToString(ret, dr.max());
    ret += ")";
}

CONV_FN(DictionaryLifetime, dl)
{
    ret += " LIFETIME(";
    if (dl.has_max())
    {
        ret += "MIN ";
        ret += std::to_string(dl.min());
        ret += " MAX ";
        ret += std::to_string(dl.max());
    }
    else
    {
        ret += std::to_string(dl.min());
    }
    ret += ")";
}

CONV_FN(CreateDictionary, create_dictionary)
{
    CreateOrReplaceToString(ret, create_dictionary.create_opt());
    ret += " DICTIONARY ";
    if (create_dictionary.if_not_exists())
    {
        ret += "IF NOT EXISTS ";
    }
    ExprSchemaTableToString(ret, create_dictionary.est());
    if (create_dictionary.has_uuid())
    {
        ret += " UUID '";
        ret += create_dictionary.uuid();
        ret += "'";
    }
    ClusterToString(ret, true, create_dictionary.cluster());
    ret += " (";
    DictionaryColumnToString(ret, create_dictionary.col());
    for (int i = 0; i < create_dictionary.other_cols_size(); i++)
    {
        ret += ", ";
        DictionaryColumnToString(ret, create_dictionary.other_cols(i));
    }
    ret += ") PRIMARY KEY ";
    TableKeyToString(ret, create_dictionary.primary_key());
    if (create_dictionary.has_source())
    {
        DictionarySourceToString(ret, create_dictionary.source());
    }
    DictionaryLayoutToString(ret, create_dictionary.layout());
    if (create_dictionary.has_range())
    {
        DictionaryRangeToString(ret, create_dictionary.range());
    }
    if (create_dictionary.has_lifetime())
    {
        DictionaryLifetimeToString(ret, create_dictionary.lifetime());
    }
    if (create_dictionary.has_setting_values())
    {
        ret += " SETTINGS(";
        SettingValuesToString(ret, create_dictionary.setting_values());
        ret += ")";
    }
    if (create_dictionary.has_comment())
    {
        ret += " COMMENT ";
        ret += create_dictionary.comment();
    }
}

CONV_FN(AddWhere, add)
{
    if (add.has_col())
    {
        ret += " AFTER ";
        ColumnPathToString(ret, 0, add.col());
    }
    else if (add.has_idx())
    {
        ret += " AFTER ";
        SQLIdentifierToString(ret, add.idx());
    }
    else if (add.has_first())
    {
        ret += " FIRST";
    }
}

CONV_FN(AddColumn, add)
{
    ColumnDefToString(ret, add.new_col());
    AddWhereToString(ret, add.add_where());
}

CONV_FN(AddIndex, add)
{
    IndexDefToString(ret, add.new_idx());
    AddWhereToString(ret, add.add_where());
}

CONV_FN(AddStatistics, add)
{
    ColumnPathListToString(ret, 0, add.cols());
    ret += " TYPE ";
    ColumnStatisticsToString(ret, add.stats());
}

CONV_FN(RemoveColumnProperty, rcs)
{
    ColumnPathToString(ret, 0, rcs.col());
    ret += " REMOVE ";
    ret += RemoveColumnProperty_ColumnProperties_Name(rcs.property());
}

CONV_FN(ModifyColumnSetting, mcp)
{
    ColumnPathToString(ret, 0, mcp.col());
    ret += " MODIFY SETTING ";
    SettingValuesToString(ret, mcp.setting_values());
}

CONV_FN(SettingList, sl)
{
    ret += sl.setting();
    for (int i = 0; i < sl.other_settings_size(); i++)
    {
        ret += ", ";
        ret += sl.other_settings(i);
    }
}

CONV_FN(RemoveColumnSetting, rcp)
{
    ColumnPathToString(ret, 0, rcp.col());
    ret += " RESET SETTING ";
    SettingListToString(ret, rcp.setting_values());
}

static void RowPolicyClausesToString(
    String & ret,
    const CreateRowPolicy & crp,
    const bool has_where_expr,
    const WhereStatement & where_expr,
    const SQLIdentifier * role,
    const bool create)
{
    if (crp.has_is_restrictive())
    {
        ret += " AS ";
        ret += crp.is_restrictive() ? "RESTRICTIVE" : "PERMISSIVE";
    }
    if (create && crp.for_select())
    {
        ret += " FOR SELECT";
    }
    if (has_where_expr)
    {
        ret += " USING ";
        WhereStatementToString(ret, where_expr);
    }
    if (role)
    {
        ret += " TO ";
        SQLIdentifierToString(ret, *role);
    }
}

static void MaskingPolicyClausesToString(
    String & ret, const CreateMaskingPolicy & cmp, const bool has_where_expr, const WhereStatement & where_expr, const SQLIdentifier * role)
{
    if (cmp.has_first_update())
    {
        ret += " UPDATE ";
        UpdateSetToString(ret, cmp.first_update());
        for (int i = 0; i < cmp.other_updates_size(); i++)
        {
            ret += ", ";
            UpdateSetToString(ret, cmp.other_updates(i));
        }
    }
    if (has_where_expr)
    {
        ret += " WHERE ";
        WhereStatementToString(ret, where_expr);
    }
    if (role)
    {
        ret += " TO ";
        SQLIdentifierToString(ret, *role);
    }
    if (cmp.has_priority())
    {
        ret += " PRIORITY ";
        ret += std::to_string(cmp.priority());
    }
}

CONV_FN(ExpireSnapshots, es)
{
    bool has_arg = false;
    auto sep = [&]() -> String { return std::exchange(has_arg, true) ? ", " : ""; };

    ret += "expire_snapshots(";
    if (es.has_positional_timestamp())
    {
        ret += sep();
        appendSQLStringLiteral(ret, es.positional_timestamp());
    }
    if (es.has_expire_before())
    {
        ret += sep() + "expire_before = ";
        appendSQLStringLiteral(ret, es.expire_before());
    }
    if (es.has_retention_period())
    {
        ret += sep() + "retention_period = ";
        appendSQLStringLiteral(ret, es.retention_period());
    }
    if (es.has_retain_last())
        ret += sep() + "retain_last = " + std::to_string(es.retain_last());
    if (es.snapshot_ids_size() > 0)
    {
        ret += sep() + "snapshot_ids = [";
        for (int j = 0; j < es.snapshot_ids_size(); j++)
        {
            if (j > 0)
                ret += ", ";
            ret += es.snapshot_ids(j);
        }
        ret += "]";
    }
    if (es.has_dry_run() && es.dry_run())
        ret += sep() + "dry_run = 1";
    ret += ")";
}

CONV_FN(RemoveOrphanFiles, ro)
{
    bool has_arg = false;
    auto sep = [&]() -> String { return std::exchange(has_arg, true) ? ", " : ""; };

    ret += "remove_orphan_files(";
    if (ro.has_older_than())
    {
        ret += sep();
        appendSQLStringLiteral(ret, ro.older_than());
    }
    if (ro.has_location())
    {
        ret += sep() + "location = ";
        appendSQLStringLiteral(ret, ro.location());
    }
    if (ro.dry_run())
        ret += sep() + "dry_run = 1";
    ret += ")";
}

CONV_FN(ExecuteCommand, ec)
{
    ret += "EXECUTE ";
    using CommandType = ExecuteCommand::CommandCase;
    switch (ec.command_case())
    {
        case CommandType::kExpireSnapshots: ExpireSnapshotsToString(ret, ec.expire_snapshots()); break;
        case CommandType::kRemoveOrphanFiles: RemoveOrphanFilesToString(ret, ec.remove_orphan_files()); break;
        default: break;
    }
}

CONV_FN(AlterItem, alter)
{
    ret += alter.paren() ? "(" : "";
    using AlterType = AlterItem::AlterOneofCase;
    switch (alter.alter_oneof_case())
    {
        case AlterType::kDel:
            ret += "DELETE";
            DeleteToString(ret, alter.del());
            break;
        case AlterType::kUpdate:
            ret += "UPDATE ";
            UpdateToString(ret, alter.update());
            break;
        case AlterType::kOrder:
            ret += "MODIFY ORDER BY ";
            TableKeyToString(ret, alter.order());
            break;
        case AlterType::kMaterializeColumn:
            ret += "MATERIALIZE COLUMN ";
            ColumnPathToString(ret, 0, alter.materialize_column().col());
            appendInPartition(ret, alter.materialize_column());
            break;
        case AlterType::kAddColumn:
            ret += "ADD COLUMN ";
            AddColumnToString(ret, alter.add_column());
            break;
        case AlterType::kDropColumn:
            ret += "DROP COLUMN ";
            ColumnPathToString(ret, 0, alter.drop_column());
            break;
        case AlterType::kRenameColumn:
            ret += "RENAME COLUMN ";
            ColumnPathToString(ret, 0, alter.rename_column().old_name());
            ret += " TO ";
            ColumnPathToString(ret, 0, alter.rename_column().new_name());
            break;
        case AlterType::kClearColumn:
            ret += "CLEAR COLUMN ";
            ColumnPathToString(ret, 0, alter.clear_column().col());
            appendInPartition(ret, alter.clear_column());
            break;
        case AlterType::kModifyColumn:
            ret += "MODIFY COLUMN ";
            AddColumnToString(ret, alter.modify_column());
            break;
        case AlterType::kAddEnumValues:
            ret += "MODIFY COLUMN ";
            ColumnPathToString(ret, 0, alter.add_enum_values().col());
            ret += " ADD ENUM VALUES";
            EnumDefToString(ret, alter.add_enum_values().new_values());
            break;
        case AlterType::kCommentColumn:
            ret += "COMMENT COLUMN ";
            ColumnPathToString(ret, 0, alter.comment_column().col());
            ret += " ";
            ret += alter.comment_column().comment();
            break;
        case AlterType::kDeleteMask:
            ret += "APPLY DELETED MASK";
            appendInPartition(ret, alter.delete_mask());
            break;
        case AlterType::kAddStats:
            ret += "ADD STATISTICS ";
            AddStatisticsToString(ret, alter.add_stats());
            break;
        case AlterType::kModStats:
            ret += "MODIFY STATISTICS ";
            AddStatisticsToString(ret, alter.mod_stats());
            break;
        case AlterType::kDropStats:
            ret += "DROP STATISTICS ";
            ColumnPathListToString(ret, 0, alter.drop_stats());
            break;
        case AlterType::kClearStats:
            ret += "CLEAR STATISTICS ";
            ColumnPathListToString(ret, 0, alter.clear_stats());
            break;
        case AlterType::kMatStats:
            ret += "MATERIALIZE STATISTICS ";
            if (alter.mat_stats().has_cols())
            {
                ColumnPathListToString(ret, 0, alter.mat_stats().cols());
            }
            else
            {
                ret += "ALL";
            }
            break;
        case AlterType::kMaterializeIndex:
            ret += "MATERIALIZE INDEX ";
            SQLIdentifierToString(ret, alter.materialize_index().idx());
            appendInPartition(ret, alter.materialize_index());
            break;
        case AlterType::kAddIndex:
            ret += "ADD ";
            AddIndexToString(ret, alter.add_index());
            break;
        case AlterType::kDropIndex:
            ret += "DROP INDEX ";
            SQLIdentifierToString(ret, alter.drop_index());
            break;
        case AlterType::kClearIndex:
            ret += "CLEAR INDEX ";
            SQLIdentifierToString(ret, alter.clear_index().idx());
            appendInPartition(ret, alter.clear_index());
            break;
        case AlterType::kColumnRemoveProperty:
            ret += "MODIFY COLUMN ";
            RemoveColumnPropertyToString(ret, alter.column_remove_property());
            break;
        case AlterType::kColumnModifyCodec:
            ret += "MODIFY COLUMN ";
            ColumnPathToString(ret, 0, alter.column_modify_codec().col());
            ret += " CODEC(";
            ret += alter.column_modify_codec().codecs();
            ret += ")";
            break;
        case AlterType::kColumnModifySetting:
            ret += "MODIFY COLUMN ";
            ModifyColumnSettingToString(ret, alter.column_modify_setting());
            break;
        case AlterType::kColumnRemoveSetting:
            ret += "MODIFY COLUMN ";
            RemoveColumnSettingToString(ret, alter.column_remove_setting());
            break;
        case AlterType::kTableModifySetting:
            ret += "MODIFY SETTING ";
            SettingValuesToString(ret, alter.table_modify_setting());
            break;
        case AlterType::kTableRemoveSetting:
            ret += "RESET SETTING ";
            SettingListToString(ret, alter.table_remove_setting());
            break;
        case AlterType::kAddProjection:
            ret += "ADD ";
            ProjectionDefToString(ret, alter.add_projection());
            break;
        case AlterType::kModifyProjection:
            ret += "MODIFY PROJECTION ";
            if (alter.modify_projection().if_exists())
            {
                ret += "IF EXISTS ";
            }
            SQLIdentifierToString(ret, alter.modify_projection().def().proj());
            ret += " ";
            ProjectionDefBodyToString(ret, alter.modify_projection().def());
            break;
        case AlterType::kRemoveProjection:
            ret += "DROP PROJECTION ";
            SQLIdentifierToString(ret, alter.remove_projection());
            break;
        case AlterType::kMaterializeProjection:
            ret += "MATERIALIZE PROJECTION ";
            SQLIdentifierToString(ret, alter.materialize_projection().proj());
            appendInPartition(ret, alter.materialize_projection());
            break;
        case AlterType::kClearProjection:
            ret += "CLEAR PROJECTION ";
            SQLIdentifierToString(ret, alter.clear_projection().proj());
            appendInPartition(ret, alter.clear_projection());
            break;
        case AlterType::kAddConstraint:
            ret += "ADD ";
            ConstraintDefToString(ret, alter.add_constraint());
            break;
        case AlterType::kRemoveConstraint:
            ret += "DROP CONSTRAINT ";
            SQLIdentifierToString(ret, alter.remove_constraint());
            break;
        case AlterType::kModifyConstraint:
            ret += "MODIFY ";
            ConstraintDefToString(ret, alter.modify_constraint());
            break;
        case AlterType::kDetachPartition:
            ret += "DETACH ";
            SinglePartitionExprToString(ret, alter.detach_partition());
            break;
        case AlterType::kDropPartition:
            ret += "DROP ";
            SinglePartitionExprToString(ret, alter.drop_partition());
            break;
        case AlterType::kDropDetachedPartition:
            ret += "DROP DETACHED ";
            SinglePartitionExprToString(ret, alter.drop_detached_partition());
            break;
        case AlterType::kAttachPartition:
            ret += "ATTACH ";
            SinglePartitionExprToString(ret, alter.attach_partition());
            break;
        case AlterType::kAttachPartitionFrom:
            ret += "ATTACH ";
            SinglePartitionExprToString(ret, alter.attach_partition_from().single_partition());
            ret += " FROM ";
            ExprSchemaTableToString(ret, alter.attach_partition_from().est());
            break;
        case AlterType::kReplacePartitionFrom:
            ret += "REPLACE ";
            SinglePartitionExprToString(ret, alter.replace_partition_from().single_partition());
            ret += " FROM ";
            ExprSchemaTableToString(ret, alter.replace_partition_from().est());
            break;
        case AlterType::kMovePartitionTo:
            ret += "MOVE ";
            SinglePartitionExprToString(ret, alter.move_partition_to().single_partition());
            ret += " TO TABLE ";
            ExprSchemaTableToString(ret, alter.move_partition_to().est());
            break;
        case AlterType::kClearColumnPartition:
            ret += "CLEAR COLUMN ";
            ColumnPathToString(ret, 0, alter.clear_column_partition().col());
            ret += " IN ";
            SinglePartitionExprToString(ret, alter.clear_column_partition().single_partition());
            break;
        case AlterType::kFreezePartition:
            ret += "FREEZE";
            if (alter.freeze_partition().has_single_partition())
            {
                ret += " ";
                SinglePartitionExprToString(ret, alter.freeze_partition().single_partition());
            }
            ret += " WITH NAME ";
            appendSQLStringLiteral(ret, alter.freeze_partition().fname());
            break;
        case AlterType::kUnfreezePartition:
            ret += "UNFREEZE";
            if (alter.unfreeze_partition().has_single_partition())
            {
                ret += " ";
                SinglePartitionExprToString(ret, alter.unfreeze_partition().single_partition());
            }
            ret += " WITH NAME ";
            appendSQLStringLiteral(ret, alter.unfreeze_partition().fname());
            break;
        case AlterType::kMovePartition:
            ret += "MOVE ";
            SinglePartitionExprToString(ret, alter.move_partition().single_partition());
            ret += " TO ";
            StorageToString(ret, alter.move_partition().storage());
            break;
        case AlterType::kClearIndexPartition:
            ret += "CLEAR INDEX ";
            SQLIdentifierToString(ret, alter.clear_index_partition().idx());
            ret += " IN ";
            SinglePartitionExprToString(ret, alter.clear_index_partition().single_partition());
            break;
        case AlterType::kComment:
            ret += "MODIFY COMMENT ";
            ret += alter.comment();
            break;
        case AlterType::kModifyTtl:
            ret += "MODIFY ";
            TTLExprToString(ret, alter.modify_ttl());
            break;
        case AlterType::kRemoveTtl: ret += "REMOVE TTL"; break;
        case AlterType::kRewriteParts:
            ret += "REWRITE PARTS";
            appendInPartition(ret, alter.rewrite_parts());
            break;
        case AlterType::kApplyPatches:
            ret += "APPLY PATCHES";
            appendInPartition(ret, alter.apply_patches());
            break;
        case AlterType::kExecuteCommand: ExecuteCommandToString(ret, alter.execute_command()); break;
        case AlterType::kFetchPartition:
            ret += "FETCH ";
            SinglePartitionExprToString(ret, alter.fetch_partition().single_partition());
            ret += " FROM ";
            appendSQLStringLiteral(ret, alter.fetch_partition().from());
            break;
        case AlterType::kModifySampleBy:
            ret += "MODIFY SAMPLE BY ";
            TableKeyToString(ret, alter.modify_sample_by());
            break;
        case AlterType::kRemoveSampleBy: ret += "REMOVE SAMPLE BY"; break;
        case AlterType::kMaterializeTtl:
            ret += "MATERIALIZE TTL";
            appendInPartition(ret, alter.materialize_ttl());
            break;
        case AlterType::kModifySqlSecurity: {
            const SQLSecurityClause & ssc = alter.modify_sql_security();

            ret += "MODIFY";
            if (ssc.has_definer() || !ssc.has_security())
            {
                ret += " DEFINER = ";
                ret += ssc.has_definer() ? ssc.definer() : "CURRENT_USER";
            }
            if (ssc.has_security())
            {
                ret += " SQL SECURITY ";
                ret += SQLSecurityClause_SQLSecurityType_Name(ssc.security());
            }
        }
        break;
        case AlterType::kModifyQuery:
            ret += "MODIFY QUERY ";
            SelectParenToString(ret, alter.modify_query());
            break;
        case AlterType::kRefresh:
            ret += "MODIFY ";
            RefreshableViewToString(ret, alter.refresh());
            break;
        case AlterType::kAlterPolicy: {
            const AlterPolicyContent & apc = alter.alter_policy();
            const SQLIdentifier * role = apc.has_role() ? &apc.role() : nullptr;

            ret += "ON ";
            ExprSchemaTableToString(ret, apc.target());
            if (apc.has_rename_to())
            {
                ret += " RENAME TO ";
                SQLIdentifierToString(ret, apc.rename_to());
            }
            if (apc.has_masking())
            {
                MaskingPolicyClausesToString(ret, apc.masking(), apc.has_where_expr(), apc.where_expr(), role);
            }
            else
            {
                RowPolicyClausesToString(ret, apc.row(), apc.has_where_expr(), apc.where_expr(), role, false);
            }
        }
        break;
        default: ret += "DELETE WHERE TRUE";
    }
    ret += alter.paren() ? ")" : "";
}

CONV_FN(Alter, alter)
{
    ret += "ALTER ";
    if ((alter.sobject() == SQLObject::TABLE || alter.sobject() == SQLObject::VIEW) && alter.is_temp())
    {
        ret += "TEMPORARY ";
    }
    /// Views and dictionaries are altered with `ALTER TABLE`, there is no `ALTER DICTIONARY`
    ret += (alter.sobject() == SQLObject::VIEW || alter.sobject() == SQLObject::DICTIONARY) ? "TABLE"
                                                                                             : SQLObjectToString(alter.sobject());
    ret += " ";
    SQLObjectNameToString(ret, alter.object());
    ClusterToString(ret, true, alter.cluster());
    ret += " ";
    AlterItemToString(ret, alter.alter());
    for (int i = 0; i < alter.other_alters_size(); i++)
    {
        ret += ", ";
        AlterItemToString(ret, alter.other_alters(i));
    }
    appendSettings(ret, alter);
}

CONV_FN(Attach, at)
{
    ret += "ATTACH ";
    ret += at.sobject() == SQLObject::VIEW ? "TABLE" : SQLObjectToString(at.sobject());
    ret += " ";
    SQLObjectNameToString(ret, at.object());
    ClusterToString(ret, true, at.cluster());
    if (at.sobject() != SQLObject::DATABASE && at.has_as_replicated())
    {
        ret += " AS";
        ret += at.as_replicated() ? "" : " NOT";
        ret += " REPLICATED";
    }
    appendSettings(ret, at);
}

CONV_FN(Detach, dt)
{
    ret += "DETACH ";
    ret += dt.sobject() == SQLObject::VIEW ? "TABLE" : SQLObjectToString(dt.sobject());
    ret += " ";
    SQLObjectNameToString(ret, dt.object());
    ClusterToString(ret, true, dt.cluster());
    if (dt.permanently())
    {
        ret += " PERMANENTLY";
    }
    if (dt.sync())
    {
        ret += " SYNC";
    }
    appendSettings(ret, dt);
}

CONV_FN(SelectIntoFile, intofile)
{
    ret += "INTO OUTFILE ";
    appendSQLStringLiteral(ret, intofile.path());
    if (intofile.has_step())
    {
        ret += " ";
        ret += SelectIntoFile_SelectIntoFileStep_Name(intofile.step());
    }
    if (intofile.has_compression())
    {
        ret += " COMPRESSION ";
        appendSQLStringLiteral(ret, intofile.compression());
        if (intofile.has_level())
        {
            ret += " LEVEL ";
            ret += std::to_string(intofile.level());
        }
    }
}

CONV_FN(TopSelect, top)
{
    SelectToString(ret, top.sel());
    if (top.has_intofile())
    {
        ret += " ";
        SelectIntoFileToString(ret, top.intofile());
    }
    if (top.has_format())
    {
        ret += " FORMAT ";
        ret += top.format();
    }
}

static void SystemCommandOnCluster(String & ret, const String & desc, const SystemCommand & cmd, const ExprSchemaTable & est)
{
    ret += desc;
    ClusterToString(ret, true, cmd.cluster());
    ret += " ";
    ExprSchemaTableToString(ret, est);
}

CONV_FN(SystemCommand, cmd)
{
    using CmdType = SystemCommand::SystemCmdOneofCase;
    /// Commands made only of keywords, with whether they accept ON CLUSTER at the end
    static const std::unordered_map<CmdType, std::pair<const char *, bool>> keyword_commands = {
        {CmdType::kReloadEmbeddedDictionaries, {"RELOAD EMBEDDED DICTIONARIES", true}},
        {CmdType::kReloadDictionaries, {"RELOAD DICTIONARIES", true}},
        {CmdType::kReloadFunctions, {"RELOAD FUNCTIONS", true}},
        {CmdType::kReloadAsynchronousMetrics, {"RELOAD ASYNCHRONOUS METRICS", true}},
        {CmdType::kDropDnsCache, {"DROP DNS CACHE", true}},
        {CmdType::kDropMarkCache, {"DROP MARK CACHE", true}},
        {CmdType::kDropUncompressedCache, {"DROP UNCOMPRESSED CACHE", true}},
        {CmdType::kDropCompiledExpressionCache, {"DROP COMPILED EXPRESSION CACHE", true}},
        {CmdType::kDropQueryCache, {"DROP QUERY CACHE", true}},
        {CmdType::kFlushLogs, {"FLUSH LOGS", true}},
        {CmdType::kReloadConfig, {"RELOAD CONFIG", true}},
        {CmdType::kReloadUsers, {"RELOAD USERS", true}},
        {CmdType::kRestartReplicas, {"RESTART REPLICAS", true}},
        {CmdType::kDropFilesystemCache, {"DROP FILESYSTEM CACHE", true}},
        {CmdType::kSyncFileCache, {"SYNC FILE CACHE", true}},
        {CmdType::kLoadPks, {"LOAD PRIMARY KEY", true}},
        {CmdType::kUnloadPks, {"UNLOAD PRIMARY KEY", true}},
        {CmdType::kStopViews, {"STOP VIEWS", false}},
        {CmdType::kStartViews, {"START VIEWS", false}},
        {CmdType::kPauseViews, {"PAUSE VIEWS", false}},
        {CmdType::kDropConnectionsCache, {"DROP CONNECTIONS CACHE", true}},
        {CmdType::kDropPrimaryIndexCache, {"DROP PRIMARY INDEX CACHE", true}},
        {CmdType::kDropIndexMarkCache, {"DROP INDEX MARK CACHE", true}},
        {CmdType::kDropIndexUncompressedCache, {"DROP INDEX UNCOMPRESSED CACHE", true}},
        {CmdType::kDropMmapCache, {"DROP MMAP CACHE", true}},
        {CmdType::kDropPageCache, {"DROP PAGE CACHE", true}},
        {CmdType::kDropSchemaCache, {"DROP SCHEMA CACHE", false}},
        {CmdType::kDropS3ClientCache, {"DROP S3 CLIENT CACHE", true}},
        {CmdType::kFlushAsyncInsertQueue, {"FLUSH ASYNC INSERT QUEUE", true}},
        {CmdType::kSyncFilesystemCache, {"SYNC FILESYSTEM CACHE", true}},
        {CmdType::kDropVectorSimilarityIndexCache, {"DROP VECTOR SIMILARITY INDEX CACHE", true}},
        {CmdType::kUnloadDictionaries, {"UNLOAD DICTIONARIES", true}},
        {CmdType::kDropQueryConditionCache, {"DROP QUERY CONDITION CACHE", true}},
        {CmdType::kIcebergMetadataCache, {"DROP ICEBERG METADATA CACHE", true}},
        {CmdType::kReconnectKeeper, {"RECONNECT ZOOKEEPER", true}},
        {CmdType::kDropTextIndexTokensCache, {"DROP TEXT INDEX TOKENS CACHE", true}},
        {CmdType::kDropTextIndexHeaderCache, {"DROP TEXT INDEX HEADER CACHE", true}},
        {CmdType::kDropTextIndexPostingsCache, {"DROP TEXT INDEX POSTINGS CACHE", true}},
        {CmdType::kDropTextIndexCaches, {"DROP TEXT INDEX CACHES", true}},
        {CmdType::kResetDdlWorker, {"RESET DDL WORKER", true}},
        {CmdType::kStopReplicatedDdlQueries, {"STOP REPLICATED DDL QUERIES", true}},
        {CmdType::kStartReplicatedDdlQueries, {"START REPLICATED DDL QUERIES", true}},
        {CmdType::kJemallocPurge, {"JEMALLOC PURGE", true}},
        {CmdType::kJemallocEnableProfile, {"JEMALLOC ENABLE PROFILE", true}},
        {CmdType::kJemallocDisableProfile, {"JEMALLOC DISABLE PROFILE", true}},
        {CmdType::kJemallocFlushProfile, {"JEMALLOC FLUSH PROFILE", true}},
        {CmdType::kSyncTransactionLog, {"SYNC TRANSACTION LOG", true}},
        {CmdType::kStartThreadFuzzer, {"START THREAD FUZZER", true}},
        {CmdType::kStopThreadFuzzer, {"STOP THREAD FUZZER", true}},
        {CmdType::kDropParquetMetadataCache, {"DROP PARQUET METADATA CACHE", true}},
        {CmdType::kDropStatisticsCache, {"DROP STATISTICS CACHE", true}},
        {CmdType::kDropColumnsCache, {"DROP COLUMNS CACHE", true}},
        {CmdType::kDropEncryptionHeadersCache, {"DROP ENCRYPTION HEADERS CACHE", true}},
        {CmdType::kDropPaimonMetadataCache, {"DROP PAIMON METADATA CACHE", true}},
        {CmdType::kDropPointInPolygonCache, {"DROP POINT IN POLYGON CACHE", true}},
        {CmdType::kDropAvroSchemaCache, {"DROP AVRO SCHEMA CACHE", true}},
        {CmdType::kDisableAllFailpoints, {"DISABLE ALL FAILPOINTS", false}},
        {CmdType::kStopAllBackground, {"STOP ALL BACKGROUND", false}},
        {CmdType::kStartAllBackground, {"START ALL BACKGROUND", false}},
        {CmdType::kPauseAllBackground, {"PAUSE ALL BACKGROUND", false}},
        {CmdType::kCancelAllBackground, {"CANCEL ALL BACKGROUND", false}},
        {CmdType::kRefreshAllBackground, {"REFRESH ALL BACKGROUND", false}},
    };

    ret += "SYSTEM ";
    if (const auto it = keyword_commands.find(cmd.system_cmd_oneof_case()); it != keyword_commands.end())
    {
        ret += it->second.first;
        if (it->second.second)
        {
            ClusterToString(ret, true, cmd.cluster());
        }
        return;
    }
    switch (cmd.system_cmd_oneof_case())
    {
        case CmdType::kReloadFunction:
            ret += "RELOAD FUNCTION";
            ClusterToString(ret, true, cmd.cluster());
            ret += " ";
            SQLIdentifierToString(ret, cmd.reload_function());
            break;
        case CmdType::kDropFormatSchemaCache:
            ret += "DROP FORMAT SCHEMA CACHE";
            if (cmd.drop_format_schema_cache())
            {
                ret += " FOR Protobuf";
            }
            break;
        case CmdType::kStopMerges: SystemCommandOnCluster(ret, "STOP MERGES", cmd, cmd.stop_merges()); break;
        case CmdType::kStartMerges: SystemCommandOnCluster(ret, "START MERGES", cmd, cmd.start_merges()); break;
        case CmdType::kStopTtlMerges: SystemCommandOnCluster(ret, "STOP TTL MERGES", cmd, cmd.stop_ttl_merges()); break;
        case CmdType::kStartTtlMerges: SystemCommandOnCluster(ret, "START TTL MERGES", cmd, cmd.start_ttl_merges()); break;
        case CmdType::kStopMoves: SystemCommandOnCluster(ret, "STOP MOVES", cmd, cmd.stop_moves()); break;
        case CmdType::kStartMoves: SystemCommandOnCluster(ret, "START MOVES", cmd, cmd.start_moves()); break;
        case CmdType::kWaitLoadingParts: SystemCommandOnCluster(ret, "WAIT LOADING PARTS", cmd, cmd.wait_loading_parts()); break;
        case CmdType::kStopFetches: SystemCommandOnCluster(ret, "STOP FETCHES", cmd, cmd.stop_fetches()); break;
        case CmdType::kStartFetches: SystemCommandOnCluster(ret, "START FETCHES", cmd, cmd.start_fetches()); break;
        case CmdType::kStopReplicatedSends: SystemCommandOnCluster(ret, "STOP REPLICATED SENDS", cmd, cmd.stop_replicated_sends()); break;
        case CmdType::kStartReplicatedSends:
            SystemCommandOnCluster(ret, "START REPLICATED SENDS", cmd, cmd.start_replicated_sends());
            break;
        case CmdType::kStopReplicationQueues:
            SystemCommandOnCluster(ret, "STOP REPLICATION QUEUES", cmd, cmd.stop_replication_queues());
            break;
        case CmdType::kStartReplicationQueues:
            SystemCommandOnCluster(ret, "START REPLICATION QUEUES", cmd, cmd.start_replication_queues());
            break;
        case CmdType::kStopPullingReplicationLog:
            SystemCommandOnCluster(ret, "STOP PULLING REPLICATION LOG", cmd, cmd.stop_pulling_replication_log());
            break;
        case CmdType::kStartPullingReplicationLog:
            SystemCommandOnCluster(ret, "START PULLING REPLICATION LOG", cmd, cmd.start_pulling_replication_log());
            break;
        case CmdType::kSyncReplica: {
            const auto & sr = cmd.sync_replica();

            SystemCommandOnCluster(ret, "SYNC REPLICA", cmd, sr.est());
            ret += " ";
            using SyncType = SyncReplica::SyncOneofCase;
            switch (sr.sync_oneof_case())
            {
                case SyncType::kStrict: ret += "STRICT"; break;
                case SyncType::kLightweight:
                    ret += "LIGHTWEIGHT";
                    if (sr.lightweight().replicas_size() > 0)
                    {
                        ret += " FROM ";
                        for (int i = 0; i < sr.lightweight().replicas_size(); i++)
                        {
                            if (i != 0)
                            {
                                ret += ", ";
                            }
                            appendSQLStringLiteral(ret, sr.lightweight().replicas(i));
                        }
                    }
                    break;
                default: ret += "PULL";
            }
        }
        break;
        case CmdType::kSyncReplicatedDatabase:
            ret += "SYNC DATABASE REPLICA";
            ClusterToString(ret, true, cmd.cluster());
            ret += " ";
            SQLIdentifierToString(ret, cmd.sync_replicated_database());
            break;
        case CmdType::kRestartReplica: SystemCommandOnCluster(ret, "RESTART REPLICA", cmd, cmd.restart_replica()); break;
        case CmdType::kRestoreReplica: SystemCommandOnCluster(ret, "RESTORE REPLICA", cmd, cmd.restore_replica()); break;
        case CmdType::kLoadPk: SystemCommandOnCluster(ret, "LOAD PRIMARY KEY", cmd, cmd.load_pk()); break;
        case CmdType::kUnloadPk: SystemCommandOnCluster(ret, "UNLOAD PRIMARY KEY", cmd, cmd.unload_pk()); break;
        case CmdType::kRefreshView:
            ret += "REFRESH VIEW ";
            ExprSchemaTableToString(ret, cmd.refresh_view());
            break;
        case CmdType::kStopView:
            ret += "STOP VIEW ";
            ExprSchemaTableToString(ret, cmd.stop_view());
            break;
        case CmdType::kStartView:
            ret += "START VIEW ";
            ExprSchemaTableToString(ret, cmd.start_view());
            break;
        case CmdType::kPauseView:
            ret += "PAUSE VIEW ";
            ExprSchemaTableToString(ret, cmd.pause_view());
            break;
        case CmdType::kCancelView:
            ret += "CANCEL VIEW ";
            ExprSchemaTableToString(ret, cmd.cancel_view());
            break;
        case CmdType::kWaitView:
            ret += "WAIT VIEW ";
            ExprSchemaTableToString(ret, cmd.wait_view());
            break;
        case CmdType::kPrewarmCache: SystemCommandOnCluster(ret, "PREWARM MARK CACHE", cmd, cmd.prewarm_cache()); break;
        case CmdType::kDropTimeSeriesCaches:
            SystemCommandOnCluster(ret, "DROP TIME SERIES CACHES", cmd, cmd.drop_time_series_caches());
            break;
        case CmdType::kPrewarmPrimaryIndexCache:
            SystemCommandOnCluster(ret, "PREWARM PRIMARY INDEX CACHE", cmd, cmd.prewarm_primary_index_cache());
            break;
        case CmdType::kReloadDictionary: SystemCommandOnCluster(ret, "RELOAD DICTIONARY", cmd, cmd.reload_dictionary()); break;
        case CmdType::kUnloadDictionary: SystemCommandOnCluster(ret, "UNLOAD DICTIONARY", cmd, cmd.unload_dictionary()); break;
        case CmdType::kFlushDistributed: SystemCommandOnCluster(ret, "FLUSH DISTRIBUTED", cmd, cmd.flush_distributed()); break;
        case CmdType::kStopDistributedSends:
            SystemCommandOnCluster(ret, "STOP DISTRIBUTED SENDS", cmd, cmd.stop_distributed_sends());
            break;
        case CmdType::kStartDistributedSends:
            SystemCommandOnCluster(ret, "START DISTRIBUTED SENDS", cmd, cmd.start_distributed_sends());
            break;
        case CmdType::kEnableFailpoint:
            ret += "ENABLE FAILPOINT ";
            ret += cmd.enable_failpoint();
            break;
        case CmdType::kDisableFailpoint:
            ret += "DISABLE FAILPOINT ";
            ret += cmd.disable_failpoint();
            break;
        case CmdType::kWaitFailpoint:
            ret += "WAIT FAILPOINT ";
            ret += cmd.wait_failpoint();
            break;
        case CmdType::kNotifyFailpoint:
            ret += "NOTIFY FAILPOINT ";
            ret += cmd.notify_failpoint();
            break;
        case CmdType::kReloadDeltaKernelTracing:
            ret += "RELOAD DELTA KERNEL TRACING ";
            ret += DeltaKernelTraceLevel_Name(cmd.reload_delta_kernel_tracing());
            break;
        case CmdType::kRestoreDatabaseReplica:
            ret += "RESTORE DATABASE REPLICA";
            ClusterToString(ret, true, cmd.cluster());
            ret += " ";
            SQLIdentifierToString(ret, cmd.restore_database_replica());
            break;
        case CmdType::kStopReplicatedView:
            ret += "STOP REPLICATED VIEW ";
            ExprSchemaTableToString(ret, cmd.stop_replicated_view());
            break;
        case CmdType::kStartReplicatedView:
            ret += "START REPLICATED VIEW ";
            ExprSchemaTableToString(ret, cmd.start_replicated_view());
            break;
        case CmdType::kUnfreeze:
            ret += "UNFREEZE WITH NAME ";
            appendSQLStringLiteral(ret, cmd.unfreeze());
            break;
        case CmdType::kDropReplica:
            ret += "DROP REPLICA";
            /// The parser accepts ON CLUSTER only right after the keyword, before the replica literal.
            ClusterToString(ret, true, cmd.cluster());
            ret += " ";
            appendSQLStringLiteral(ret, cmd.drop_replica().replica());
            if (cmd.drop_replica().has_est())
            {
                ret += " FROM TABLE ";
                ExprSchemaTableToString(ret, cmd.drop_replica().est());
            }
            else if (cmd.drop_replica().has_database())
            {
                ret += " FROM DATABASE ";
                SQLIdentifierToString(ret, cmd.drop_replica().database());
            }
            break;
        case CmdType::kDropDatabaseReplica:
            ret += "DROP DATABASE REPLICA";
            /// The parser accepts ON CLUSTER only right after the keyword, before the replica literal.
            ClusterToString(ret, true, cmd.cluster());
            ret += " ";
            appendSQLStringLiteral(ret, cmd.drop_database_replica().replica());
            if (cmd.drop_database_replica().has_shard())
            {
                ret += " FROM SHARD ";
                appendSQLStringLiteral(ret, cmd.drop_database_replica().shard());
            }
            if (cmd.drop_database_replica().has_database())
            {
                ret += " FROM DATABASE ";
                SQLIdentifierToString(ret, cmd.drop_database_replica().database());
            }
            break;
        case CmdType::kDropDiskMetadataCache:
            ret += "DROP DISK METADATA CACHE";
            ClusterToString(ret, true, cmd.cluster());
            ret += " ";
            ret += cmd.drop_disk_metadata_cache();
            break;
        case CmdType::kDropDistributedCache: {
            const auto & ddc = cmd.drop_distributed_cache();

            ret += "DROP DISTRIBUTED CACHE";
            if (ddc.has_connections())
            {
                ret += " CONNECTIONS";
            }
            else if (ddc.has_server_id())
            {
                ret += " ";
                appendSQLStringLiteral(ret, ddc.server_id());
            }
            ClusterToString(ret, true, cmd.cluster());
            break;
        }
        case CmdType::kResetFilelog: {
            const auto & rfl = cmd.reset_filelog();
            SystemCommandOnCluster(ret, "RESET FILELOG", cmd, rfl.table());
            if (rfl.has_file())
            {
                ret += " FILE ";
                appendSQLStringLiteral(ret, rfl.file());
                if (rfl.has_offset())
                {
                    ret += " OFFSET ";
                    ret += std::to_string(rfl.offset());
                }
                else if (rfl.has_to_end())
                {
                    ret += " TO END";
                }
            }
            break;
        }
        case CmdType::kFlushObjectStorageQueue: {
            const auto & foq = cmd.flush_object_storage_queue();
            SystemCommandOnCluster(ret, "FLUSH OBJECT STORAGE QUEUE", cmd, foq.table());
            ret += " PATH ";
            appendSQLStringLiteral(ret, foq.path());
            break;
        }
        case CmdType::kStopVirtualPartsUpdate:
            SystemCommandOnCluster(ret, "STOP VIRTUAL PARTS UPDATE", cmd, cmd.stop_virtual_parts_update());
            break;
        case CmdType::kStartVirtualPartsUpdate:
            SystemCommandOnCluster(ret, "START VIRTUAL PARTS UPDATE", cmd, cmd.start_virtual_parts_update());
            break;
        case CmdType::kStopReduceBlockingParts:
            SystemCommandOnCluster(ret, "STOP REDUCE BLOCKING PARTS", cmd, cmd.stop_reduce_blocking_parts());
            break;
        case CmdType::kStartReduceBlockingParts:
            SystemCommandOnCluster(ret, "START REDUCE BLOCKING PARTS", cmd, cmd.start_reduce_blocking_parts());
            break;
        case CmdType::kWaitBlobsCleanup:
            ret += "WAIT BLOBS CLEANUP";
            ClusterToString(ret, true, cmd.cluster());
            ret += " ";
            appendSQLStringLiteral(ret, cmd.wait_blobs_cleanup());
            break;
        case CmdType::kUnlockSnapshot:
            ret += "UNLOCK SNAPSHOT ";
            appendSQLStringLiteral(ret, cmd.unlock_snapshot().name());
            if (cmd.unlock_snapshot().has_from())
            {
                ret += " FROM ";
                BackupOutToString(ret, cmd.unlock_snapshot().from());
            }
            break;
        case CmdType::kRestartDisk:
            ret += "RESTART DISK";
            ClusterToString(ret, true, cmd.cluster());
            ret += " ";
            appendSQLStringLiteral(ret, cmd.restart_disk());
            break;
        /// Background controls, they reject ON CLUSTER
        case CmdType::kStopBackground:
            ret += "STOP ";
            ExprSchemaTableToString(ret, cmd.stop_background());
            break;
        case CmdType::kStartBackground:
            ret += "START ";
            ExprSchemaTableToString(ret, cmd.start_background());
            break;
        case CmdType::kPauseBackground:
            ret += "PAUSE ";
            ExprSchemaTableToString(ret, cmd.pause_background());
            break;
        case CmdType::kCancelBackground:
            ret += "CANCEL ";
            ExprSchemaTableToString(ret, cmd.cancel_background());
            break;
        case CmdType::kRefreshBackground:
            ret += "REFRESH ";
            ExprSchemaTableToString(ret, cmd.refresh_background());
            break;
        default: ret += "FLUSH LOGS";
    }
}

CONV_FN(BackupRestoreObject, bobject)
{
    const bool is_table = bobject.sobject() == SQLObject::TABLE;

    if ((is_table || bobject.sobject() == SQLObject::VIEW) && bobject.is_temp())
    {
        ret += "TEMPORARY ";
    }
    ret += SQLObjectToString(bobject.sobject());
    ret += " ";
    SQLObjectNameToString(ret, bobject.object());
    if (is_table && !bobject.is_temp() && bobject.partitions_size())
    {
        ret += " PARTITIONS ";
        for (int i = 0; i < bobject.partitions_size(); i++)
        {
            if (i != 0)
            {
                ret += ", ";
            }
            PartitionExprToString(ret, bobject.partitions(i));
        }
    }
}

CONV_FN(BackupRestoreElement, backup)
{
    using BackupType = BackupRestoreElement::BackupOneofCase;
    switch (backup.backup_oneof_case())
    {
        case BackupType::kBobject: BackupRestoreObjectToString(ret, backup.bobject()); break;
        default: ret += "ALL"; break;
    }
    if (backup.except_data_tables_size())
    {
        ret += " EXCEPT DATA FROM TABLE";
        ret += backup.except_data_plural() ? "S" : "";
        ret += " ";
        for (int i = 0; i < backup.except_data_tables_size(); i++)
        {
            if (i != 0)
            {
                ret += ", ";
            }
            ExprSchemaTableToString(ret, backup.except_data_tables(i));
        }
    }
}

CONV_FN(BackupRestore, backup)
{
    const BackupRestore_BackupCommand & command = backup.command();
    const bool backup_cmd = command == BackupRestore_BackupCommand_BACKUP;

    ret += BackupRestore_BackupCommand_Name(command);
    ret += " ";
    if (backup_cmd && backup.has_from_snapshot())
    {
        ret += "FROM SNAPSHOT ";
        BackupOutToString(ret, backup.from_snapshot());
    }
    else
    {
        BackupRestoreElementToString(ret, backup.backup_element());
    }
    ClusterToString(ret, true, backup.cluster());
    ret += " ";
    ret += backup_cmd ? "TO" : "FROM";
    ret += " ";
    BackupOutToString(ret, backup.out());
    appendSettings(ret, backup);
    if (backup.has_sync())
    {
        ret += " ";
        ret += BackupRestore_SyncOrAsync_Name(backup.sync());
    }
    if (backup.has_format())
    {
        ret += " FORMAT ";
        ret += backup.format();
    }
}

CONV_FN(SnapshotQuery, sq)
{
    ret += "SNAPSHOT ";
    BackupRestoreElementToString(ret, sq.element());
    ret += " TO ";
    BackupOutToString(ret, sq.out());
}

CONV_FN(Rename, ren)
{
    ret += "RENAME ";
    /// RENAME VIEW is a typo
    ret += ren.sobject() == SQLObject::VIEW ? "TABLE" : SQLObjectToString(ren.sobject());
    ret += " ";
    SQLObjectNameToString(ret, ren.old_object());
    ret += " TO ";
    SQLObjectNameToString(ret, ren.new_object());
    ClusterToString(ret, true, ren.cluster());
    appendSettings(ret, ren);
}

CONV_FN(Kill, kil)
{
    ret += "KILL ";
    ret += Kill_KillEnum_Name(kil.command());
    ClusterToString(ret, true, kil.cluster());
    ret += " WHERE ";
    WhereStatementToString(ret, kil.where());
    if (kil.has_option())
    {
        ret += " ";
        ret += Kill_KillOption_Name(kil.option());
    }
    if (kil.has_format())
    {
        ret += " FORMAT ";
        ret += kil.format();
    }
    appendSettings(ret, kil);
}

CONV_FN(ShowObject, sh)
{
    if (sh.create())
    {
        ret += "CREATE ";
    }
    if ((sh.sobject() == SQLObject::TABLE || sh.sobject() == SQLObject::VIEW) && sh.is_temp())
    {
        ret += "TEMPORARY ";
    }
    ret += SQLObjectToString(sh.sobject());
    ret += " ";
    SQLObjectNameToString(ret, sh.object());
}

CONV_FN(ShowDictionaries, sh)
{
    ret += "DICTIONARIES";
    if (sh.has_database())
    {
        ret += " FROM ";
        SQLIdentifierToString(ret, sh.database());
    }
}

CONV_FN(ShowColumns, sh)
{
    if (sh.extended())
    {
        ret += "EXTENDED ";
    }
    if (sh.full())
    {
        ret += "FULL ";
    }
    ret += "COLUMNS FROM ";
    SQLIdentifierToString(ret, sh.est().table());
    if (sh.est().has_database())
    {
        ret += " FROM ";
        SQLIdentifierToString(ret, sh.est().database());
    }
}

CONV_FN(ShowIndex, sh)
{
    if (sh.extended())
    {
        ret += "EXTENDED ";
    }
    ret += ShowIndex_IndexShow_Name(sh.key());
    ret += " FROM ";
    SQLIdentifierToString(ret, sh.est().table());
    if (sh.est().has_database())
    {
        ret += " FROM ";
        SQLIdentifierToString(ret, sh.est().database());
    }
}

CONV_FN(ShowStatement, sh)
{
    ret += "SHOW ";
    using ShowType = ShowStatement::ShowOneofCase;
    switch (sh.show_oneof_case())
    {
        case ShowType::kObject: ShowObjectToString(ret, sh.object()); break;
        case ShowType::kDatabases: ret += "DATABASES"; break;
        case ShowType::kColumns: ShowColumnsToString(ret, sh.columns()); break;
        case ShowType::kDictionaries: ShowDictionariesToString(ret, sh.dictionaries()); break;
        case ShowType::kIndexes: ShowIndexToString(ret, sh.indexes()); break;
        case ShowType::kProcesslist: ret += "PROCESSLIST"; break;
        case ShowType::kGrants:
            ret += "GRANTS WITH IMPLICIT";
            ret += sh.grants() ? " FINAL" : "";
            break;
        case ShowType::kUsers: ret += "USERS"; break;
        case ShowType::kRoles:
            ret += sh.roles() ? "CURRENT" : "ENABLED";
            ret += " ROLES";
            break;
        case ShowType::kProfiles:
            ret += sh.profiles() ? "SETTINGS " : "";
            ret += "PROFILES";
            break;
        case ShowType::kPolicies:
            ret += "ROW POLICIES ON ";
            ExprSchemaTableToString(ret, sh.policies());
            break;
        case ShowType::kQuotas: ret += "QUOTAS"; break;
        case ShowType::kQuota:
            ret += sh.quota() ? "CURRENT " : "";
            ret += "QUOTA";
            break;
        case ShowType::kAccess: ret += "ACCESS"; break;
        case ShowType::kCluster:
            ret += "CLUSTER ";
            if (sh.cluster().has_cluster())
            {
                appendSQLStringLiteral(ret, sh.cluster().cluster());
            }
            break;
        case ShowType::kClusters: ret += "CLUSTERS"; break;
        case ShowType::kSettings:
            ret += sh.settings() ? "CHANGED " : "";
            ret += "SETTINGS";
            break;
        case ShowType::kSetting:
            ret += "SETTING ";
            ret += sh.setting();
            break;
        case ShowType::kFilesystemCaches: ret += "FILESYSTEM CACHES"; break;
        case ShowType::kEngines: ret += "ENGINES"; break;
        case ShowType::kFunctions: ret += "FUNCTIONS"; break;
        case ShowType::kMerges: ret += "MERGES"; break;
        default: ret += "DATABASES";
    }
    appendSettings(ret, sh);
}

CONV_FN(CreatePolicy, cp)
{
    const bool is_masking = cp.has_masking();
    const SQLIdentifier * role = cp.has_role() ? &cp.role() : nullptr;

    ret += "CREATE ";
    ret += is_masking ? "MASKING" : "ROW";
    ret += " POLICY ";
    if (cp.create_opt() == CreateReplaceOption::Create && cp.if_not_exists())
    {
        ret += "IF NOT EXISTS ";
    }
    else if (cp.create_opt() != CreateReplaceOption::Create)
    {
        ret += "OR REPLACE ";
    }
    SQLIdentifierToString(ret, cp.policy());
    ClusterToString(ret, true, cp.cluster());
    ret += " ON ";
    ExprSchemaTableToString(ret, cp.target());
    if (is_masking)
    {
        MaskingPolicyClausesToString(ret, cp.masking(), cp.has_where_expr(), cp.where_expr(), role);
    }
    else if (cp.has_row())
    {
        RowPolicyClausesToString(ret, cp.row(), cp.has_where_expr(), cp.where_expr(), role, true);
    }
}

CONV_FN(CreateHypotheticalIndex, hi)
{
    const IndexDef & idef = hi.create_def();

    ret += "CREATE HYPOTHETICAL INDEX ";
    if (hi.if_not_exists())
    {
        ret += "IF NOT EXISTS ";
    }
    if (idef.has_idx())
    {
        SQLIdentifierToString(ret, idef.idx());
    }
    else
    {
        ret += "hi0";
    }
    ret += " ON ";
    ExprSchemaTableToString(ret, hi.est());
    ret += " (";
    ExprToString(ret, idef.expr());
    ret += ")";
    IndexDefTypeToString(ret, idef);
}

CONV_FN(CreateHypotheticalProjection, hp)
{
    const ProjectionDef & pdef = hp.create_def();

    ret += "CREATE HYPOTHETICAL PROJECTION ";
    if (hp.if_not_exists())
    {
        ret += "IF NOT EXISTS ";
    }
    SQLIdentifierToString(ret, pdef.proj());
    ret += " ON ";
    ExprSchemaTableToString(ret, hp.est());
    ret += " ";
    ProjectionDefBodyToString(ret, pdef);
}

CONV_FN(SQLQueryInner, query)
{
    using QueryType = SQLQueryInner::QueryInnerOneofCase;
    switch (query.query_inner_oneof_case())
    {
        case QueryType::kSelect: TopSelectToString(ret, query.select()); break;
        case QueryType::kCreateTable: CreateTableToString(ret, query.create_table()); break;
        case QueryType::kDrop: DropToString(ret, query.drop()); break;
        case QueryType::kInsert: InsertToString(ret, query.insert()); break;
        case QueryType::kDel: LightDeleteToString(ret, query.del()); break;
        case QueryType::kUpt: LightUpdateToString(ret, query.upt()); break;
        case QueryType::kTrunc: TruncateToString(ret, query.trunc()); break;
        case QueryType::kOpt: OptimizeTableToString(ret, query.opt()); break;
        case QueryType::kCheck: CheckTableToString(ret, query.check()); break;
        case QueryType::kDesc: DescribeStatementToString(ret, query.desc()); break;
        case QueryType::kExchange: ExchangeToString(ret, query.exchange()); break;
        case QueryType::kAlter: AlterToString(ret, query.alter()); break;
        case QueryType::kSettingValues:
            ret += "SET ";
            SettingValuesToString(ret, query.setting_values());
            break;
        case QueryType::kCreateView: CreateViewToString(ret, query.create_view()); break;
        case QueryType::kAttach: AttachToString(ret, query.attach()); break;
        case QueryType::kDetach: DetachToString(ret, query.detach()); break;
        case QueryType::kCreateDatabase: CreateDatabaseToString(ret, query.create_database()); break;
        case QueryType::kCreateFunction: CreateFunctionToString(ret, query.create_function()); break;
        case QueryType::kSystemCmd: SystemCommandToString(ret, query.system_cmd()); break;
        case QueryType::kBackupRestore: BackupRestoreToString(ret, query.backup_restore()); break;
        case QueryType::kCreateDictionary: CreateDictionaryToString(ret, query.create_dictionary()); break;
        case QueryType::kRename: RenameToString(ret, query.rename()); break;
        case QueryType::kKill: KillToString(ret, query.kill()); break;
        case QueryType::kShow: ShowStatementToString(ret, query.show()); break;
        case QueryType::kCreatePolicy: CreatePolicyToString(ret, query.create_policy()); break;
        case QueryType::kSnapshotQuery: SnapshotQueryToString(ret, query.snapshot_query()); break;
        case QueryType::kCreateHypoIndex: CreateHypotheticalIndexToString(ret, query.create_hypo_index()); break;
        case QueryType::kCreateHypoProjection: CreateHypotheticalProjectionToString(ret, query.create_hypo_projection()); break;
        default: ret += "SELECT 1";
    }
}

CONV_FN(ExplainQuery, explain)
{
    if (explain.is_explain())
    {
        ret += "EXPLAIN ";
        if (explain.has_expl())
        {
            String nexplain = ExplainQuery_ExplainValues_Name(explain.expl());

            std::replace(nexplain.begin(), nexplain.end(), '_', ' ');
            ret += nexplain;
            ret += " ";
        }
        if (explain.opts_size())
        {
            for (int i = 0; i < explain.opts_size(); i++)
            {
                const ExplainOption & eopt = explain.opts(i);

                if (i != 0)
                {
                    ret += ", ";
                }
                ret += ExplainOption_ExplainOpt_Name(eopt.opt());
                ret += " = ";
                ret += std::to_string(eopt.val());
            }
            ret += " ";
        }
    }
    if (!explain.has_expl() || explain.expl() != ExplainQuery_ExplainValues_CURRENT_TRANSACTION)
    {
        SQLQueryInnerToString(ret, explain.inner_query());
    }
}

CONV_FN(SingleSQLQuery, query)
{
    using QueryType = SingleSQLQuery::QueryOneofCase;
    switch (query.query_oneof_case())
    {
        case QueryType::kExplain: ExplainQueryToString(ret, query.explain()); break;
        case QueryType::kStartTrans: ret += "BEGIN TRANSACTION"; break;
        case QueryType::kCommitTrans: ret += "COMMIT"; break;
        case QueryType::kRollbackTrans: ret += "ROLLBACK"; break;
        default: ret += "SELECT 1";
    }
}

void SQLExprToString(String & ret, const Expr & expr)
{
    ExprToString(ret, expr);
}

void SQLQueryToString(String & ret, const SQLQuery & query)
{
    SingleSQLQueryToString(ret, query.single_query());
    for (int i = 0; i < query.parallel_queries_size(); i++)
    {
        ret += " PARALLEL WITH ";
        SingleSQLQueryToString(ret, query.parallel_queries(i));
    }
    ret += ";";
}

}
