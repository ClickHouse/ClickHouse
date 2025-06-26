#include <algorithm>
#include <cctype>
#include <cstdint>

#include <Client/BuzzHouse/AST/SQLProtoStr.h>
#include <Client/BuzzHouse/Utils/HugeInt.h>
#include <Client/BuzzHouse/Utils/UHugeInt.h>

#define CONV_FN(TYPE, VAR_NAME) void TYPE##ToString(String & ret, const TYPE &(VAR_NAME))
#define CONV_FN_QUOTE(TYPE, VAR_NAME) void TYPE##ToString(String & ret, const uint32_t quote, const TYPE &(VAR_NAME))

namespace BuzzHouse
{

static constexpr char hexDigits[] = "0123456789ABCDEF";
static constexpr char digits[] = "0123456789";

CONV_FN(Expr, expr);
CONV_FN(Select, select);

CONV_FN_QUOTE(Column, col)
{
    for (const auto & c : col.column())
    {
        if (c == '`')
        {
            for (uint32_t i = 0; i < (1 << quote); i++)
            {
                ret += "`";
            }
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

CONV_FN(Index, idx)
{
    ret += idx.index();
}

CONV_FN(Projection, proj)
{
    ret += proj.projection();
}

CONV_FN(Constraint, constr)
{
    ret += constr.constraint();
}

CONV_FN(Database, db)
{
    ret += db.database();
}

CONV_FN(Table, tab)
{
    ret += tab.table();
}

CONV_FN(Function, func)
{
    ret += func.function();
}

void ClusterToString(String & ret, const bool clause, const Cluster & cl)
{
    if (clause)
    {
        ret += " ON CLUSTER ";
    }
    ret += "'";
    ret += cl.cluster();
    ret += "'";
}

CONV_FN(Window, win)
{
    ret += win.window();
}

CONV_FN(Storage, store)
{
    ret += Storage_DataStorage_Name(store.storage());
    ret += " '";
    ret += store.storage_name();
    ret += "'";
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
            case '\'':
                ret += "\\'";
                break;
            case '\\':
                ret += "\\\\";
                break;
            case '\b':
                ret += "\\b";
                break;
            case '\f':
                ret += "\\f";
                break;
            case '\r':
                ret += "\\r";
                break;
            case '\n':
                ret += "\\n";
                break;
            case '\t':
                ret += "\\t";
                break;
            case '\0':
                ret += "\\0";
                break;
            case '\a':
                ret += "\\a";
                break;
            case '\v':
                ret += "\\v";
                break;
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

CONV_FN(ExprSchemaTable, st)
{
    if (st.has_database())
    {
        DatabaseToString(ret, st.database());
        ret += ".";
    }
    TableToString(ret, st.table());
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
            ret += std::to_string((fa.tuple_index() % 9) + 1);
            break;
        default:
            ret += "[1]";
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
        DatabaseToString(ret, stc.database());
        ret += ".";
    }
    if (stc.has_table())
    {
        TableToString(ret, stc.table());
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

CONV_FN(NumericLiteral, nl)
{
    ret += "(";
    if (nl.digits_size() > 0)
    {
        if (nl.negative())
        {
            ret += "-";
        }
        ret += digits[std::min<uint32_t>(UINT32_C(1), nl.digits(0) % 10)];
        for (int i = 1; i < nl.digits_size(); i++)
        {
            ret += digits[nl.digits(i) % 10];
        }
    }
    if (nl.decimal_point())
    {
        if (nl.digits_size() == 0)
        {
            ret += "0";
        }
        ret += ".";
        if (nl.dec_digits_size() == 0)
        {
            ret += "0";
        }
        else
        {
            for (int i = 0; i < nl.dec_digits_size(); i++)
            {
                ret += digits[nl.dec_digits(i) % 10];
            }
        }
    }
    if (nl.exp_digits_size() > 0)
    {
        if (nl.digits_size() == 0 && !nl.decimal_point())
        {
            ret += "1";
        }
        ret += "E";
        if (nl.negative_exp())
        {
            ret += "-";
        }
        for (int i = 0; i < nl.exp_digits_size(); i++)
        {
            ret += digits[nl.exp_digits(i) % 10];
        }
    }
    if (nl.digits_size() == 0 && !nl.decimal_point() && nl.exp_digits_size() == 0)
    {
        ret += "1";
    }
    ret += ")";
}

CONV_FN(HugeIntLiteral, huge)
{
    const HugeInt val(huge.upper(), huge.lower());
    ret += val.toString();
}

CONV_FN(UHugeIntLiteral, uhuge)
{
    const UHugeInt val(uhuge.upper(), uhuge.lower());
    ret += val.toString();
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
    if (val.paren())
    {
        ret += "(";
    }
    switch (val.val())
    {
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_NULL:
            ret += "NULL";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_TRUE:
            ret += "TRUE";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_FALSE:
            ret += "FALSE";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_ZERO:
            ret += "0";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_ONE:
            ret += "1";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_MINUS_ONE:
            ret += "-1";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_EMPTY_STRING:
            ret += "''";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_EMPTY_ARRAY:
            ret += "[]";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_EMPTY_TUPLE:
            ret += "()";
            break;
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
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_EMPTY_JSON:
            ret += "'{}'";
            if (val.paren())
            {
                ret += "::JSON";
            }
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_MINUS_ZERO_FP:
            ret += "-0.0";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_PLUS_ZERO_FP:
            ret += "+0.0";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_ZERO_FP:
            ret += "0.0";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_INF:
            ret += "inf";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_PLUS_INF:
            ret += "+inf";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_MINUS_INF:
            ret += "-inf";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_NAN:
            ret += "nan";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_PLUS_NAN:
            ret += "+nan";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_MINUS_NAN:
            ret += "-nan";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_HAPPY:
            ret += "'😂'";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_TEN_HAPPY:
            ret += "'😂😂😂😂😂😂😂😂😂😂'";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MIN_INT32:
            ret += std::to_string(std::numeric_limits<int32_t>::min());
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_INT32:
            ret += std::to_string(std::numeric_limits<int32_t>::max());
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MIN_INT64:
            ret += std::to_string(std::numeric_limits<int64_t>::min());
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_INT64:
            ret += std::to_string(std::numeric_limits<int64_t>::max());
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MIN_INT128:
            ret += "-170141183460469231731687303715884105728";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_INT128:
            ret += "170141183460469231731687303715884105727";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MIN_INT256:
            ret += "-57896044618658097711785492504343953926634992332820282019728792003956564819968";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_INT256:
            ret += "57896044618658097711785492504343953926634992332820282019728792003956564819967";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_UINT32:
            ret += std::to_string(std::numeric_limits<uint32_t>::max());
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_UINT64:
            ret += std::to_string(std::numeric_limits<uint64_t>::max());
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_UINT128:
            ret += "340282366920938463463374607431768211455";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_UINT256:
            ret += "115792089237316195423570985008687907853269984665640564039457584007913129639935";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MIN_DATE:
            ret += "'1970-01-01'";
            if (val.paren())
            {
                ret += "::Date";
            }
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_DATE:
            ret += "'2149-06-06'";
            if (val.paren())
            {
                ret += "::Date";
            }
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MIN_DATE32:
            ret += "'1900-01-01'";
            if (val.paren())
            {
                ret += "::Date32";
            }
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_DATE32:
            ret += "'2299-12-31'";
            if (val.paren())
            {
                ret += "::Date32";
            }
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MIN_TIME:
            ret += "'-999:59:59'";
            if (val.paren())
            {
                ret += "::Time";
            }
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_TIME:
            ret += "'999:59:59'";
            if (val.paren())
            {
                ret += "::Time";
            }
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MIN_TIME64:
            ret += "'000:00:00'";
            if (val.paren())
            {
                ret += "::Time64";
            }
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_TIME64:
            ret += "'999:59:59.99999999'";
            if (val.paren())
            {
                ret += "::Time64";
            }
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MIN_DATETIME:
            ret += "'1970-01-01 00:00:00'";
            if (val.paren())
            {
                ret += "::DateTime";
            }
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_DATETIME:
            ret += "'2106-02-07 06:28:15'";
            if (val.paren())
            {
                ret += "::DateTime";
            }
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MIN_DATETIME64:
            ret += "'1900-01-01 00:00:00'";
            if (val.paren())
            {
                ret += "::DateTime64";
            }
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_MAX_DATETIME64:
            ret += "'2299-12-31 23:59:59.99999999'";
            if (val.paren())
            {
                ret += "::DateTime64";
            }
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_NULL_CHAR:
            ret += "'\\0'";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_DEFAULT:
            ret += "DEFAULT";
            break;
        case SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_STAR:
            ret += "*";
            break;
    }
    if (val.paren())
    {
        ret += ")";
    }
}

CONV_FN(LiteralValue, lit_val)
{
    using LitValType = LiteralValue::LitValOneofCase;
    switch (lit_val.lit_val_oneof_case())
    {
        case LitValType::kIntLit:
            IntLiteralToString(ret, lit_val.int_lit());
            break;
        case LitValType::kNoQuoteStr:
            ret += lit_val.no_quote_str();
            break;
        case LitValType::kHexString: {
            const String & s = lit_val.hex_string();
            ret += "x'";
            for (size_t i = 0; i < s.length(); i++)
            {
                const uint8_t & x = static_cast<uint8_t>(s[i]);
                ret += hexDigits[(x & 0xF0) >> 4];
                ret += hexDigits[x & 0x0F];
            }
            ret += '\'';
        }
        break;
        case LitValType::kHeredoc:
            ret += "$heredoc$";
            ret += lit_val.heredoc();
            ret += "$heredoc$";
            break;
        case LitValType::kNumericLit:
            NumericLiteralToString(ret, lit_val.numeric_lit());
            break;
        case LitValType::kStringLit:
            ret += '\'';
            ConvertToSQLString(ret, lit_val.string_lit());
            ret += '\'';
            break;
        case LitValType::kSpecialVal:
            SpecialValToString(ret, lit_val.special_val());
            break;
        default:
            ret += "1";
    }
}

CONV_FN(UnaryExpr, uexpr)
{
    ret += "(";
    switch (uexpr.unary_op())
    {
        case UNOP_MINUS:
            ret += "-";
            break;
        case UNOP_PLUS:
            ret += "+";
            break;
        case UNOP_NOT:
            ret += "NOT ";
            break;
    }
    ExprToString(ret, uexpr.expr());
    ret += ")";
}

CONV_FN(BinaryOperator, bop)
{
    switch (bop)
    {
        case BINOP_LE:
            ret += " < ";
            break;
        case BINOP_LEQ:
            ret += " <= ";
            break;
        case BINOP_GR:
            ret += " > ";
            break;
        case BINOP_GREQ:
            ret += " >= ";
            break;
        case BINOP_EQ:
            ret += " = ";
            break;
        case BINOP_EQEQ:
            ret += " == ";
            break;
        case BINOP_NOTEQ:
            ret += " != ";
            break;
        case BINOP_LEGR:
            ret += " <> ";
            break;
        case BINOP_IS_NOT_DISTINCT_FROM:
            ret += " IS NOT DISTINCT FROM ";
            break;
        case BINOP_LEEQGR:
            ret += " <=> ";
            break;
        case BINOP_AND:
            ret += " AND ";
            break;
        case BINOP_OR:
            ret += " OR ";
            break;
        case BINOP_CONCAT:
            ret += " || ";
            break;
        case BINOP_STAR:
            ret += " * ";
            break;
        case BINOP_SLASH:
            ret += " / ";
            break;
        case BINOP_PERCENT:
            ret += " % ";
            break;
        case BINOP_PLUS:
            ret += " + ";
            break;
        case BINOP_MINUS:
            ret += " - ";
            break;
        case BINOP_DIV:
            ret += " DIV ";
            break;
        case BINOP_MOD:
            ret += " MOD ";
            break;
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

static void BottomTypeNameToString(String & ret, const uint32_t quote, const bool lcard, const BottomTypeName & btn)
{
    using BottomTypeNameType = BottomTypeName::BottomOneOfCase;
    switch (btn.bottom_one_of_case())
    {
        case BottomTypeNameType::kIntegers:
            ret += Integers_Name(btn.integers());
            break;
        case BottomTypeNameType::kFloats:
            ret += FloatingPoints_Name(btn.floats());
            break;
        case BottomTypeNameType::kStandardString:
            ret += "String";
            break;
        case BottomTypeNameType::kFixedString:
            ret += "FixedString(";
            ret += std::to_string(std::max<uint32_t>(1, btn.fixed_string()));
            ret += ")";
            break;
        case BottomTypeNameType::kDates:
            ret += Dates_Name(btn.dates());
            break;
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
                    ret += "'";
                    ret += dt.timezone();
                    ret += "'";
                }
                ret += ")";
            }
        }
        break;
        case BottomTypeNameType::kBoolean:
            ret += "Bool";
            break;
        case BottomTypeNameType::kUuid:
            ret += "UUID";
            break;
        case BottomTypeNameType::kIPv4:
            ret += "IPv4";
            break;
        case BottomTypeNameType::kIPv6:
            ret += "IPv6";
            break;
        case BottomTypeNameType::kDecimal: {
            const Decimal & dec = btn.decimal();

            ret += "Decimal";
            if (dec.has_decimaln())
            {
                uint32_t precision = 0;
                const DecimalN & dn = dec.decimaln();

                switch (dn.precision())
                {
                    case DecimalN_DecimalPrecision::DecimalN_DecimalPrecision_D32:
                        precision = 9;
                        break;
                    case DecimalN_DecimalPrecision::DecimalN_DecimalPrecision_D64:
                        precision = 18;
                        break;
                    case DecimalN_DecimalPrecision::DecimalN_DecimalPrecision_D128:
                        precision = 38;
                        break;
                    case DecimalN_DecimalPrecision::DecimalN_DecimalPrecision_D256:
                        precision = 76;
                        break;
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
                                else if (jspec.has_skip_path())
                                {
                                    ret += "SKIP ";
                                    ColumnPathToString(ret, quote, jspec.skip_path());
                                }
                                else if (jspec.has_path_type())
                                {
                                    const JSONPathType & jpt = jspec.path_type();

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
                        ret += "(";
                        EnumDefValueToString(ret, edef.first_value());
                        for (int i = 0; i < edef.other_values_size(); i++)
                        {
                            ret += ", ";
                            EnumDefValueToString(ret, edef.other_values(i));
                        }
                        ret += ")";
                    }
                    break;
                    default:
                        ret += "Int";
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
        case TopTypeNameType::kNonNullable:
            BottomTypeNameToString(ret, quote, false, ttn.non_nullable());
            break;
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
        case TopTypeNameType::kGeo:
            ret += GeoTypes_Name(ttn.geo());
            break;
        default:
            ret += "Int";
    }
}

CONV_FN_QUOTE(TypeName, tp)
{
    TopTypeNameToString(ret, quote, tp.type());
}

CONV_FN(CastExpr, cexpr)
{
    ret += "CAST(";
    ExprToString(ret, cexpr.expr());
    ret += " AS ";
    TypeNameToString(ret, 0, cexpr.type_name());
    ret += ")";
}

CONV_FN(ExprLike, elike)
{
    ExprToString(ret, elike.expr1());
    ret += " ";
    if (elike.not_() && elike.keyword() != ExprLike_PossibleKeywords::ExprLike_PossibleKeywords_REGEXP)
        ret += "NOT ";
    ret += ExprLike_PossibleKeywords_Name(elike.keyword());
    ret += " ";
    ExprToString(ret, elike.expr2());
}

CONV_FN(CondExpr, econd)
{
    ret += "((";
    ExprToString(ret, econd.expr1());
    ret += ") ? (";
    ExprToString(ret, econd.expr2());
    ret += ") : (";
    ExprToString(ret, econd.expr3());
    ret += "))";
}

CONV_FN(ExprNullTests, ent)
{
    ExprToString(ret, ent.expr());
    ret += " IS";
    ret += ent.not_() ? " NOT" : "";
    ret += " NULL";
}

CONV_FN(ExprBetween, ebetween)
{
    ret += "((";
    ExprToString(ret, ebetween.expr1());
    ret += ") ";
    if (ebetween.not_())
        ret += "NOT ";
    ret += "BETWEEN (";
    ExprToString(ret, ebetween.expr2());
    ret += ") AND (";
    ExprToString(ret, ebetween.expr3());
    ret += "))";
}

CONV_FN(ExplainQuery, explain);

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
    using InType = ExprIn::InOneofCase;
    switch (ein.in_oneof_case())
    {
        case InType::kSingleExpr:
            ExprToString(ret, ein.single_expr());
            break;
        case InType::kExprs:
            ret += "(";
            ExprListToString(ret, ein.exprs());
            ret += ")";
            break;
        case InType::kSel:
            ret += "(";
            ExplainQueryToString(ret, ein.sel());
            ret += ")";
            break;
        default:
            ret += "1";
    }
}

CONV_FN(ExprAny, eany)
{
    ExprToString(ret, eany.expr());
    BinaryOperatorToString(ret, static_cast<BinaryOperator>(((static_cast<int>(eany.op()) % 8) + 1)));
    ret += eany.anyall() ? "ALL" : "ANY";
    ret += "(";
    ExplainQueryToString(ret, eany.sel());
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
    ret += "(";
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
    ret += ") -> ";
    ExprToString(ret, lambda.expr());
}

CONV_FN(SQLFuncName, sfn)
{
    if (sfn.has_catalog_func())
    {
        ret += SQLFunc_Name(sfn.catalog_func()).substr(4);
    }
    else if (sfn.has_function())
    {
        FunctionToString(ret, sfn.function());
    }
    else
    {
        ret += "count";
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
    for (int i = 0; i < sfc.args_size(); i++)
    {
        const SQLFuncArg & sfa = sfc.args(i);

        if (i != 0)
        {
            ret += ", ";
        }
        if (sfa.has_lambda())
        {
            LambdaExprToString(ret, sfa.lambda());
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
        ret += " COLLATE '";
        ret += eot.collation();
        ret += "'";
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
    ret += "INTERVAL (";
    ExprToString(ret, ie.expr());
    ret += ") ";
    ret += IntervalExpr_Interval_Name(ie.interval());
}

CONV_FN(ComplicatedExpr, expr)
{
    using ExprType = ComplicatedExpr::ComplicatedExprOneofCase;
    switch (expr.complicated_expr_oneof_case())
    {
        case ExprType::kExprStc:
            ExprSchemaTableColumnToString(ret, expr.expr_stc());
            break;
        case ExprType::kUnaryExpr:
            UnaryExprToString(ret, expr.unary_expr());
            break;
        case ExprType::kBinaryExpr:
            BinaryExprToString(ret, expr.binary_expr());
            break;
        case ExprType::kAliasExpr:
            ExprColAliasToString(ret, expr.alias_expr());
            break;
        case ExprType::kCastExpr:
            CastExprToString(ret, expr.cast_expr());
            break;
        case ExprType::kExprBetween:
            ExprBetweenToString(ret, expr.expr_between());
            break;
        case ExprType::kExprIn:
            ExprInToString(ret, expr.expr_in());
            break;
        case ExprType::kExprAny:
            ExprAnyToString(ret, expr.expr_any());
            break;
        case ExprType::kExprNullTests:
            ExprNullTestsToString(ret, expr.expr_null_tests());
            break;
        case ExprType::kExprCase:
            ExprCaseToString(ret, expr.expr_case());
            break;
        case ExprType::kExprExists:
            ExprExistsToString(ret, expr.expr_exists());
            break;
        case ExprType::kExprLike:
            ExprLikeToString(ret, expr.expr_like());
            break;
        case ExprType::kExprCond:
            CondExprToString(ret, expr.expr_cond());
            break;
        case ExprType::kSubquery:
            ret += "(";
            ExplainQueryToString(ret, expr.subquery());
            ret += ")";
            break;
        case ExprType::kFuncCall:
            SQLFuncCallToString(ret, expr.func_call());
            break;
        case ExprType::kWindowCall:
            WindowFuncCallToString(ret, expr.window_call());
            break;
        case ExprType::kInterval:
            IntervalExprToString(ret, expr.interval());
            break;
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
            TableToString(ret, expr.table());
            ret += ".*";
            break;
        default:
            ret += "1";
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
    if (expr.has_field())
    {
        FieldAccessToString(ret, expr.field());
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

CONV_FN(JoinedQuery, tos);
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
        for (int i = 0; i < uexpr.columns_size(); i++)
        {
            if (i != 0)
            {
                ret += ", ";
            }
            ExprColumnToString(ret, uexpr.columns(i));
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
    const bool has_cross_or_paste = jcc.join_op() > JoinType::J_FULL;

    if (jcc.global())
    {
        ret += " GLOBAL";
    }
    if (jcc.join_op() != JoinType::J_INNER || !jcc.has_join_const() || jcc.join_const() < JoinConst::J_SEMI)
    {
        ret += " ";
        ret += JoinType_Name(jcc.join_op()).substr(2);
    }
    if (!has_cross_or_paste && jcc.has_join_const())
    {
        ret += " ";
        ret += JoinConst_Name(jcc.join_const()).substr(2);
    }
    ret += " JOIN ";
    TableOrSubqueryToString(ret, jcc.tos());
    if (!has_cross_or_paste)
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

CONV_FN(FileFunc, ff)
{
    ret += FileFunc_FName_Name(ff.fname());
    ret += "(";
    if (ff.has_cluster() && ff.fname() == FileFunc_FName_fileCluster)
    {
        ClusterToString(ret, false, ff.cluster());
        ret += ", ";
    }
    ret += "'";
    ret += ff.path();
    ret += "', '";
    if (ff.has_informat())
    {
        ret += InFormat_Name(ff.informat()).substr(3);
    }
    else if (ff.has_outformat())
    {
        ret += OutFormat_Name(ff.outformat()).substr(4);
    }
    else
    {
        ret += "CSV";
    }
    ret += "'";
    if (ff.has_structure())
    {
        ret += ", ";
        ExprToString(ret, ff.structure());
    }
    if (ff.has_fcomp())
    {
        ret += ", '";
        ret += FileCompression_Name(ff.fcomp()).substr(4);
        ret += "'";
    }
    ret += ")";
}

CONV_FN(FormatFunc, ff)
{
    ret += "format(";
    ret += InFormat_Name(ff.format()).substr(3);
    if (ff.has_structure())
    {
        ret += ", ";
        ExprToString(ret, ff.structure());
    }
    ret += ", $$\n";
    ret += ff.data();
    ret += "$$)";
}

CONV_FN(GenerateSeriesFunc, gsf)
{
    ret += GenerateSeriesFunc_GSName_Name(gsf.fname());
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
    ret += ")";
}

static void FlatExprSchemaTableToString(String & ret, const ExprSchemaTable & est, const String & separator)
{
    ret += "'";
    if (est.has_database())
    {
        DatabaseToString(ret, est.database());
    }
    else
    {
        ret += "default";
    }
    ret += separator;
    TableToString(ret, est.table());
    ret += "'";
}

CONV_FN(TableFunction, tf);

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
        case TableOrFunctionType::kTfunc:
            TableFunctionToString(ret, tof.tfunc());
            break;
        case TableOrFunctionType::kSelect:
            ret += tudf ? "view" : "";
            ret += "(";
            ExplainQueryToString(ret, tof.select());
            ret += ")";
            break;
        default:
            ret += "numbers(10)";
    }
}

CONV_FN(RemoteFunc, rfunc)
{
    const TableOrFunction & tof = rfunc.tof();

    ret += RemoteFunc_RName_Name(rfunc.rname());
    ret += "('";
    ret += rfunc.address();
    ret += "', ";
    TableOrFunctionToString(ret, true, tof);
    if (tof.has_est())
    {
        if (rfunc.has_user() && !rfunc.user().empty())
        {
            ret += ", '";
            ret += rfunc.user();
            ret += "'";
        }
        if (rfunc.has_password())
        {
            ret += ", '";
            ret += rfunc.password();
            ret += "'";
        }
    }
    ret += ")";
}

CONV_FN(MySQLFunc, mfunc)
{
    ret += "mysql('";
    ret += mfunc.address();
    ret += "', '";
    ret += mfunc.rdatabase();
    ret += "', '";
    ret += mfunc.rtable();
    ret += "', '";
    ret += mfunc.user();
    ret += "', '";
    ret += mfunc.password();
    ret += "')";
}

CONV_FN(PostgreSQLFunc, pfunc)
{
    ret += "postgresql('";
    ret += pfunc.address();
    ret += "', '";
    ret += pfunc.rdatabase();
    ret += "', '";
    ret += pfunc.rtable();
    ret += "', '";
    ret += pfunc.user();
    ret += "', '";
    ret += pfunc.password();
    ret += "'";
    if (pfunc.has_rschema())
    {
        ret += ", '";
        ret += pfunc.rschema();
        ret += "'";
    }
    ret += ")";
}

CONV_FN(SQLiteFunc, sfunc)
{
    ret += "sqlite('";
    ret += sfunc.rdatabase();
    ret += "', '";
    ret += sfunc.rtable();
    ret += "')";
}

CONV_FN(S3Func, sfunc)
{
    ret += S3Func_FName_Name(sfunc.fname());
    ret += "(";
    if (sfunc.has_cluster() && sfunc.fname() == S3Func_FName_s3Cluster)
    {
        ClusterToString(ret, false, sfunc.cluster());
        ret += ", ";
    }
    ret += "'";
    ret += sfunc.resource();
    ret += "', '";
    ret += sfunc.user();
    ret += "', '";
    ret += sfunc.password();
    ret += "', '";
    ret += InOutFormat_Name(sfunc.format()).substr(6);
    ret += "', ";
    ExprToString(ret, sfunc.structure());
    if (sfunc.has_fcomp())
    {
        ret += ", '";
        ret += sfunc.fcomp();
        ret += "'";
    }
    ret += ")";
}

CONV_FN(AzureBlobStorageFunc, azure)
{
    ret += AzureBlobStorageFunc_FName_Name(azure.fname());
    ret += "(";
    if (azure.has_cluster() && azure.fname() == AzureBlobStorageFunc_FName_azureBlobStorageCluster)
    {
        ClusterToString(ret, false, azure.cluster());
        ret += ", ";
    }
    ret += "'";
    ret += azure.connection_string();
    ret += "', '";
    ret += azure.container();
    ret += "', '";
    ret += azure.blobpath();
    ret += "'";
    if (azure.has_user())
    {
        ret += ", '";
        ret += azure.user();
        ret += "'";
    }
    if (azure.has_password())
    {
        ret += ", '";
        ret += azure.password();
        ret += "'";
    }
    if (azure.has_format())
    {
        ret += ", '";
        ret += InOutFormat_Name(azure.format()).substr(6);
        ret += "'";
    }
    if (azure.has_fcomp())
    {
        ret += ", '";
        ret += azure.fcomp();
        ret += "'";
    }
    if (azure.has_structure())
    {
        ret += ", ";
        ExprToString(ret, azure.structure());
    }
    ret += ")";
}

CONV_FN(URLFunc, url)
{
    ret += URLFunc_FName_Name(url.fname());
    ret += "(";
    if (url.has_cluster() && url.fname() == URLFunc_FName_urlCluster)
    {
        ClusterToString(ret, false, url.cluster());
        ret += ", ";
    }
    ret += "'";
    ret += url.uurl();
    ret += "'";
    if (url.has_informat())
    {
        ret += ", '";
        ret += InFormat_Name(url.informat()).substr(3);
        ret += "'";
    }
    else if (url.has_outformat())
    {
        ret += ", '";
        ret += OutFormat_Name(url.outformat()).substr(4);
        ret += "'";
    }
    else if (url.has_inoutformat())
    {
        ret += ", '";
        ret += InOutFormat_Name(url.inoutformat()).substr(6);
        ret += "'";
    }
    if (url.has_structure())
    {
        ret += ", ";
        ExprToString(ret, url.structure());
    }
    ret += ")";
}

CONV_FN(SQLTableFuncCall, sfc)
{
    ret += SQLTableFunc_Name(sfc.func()).substr(2);
    ret += '(';
    for (int i = 0; i < sfc.args_size(); i++)
    {
        const SQLFuncArg & sfa = sfc.args(i);

        if (i != 0)
        {
            ret += ", ";
        }
        if (sfa.has_lambda())
        {
            LambdaExprToString(ret, sfa.lambda());
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
    ret += ')';
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
    ret += "'";
    ret += mfunc.mtable();
    ret += "')";
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
    ret += ", ";
    TableOrFunctionToString(ret, true, tof);
    if (tof.has_est() && cluster.has_sharding_key())
    {
        ret += ", '";
        ret += cluster.sharding_key();
        ret += "'";
    }
    ret += ")";
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
    ret += ")";
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
    ret += ")";
}

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
    ret += tudf ? ")" : "";
}

CONV_FN(TableFunction, tf)
{
    using TableFunctionType = TableFunction::JtfOneofCase;
    switch (tf.jtf_oneof_case())
    {
        case TableFunctionType::kFile:
            FileFuncToString(ret, tf.file());
            break;
        case TableFunctionType::kFormat:
            FormatFuncToString(ret, tf.format());
            break;
        case TableFunctionType::kGseries:
            GenerateSeriesFuncToString(ret, tf.gseries());
            break;
        case TableFunctionType::kRemote:
            RemoteFuncToString(ret, tf.remote());
            break;
        case TableFunctionType::kMysql:
            MySQLFuncToString(ret, tf.mysql());
            break;
        case TableFunctionType::kPostgresql:
            PostgreSQLFuncToString(ret, tf.postgresql());
            break;
        case TableFunctionType::kSqite:
            SQLiteFuncToString(ret, tf.sqite());
            break;
        case TableFunctionType::kS3:
            S3FuncToString(ret, tf.s3());
            break;
        case TableFunctionType::kFunc:
            SQLTableFuncCallToString(ret, tf.func());
            break;
        case TableFunctionType::kMerge:
            MergeFuncToString(ret, tf.merge());
            break;
        case TableFunctionType::kCluster:
            ClusterFuncToString(ret, tf.cluster());
            break;
        case TableFunctionType::kMtindex:
            MergeTreeIndexFuncToString(ret, tf.mtindex());
            break;
        case TableFunctionType::kLoop:
            ret += "loop(";
            TableOrFunctionToString(ret, true, tf.loop());
            ret += ")";
            break;
        case TableFunctionType::kGrandom:
            GenerateRandomFuncToString(ret, tf.grandom());
            break;
        case TableFunctionType::kValues:
            ValuesStatementToString(ret, true, tf.values());
            break;
        case TableFunctionType::kDictionary:
            ret += "dictionary(";
            FlatExprSchemaTableToString(ret, tf.dictionary(), ".");
            ret += ")";
            break;
        case TableFunctionType::kAzure:
            AzureBlobStorageFuncToString(ret, tf.azure());
            break;
        case TableFunctionType::kUrl:
            URLFuncToString(ret, tf.url());
            break;
        default:
            ret += "numbers(10)";
    }
}

CONV_FN(JoinedTableOrFunction, jtf)
{
    const TableOrFunction & tof = jtf.tof();

    TableOrFunctionToString(ret, false, tof);
    if (jtf.has_table_alias())
    {
        ret += " AS ";
        TableToString(ret, jtf.table_alias());
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
            ColumnToString(ret, 1, jtf.col_aliases(i));
        }
        ret += ")";
    }
    if (jtf.final())
    {
        ret += " FINAL";
    }
}

CONV_FN(TableOrSubquery, tos)
{
    using JoinedType = TableOrSubquery::TosOneofCase;
    switch (tos.tos_oneof_case())
    {
        case JoinedType::kJoinedTable:
            JoinedTableOrFunctionToString(ret, tos.joined_table());
            break;
        case JoinedType::kJoinedQuery:
            JoinedQueryToString(ret, tos.joined_query());
            break;
        default:
            ret += "(SELECT 1 c0) t0";
    }
}

CONV_FN(JoinedQuery, tos)
{
    if (tos.tos_list_size() > 0)
    {
        for (int i = 0; i < tos.tos_list_size(); i++)
        {
            if (i != 0)
            {
                ret += ", ";
            }
            TableOrSubqueryToString(ret, tos.tos_list(i));
        }
    }
    else
    {
        JoinClauseToString(ret, tos.join_clause());
    }
}

CONV_FN(FromStatement, fs)
{
    ret += "FROM ";
    JoinedQueryToString(ret, fs.tos());
}

CONV_FN(ColumnComparison, cc)
{
    ExprSchemaTableColumnToString(ret, cc.col());
    BinaryOperatorToString(ret, cc.op());
    ExprToString(ret, cc.expr());
}

CONV_FN(ExprComparisonHighProbability, echp)
{
    if (echp.has_cc())
    {
        ColumnComparisonToString(ret, echp.cc());
    }
    else if (echp.has_expr())
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
        case GroupByListType::kExprs:
            ExprListToString(ret, gbl.exprs());
            break;
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
        default:
            ret += "GROUPING SETS(())";
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

CONV_FN(LimitStatement, ls)
{
    ret += "LIMIT ";
    ExprToString(ret, ls.limit());
    if (ls.has_offset())
    {
        ret += ", ";
        ExprToString(ret, ls.offset());
    }
    if (ls.with_ties())
    {
        ret += " WITH TIES";
    }
    if (ls.has_limit_by())
    {
        ret += " BY ";
        ExprToString(ret, ls.limit_by());
    }
}

CONV_FN(FetchStatement, fet)
{
    ret += "FETCH ";
    ret += fet.first() ? "FIRST" : "NEXT";
    ret += " ";
    ExprToString(ret, fet.row_count());
    ret += " ROW";
    ret += fet.rows() ? "S" : "";
    ret += " ";
    ret += fet.only() ? "ONLY" : "WITH TIES";
}

CONV_FN(OffsetStatement, off)
{
    ret += "OFFSET ";
    ExprToString(ret, off.row_count());
    ret += " ROW";
    ret += off.rows() ? "S" : "";
    if (off.has_fetch())
    {
        ret += " ";
        FetchStatementToString(ret, off.fetch());
    }
}

CONV_FN(SelectStatementCore, ssc)
{
    const bool from_first
        = ssc.from_first() && ssc.has_from() && ssc.from().tos().tos_list_size() == 0 && ssc.from().tos().join_clause().clauses_size() == 0;

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
    if (ssc.has_limit())
    {
        ret += " ";
        LimitStatementToString(ret, ssc.limit());
    }
    else if (ssc.has_offset())
    {
        ret += " ";
        OffsetStatementToString(ret, ssc.offset());
    }
}

CONV_FN(SetQuery, setq)
{
    ret += "(";
    ExplainQueryToString(ret, setq.sel1());
    ret += ") ";
    ret += SetQuery_SetOp_Name(setq.set_op());
    ret += " ";
    ret += AllOrDistinct_Name(setq.s_or_d());
    ret += " (";
    ExplainQueryToString(ret, setq.sel2());
    ret += ")";
}

CONV_FN(CTEquery, cteq)
{
    TableToString(ret, cteq.table());
    ret += " AS (";
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
    ret += "WITH ";
    SingleCTEToString(ret, cteq.cte());
    for (int i = 0; i < cteq.other_ctes_size(); i++)
    {
        ret += ", ";
        SingleCTEToString(ret, cteq.other_ctes(i));
    }
    ret += " ";
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
    if (select.has_setting_values())
    {
        ret += " SETTINGS ";
        SettingValuesToString(ret, select.setting_values());
    }
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

CONV_FN(CodecParam, cp)
{
    ret += CompressionCodec_Name(cp.codec()).substr(5);
    if (cp.params_size())
    {
        ret += "(";
        for (int i = 0; i < cp.params_size(); i++)
        {
            if (i != 0)
            {
                ret += ", ";
            }
            ret += std::to_string(cp.params(i));
        }
        ret += ")";
    }
}

CONV_FN(DatabaseEngineParam, dep)
{
    using DatabaseEngineParamType = DatabaseEngineParam::DatabaseEngineParamOneofCase;
    switch (dep.database_engine_param_oneof_case())
    {
        case DatabaseEngineParamType::kSvalue:
            ret += "'";
            ret += dep.svalue();
            ret += "'";
            break;
        case DatabaseEngineParamType::kDatabase:
            ret += "'";
            DatabaseToString(ret, dep.database());
            ret += "'";
            break;
        case DatabaseEngineParamType::kDisk:
            ret += "Disk('";
            ret += dep.disk().disk();
            ret += "', '";
            DatabaseToString(ret, dep.disk().database());
            ret += "')";
            break;
        default:
            ret += "d0";
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
}

CONV_FN(CreateDatabase, create_database)
{
    ret += "CREATE DATABASE ";
    if (create_database.if_not_exists())
    {
        ret += "IF NOT EXISTS ";
    }
    DatabaseToString(ret, create_database.database());
    if (create_database.has_cluster())
    {
        ClusterToString(ret, true, create_database.cluster());
    }
    ret += " ENGINE = ";
    DatabaseEngineToString(ret, create_database.dengine());
    if (create_database.has_comment())
    {
        ret += " COMMENT ";
        ret += create_database.comment();
    }
    if (create_database.has_setting_values())
    {
        ret += " SETTINGS ";
        SettingValuesToString(ret, create_database.setting_values());
    }
}

CONV_FN(CreateFunction, create_function)
{
    ret += "CREATE FUNCTION ";
    FunctionToString(ret, create_function.function());
    if (create_function.has_cluster())
    {
        ClusterToString(ret, true, create_function.cluster());
    }
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

CONV_FN(CodecList, cl)
{
    ret += "CODEC(";
    CodecParamToString(ret, cl.codec());
    for (int i = 0; i < cl.other_codecs_size(); i++)
    {
        ret += ", ";
        CodecParamToString(ret, cl.other_codecs(i));
    }
    ret += ")";
}

CONV_FN(ColumnDef, cdf)
{
    ret += "`";
    ColumnToString(ret, 1, cdf.col());
    ret += "` ";
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
        ret += " ";
        CodecListToString(ret, cdf.codecs());
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

CONV_FN(IndexParam, ip)
{
    using IndexParamType = IndexParam::IndexParamOneofCase;
    switch (ip.index_param_oneof_case())
    {
        case IndexParamType::kIval:
            ret += std::to_string(ip.ival());
            break;
        case IndexParamType::kDval:
            ret += std::to_string(ip.dval());
            break;
        case IndexParamType::kSval:
            ret += "'";
            ret += ip.sval();
            ret += "'";
            break;
        case IndexParamType::kUnescapedSval:
            ret += ip.unescaped_sval();
            break;
        default:
            ret += "0";
    }
}

CONV_FN(IndexDef, idef)
{
    ret += "INDEX ";
    IndexToString(ret, idef.idx());
    ret += " ";
    ExprToString(ret, idef.expr());
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

CONV_FN(ProjectionDef, proj_def)
{
    ret += "PROJECTION ";
    ProjectionToString(ret, proj_def.proj());
    ret += " (";
    SelectToString(ret, proj_def.select());
    ret += ")";
}

CONV_FN(ConstraintDef, const_def)
{
    ret += "CONSTRAINT ";
    ConstraintToString(ret, const_def.constr());
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
        case CreateDefType::kColDef:
            ColumnDefToString(ret, tdef.col_def());
            break;
        case CreateDefType::kIdxDef:
            IndexDefToString(ret, tdef.idx_def());
            break;
        case CreateDefType::kProjDef:
            ProjectionDefToString(ret, tdef.proj_def());
            break;
        case CreateDefType::kConstDef:
            ConstraintDefToString(ret, tdef.const_def());
            break;
        default:
            ret += "c0 Int";
    }
}

CONV_FN(TableDef, ct)
{
    ColumnDefToString(ret, ct.col_def());
    for (int i = 0; i < ct.other_defs_size(); i++)
    {
        ret += ", ";
        TableDefItemToString(ret, ct.other_defs(i));
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
        case TableEngineParamType::kCols:
            ColumnPathToString(ret, 0, tep.cols());
            break;
        case TableEngineParamType::kIn:
            ret += InFormat_Name(tep.in()).substr(3);
            break;
        case TableEngineParamType::kOut:
            ret += OutFormat_Name(tep.out()).substr(4);
            break;
        case TableEngineParamType::kInOut:
            ret += InOutFormat_Name(tep.in_out()).substr(6);
            break;
        case TableEngineParamType::kJoinOp:
            ret += JoinType_Name(tep.join_op()).substr(2);
            break;
        case TableEngineParamType::kJoinConst:
            ret += JoinConst_Name(tep.join_const()).substr(2);
            break;
        case TableEngineParamType::kDatabase:
            DatabaseToString(ret, tep.database());
            break;
        case TableEngineParamType::kTable:
            TableToString(ret, tep.table());
            break;
        case TableEngineParamType::kNum:
            ret += std::to_string(tep.num());
            break;
        case TableEngineParamType::kSvalue:
            ret += "'";
            ret += tep.svalue();
            ret += "'";
            break;
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
        case TableEngineParamType::kEst:
            FlatExprSchemaTableToString(ret, tep.est(), ".");
            break;
        case TableEngineParamType::kExpr:
            ExprToString(ret, tep.expr());
            break;
        default:
            ret += "c0";
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
        case TTLUpdateType::kDel:
            TTLDeleteToString(ret, upt.del());
            break;
        case TTLUpdateType::kCodecs:
            ret += "RECOMPRESS ";
            CodecListToString(ret, upt.codecs());
            break;
        case TTLUpdateType::kStorage:
            ret += "TO ";
            StorageToString(ret, upt.storage());
            break;
        default:
            ret += "DELETE";
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
    ret += " ";
    if (ttl_entry.has_update())
    {
        TTLUpdateToString(ret, ttl_entry.update());
    }
    else if (ttl_entry.has_group_by())
    {
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
            && ((teng >= TableEngineValues::MergeTree && teng <= TableEngineValues::VersionedCollapsingMergeTree)
                || teng == TableEngineValues::Set || teng == TableEngineValues::Join))
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
    if (te.has_setting_values())
    {
        ret += " SETTINGS ";
        SettingValuesToString(ret, te.setting_values());
    }
}

CONV_FN(CreateTableAs, create_table)
{
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
    ret += " AS (";
    SelectToString(ret, create_table.sel());
    ret += ")";
}

static void CreateOrReplaceToString(String & ret, const CreateReplaceOption & cro)
{
    switch (cro)
    {
        case CreateReplaceOption::Create:
            ret += "CREATE";
            break;
        case CreateReplaceOption::Replace:
            ret += "REPLACE";
            break;
        case CreateReplaceOption::CreateOrReplace:
            ret += "CREATE OR REPLACE";
            break;
    }
}

CONV_FN(CreateTable, create_table)
{
    CreateOrReplaceToString(ret, create_table.create_opt());
    ret += " ";
    if (create_table.create_opt() == CreateReplaceOption::Create && create_table.is_temp())
    {
        ret += "TEMPORARY ";
    }
    ret += "TABLE ";
    if (create_table.if_not_exists())
    {
        ret += "IF NOT EXISTS ";
    }
    ExprSchemaTableToString(ret, create_table.est());
    if (create_table.has_cluster())
    {
        ClusterToString(ret, true, create_table.cluster());
    }
    ret += " ";
    if (create_table.has_table_def())
    {
        ret += "(";
        TableDefToString(ret, create_table.table_def());
        ret += ")";
    }
    else if (create_table.has_table_as())
    {
        CreateTableAsToString(ret, create_table.table_as());
    }
    TableEngineToString(ret, create_table.engine());
    if (create_table.has_ttl_expr())
    {
        ret += " ";
        TTLExprToString(ret, create_table.ttl_expr());
    }
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
        case SQLObjectNameType::kEst:
            ExprSchemaTableToString(ret, son.est());
            break;
        case SQLObjectNameType::kDatabase:
            DatabaseToString(ret, son.database());
            break;
        case SQLObjectNameType::kFunction:
            FunctionToString(ret, son.function());
            break;
        default:
            ret += "t0";
    }
}

CONV_FN(Drop, dt)
{
    const bool is_table = dt.sobject() == SQLObject::TABLE;

    ret += "DROP ";
    if (is_table && dt.is_temp())
    {
        ret += "TEMPORARY ";
    }
    ret += SQLObject_Name(dt.sobject());
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
    for (int i = 0; i < dt.other_objects_size(); i++)
    {
        ret += ", ";
        SQLObjectNameToString(ret, dt.other_objects(i));
    }
    if (dt.has_cluster())
    {
        ClusterToString(ret, true, dt.cluster());
    }
    if (dt.sync())
    {
        ret += " SYNC";
    }
    if (dt.has_setting_values())
    {
        ret += " SETTINGS ";
        SettingValuesToString(ret, dt.setting_values());
    }
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
        SelectToString(ret, insert.select());
    }
    else if (insert.has_insert_file())
    {
        const InsertFromFile & insert_file = insert.insert_file();

        ret += "FROM INFILE '";
        ret += insert_file.path();
        ret += "'";
        if (insert_file.has_fcomp())
        {
            ret += " COMPRESSION '";
            ret += FileCompression_Name(insert_file.fcomp()).substr(4);
            ret += "'";
        }
        if (insert.has_setting_values())
        {
            ret += " SETTINGS ";
            SettingValuesToString(ret, insert.setting_values());
        }
        ret += " FORMAT ";
        ret += InFormat_Name(insert_file.format()).substr(3);
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
        case PartitionType::kPart:
            ret += "'";
            ret += pexpr.part();
            ret += "'";
            break;
        case PartitionType::kPartition:
            ret += "$piddef$";
            ret += pexpr.partition();
            ret += "$piddef$";
            break;
        case PartitionType::kPartitionId:
            ret += "ID '";
            ret += pexpr.partition_id();
            ret += "'";
            break;
        case PartitionType::kAll:
            ret += "ALL";
            break;
        default:
            ret += "tuple()";
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

CONV_FN(Delete, del)
{
    if (del.has_single_partition())
    {
        ret += " IN ";
        SinglePartitionExprToString(ret, del.single_partition());
    }
    ret += " WHERE ";
    WhereStatementToString(ret, del.where());
}

CONV_FN(LightDelete, del)
{
    ret += "DELETE FROM ";
    ExprSchemaTableToString(ret, del.est());
    if (del.has_cluster())
    {
        ClusterToString(ret, true, del.cluster());
    }
    DeleteToString(ret, del.del());
    if (del.has_setting_values())
    {
        ret += " SETTINGS ";
        SettingValuesToString(ret, del.setting_values());
    }
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
    if (upt.has_single_partition())
    {
        ret += " IN ";
        SinglePartitionExprToString(ret, upt.single_partition());
    }
    ret += " WHERE ";
    WhereStatementToString(ret, upt.where());
}

CONV_FN(LightUpdate, upt)
{
    ret += "UPDATE ";
    ExprSchemaTableToString(ret, upt.est());
    if (upt.has_cluster())
    {
        ClusterToString(ret, true, upt.cluster());
    }
    ret += " SET ";
    UpdateToString(ret, upt.upt());
    if (upt.has_setting_values())
    {
        ret += " SETTINGS ";
        SettingValuesToString(ret, upt.setting_values());
    }
}

CONV_FN(Truncate, trunc)
{
    ret += "TRUNCATE ";
    using TruncateType = Truncate::TruncateOneofCase;
    switch (trunc.truncate_oneof_case())
    {
        case TruncateType::kEst:
            ExprSchemaTableToString(ret, trunc.est());
            break;
        case TruncateType::kAllTables:
            ret += "ALL TABLES FROM ";
            DatabaseToString(ret, trunc.all_tables());
            break;
        case TruncateType::kDatabase:
            ret += "DATABASE ";
            DatabaseToString(ret, trunc.database());
            break;
        default:
            ret += "t0";
    }
    if (trunc.has_cluster())
    {
        ClusterToString(ret, true, trunc.cluster());
    }
    if (trunc.sync())
    {
        ret += " SYNC";
    }
    if (trunc.has_setting_values())
    {
        ret += " SETTINGS ";
        SettingValuesToString(ret, trunc.setting_values());
    }
}

CONV_FN(CheckTable, ct)
{
    ret += "CHECK TABLE ";
    ExprSchemaTableToString(ret, ct.est());
    if (ct.has_single_partition())
    {
        ret += " ";
        SinglePartitionExprToString(ret, ct.single_partition());
    }
    if (ct.has_setting_values())
    {
        ret += " SETTINGS ";
        SettingValuesToString(ret, ct.setting_values());
    }
}

CONV_FN(DescTable, dt)
{
    ret += "DESCRIBE ";
    using DescType = DescTable::DescOneofCase;
    switch (dt.desc_oneof_case())
    {
        case DescType::kEst:
            ExprSchemaTableToString(ret, dt.est());
            break;
        case DescType::kSel:
            ret += "(";
            SelectToString(ret, dt.sel());
            ret += ")";
            break;
        case DescType::kStf:
            SQLTableFuncCallToString(ret, dt.stf());
            break;
        default:
            ret += "t0";
    }
    if (dt.has_setting_values())
    {
        ret += " SETTINGS ";
        SettingValuesToString(ret, dt.setting_values());
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
    if (ot.has_cluster())
    {
        ClusterToString(ret, true, ot.cluster());
    }
    if (ot.has_single_partition())
    {
        ret += " ";
        SinglePartitionExprToString(ret, ot.single_partition());
    }
    if (ot.final())
    {
        ret += " FINAL";
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
    if (ot.has_setting_values())
    {
        ret += " SETTINGS ";
        SettingValuesToString(ret, ot.setting_values());
    }
}

CONV_FN(Exchange, et)
{
    ret += "EXCHANGE ";
    switch (et.sobject())
    {
        case SQLObject::TABLE:
            ret += "TABLES";
            break;
        case SQLObject::VIEW:
            ret += "VIEWS";
            break;
        case SQLObject::DICTIONARY:
            ret += "DICTIONARIES";
            break;
        case SQLObject::DATABASE:
            ret += "DATABASES";
            break;
        case SQLObject::FUNCTION:
            ret += "FUNCTIONS";
            break;
    }
    ret += " ";
    SQLObjectNameToString(ret, et.object1());
    ret += " AND ";
    SQLObjectNameToString(ret, et.object2());
    if (et.has_cluster())
    {
        ClusterToString(ret, true, et.cluster());
    }
    if (et.has_setting_values())
    {
        ret += " SETTINGS ";
        SettingValuesToString(ret, et.setting_values());
    }
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
    ret += " RANDOMIZE FOR ";
    RefreshIntervalToString(ret, rv.randomize());
    if (rv.append())
    {
        ret += " APPEND";
    }
}

CONV_FN(CreateView, create_view)
{
    const bool replace = create_view.create_opt() != CreateReplaceOption::Create;
    const bool materialized = create_view.materialized();
    const bool refreshable = create_view.has_refresh();

    CreateOrReplaceToString(ret, create_view.create_opt());
    ret += " ";
    if (replace)
    {
        ret += "TABLE";
    }
    else
    {
        if (materialized)
        {
            ret += "MATERIALIZED ";
        }
        ret += "VIEW";
    }
    ret += " ";
    if (create_view.if_not_exists())
    {
        ret += "IF NOT EXISTS ";
    }
    ExprSchemaTableToString(ret, create_view.est());
    if (create_view.has_cluster())
    {
        ClusterToString(ret, true, create_view.cluster());
    }
    if (materialized)
    {
        if (!replace && refreshable)
        {
            ret += " ";
            RefreshableViewToString(ret, create_view.refresh());
        }
        if (!replace && create_view.has_to())
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
        if (!replace && refreshable && create_view.empty())
        {
            ret += " EMPTY";
        }
    }
    ret += " AS (";
    SelectToString(ret, create_view.select());
    ret += ")";
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

    ret += DictionarySourceDetails_DictionarySourceType_Name(dsd.source());
    ret += "(";
    if (dsd.has_est())
    {
        const String & separator = dsd.source() == DictionarySourceDetails::MONGODB ? "' COLLECTION '" : "' TABLE '";

        ret += "DB ";
        FlatExprSchemaTableToString(ret, dsd.est(), separator);
        has_something = true;
    }
    if (dsd.has_host())
    {
        if (has_something)
        {
            ret += " ";
        }
        ret += "HOST '";
        ret += dsd.host();
        ret += "'";
        has_something = true;
    }
    if (dsd.has_port())
    {
        if (has_something)
        {
            ret += " ";
        }
        ret += "PORT '";
        ret += dsd.port();
        ret += "'";
        has_something = true;
    }
    if (dsd.has_user())
    {
        if (has_something)
        {
            ret += " ";
        }
        ret += "USER '";
        ret += dsd.user();
        ret += "'";
        has_something = true;
    }
    if (dsd.has_password())
    {
        if (has_something)
        {
            ret += " ";
        }
        ret += "PASSWORD '";
        ret += dsd.password();
        ret += "'";
        has_something = true;
    }
    if (dsd.source() == DictionarySourceDetails::REDIS && dsd.has_redis_storage())
    {
        if (has_something)
        {
            ret += " ";
        }
        ret += "STORAGE_TYPE '";
        ret += DictionarySourceDetails_RedisStorageType_Name(dsd.redis_storage());
        ret += "'";
    }
    ret += ")";
}

CONV_FN(DictionarySource, ds)
{
    ret += " SOURCE(";
    using DictionarySourceType = DictionarySource::DictionarySourceOneofCase;
    switch (ds.dictionary_source_oneof_case())
    {
        case DictionarySourceType::kSource:
            DictionarySourceDetailsToString(ret, ds.source());
            break;
        default:
            ret += "NULL()";
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
    ret += "MAX ";
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
    if (create_dictionary.has_cluster())
    {
        ClusterToString(ret, true, create_dictionary.cluster());
    }
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
        IndexToString(ret, add.idx());
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

CONV_FN(SettingList, pl)
{
    ret += pl.setting();
    for (int i = 0; i < pl.other_settings_size(); i++)
    {
        ret += ", ";
        ret += pl.other_settings(i);
    }
}

CONV_FN(RemoveColumnSetting, rcp)
{
    ColumnPathToString(ret, 0, rcp.col());
    ret += " RESET SETTING ";
    SettingListToString(ret, rcp.setting_values());
}

CONV_FN(AlterItem, alter)
{
    ret += "(";
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
            if (alter.materialize_column().has_single_partition())
            {
                ret += " IN ";
                SinglePartitionExprToString(ret, alter.materialize_column().single_partition());
            }
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
            if (alter.clear_column().has_single_partition())
            {
                ret += " IN ";
                SinglePartitionExprToString(ret, alter.clear_column().single_partition());
            }
            break;
        case AlterType::kModifyColumn:
            ret += "MODIFY COLUMN ";
            AddColumnToString(ret, alter.modify_column());
            break;
        case AlterType::kCommentColumn:
            ret += "COMMENT COLUMN ";
            ColumnPathToString(ret, 0, alter.comment_column().col());
            ret += " ";
            ret += alter.comment_column().comment();
            break;
        case AlterType::kDeleteMask:
            ret += "APPLY DELETED MASK";
            if (alter.delete_mask().has_single_partition())
            {
                ret += " IN ";
                SinglePartitionExprToString(ret, alter.delete_mask().single_partition());
            }
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
            ColumnPathListToString(ret, 0, alter.mat_stats());
            break;
        case AlterType::kMaterializeIndex:
            ret += "MATERIALIZE INDEX ";
            IndexToString(ret, alter.materialize_index().idx());
            if (alter.materialize_index().has_single_partition())
            {
                ret += " IN ";
                SinglePartitionExprToString(ret, alter.materialize_index().single_partition());
            }
            break;
        case AlterType::kAddIndex:
            ret += "ADD ";
            AddIndexToString(ret, alter.add_index());
            break;
        case AlterType::kDropIndex:
            ret += "DROP INDEX ";
            IndexToString(ret, alter.drop_index());
            break;
        case AlterType::kClearIndex:
            ret += "CLEAR INDEX ";
            IndexToString(ret, alter.clear_index().idx());
            if (alter.clear_index().has_single_partition())
            {
                ret += " IN ";
                SinglePartitionExprToString(ret, alter.clear_index().single_partition());
            }
            break;
        case AlterType::kColumnRemoveProperty:
            ret += "MODIFY COLUMN ";
            RemoveColumnPropertyToString(ret, alter.column_remove_property());
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
        case AlterType::kRemoveProjection:
            ret += "DROP PROJECTION ";
            ProjectionToString(ret, alter.remove_projection());
            break;
        case AlterType::kMaterializeProjection:
            ret += "MATERIALIZE PROJECTION ";
            ProjectionToString(ret, alter.materialize_projection().proj());
            if (alter.materialize_projection().has_single_partition())
            {
                ret += " IN ";
                SinglePartitionExprToString(ret, alter.materialize_projection().single_partition());
            }
            break;
        case AlterType::kClearProjection:
            ret += "CLEAR PROJECTION ";
            ProjectionToString(ret, alter.clear_projection().proj());
            if (alter.clear_projection().has_single_partition())
            {
                ret += " IN ";
                SinglePartitionExprToString(ret, alter.clear_projection().single_partition());
            }
            break;
        case AlterType::kAddConstraint:
            ret += "ADD ";
            ConstraintDefToString(ret, alter.add_constraint());
            break;
        case AlterType::kRemoveConstraint:
            ret += "DROP CONSTRAINT ";
            ConstraintToString(ret, alter.remove_constraint());
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
        case AlterType::kForgetPartition:
            ret += "FORGET ";
            SinglePartitionExprToString(ret, alter.forget_partition());
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
            ret += " WITH NAME 'f";
            ret += std::to_string(alter.freeze_partition().fname());
            ret += "'";
            break;
        case AlterType::kUnfreezePartition:
            ret += "UNFREEZE";
            if (alter.unfreeze_partition().has_single_partition())
            {
                ret += " ";
                SinglePartitionExprToString(ret, alter.unfreeze_partition().single_partition());
            }
            ret += " WITH NAME 'f";
            ret += std::to_string(alter.unfreeze_partition().fname());
            ret += "'";
            break;
        case AlterType::kMovePartition:
            ret += "MOVE ";
            SinglePartitionExprToString(ret, alter.move_partition().single_partition());
            ret += " TO ";
            StorageToString(ret, alter.move_partition().storage());
            break;
        case AlterType::kClearIndexPartition:
            ret += "CLEAR INDEX ";
            IndexToString(ret, alter.clear_index_partition().idx());
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
        case AlterType::kRemoveTtl:
            ret += "REMOVE TTL";
            break;
        case AlterType::kModifyQuery:
            ret += "MODIFY QUERY ";
            SelectToString(ret, alter.modify_query());
            break;
        case AlterType::kRefresh:
            ret += "MODIFY ";
            RefreshableViewToString(ret, alter.refresh());
            break;
        default:
            ret += "DELETE WHERE TRUE";
    }
    ret += ")";
}

CONV_FN(Alter, alter)
{
    const bool is_table = alter.sobject() == SQLObject::TABLE;

    ret += "ALTER ";
    if (is_table && alter.is_temp())
    {
        ret += "TEMPORARY ";
    }
    ret += SQLObject_Name(alter.sobject());
    ret += " ";
    SQLObjectNameToString(ret, alter.object());
    if (alter.has_cluster())
    {
        ClusterToString(ret, true, alter.cluster());
    }
    ret += " ";
    AlterItemToString(ret, alter.alter());
    for (int i = 0; i < alter.other_alters_size(); i++)
    {
        ret += ", ";
        AlterItemToString(ret, alter.other_alters(i));
    }
    if (alter.has_setting_values())
    {
        ret += " SETTINGS ";
        SettingValuesToString(ret, alter.setting_values());
    }
}

CONV_FN(Attach, at)
{
    ret += "ATTACH ";
    ret += SQLObject_Name(at.sobject());
    ret += " ";
    SQLObjectNameToString(ret, at.object());
    if (at.has_cluster())
    {
        ClusterToString(ret, true, at.cluster());
    }
    if (at.sobject() != SQLObject::DATABASE && at.has_as_replicated())
    {
        ret += " AS";
        ret += at.as_replicated() ? "" : " NOT";
        ret += " REPLICATED";
    }
    if (at.has_setting_values())
    {
        ret += " SETTINGS ";
        SettingValuesToString(ret, at.setting_values());
    }
}

CONV_FN(Detach, dt)
{
    ret += "DETACH ";
    ret += SQLObject_Name(dt.sobject());
    ret += " ";
    SQLObjectNameToString(ret, dt.object());
    if (dt.has_cluster())
    {
        ClusterToString(ret, true, dt.cluster());
    }
    if (dt.permanently())
    {
        ret += " PERMANENTLY";
    }
    if (dt.sync())
    {
        ret += " SYNC";
    }
    if (dt.has_setting_values())
    {
        ret += " SETTINGS ";
        SettingValuesToString(ret, dt.setting_values());
    }
}

CONV_FN(SelectIntoFile, intofile)
{
    ret += "INTO OUTFILE '";
    ret += intofile.path();
    ret += "'";
    if (intofile.tstdout())
    {
        ret += " AND STDOUT";
    }
    if (intofile.has_step())
    {
        ret += " ";
        ret += SelectIntoFile_SelectIntoFileStep_Name(intofile.step());
    }
    if (intofile.has_compression())
    {
        ret += " COMPRESSION '";
        ret += FileCompression_Name(intofile.compression()).substr(4);
        ret += "'";
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
        ret += OutFormat_Name(top.format()).substr(4);
    }
}

static void SystemCommandOnCluster(String & ret, const String & desc, const SystemCommand & cmd, const ExprSchemaTable & est)
{
    ret += desc;
    if (cmd.has_cluster())
    {
        ClusterToString(ret, true, cmd.cluster());
    }
    ret += " ";
    ExprSchemaTableToString(ret, est);
}

CONV_FN(SystemCommand, cmd)
{
    bool can_set_cluster = false;

    ret += "SYSTEM ";
    using CmdType = SystemCommand::SystemCmdOneofCase;
    switch (cmd.system_cmd_oneof_case())
    {
        case CmdType::kReloadEmbeddedDictionaries:
            ret += "RELOAD EMBEDDED DICTIONARIES";
            can_set_cluster = true;
            break;
        case CmdType::kReloadDictionaries:
            ret += "RELOAD DICTIONARIES";
            can_set_cluster = true;
            break;
        case CmdType::kReloadModels:
            ret += "RELOAD MODELS";
            can_set_cluster = true;
            break;
        case CmdType::kReloadFunctions:
            ret += "RELOAD FUNCTIONS";
            can_set_cluster = true;
            break;
        case CmdType::kReloadFunction:
            ret += "RELOAD FUNCTION";
            if (cmd.has_cluster())
            {
                ClusterToString(ret, true, cmd.cluster());
            }
            ret += " ";
            FunctionToString(ret, cmd.reload_function());
            break;
        case CmdType::kReloadAsynchronousMetrics:
            ret += "RELOAD ASYNCHRONOUS METRICS";
            can_set_cluster = true;
            break;
        case CmdType::kDropDnsCache:
            ret += "DROP DNS CACHE";
            can_set_cluster = true;
            break;
        case CmdType::kDropMarkCache:
            ret += "DROP MARK CACHE";
            can_set_cluster = true;
            break;
        case CmdType::kDropUncompressedCache:
            ret += "DROP UNCOMPRESSED CACHE";
            can_set_cluster = true;
            break;
        case CmdType::kDropCompiledExpressionCache:
            ret += "DROP COMPILED EXPRESSION CACHE";
            can_set_cluster = true;
            break;
        case CmdType::kDropQueryCache:
            ret += "DROP QUERY CACHE";
            can_set_cluster = true;
            break;
        case CmdType::kDropFormatSchemaCache:
            ret += "DROP FORMAT SCHEMA CACHE";
            if (cmd.drop_format_schema_cache())
            {
                ret += " FOR Protobuf";
            }
            break;
        case CmdType::kFlushLogs:
            ret += "FLUSH LOGS";
            can_set_cluster = true;
            break;
        case CmdType::kReloadConfig:
            ret += "RELOAD CONFIG";
            can_set_cluster = true;
            break;
        case CmdType::kReloadUsers:
            ret += "RELOAD USERS";
            can_set_cluster = true;
            break;
        case CmdType::kStopMerges:
            SystemCommandOnCluster(ret, "STOP MERGES", cmd, cmd.stop_merges());
            break;
        case CmdType::kStartMerges:
            SystemCommandOnCluster(ret, "START MERGES", cmd, cmd.start_merges());
            break;
        case CmdType::kStopTtlMerges:
            SystemCommandOnCluster(ret, "STOP TTL MERGES", cmd, cmd.stop_ttl_merges());
            break;
        case CmdType::kStartTtlMerges:
            SystemCommandOnCluster(ret, "START TTL MERGES", cmd, cmd.start_ttl_merges());
            break;
        case CmdType::kStopMoves:
            SystemCommandOnCluster(ret, "STOP MOVES", cmd, cmd.stop_moves());
            break;
        case CmdType::kStartMoves:
            SystemCommandOnCluster(ret, "START MOVES", cmd, cmd.start_moves());
            break;
        case CmdType::kWaitLoadingParts:
            SystemCommandOnCluster(ret, "WAIT LOADING PARTS", cmd, cmd.wait_loading_parts());
            break;
        case CmdType::kStopFetches:
            SystemCommandOnCluster(ret, "STOP FETCHES", cmd, cmd.stop_fetches());
            break;
        case CmdType::kStartFetches:
            SystemCommandOnCluster(ret, "START FETCHES", cmd, cmd.start_fetches());
            break;
        case CmdType::kStopReplicatedSends:
            SystemCommandOnCluster(ret, "STOP REPLICATED SENDS", cmd, cmd.stop_replicated_sends());
            break;
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
        case CmdType::kSyncReplica:
            SystemCommandOnCluster(ret, "SYNC REPLICA", cmd, cmd.sync_replica().est());
            ret += " ";
            ret += SyncReplica_SyncPolicy_Name(cmd.sync_replica().policy());
            break;
        case CmdType::kSyncReplicatedDatabase:
            ret += "SYNC DATABASE REPLICA";
            if (cmd.has_cluster())
            {
                ClusterToString(ret, true, cmd.cluster());
            }
            ret += " ";
            DatabaseToString(ret, cmd.sync_replicated_database());
            break;
        case CmdType::kRestartReplica:
            SystemCommandOnCluster(ret, "RESTART REPLICA", cmd, cmd.restart_replica());
            break;
        case CmdType::kRestoreReplica:
            SystemCommandOnCluster(ret, "RESTORE REPLICA", cmd, cmd.restore_replica());
            break;
        case CmdType::kRestartReplicas:
            ret += "RESTART REPLICAS";
            can_set_cluster = true;
            break;
        case CmdType::kDropFilesystemCache:
            ret += "DROP FILESYSTEM CACHE";
            can_set_cluster = true;
            break;
        case CmdType::kSyncFileCache:
            ret += "SYNC FILE CACHE";
            can_set_cluster = true;
            break;
        case CmdType::kLoadPks:
            ret += "LOAD PRIMARY KEY";
            can_set_cluster = true;
            break;
        case CmdType::kLoadPk:
            SystemCommandOnCluster(ret, "LOAD PRIMARY KEY", cmd, cmd.load_pk());
            break;
        case CmdType::kUnloadPks:
            ret += "UNLOAD PRIMARY KEY";
            can_set_cluster = true;
            break;
        case CmdType::kUnloadPk:
            SystemCommandOnCluster(ret, "UNLOAD PRIMARY KEY", cmd, cmd.unload_pk());
            break;
        case CmdType::kRefreshViews:
            ret += "REFRESH VIEWS";
            break;
        case CmdType::kRefreshView:
            ret += "REFRESH VIEW ";
            ExprSchemaTableToString(ret, cmd.refresh_view());
            break;
        case CmdType::kStopViews:
            ret += "STOP VIEWS";
            break;
        case CmdType::kStopView:
            ret += "STOP VIEW ";
            ExprSchemaTableToString(ret, cmd.stop_view());
            break;
        case CmdType::kStartViews:
            ret += "START VIEWS";
            break;
        case CmdType::kStartView:
            ret += "START VIEW ";
            ExprSchemaTableToString(ret, cmd.start_view());
            break;
        case CmdType::kCancelView:
            ret += "CANCEL VIEW ";
            ExprSchemaTableToString(ret, cmd.cancel_view());
            break;
        case CmdType::kWaitView:
            ret += "WAIT VIEW ";
            ExprSchemaTableToString(ret, cmd.wait_view());
            break;
        case CmdType::kPrewarmCache:
            SystemCommandOnCluster(ret, "PREWARM MARK CACHE", cmd, cmd.prewarm_cache());
            break;
        case CmdType::kPrewarmPrimaryIndexCache:
            SystemCommandOnCluster(ret, "PREWARM PRIMARY INDEX CACHE", cmd, cmd.prewarm_primary_index_cache());
            break;
        case CmdType::kDropConnectionsCache:
            ret += "DROP CONNECTIONS CACHE";
            can_set_cluster = true;
            break;
        case CmdType::kDropPrimaryIndexCache:
            ret += "DROP PRIMARY INDEX CACHE";
            can_set_cluster = true;
            break;
        case CmdType::kDropIndexMarkCache:
            ret += "DROP INDEX MARK CACHE";
            can_set_cluster = true;
            break;
        case CmdType::kDropIndexUncompressedCache:
            ret += "DROP INDEX UNCOMPRESSED CACHE";
            can_set_cluster = true;
            break;
        case CmdType::kDropMmapCache:
            ret += "DROP MMAP CACHE";
            can_set_cluster = true;
            break;
        case CmdType::kDropPageCache:
            ret += "DROP PAGE CACHE";
            can_set_cluster = true;
            break;
        case CmdType::kDropSchemaCache:
            ret += "DROP SCHEMA CACHE";
            can_set_cluster = true;
            break;
        case CmdType::kDropS3ClientCache:
            ret += "DROP S3 CLIENT CACHE";
            can_set_cluster = true;
            break;
        case CmdType::kFlushAsyncInsertQueue:
            ret += "FLUSH ASYNC INSERT QUEUE";
            can_set_cluster = true;
            break;
        case CmdType::kSyncFilesystemCache:
            ret += "SYNC FILESYSTEM CACHE";
            can_set_cluster = true;
            break;
        case CmdType::kDropVectorSimilarityIndexCache:
            ret += "DROP VECTOR SIMILARITY INDEX CACHE";
            can_set_cluster = true;
            break;
        case CmdType::kReloadDictionary:
            SystemCommandOnCluster(ret, "RELOAD DICTIONARY", cmd, cmd.reload_dictionary());
            break;
        case CmdType::kFlushDistributed:
            SystemCommandOnCluster(ret, "FLUSH DISTRIBUTED", cmd, cmd.flush_distributed());
            break;
        case CmdType::kStopDistributedSends:
            SystemCommandOnCluster(ret, "STOP DISTRIBUTED SENDS", cmd, cmd.stop_distributed_sends());
            break;
        case CmdType::kStartDistributedSends:
            SystemCommandOnCluster(ret, "START DISTRIBUTED SENDS", cmd, cmd.start_distributed_sends());
            break;
        case CmdType::kDropQueryConditionCache:
            ret += "DROP QUERY CONDITION CACHE";
            can_set_cluster = true;
            break;
        default:
            ret += "FLUSH LOGS";
    }
    if (can_set_cluster && cmd.has_cluster())
    {
        ClusterToString(ret, true, cmd.cluster());
    }
}

CONV_FN(BackupRestoreObject, bobject)
{
    const bool is_table = bobject.sobject() == SQLObject::TABLE;

    if (is_table && bobject.is_temp())
    {
        ret += "TEMPORARY ";
    }
    ret += SQLObject_Name(bobject.sobject());
    ret += " ";
    SQLObjectNameToString(ret, bobject.object());
    if (bobject.has_alias())
    {
        ret += " AS ";
        SQLObjectNameToString(ret, bobject.alias());
    }
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
        case BackupType::kBobject:
            BackupRestoreObjectToString(ret, backup.bobject());
            break;
        default:
            ret += "ALL";
            break;
    }
    if (backup.except_tables_size() && (!backup.has_bobject() || backup.bobject().sobject() == SQLObject::DATABASE))
    {
        ret += " EXCEPT";
        ret += backup.has_bobject() ? " TABLES" : "";
        ret += " ";
        for (int i = 0; i < backup.except_tables_size(); i++)
        {
            if (i != 0)
            {
                ret += ", ";
            }
            ExprSchemaTableToString(ret, backup.except_tables(i));
        }
    }
}

CONV_FN(BackupRestore, backup)
{
    const BackupRestore_BackupCommand & command = backup.command();
    const BackupRestore_BackupOutput & output = backup.out();

    ret += BackupRestore_BackupCommand_Name(command);
    ret += " ";
    BackupRestoreElementToString(ret, backup.backup_element());
    for (int i = 0; i < backup.other_elements_size(); i++)
    {
        ret += ", ";
        BackupRestoreElementToString(ret, backup.other_elements(i));
    }
    if (backup.has_cluster())
    {
        ClusterToString(ret, true, backup.cluster());
    }
    ret += " ";
    ret += command == BackupRestore_BackupCommand_BACKUP ? "TO" : "FROM";
    ret += " ";
    ret += BackupRestore_BackupOutput_Name(output);
    if (output != BackupRestore_BackupOutput_Null)
    {
        ret += "(";
        for (int i = 0; i < backup.out_params_size(); i++)
        {
            if (i != 0)
            {
                ret += ", ";
            }
            ret += "'";
            ret += backup.out_params(i);
            ret += "'";
        }
        ret += ")";
    }
    if (backup.has_setting_values())
    {
        ret += " SETTINGS ";
        SettingValuesToString(ret, backup.setting_values());
    }
    if (backup.has_sync())
    {
        ret += " ";
        ret += BackupRestore_SyncOrAsync_Name(backup.sync());
    }
    if (backup.has_informat())
    {
        ret += " FORMAT ";
        ret += InFormat_Name(backup.informat()).substr(3);
    }
    else if (backup.has_outformat())
    {
        ret += " FORMAT ";
        ret += OutFormat_Name(backup.outformat()).substr(4);
    }
}

CONV_FN(Rename, ren)
{
    ret += "RENAME ";
    ret += SQLObject_Name(ren.sobject());
    ret += " ";
    SQLObjectNameToString(ret, ren.old_object());
    ret += " TO ";
    SQLObjectNameToString(ret, ren.new_object());
    if (ren.has_cluster())
    {
        ClusterToString(ret, true, ren.cluster());
    }
    if (ren.has_setting_values())
    {
        ret += " SETTINGS ";
        SettingValuesToString(ret, ren.setting_values());
    }
}

CONV_FN(Kill, kil)
{
    ret += "KILL ";
    ret += Kill_KillEnum_Name(kil.command());
    if (kil.has_cluster())
    {
        ClusterToString(ret, true, kil.cluster());
    }
    ret += " WHERE ";
    WhereStatementToString(ret, kil.where());
    if (kil.has_option())
    {
        ret += " ";
        ret += Kill_KillOption_Name(kil.option());
    }
    if (kil.has_informat())
    {
        ret += " FORMAT ";
        ret += InFormat_Name(kil.informat()).substr(3);
    }
    else if (kil.has_outformat())
    {
        ret += " FORMAT ";
        ret += OutFormat_Name(kil.outformat()).substr(4);
    }
    if (kil.has_setting_values())
    {
        ret += " SETTINGS ";
        SettingValuesToString(ret, kil.setting_values());
    }
}

CONV_FN(SQLQueryInner, query)
{
    using QueryType = SQLQueryInner::QueryInnerOneofCase;
    switch (query.query_inner_oneof_case())
    {
        case QueryType::kSelect:
            TopSelectToString(ret, query.select());
            break;
        case QueryType::kCreateTable:
            CreateTableToString(ret, query.create_table());
            break;
        case QueryType::kDrop:
            DropToString(ret, query.drop());
            break;
        case QueryType::kInsert:
            InsertToString(ret, query.insert());
            break;
        case QueryType::kDel:
            LightDeleteToString(ret, query.del());
            break;
        case QueryType::kUpt:
            LightUpdateToString(ret, query.upt());
            break;
        case QueryType::kTrunc:
            TruncateToString(ret, query.trunc());
            break;
        case QueryType::kOpt:
            OptimizeTableToString(ret, query.opt());
            break;
        case QueryType::kCheck:
            CheckTableToString(ret, query.check());
            break;
        case QueryType::kDesc:
            DescTableToString(ret, query.desc());
            break;
        case QueryType::kExchange:
            ExchangeToString(ret, query.exchange());
            break;
        case QueryType::kAlter:
            AlterToString(ret, query.alter());
            break;
        case QueryType::kSettingValues:
            ret += "SET ";
            SettingValuesToString(ret, query.setting_values());
            break;
        case QueryType::kCreateView:
            CreateViewToString(ret, query.create_view());
            break;
        case QueryType::kAttach:
            AttachToString(ret, query.attach());
            break;
        case QueryType::kDetach:
            DetachToString(ret, query.detach());
            break;
        case QueryType::kCreateDatabase:
            CreateDatabaseToString(ret, query.create_database());
            break;
        case QueryType::kCreateFunction:
            CreateFunctionToString(ret, query.create_function());
            break;
        case QueryType::kSystemCmd:
            SystemCommandToString(ret, query.system_cmd());
            break;
        case QueryType::kBackupRestore:
            BackupRestoreToString(ret, query.backup_restore());
            break;
        case QueryType::kCreateDictionary:
            CreateDictionaryToString(ret, query.create_dictionary());
            break;
        case QueryType::kRename:
            RenameToString(ret, query.rename());
            break;
        case QueryType::kKill:
            KillToString(ret, query.kill());
            break;
        default:
            ret += "SELECT 1";
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
    SQLQueryInnerToString(ret, explain.inner_query());
}

CONV_FN(SingleSQLQuery, query)
{
    using QueryType = SingleSQLQuery::QueryOneofCase;
    switch (query.query_oneof_case())
    {
        case QueryType::kExplain:
            ExplainQueryToString(ret, query.explain());
            break;
        case QueryType::kStartTrans:
            ret += "BEGIN TRANSACTION";
            break;
        case QueryType::kCommitTrans:
            ret += "COMMIT";
            break;
        case QueryType::kRollbackTrans:
            ret += "ROLLBACK";
            break;
        default:
            ret += "SELECT 1";
    }
}

CONV_FN(SQLQuery, query)
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
