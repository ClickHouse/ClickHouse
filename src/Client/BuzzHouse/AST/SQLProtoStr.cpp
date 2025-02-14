#include <algorithm>
#include <cctype>
#include <cstdint>
#include <limits>

#include <Client/BuzzHouse/AST/SQLProtoStr.h>
#include <Client/BuzzHouse/Utils/HugeInt.h>
#include <Client/BuzzHouse/Utils/UHugeInt.h>

#define CONV_FN(TYPE, VAR_NAME) void TYPE##ToString(std::string & ret, const TYPE &(VAR_NAME))
#define CONV_FN_QUOTE(TYPE, VAR_NAME) void TYPE##ToString(std::string & ret, const uint32_t quote, const TYPE &(VAR_NAME))

namespace BuzzHouse
{

static constexpr char hex_digits[] = "0123456789ABCDEF";
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

CONV_FN(Window, win)
{
    ret += "w";
    ret += std::to_string(win.window());
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
    ExprToString(ret, eca.expr());
    if (eca.has_col_alias())
    {
        ret += " AS `";
        ColumnToString(ret, 1, eca.col_alias());
        ret += "`";
    }
}

void convertToSQLString(std::string & ret, const std::string & s)
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
                    ret += hex_digits[(x & 0xF0) >> 4];
                    ret += hex_digits[x & 0x0F];
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
    HugeInt val(huge.upper(), huge.lower());
    val.toString(ret);
}

CONV_FN(UHugeIntLiteral, uhuge)
{
    UHugeInt val(uhuge.upper(), uhuge.lower());
    val.toString(ret);
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
            const std::string & s = lit_val.hex_string();
            ret += "x'";
            for (size_t i = 0; i < s.length(); i++)
            {
                const uint8_t & x = static_cast<uint8_t>(s[i]);
                ret += hex_digits[(x & 0xF0) >> 4];
                ret += hex_digits[x & 0x0F];
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
            convertToSQLString(ret, lit_val.string_lit());
            ret += '\'';
            break;
        case LitValType::kSpecialVal: {
            switch (lit_val.special_val())
            {
                case SpecialVal::VAL_NULL:
                    ret += "NULL";
                    break;
                case SpecialVal::VAL_TRUE:
                    ret += "TRUE";
                    break;
                case SpecialVal::VAL_FALSE:
                    ret += "FALSE";
                    break;
                case SpecialVal::VAL_ZERO:
                    ret += "0";
                    break;
                case SpecialVal::VAL_ONE:
                    ret += "1";
                    break;
                case SpecialVal::VAL_MINUS_ONE:
                    ret += "(-1)";
                    break;
                case SpecialVal::VAL_EMPTY_STRING:
                    ret += "''";
                    break;
                case SpecialVal::VAL_EMPTY_ARRAY:
                    ret += "[]";
                    break;
                case SpecialVal::VAL_EMPTY_TUPLE:
                    ret += "()";
                    break;
                case SpecialVal::VAL_EMPTY_MAP:
                    ret += "map()";
                    break;
                case SpecialVal::VAL_EMPTY_JSON:
                    ret += "'{}'::JSON";
                    break;
                case SpecialVal::VAL_MINUS_ZERO_FP:
                    ret += "(-0.0)";
                    break;
                case SpecialVal::VAL_PLUS_ZERO_FP:
                    ret += "+0.0";
                    break;
                case SpecialVal::VAL_ZERO_FP:
                    ret += "0.0";
                    break;
                case SpecialVal::VAL_INF:
                    ret += "inf";
                    break;
                case SpecialVal::VAL_PLUS_INF:
                    ret += "+inf";
                    break;
                case SpecialVal::VAL_MINUS_INF:
                    ret += "(-inf)";
                    break;
                case SpecialVal::VAL_NAN:
                    ret += "nan";
                    break;
                case SpecialVal::VAL_PLUS_NAN:
                    ret += "+nan";
                    break;
                case SpecialVal::VAL_MINUS_NAN:
                    ret += "(-nan)";
                    break;
                case SpecialVal::VAL_HAPPY:
                    ret += "'😂'";
                    break;
                case SpecialVal::VAL_TEN_HAPPY:
                    ret += "'😂😂😂😂😂😂😂😂😂😂'";
                    break;
                case SpecialVal::MIN_INT32:
                    ret += "(";
                    ret += std::to_string(std::numeric_limits<int32_t>::min());
                    ret += ")";
                    break;
                case SpecialVal::MAX_INT32:
                    ret += std::to_string(std::numeric_limits<int32_t>::max());
                    break;
                case SpecialVal::MIN_INT64:
                    ret += "(";
                    ret += std::to_string(std::numeric_limits<int64_t>::min());
                    ret += ")";
                    break;
                case SpecialVal::MAX_INT64:
                    ret += std::to_string(std::numeric_limits<int64_t>::max());
                    break;
                case SpecialVal::MIN_INT128:
                    ret += "(-170141183460469231731687303715884105728)";
                    break;
                case SpecialVal::MAX_INT128:
                    ret += "170141183460469231731687303715884105727";
                    break;
                case SpecialVal::MIN_INT256:
                    ret += "(-57896044618658097711785492504343953926634992332820282019728792003956564819968)";
                    break;
                case SpecialVal::MAX_INT256:
                    ret += "57896044618658097711785492504343953926634992332820282019728792003956564819967";
                    break;
                case SpecialVal::MAX_UINT32:
                    ret += std::to_string(std::numeric_limits<uint32_t>::max());
                    break;
                case SpecialVal::MAX_UINT64:
                    ret += std::to_string(std::numeric_limits<uint64_t>::max());
                    break;
                case SpecialVal::MAX_UINT128:
                    ret += "340282366920938463463374607431768211455";
                    break;
                case SpecialVal::MAX_UINT256:
                    ret += "115792089237316195423570985008687907853269984665640564039457584007913129639935";
                    break;
                case SpecialVal::MIN_DATE:
                    ret += "'1970-01-01'::Date";
                    break;
                case SpecialVal::MAX_DATE:
                    ret += "'2149-06-06'::Date";
                    break;
                case SpecialVal::MIN_DATE32:
                    ret += "'1900-01-01'::Date32";
                    break;
                case SpecialVal::MAX_DATE32:
                    ret += "'2299-12-31'::Date32";
                    break;
                case SpecialVal::MIN_DATETIME:
                    ret += "'1970-01-01 00:00:00'::DateTime";
                    break;
                case SpecialVal::MAX_DATETIME:
                    ret += "'2106-02-07 06:28:15'::DateTime";
                    break;
                case SpecialVal::MIN_DATETIME64:
                    ret += "'1900-01-01 00:00:00'::DateTime64";
                    break;
                case SpecialVal::MAX_DATETIME64:
                    ret += "'2299-12-31 23:59:59.99999999'::DateTime64";
                    break;
                case SpecialVal::VAL_NULL_CHAR:
                    ret += "'\\0'";
                    break;
                case SpecialVal::VAL_DEFAULT:
                    ret += "DEFAULT";
                    break;
                case SpecialVal::VAL_STAR:
                    ret += "*";
                    break;
            }
        }
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
    }
}

CONV_FN(BinaryExpr, bexpr)
{
    ExprToString(ret, bexpr.lhs());
    BinaryOperatorToString(ret, bexpr.op());
    ExprToString(ret, bexpr.rhs());
}

CONV_FN(ParenthesesExpr, pexpr)
{
    ret += "(";
    ExprColAliasToString(ret, pexpr.expr());
    for (int i = 0; i < pexpr.other_exprs_size(); i++)
    {
        ret += ", ";
        ExprColAliasToString(ret, pexpr.other_exprs(i));
    }
    ret += ")";
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

void BottomTypeNameToString(std::string & ret, const uint32_t quote, const bool lcard, const BottomTypeName & btn)
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
        default: {
            if (lcard)
            {
                ret += "Int";
            }
            else
            {
                switch (btn.bottom_one_of_case())
                {
                    case BottomTypeNameType::kDecimal: {
                        const Decimal & dec = btn.decimal();
                        ret += "Decimal";
                        if (dec.has_precision())
                        {
                            const uint32_t precision = std::max<uint32_t>(1, dec.precision() % 77);

                            ret += "(";
                            ret += std::to_string(precision);
                            if (dec.has_scale())
                            {
                                ret += ",";
                                ret += std::to_string(dec.scale() % (precision + 1));
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
                    case BottomTypeNameType::kIPv4:
                        ret += "IPv4";
                        break;
                    case BottomTypeNameType::kIPv6:
                        ret += "IPv6";
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
    if (elike.not_())
        ret += "NOT ";
    ret += "LIKE ";
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
    ret += "IN (";
    if (ein.has_exprs())
    {
        ExprListToString(ret, ein.exprs());
    }
    else if (ein.has_sel())
    {
        SelectToString(ret, ein.sel());
    }
    else
    {
        ret += "1";
    }
    ret += ")";
}

CONV_FN(ExprAny, eany)
{
    ExprToString(ret, eany.expr());
    BinaryOperatorToString(ret, static_cast<BinaryOperator>(((static_cast<int>(eany.op()) % 8) + 1)));
    ret += eany.anyall() ? "ALL" : "ANY";
    ret += "(";
    SelectToString(ret, eany.sel());
    ret += ")";
}

CONV_FN(ExprExists, exists)
{
    if (exists.not_())
        ret += "NOT ";
    ret += "EXISTS (";
    SelectToString(ret, exists.select());
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
    if (sfc.args_size() == 1 && sfc.distinct())
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
    std::string next = FrameSpecSubLeftExpr_Which_Name(fssle.which());

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
    std::string next = FrameSpecSubRightExpr_Which_Name(fsslr.which());

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
    else if (wc.has_win_name())
    {
        WindowToString(ret, wc.win_name());
    }
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
        case ExprType::kParExpr:
            ParenthesesExprToString(ret, expr.par_expr());
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
            SelectToString(ret, expr.subquery());
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
        case ExprType::kArray: {
            const ArraySequence & vals = expr.array();

            ret += "[";
            for (int i = 0; i < vals.values_size(); i++)
            {
                if (i != 0)
                {
                    ret += ",";
                }
                ExprToString(ret, vals.values(i));
            }
            ret += "]";
        }
        break;
        case ExprType::kTuple: {
            const TupleSequence & vals = expr.tuple();

            ret += "(";
            for (int i = 0; i < vals.values_size(); i++)
            {
                if (i != 0)
                {
                    ret += ",";
                }
                ret += "(";
                ExprListToString(ret, vals.values(i));
                ret += ")";
            }
            ret += ")";
        }
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

// ~~~~SELECT~~~~

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
    else if (rc.has_table_star())
    {
        TableToString(ret, rc.table_star());
        ret += ".*";
    }
    else
    {
        ret += "*";
    }
}

CONV_FN(JoinedQuery, tos);
CONV_FN(TableOrSubquery, tos);

CONV_FN(ExprColumnList, cl)
{
    ExprColumnToString(ret, cl.col());
    for (int i = 0; i < cl.extra_cols_size(); i++)
    {
        ret += ", ";
        ExprColumnToString(ret, cl.extra_cols(i));
    }
}

CONV_FN(JoinConstraint, jc)
{
    if (jc.has_on_expr())
    {
        ret += " ON ";
        ExprToString(ret, jc.on_expr());
    }
    else if (jc.has_using_expr())
    {
        ret += " USING (";
        ExprColumnListToString(ret, jc.using_expr().col_list());
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

CONV_FN(JoinedDerivedQuery, tos)
{
    ret += "(";
    SelectToString(ret, tos.select());
    ret += ")";
    if (tos.has_table_alias())
    {
        ret += " ";
        TableToString(ret, tos.table_alias());
    }
}

CONV_FN(JoinedTable, jt)
{
    ExprSchemaTableToString(ret, jt.est());
    if (jt.has_table_alias())
    {
        ret += " ";
        TableToString(ret, jt.table_alias());
    }
    if (jt.final())
    {
        ret += " FINAL";
    }
}

CONV_FN(FileFunc, ff)
{
    ret += "file('";
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
    ret += "', '";
    ret += ff.structure();
    ret += "'";
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
        ret += "', '";
        ret += ff.structure();
        ret += "'";
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

CONV_FN(RemoteFunc, rfunc)
{
    ret += "remote('";
    ret += rfunc.address();
    ret += "'";
    if (rfunc.has_rdatabase())
    {
        ret += ", '";
        ret += rfunc.rdatabase();
        ret += "'";
    }
    if (rfunc.has_rtable())
    {
        ret += ", '";
        ret += rfunc.rtable();
        ret += "'";
    }
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
    ret += "s3('";
    ret += sfunc.resource();
    ret += "', '";
    ret += sfunc.user();
    ret += "', '";
    ret += sfunc.password();
    ret += "', '";
    ret += InOutFormat_Name(sfunc.format()).substr(6);
    ret += "', '";
    ret += sfunc.structure();
    ret += "'";
    if (sfunc.has_fcomp())
    {
        ret += ", '";
        ret += sfunc.fcomp();
        ret += "'";
    }
    ret += ")";
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
        default:
            ret += "numbers(10)";
    }
}

CONV_FN(JoinedTableFunction, jtf)
{
    TableFunctionToString(ret, jtf.tfunc());
    if (jtf.has_table_alias())
    {
        ret += " ";
        TableToString(ret, jtf.table_alias());
    }
}

CONV_FN(TableOrSubquery, tos)
{
    using JoinedType = TableOrSubquery::TosOneofCase;
    switch (tos.tos_oneof_case())
    {
        case JoinedType::kJoinedTable:
            JoinedTableToString(ret, tos.joined_table());
            break;
        case JoinedType::kJoinedDerivedQuery:
            JoinedDerivedQueryToString(ret, tos.joined_derived_query());
            break;
        case JoinedType::kJoinedTableFunction:
            JoinedTableFunctionToString(ret, tos.joined_table_function());
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
        ExprToString(ret, gbs.having_expr());
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
    if (ssc.has_from())
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
    SelectToString(ret, setq.sel1());
    ret += ") ";
    ret += SetQuery_SetOp_Name(setq.set_op());
    ret += " ";
    ret += AllOrDistinct_Name(setq.s_or_d());
    ret += " (";
    SelectToString(ret, setq.sel2());
    ret += ")";
}

CONV_FN(CTEquery, cteq)
{
    TableToString(ret, cteq.table());
    ret += " AS (";
    SelectToString(ret, cteq.query());
    ret += ")";
}

CONV_FN(CTEs, cteq)
{
    ret += "WITH ";
    CTEqueryToString(ret, cteq.cte());
    for (int i = 0; i < cteq.other_ctes_size(); i++)
    {
        ret += ", ";
        CTEqueryToString(ret, cteq.other_ctes(i));
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

CONV_FN(DatabaseEngine, deng)
{
    const DatabaseEngineValues dengine = deng.engine();

    ret += DatabaseEngineValues_Name(dengine).substr(1);
    if (dengine == DatabaseEngineValues::DReplicated)
    {
        ret += "('/test/db";
        ret += std::to_string(deng.zoo_path());
        ret += "', 's1', 'r1')";
    }
}

CONV_FN(CreateDatabase, create_database)
{
    ret += "CREATE DATABASE ";
    DatabaseToString(ret, create_database.database());
    ret += " ENGINE = ";
    DatabaseEngineToString(ret, create_database.dengine());
    if (create_database.has_comment())
    {
        ret += " COMMENT ";
        ret += create_database.comment();
    }
}

CONV_FN(CreateFunction, create_function)
{
    ret += "CREATE FUNCTION ";
    FunctionToString(ret, create_function.function());
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
    if (cdf.has_settings())
    {
        ret += " SETTINGS(";
        SettingValuesToString(ret, cdf.settings());
        ret += ")";
    }
}

CONV_FN(IndexParam, ip)
{
    if (ip.has_ival())
    {
        ret += std::to_string(ip.ival());
    }
    else if (ip.has_dval())
    {
        ret += std::to_string(ip.dval());
    }
    else
    {
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
            tep.regexp();
            ret += "')";
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
    const TableEngineValues tengine = te.engine();

    ret += " ENGINE = ";
    if (te.has_toption() && tengine >= TableEngineValues::MergeTree && tengine <= TableEngineValues::VersionedCollapsingMergeTree)
    {
        ret += TableEngineOption_Name(te.toption()).substr(1);
    }
    ret += TableEngineValues_Name(tengine);
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
    if (te.has_settings())
    {
        ret += " SETTINGS ";
        SettingValuesToString(ret, te.settings());
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

CONV_FN(CreateTable, create_table)
{
    ret += create_table.replace() ? "REPLACE" : "CREATE";
    ret += " ";
    if (!create_table.replace() && create_table.is_temp())
    {
        ret += "TEMPORARY ";
    }
    ret += "TABLE ";
    if (create_table.if_not_exists())
    {
        ret += "IF NOT EXISTS ";
    }
    ExprSchemaTableToString(ret, create_table.est());
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
        ret += " AS (";
        SelectToString(ret, create_table.as_select_stmt());
        ret += ")";
    }
    if (create_table.has_comment())
    {
        ret += " COMMENT ";
        ret += create_table.comment();
    }
}

CONV_FN(SQLObjectName, dt)
{
    if (dt.has_est())
    {
        ExprSchemaTableToString(ret, dt.est());
    }
    else if (dt.has_database())
    {
        DatabaseToString(ret, dt.database());
    }
    else if (dt.has_function())
    {
        FunctionToString(ret, dt.function());
    }
    else
    {
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

CONV_FN(ValuesStatement, values)
{
    if (values.has_setting_values())
    {
        ret += "SETTINGS ";
        SettingValuesToString(ret, values.setting_values());
        ret += " ";
    }
    ret += "VALUES (";
    ExprListToString(ret, values.expr_list());
    ret += ")";
    for (int i = 0; i < values.extra_expr_lists_size(); i++)
    {
        ret += ", (";
        ExprListToString(ret, values.extra_expr_lists(i));
        ret += ")";
    }
}

CONV_FN(InsertFromFile, insert_file)
{
    ret += "FROM INFILE '";
    ret += insert_file.path();
    ret += "'";
    if (insert_file.has_fcomp())
    {
        ret += " COMPRESSION '";
        ret += FileCompression_Name(insert_file.fcomp()).substr(4);
        ret += "'";
    }
    if (insert_file.has_settings())
    {
        ret += " SETTINGS ";
        SettingValuesToString(ret, insert_file.settings());
    }
    ret += " FORMAT ";
    ret += InFormat_Name(insert_file.format()).substr(3);
}

CONV_FN(InsertSelect, insert_sel)
{
    if (insert_sel.has_setting_values())
    {
        ret += "SETTINGS ";
        SettingValuesToString(ret, insert_sel.setting_values());
        ret += " ";
    }
    SelectToString(ret, insert_sel.select());
}

CONV_FN(InsertStringQuery, insert_query)
{
    if (insert_query.has_setting_values())
    {
        ret += "SETTINGS ";
        SettingValuesToString(ret, insert_query.setting_values());
        ret += " ";
    }
    ret += "VALUES ";
    ret += insert_query.query();
}

CONV_FN(Insert, insert)
{
    if (insert.has_ctes() && insert.has_est() && insert.has_insert_select())
    {
        CTEsToString(ret, insert.ctes());
    }
    ret += "INSERT INTO TABLE ";
    if (insert.has_est())
    {
        ExprSchemaTableToString(ret, insert.est());
    }
    else if (insert.has_tfunction())
    {
        ret += "FUNCTION ";
        TableFunctionToString(ret, insert.tfunction());
    }
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
    if (insert.has_values())
    {
        ValuesStatementToString(ret, insert.values());
    }
    else if (insert.has_insert_select())
    {
        InsertSelectToString(ret, insert.insert_select());
    }
    else if (insert.has_insert_file())
    {
        InsertFromFileToString(ret, insert.insert_file());
    }
    else if (insert.has_query())
    {
        InsertStringQueryToString(ret, insert.query());
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
            ret += "PART '";
            ret += pexpr.part();
            ret += "'";
            break;
        case PartitionType::kPartition:
            ret += "PARTITION $piddef$";
            ret += pexpr.partition();
            ret += "$piddef$";
            break;
        case PartitionType::kPartitionId:
            ret += "PARTITION ID '";
            ret += pexpr.partition_id();
            ret += "'";
            break;
        case PartitionType::kAll:
            ret += "PARTITION ALL";
            break;
        default:
            ret += "PARTITION tuple()";
    }
}

CONV_FN(LightDelete, del)
{
    ret += "DELETE FROM ";
    ExprSchemaTableToString(ret, del.est());
    if (del.has_partition())
    {
        ret += " IN ";
        PartitionExprToString(ret, del.partition());
    }
    ret += " WHERE ";
    WhereStatementToString(ret, del.where());
    if (del.has_setting_values())
    {
        ret += " SETTINGS ";
        SettingValuesToString(ret, del.setting_values());
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
    if (ct.has_partition())
    {
        ret += " ";
        PartitionExprToString(ret, ct.partition());
    }
    if (ct.has_setting_values())
    {
        ret += " SETTINGS ";
        SettingValuesToString(ret, ct.setting_values());
    }
}

CONV_FN(DescTable, dt)
{
    ret += "DESCRIBE TABLE ";
    ExprSchemaTableToString(ret, dt.est());
    if (dt.has_setting_values())
    {
        ret += " SETTINGS ";
        SettingValuesToString(ret, dt.setting_values());
    }
}

CONV_FN(DeduplicateExpr, de)
{
    ret += " DEDUPLICATE";
    if (de.has_col_list())
    {
        ret += " BY ";
        ExprColumnListToString(ret, de.col_list());
    }
    else if (de.has_ded_star())
    {
        ret += " BY *";
    }
    else if (de.has_ded_star_except())
    {
        ret += " BY * EXCEPT (";
        ExprColumnListToString(ret, de.ded_star_except());
        ret += ")";
    }
}

CONV_FN(OptimizeTable, ot)
{
    ret += "OPTIMIZE TABLE ";
    ExprSchemaTableToString(ret, ot.est());
    if (ot.has_partition())
    {
        ret += " ";
        PartitionExprToString(ret, ot.partition());
    }
    if (ot.final())
    {
        ret += " FINAL";
    }
    if (ot.has_dedup())
    {
        DeduplicateExprToString(ret, ot.dedup());
    }
    if (ot.cleanup())
    {
        ret += " CLEANUP";
    }
    if (ot.has_setting_values())
    {
        ret += " SETTINGS ";
        SettingValuesToString(ret, ot.setting_values());
    }
}

CONV_FN(ExchangeTables, et)
{
    ret += "EXCHANGE TABLES ";
    ExprSchemaTableToString(ret, et.est1());
    ret += " AND ";
    ExprSchemaTableToString(ret, et.est2());
    if (et.has_setting_values())
    {
        ret += " SETTINGS ";
        SettingValuesToString(ret, et.setting_values());
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
    if (upt.has_partition())
    {
        ret += " IN ";
        PartitionExprToString(ret, upt.partition());
    }
    ret += " WHERE ";
    WhereStatementToString(ret, upt.where());
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
    const bool materialized = create_view.materialized();
    const bool refreshable = create_view.has_refresh();

    ret += create_view.replace() ? "REPLACE" : "CREATE";
    ret += " ";
    if (materialized)
    {
        ret += "MATERIALIZED ";
    }
    ret += "VIEW ";
    ExprSchemaTableToString(ret, create_view.est());
    if (materialized)
    {
        if (refreshable)
        {
            ret += " ";
            RefreshableViewToString(ret, create_view.refresh());
        }
        if (create_view.has_to_est())
        {
            ret += " TO ";
            ExprSchemaTableToString(ret, create_view.to_est());
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
    ret += " AS (";
    SelectToString(ret, create_view.select());
    ret += ")";
    if (create_view.has_comment())
    {
        ret += " COMMENT ";
        ret += create_view.comment();
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
    SettingValuesToString(ret, mcp.settings());
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
    SettingListToString(ret, rcp.settings());
}

CONV_FN(HeavyDelete, hdel)
{
    if (hdel.has_partition())
    {
        ret += "IN ";
        PartitionExprToString(ret, hdel.partition());
        ret += " ";
    }
    ret += "WHERE ";
    WhereStatementToString(ret, hdel.del());
}

CONV_FN(AlterTableItem, alter)
{
    ret += "(";
    using AlterType = AlterTableItem::AlterOneofCase;
    switch (alter.alter_oneof_case())
    {
        case AlterType::kDel:
            ret += "DELETE ";
            HeavyDeleteToString(ret, alter.del());
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
            if (alter.materialize_column().has_partition())
            {
                ret += " IN ";
                PartitionExprToString(ret, alter.materialize_column().partition());
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
            if (alter.clear_column().has_partition())
            {
                ret += " IN ";
                PartitionExprToString(ret, alter.clear_column().partition());
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
            if (alter.delete_mask().has_partition())
            {
                ret += " IN ";
                PartitionExprToString(ret, alter.delete_mask().partition());
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
            if (alter.materialize_index().has_partition())
            {
                ret += " IN ";
                PartitionExprToString(ret, alter.materialize_index().partition());
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
            if (alter.clear_index().has_partition())
            {
                ret += " IN ";
                PartitionExprToString(ret, alter.clear_index().partition());
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
            if (alter.materialize_projection().has_partition())
            {
                ret += " IN ";
                PartitionExprToString(ret, alter.materialize_projection().partition());
            }
            break;
        case AlterType::kClearProjection:
            ret += "CLEAR PROJECTION ";
            ProjectionToString(ret, alter.clear_projection().proj());
            if (alter.clear_projection().has_partition())
            {
                ret += " IN ";
                PartitionExprToString(ret, alter.clear_projection().partition());
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
            PartitionExprToString(ret, alter.detach_partition());
            break;
        case AlterType::kDropPartition:
            ret += "DROP ";
            PartitionExprToString(ret, alter.drop_partition());
            break;
        case AlterType::kDropDetachedPartition:
            ret += "DROP DETACHED ";
            PartitionExprToString(ret, alter.drop_detached_partition());
            break;
        case AlterType::kForgetPartition:
            ret += "FORGET ";
            PartitionExprToString(ret, alter.forget_partition());
            break;
        case AlterType::kAttachPartition:
            ret += "ATTACH ";
            PartitionExprToString(ret, alter.attach_partition());
            break;
        case AlterType::kAttachPartitionFrom:
            ret += "ATTACH ";
            PartitionExprToString(ret, alter.attach_partition_from().partition());
            ret += " FROM ";
            ExprSchemaTableToString(ret, alter.attach_partition_from().est());
            break;
        case AlterType::kReplacePartitionFrom:
            ret += "REPLACE ";
            PartitionExprToString(ret, alter.replace_partition_from().partition());
            ret += " FROM ";
            ExprSchemaTableToString(ret, alter.replace_partition_from().est());
            break;
        case AlterType::kMovePartitionTo:
            ret += "MOVE ";
            PartitionExprToString(ret, alter.move_partition_to().partition());
            ret += " FROM ";
            ExprSchemaTableToString(ret, alter.move_partition_to().est());
            break;
        case AlterType::kClearColumnPartition:
            ret += "CLEAR COLUMN ";
            ColumnPathToString(ret, 0, alter.clear_column_partition().col());
            ret += " IN ";
            PartitionExprToString(ret, alter.clear_column_partition().partition());
            break;
        case AlterType::kFreezePartition:
            ret += "FREEZE";
            if (alter.freeze_partition().has_partition())
            {
                ret += " ";
                PartitionExprToString(ret, alter.freeze_partition().partition());
            }
            ret += " WITH NAME 'f";
            ret += std::to_string(alter.freeze_partition().fname());
            ret += "'";
            break;
        case AlterType::kUnfreezePartition:
            ret += "UNFREEZE";
            if (alter.unfreeze_partition().has_partition())
            {
                ret += " ";
                PartitionExprToString(ret, alter.unfreeze_partition().partition());
            }
            ret += " WITH NAME 'f";
            ret += std::to_string(alter.unfreeze_partition().fname());
            ret += "'";
            break;
        case AlterType::kMovePartition:
            ret += "MOVE ";
            PartitionExprToString(ret, alter.move_partition().partition());
            ret += " TO ";
            StorageToString(ret, alter.move_partition().storage());
            break;
        case AlterType::kClearIndexPartition:
            ret += "CLEAR INDEX ";
            IndexToString(ret, alter.clear_index_partition().idx());
            ret += " IN ";
            PartitionExprToString(ret, alter.clear_index_partition().partition());
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

CONV_FN(AlterTable, alter_table)
{
    ret += "ALTER ";
    if (alter_table.is_temp())
    {
        ret += "TEMPORARY ";
    }
    ret += "TABLE ";
    ExprSchemaTableToString(ret, alter_table.est());
    ret += " ";
    AlterTableItemToString(ret, alter_table.alter());
    for (int i = 0; i < alter_table.other_alters_size(); i++)
    {
        ret += ", ";
        AlterTableItemToString(ret, alter_table.other_alters(i));
    }
    if (alter_table.has_setting_values())
    {
        ret += " SETTINGS ";
        SettingValuesToString(ret, alter_table.setting_values());
    }
}

CONV_FN(Attach, at)
{
    ret += "ATTACH ";
    ret += SQLObject_Name(at.sobject());
    ret += " ";
    SQLObjectNameToString(ret, at.object());
    if (at.has_as_replicated())
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

CONV_FN(SystemCommand, cmd)
{
    ret += "SYSTEM ";
    using CmdType = SystemCommand::SystemCmdOneofCase;
    switch (cmd.system_cmd_oneof_case())
    {
        case CmdType::kReloadEmbeddedDictionaries:
            ret += "RELOAD EMBEDDED DICTIONARIES";
            break;
        case CmdType::kReloadDictionaries:
            ret += "RELOAD DICTIONARIES";
            break;
        case CmdType::kReloadModels:
            ret += "RELOAD MODELS";
            break;
        case CmdType::kReloadFunctions:
            ret += "RELOAD FUNCTIONS";
            break;
        case CmdType::kReloadFunction:
            ret += "RELOAD FUNCTION ";
            FunctionToString(ret, cmd.reload_function());
            break;
        case CmdType::kReloadAsynchronousMetrics:
            ret += "RELOAD ASYNCHRONOUS METRICS";
            break;
        case CmdType::kDropDnsCache:
            ret += "DROP DNS CACHE";
            break;
        case CmdType::kDropMarkCache:
            ret += "DROP MARK CACHE";
            break;
        case CmdType::kDropUncompressedCache:
            ret += "DROP UNCOMPRESSED CACHE";
            break;
        case CmdType::kDropCompiledExpressionCache:
            ret += "DROP COMPILED EXPRESSION CACHE";
            break;
        case CmdType::kDropQueryCache:
            ret += "DROP QUERY CACHE";
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
            break;
        case CmdType::kReloadConfig:
            ret += "RELOAD CONFIG";
            break;
        case CmdType::kReloadUsers:
            ret += "RELOAD USERS";
            break;
        case CmdType::kStopMerges:
            ret += "STOP MERGES ";
            ExprSchemaTableToString(ret, cmd.stop_merges());
            break;
        case CmdType::kStartMerges:
            ret += "START MERGES ";
            ExprSchemaTableToString(ret, cmd.start_merges());
            break;
        case CmdType::kStopTtlMerges:
            ret += "STOP TTL MERGES ";
            ExprSchemaTableToString(ret, cmd.stop_ttl_merges());
            break;
        case CmdType::kStartTtlMerges:
            ret += "START TTL MERGES ";
            ExprSchemaTableToString(ret, cmd.start_ttl_merges());
            break;
        case CmdType::kStopMoves:
            ret += "STOP MOVES ";
            ExprSchemaTableToString(ret, cmd.stop_moves());
            break;
        case CmdType::kStartMoves:
            ret += "START MOVES ";
            ExprSchemaTableToString(ret, cmd.start_moves());
            break;
        case CmdType::kWaitLoadingParts:
            ret += "WAIT LOADING PARTS ";
            ExprSchemaTableToString(ret, cmd.wait_loading_parts());
            break;
        case CmdType::kStopFetches:
            ret += "STOP FETCHES ";
            ExprSchemaTableToString(ret, cmd.stop_fetches());
            break;
        case CmdType::kStartFetches:
            ret += "START FETCHES ";
            ExprSchemaTableToString(ret, cmd.start_fetches());
            break;
        case CmdType::kStopReplicatedSends:
            ret += "STOP REPLICATED SENDS ";
            ExprSchemaTableToString(ret, cmd.stop_replicated_sends());
            break;
        case CmdType::kStartReplicatedSends:
            ret += "START REPLICATED SENDS ";
            ExprSchemaTableToString(ret, cmd.start_replicated_sends());
            break;
        case CmdType::kStopReplicationQueues:
            ret += "STOP REPLICATION QUEUES ";
            ExprSchemaTableToString(ret, cmd.stop_replication_queues());
            break;
        case CmdType::kStartReplicationQueues:
            ret += "START REPLICATION QUEUES ";
            ExprSchemaTableToString(ret, cmd.start_replication_queues());
            break;
        case CmdType::kStopPullingReplicationLog:
            ret += "STOP PULLING REPLICATION LOG ";
            ExprSchemaTableToString(ret, cmd.stop_pulling_replication_log());
            break;
        case CmdType::kStartPullingReplicationLog:
            ret += "START PULLING REPLICATION LOG ";
            ExprSchemaTableToString(ret, cmd.start_pulling_replication_log());
            break;
        case CmdType::kSyncReplica:
            ret += "SYNC REPLICA ";
            ExprSchemaTableToString(ret, cmd.sync_replica().est());
            ret += " ";
            ret += SyncReplica_SyncPolicy_Name(cmd.sync_replica().policy());
            break;
        case CmdType::kSyncReplicatedDatabase:
            ret += "SYNC DATABASE REPLICA ";
            DatabaseToString(ret, cmd.sync_replicated_database());
            break;
        case CmdType::kRestartReplica:
            ret += "SYNC RESTART REPLICA ";
            ExprSchemaTableToString(ret, cmd.restart_replica());
            break;
        case CmdType::kRestoreReplica:
            ret += "SYNC RESTORE REPLICA ";
            ExprSchemaTableToString(ret, cmd.restore_replica());
            break;
        case CmdType::kRestartReplicas:
            ret += "RESTART REPLICAS";
            break;
        case CmdType::kDropFilesystemCache:
            ret += "DROP FILESYSTEM CACHE";
            break;
        case CmdType::kSyncFileCache:
            ret += "SYNC FILE CACHE";
            break;
        case CmdType::kLoadPks:
            ret += "LOAD PRIMARY KEY";
            break;
        case CmdType::kLoadPk:
            ret += "LOAD PRIMARY KEY ";
            ExprSchemaTableToString(ret, cmd.load_pk());
            break;
        case CmdType::kUnloadPks:
            ret += "UNLOAD PRIMARY KEY";
            break;
        case CmdType::kUnloadPk:
            ret += "UNLOAD PRIMARY KEY ";
            ExprSchemaTableToString(ret, cmd.unload_pk());
            break;
        case CmdType::kRefreshViews:
            ret += "REFRESH VIEW";
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
            ret += "PREWARM MARK CACHE ";
            ExprSchemaTableToString(ret, cmd.prewarm_cache());
            break;
        case CmdType::kPrewarmPrimaryIndexCache:
            ret += "PREWARM PRIMARY INDEX CACHE ";
            ExprSchemaTableToString(ret, cmd.prewarm_primary_index_cache());
            break;
        case CmdType::kDropConnectionsCache:
            ret += "DROP CONNECTIONS CACHE";
            break;
        case CmdType::kDropPrimaryIndexCache:
            ret += "DROP PRIMARY INDEX CACHE";
            break;
        case CmdType::kDropIndexMarkCache:
            ret += "DROP INDEX MARK CACHE";
            break;
        case CmdType::kDropIndexUncompressedCache:
            ret += "DROP INDEX UNCOMPRESSED CACHE";
            break;
        case CmdType::kDropMmapCache:
            ret += "DROP MMAP CACHE";
            break;
        case CmdType::kDropPageCache:
            ret += "DROP PAGE CACHE";
            break;
        case CmdType::kDropSchemaCache:
            ret += "DROP SCHEMA CACHE";
            break;
        case CmdType::kDropS3ClientCache:
            ret += "DROP S3 CLIENT CACHE";
            break;
        case CmdType::kFlushAsyncInsertQueue:
            ret += "FLUSH ASYNC INSERT QUEUE";
            break;
        case CmdType::kSyncFilesystemCache:
            ret += "SYNC FILESYSTEM CACHE";
            break;
        case CmdType::kDropCache:
            ret += "DROP CACHE";
            break;
        default:
            ret += "REFRESH VIEW";
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
            ExchangeTablesToString(ret, query.exchange());
            break;
        case QueryType::kAlterTable:
            AlterTableToString(ret, query.alter_table());
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
        default:
            ret += "SELECT 1";
    }
}

CONV_FN(ExplainQuery, explain)
{
    ret += "EXPLAIN";
    if (explain.has_expl())
    {
        ret += " ";
        ret += ExplainQuery_ExplainValues_Name(explain.expl());
        std::replace(ret.begin(), ret.end(), '_', ' ');
    }
    if (explain.has_expl() && explain.expl() <= ExplainQuery_ExplainValues::ExplainQuery_ExplainValues_QUERY_TREE && explain.opts_size())
    {
        ret += " ";
        for (int i = 0; i < explain.opts_size(); i++)
        {
            std::string ostr;
            const ExplainOption & eopt = explain.opts(i);

            if (i != 0)
            {
                ret += ", ";
            }
            switch (explain.expl())
            {
                case ExplainQuery_ExplainValues::ExplainQuery_ExplainValues_PLAN:
                    ostr += ExpPlanOpt_Name(static_cast<ExpPlanOpt>((eopt.opt() % 5) + 1));
                    break;
                case ExplainQuery_ExplainValues::ExplainQuery_ExplainValues_PIPELINE:
                    ostr += ExpPipelineOpt_Name(static_cast<ExpPipelineOpt>((eopt.opt() % 3) + 1));
                    break;
                case ExplainQuery_ExplainValues::ExplainQuery_ExplainValues_QUERY_TREE:
                    ostr += ExpQueryTreeOpt_Name(static_cast<ExpQueryTreeOpt>((eopt.opt() % 3) + 1));
                    break;
                default:
                    assert(0);
            }
            ostr = ostr.substr(5);
            std::transform(ostr.begin(), ostr.end(), ostr.begin(), [](unsigned char c) { return std::tolower(c); });
            ret += ostr;
            ret += " = ";
            ret += std::to_string(eopt.val());
        }
    }
    ret += " ";
    SQLQueryInnerToString(ret, explain.inner_query());
}

CONV_FN(SQLQuery, query)
{
    using QueryType = SQLQuery::QueryOneofCase;
    switch (query.query_oneof_case())
    {
        case QueryType::kInnerQuery:
            SQLQueryInnerToString(ret, query.inner_query());
            break;
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
    ret += ";";
}

}
