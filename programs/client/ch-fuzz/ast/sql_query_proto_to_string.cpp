#include <algorithm>
#include <cctype>
#include <cstdint>
#include <string>
#include <limits>
#include <random>

#include "sql_grammar.pb.h"

using namespace sql_query_grammar;

#define CONV_FN(TYPE, VAR_NAME) void TYPE##ToString(std::string &ret, const TYPE& VAR_NAME)
#define CONV_FN_ESCAPE(TYPE, VAR_NAME) void TYPE##ToString(std::string &ret, const bool escape, const TYPE& VAR_NAME)

namespace chfuzz {

static constexpr char hex_digits[] = "0123456789ABCDEF";
static constexpr char digits[] = "0123456789";

CONV_FN(Expr, expr);
CONV_FN(Select, select);

// ~~~~Numbered values to string~~~

CONV_FN(Column, col) {
  ret += col.column();
}

CONV_FN(Index, idx) {
  ret += idx.index();
}

CONV_FN(Projection, proj) {
  ret += proj.projection();
}

CONV_FN(Constraint, constr) {
  ret += constr.constraint();
}

CONV_FN(Table, table) {
  ret += table.table();
}

CONV_FN(Window, window) {
  ret += "w";
  ret += std::to_string(window.window());
}

CONV_FN(ExprColAlias, eca) {
  ExprToString(ret, eca.expr());
  if (eca.has_col_alias()) {
    ret += " AS ";
    ColumnToString(ret, eca.col_alias());
  }
}

/*CONV_FN(Schema, schema) {
  if (schema.main()) {
    return "main";
  }
  if (schema.temp()) {
    return "temp";
  }
  std::string ret("Schema");
  ret += std::to_string(schema.schema() % kMaxSchemaNumber);
  return ret;
}*/

void ConvertToSqlString(std::string &ret, const std::string& s) {
  for (size_t i = 0; i < s.length() ; i++) {
    const char &c = s[i];

    switch(c) {
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
        if (c > static_cast<char>(31) && c < static_cast<char>(127)) {
          ret += c;
        } else {
          const uint8_t &x = static_cast<uint8_t>(c);

          ret += "\\x";
          ret += hex_digits[(x & 0xF0) >> 4];
          ret += hex_digits[x & 0x0F];
        }
      }
    }
  }
}

CONV_FN(ExprSchemaTable, st) {
  /*if (st.has_schema_name()) {
    ret += SchemaToString(st.schema_name());
    ret += ".";
  }*/
  TableToString(ret, st.table_name());
}


CONV_FN_ESCAPE(TypeName, top);
CONV_FN_ESCAPE(TopTypeName, top);

CONV_FN(JSONColumn, jcol) {
  ret += ".";
  if (jcol.has_json_col()) {
    ret += "^";
  }
  ColumnToString(ret, jcol.col());
  if (jcol.has_json_array()) {
    const uint32_t limit = (jcol.json_array() % 4) + 1;

    for (uint32_t j = 0; j < limit; j++) {
      ret += "[]";
    }
  }
}

CONV_FN(JSONColumns, jcols) {
  JSONColumnToString(ret, jcols.jcol());
  for (int i = 0; i < jcols.other_jcols_size(); i++) {
    JSONColumnToString(ret, jcols.other_jcols(i));
  }
  if (jcols.has_json_cast()) {
    ret += "::";
    TypeNameToString(ret, false, jcols.json_cast());
  } else if (jcols.has_json_reinterpret()) {
    ret += ".:`";
    TypeNameToString(ret, true, jcols.json_reinterpret());
    ret += "`";
  }
}

CONV_FN(FieldAccess, fa) {
  using FieldType = FieldAccess::NestedOneofCase;
  switch (fa.nested_oneof_case()) {
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
      ret += "['";
      ColumnToString(ret, fa.array_key());
      ret += "']";
      break;
    case FieldType::kTupleIndex:
      ret += ".";
      ret += std::to_string((fa.tuple_index() % 9) + 1);
      break;
    default:
      ret += "[1]";
  }
}

CONV_FN(ExprColumn, ec) {
  ColumnToString(ret, ec.col());
  if (ec.has_subcols()) {
    JSONColumnsToString(ret, ec.subcols());
  }
  if (ec.has_dynamic_subtype()) {
    ret += ".`";
    TypeNameToString(ret, true, ec.dynamic_subtype());
    ret += "`";
  }
  if (ec.null()) {
    ret += ".null";
  }
}

CONV_FN(ExprSchemaTableColumn, stc) {
  /*if (stc.has_schema()) {
    ret += SchemaToString(stc.schema());
    ret += ".";
  }*/
  if (stc.has_table()) {
    TableToString(ret, stc.table());
    ret += ".";
  }
  ExprColumnToString(ret, stc.col());
}

CONV_FN(ExprList, me) {
  ExprToString(ret, me.expr());
  for (int i = 0; i < me.extra_exprs_size(); i++) {
    ret += ", ";
    ExprToString(ret, me.extra_exprs(i));
  }
}

// ~~~~Expression stuff~~~~
CONV_FN(NumericLiteral, nl) {
  ret += "(";
  if (nl.digits_size() > 0) {
    if (nl.negative()) {
      ret += "-";
    }
    ret += digits[std::min<uint32_t>(UINT32_C(1), nl.digits(0) % 10)];
    for (int i = 1; i < nl.digits_size(); i++) {
      ret += digits[nl.digits(i) % 10];
    }
  }
  if (nl.decimal_point()) {
    if (nl.digits_size() == 0) {
      ret += "0";
    }
    ret += ".";
    if (nl.dec_digits_size() == 0) {
      ret += "0";
    } else {
      for (int i = 0; i < nl.dec_digits_size(); i++) {
        ret += digits[nl.dec_digits(i) % 10];
      }
    }
  }
  if (nl.exp_digits_size() > 0) {
    if (nl.digits_size() == 0 && !nl.decimal_point()) {
      ret += "1";
    }
    ret += "E";
    if (nl.negative_exp()) {
      ret += "-";
    }
    for (int i = 0; i < nl.exp_digits_size(); i++) {
      ret += digits[nl.exp_digits(i) % 10];
    }
  }
  if (nl.digits_size() == 0 && !nl.decimal_point() && nl.exp_digits_size() == 0) {
    ret += "1";
  }
  ret += ")";
}

static void BuildJson(std::string &ret, const int depth, const int width, std::mt19937 &gen);
static void AddJsonArray(std::string &ret, const int depth, const int width, std::mt19937 &gen);
static void AddJsonElement(std::string &ret, std::mt19937 &gen);

static void
AddJsonArray(std::string &ret, const int depth, const int width, std::mt19937 &gen) {
  std::uniform_int_distribution<int> jopt(1, 3);
  int nelems = 0, next_width = 0;

  if (width) {
    std::uniform_int_distribution<int> alen(1, width);
    nelems = alen(gen);
  }
  ret += "[";
  next_width = nelems;
  for (int j = 0 ; j < nelems ; j++) {
    if (j != 0) {
      ret += ",";
    }
    if (depth) {
      const int noption = jopt(gen);

      switch (noption) {
        case 1: //object
          BuildJson(ret, depth - 1, next_width, gen);
          break;
        case 2: //array
          AddJsonArray(ret, depth - 1, next_width, gen);
          break;
        case 3: //others
          AddJsonElement(ret, gen);
          break;
        default:
          assert(0);
      }
    } else {
      AddJsonElement(ret, gen);
    }
    next_width--;
  }
  ret += "]";
}

static void
AddJsonElement(std::string &ret, std::mt19937 &gen) {
  std::uniform_int_distribution<int> opts(1, 10);
  const int noption = opts(gen);

    switch (noption) {
      case 1:
        ret += "false";
        break;
      case 2:
        ret += "true";
        break;
      case 3:
        ret += "null";
        break;
      case 4:
      case 5:
      case 6:
      case 7: { //number
        std::uniform_int_distribution<int> numbers(-1000, 1000);
        ret += std::to_string(numbers(gen));
      } break;
      case 8:
      case 9:
      case 10: { //string
        std::uniform_int_distribution<int> slen(0, 200);
        std::uniform_int_distribution<uint8_t> chars(32, 127);
        const int nlen = slen(gen);

        ret += '"';
        for (int i = 0 ; i < nlen ; i++) {
          const uint8_t nchar = chars(gen);

          switch (nchar) {
            case 127:
              ret += "😂";
              break;
            case static_cast<int>('"'):
              ret += "\\\"";
              break;
            case static_cast<int>('\\'):
              ret += "\\\\";
              break;
            default:
              ret += static_cast<char>(nchar);
          }
        }
        ret += '"';
      } break;
      default:
        assert(0);
    }
}

static void
BuildJson(std::string &ret, const int depth, const int width, std::mt19937 &gen) {
  ret += "{";
  if (depth) {
    std::uniform_int_distribution<int> childd(1, 10);
    const int nchildren = childd(gen);

    for (int i = 0 ; i < nchildren ; i++) {
      std::uniform_int_distribution<int> copt(0, 5);
      std::uniform_int_distribution<int> jopt(1, 3);
      const int noption = jopt(gen);

      if (i != 0) {
        ret += ",";
      }
      ret += "\"c";
      ret += std::to_string(copt(gen));
      ret += "\":";
      switch (noption) {
        case 1: //object
          BuildJson(ret, depth - 1, width, gen);
          break;
        case 2: //array
          AddJsonArray(ret, depth - 1, width, gen);
          break;
        case 3: //others
          AddJsonElement(ret, gen);
          break;
      default:
          assert(0);
      }
    }
  }
  ret += "}";
}

CONV_FN(IntLiteral, int_val) {
  if (int_val.has_int_lit()) {
    ret += std::to_string(int_val.int_lit());
  } else if (int_val.has_uint_lit()) {
    ret += std::to_string(int_val.uint_lit());
  } else {
    ret += "0";
  }
  if (int_val.has_integers()) {
    ret += "::";
    ret += Integers_Name(int_val.integers());
  }
}

CONV_FN(LiteralValue, lit_val) {
  using LitValType = LiteralValue::LitValOneofCase;
  switch (lit_val.lit_val_oneof_case()) {
    case LitValType::kIntLit:
      IntLiteralToString(ret, lit_val.int_lit());
      break;
    case LitValType::kNoQuoteStr:
      ret += lit_val.no_quote_str();
      break;
    case LitValType::kHexString: {
      const std::string &s = lit_val.hex_string();
      ret += "x'";
      for (size_t i = 0; i < s.length(); i++) {
          const uint8_t &x = static_cast<uint8_t>(s[i]);
          ret += hex_digits[(x & 0xF0) >> 4];
          ret += hex_digits[x & 0x0F];
      }
      ret += '\'';
      } break;
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
      ConvertToSqlString(ret, lit_val.string_lit());
      ret += '\'';
      break;
    case LitValType::kJsonValue:
    case LitValType::kJsonString: {
      const bool is_json_value = lit_val.lit_val_oneof_case() == LitValType::kJsonValue;
      std::mt19937 gen(is_json_value ? lit_val.json_value() : lit_val.json_string());
      std::uniform_int_distribution<int> dopt(1, 3), wopt(1, 3);

      ret += "$jstr$";
      BuildJson(ret, dopt(gen), wopt(gen), gen);
      ret += "$jstr$";
      if (is_json_value) {
        ret += "::JSON";
      }
    } break;
    case LitValType::kSpecialVal: {
      switch (lit_val.special_val()) {
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
        case SpecialVal::MAX_UINT32:
          ret += std::to_string(std::numeric_limits<uint32_t>::max());
          break;
        case SpecialVal::MAX_UINT64:
          ret += std::to_string(std::numeric_limits<uint64_t>::max());
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
    } break;
    default:
      ret += "1";
  }
}

CONV_FN(UnaryExpr, uexpr) {
  ret += "(";
  switch (uexpr.unary_op()) {
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

CONV_FN(BinaryOperator, bop) {
  switch (bop) {
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

CONV_FN(BinaryExpr, bexpr) {
  ExprToString(ret, bexpr.lhs());
  BinaryOperatorToString(ret, bexpr.op());
  ExprToString(ret, bexpr.rhs());
}

CONV_FN(ParenthesesExpr, pexpr) {
  ret += "(";
  ExprColAliasToString(ret, pexpr.expr());
  for (int i = 0; i < pexpr.other_exprs_size(); i++) {
    ret += ", ";
    ExprColAliasToString(ret, pexpr.other_exprs(i));
  }
  ret += ")";
}

CONV_FN(ColumnPath, ic) {
  ColumnToString(ret, ic.col());
  for (int i = 0; i < ic.sub_cols_size(); i++) {
    ret += ".";
    ColumnToString(ret, ic.sub_cols(i));
  }
}

CONV_FN(LowCardinality, lc) {
  using LcardTypeNameType = LowCardinality::LcardOneOfCase;
  switch (lc.lcard_one_of_case()) {
    case LcardTypeNameType::kIntegers:
      ret += Integers_Name(lc.integers());
      break;
    case LcardTypeNameType::kFloats:
      ret += FloatingPoints_Name(lc.floats());
      break;
    case LcardTypeNameType::kSqlString:
      ret += "String";
      break;
    case LcardTypeNameType::kFixedString:
      ret += "FixedString(";
      ret += std::to_string(std::max<uint32_t>(1, lc.fixed_string()));
      ret += ")";
      break;
    case LcardTypeNameType::kDates:
      ret += Dates_Name(lc.dates());
      break;
    default:
      ret += "Int";
  }
}

CONV_FN_ESCAPE(BottomTypeName, btn) {
  using BottomTypeNameType = BottomTypeName::BottomOneOfCase;
  switch (btn.bottom_one_of_case()) {
    case BottomTypeNameType::kIntegers:
      ret += Integers_Name(btn.integers());
      break;
    case BottomTypeNameType::kFloats:
      ret += FloatingPoints_Name(btn.floats());
      break;
    case BottomTypeNameType::kDecimal: {
      const sql_query_grammar::Decimal &dec = btn.decimal();
      ret += "Decimal";
      if (dec.has_precision()) {
        const uint32_t precision = (dec.precision() % 76) + 1;

        ret += "(";
        ret += std::to_string(precision);
        if (dec.has_scale()) {
          ret += ",";
          ret += std::to_string(dec.scale() % precision);
        }
        ret += ")";
      }
    } break;
    case BottomTypeNameType::kSqlString:
      ret += "String";
      break;
    case BottomTypeNameType::kFixedString:
      ret += "FixedString(";
      ret += std::to_string(std::max<uint32_t>(1, btn.fixed_string()));
      ret += ")";
      break;
    case BottomTypeNameType::kUuid:
      ret += "UUID";
      break;
    case BottomTypeNameType::kDates:
      ret += Dates_Name(btn.dates());
      break;
    case BottomTypeNameType::kJson: {
      const sql_query_grammar::JsonDef &jdef = btn.json();

      ret += "JSON";
      if (jdef.spec_size() > 0) {
        ret += "(";
        for (int i = 0 ; i < jdef.spec_size(); i++) {
          const sql_query_grammar::JsonDefItem &jspec = jdef.spec(i);

          if (i != 0) {
            ret += ", ";
          }
          if (jspec.has_max_dynamic_types()) {
            ret += "max_dynamic_types=";
            ret += std::to_string(jspec.max_dynamic_types() % 33);
          } else if (jspec.has_max_dynamic_paths()) {
            ret += "max_dynamic_paths=";
            ret += std::to_string(jspec.max_dynamic_paths() % 1025);
          } else if (jspec.has_skip_path()) {
            ret += "SKIP ";
            ColumnPathToString(ret, jspec.skip_path());
          } else if (jspec.has_path_type()) {
            const sql_query_grammar::JsonPathType &jpt = jspec.path_type();

            ColumnPathToString(ret, jpt.col());
            ret += " ";
            TopTypeNameToString(ret, escape, jpt.type());
          } else {
            ret += "max_dynamic_types=8";
          }
        }
        ret += ")";
      }
    } break;
    case BottomTypeNameType::kDynamic:
      ret += "Dynamic";
      if (btn.dynamic().has_ntypes()) {
        ret += "(max_types=";
        ret += std::to_string((btn.dynamic().ntypes() % 255) + 1);
        ret += ")";
      }
      break;
    case BottomTypeNameType::kEnumDef: {
      const sql_query_grammar::EnumDef &edef = btn.enum_def();
      const std::string first_val = std::to_string(edef.first_value());

      ret += "Enum";
      if (edef.has_bits()) {
        ret += edef.bits() ? "16" : "8";
      }
      ret += "(";
      if (escape) {
        ret += "\\";
      }
      ret += "'";
      ret += first_val;
      if (escape) {
        ret += "\\";
      }
      ret += "'";
      ret += " = ";
      ret += first_val;
      for (int i = 0 ; i < edef.other_values_size(); i++) {
        const std::string next_val = std::to_string(edef.other_values(i));

        ret += ", ";
        if (escape) {
          ret += "\\";
        }
        ret += "'";
        ret += next_val;
        if (escape) {
          ret += "\\";
        }
        ret += "'";
        ret += " = ";
        ret += next_val;
      }
      ret += ")";
    } break;
    case BottomTypeNameType::kLcard:
      LowCardinalityToString(ret, btn.lcard());
      break;
    default:
      ret += "Int";
  }
}

CONV_FN_ESCAPE(TypeColumnDef, col_def) {
  ColumnToString(ret, col_def.col());
  ret += " ";
  TopTypeNameToString(ret, escape, col_def.type_name());
}

CONV_FN_ESCAPE(TopTypeName, ttn) {
  using TopTypeNameType = TopTypeName::TypeOneofCase;
  switch (ttn.type_oneof_case()) {
    case TopTypeNameType::kNonNullable:
      BottomTypeNameToString(ret, escape, ttn.non_nullable());
      break;
    case TopTypeNameType::kNullable:
      ret += "Nullable(";
      BottomTypeNameToString(ret, escape, ttn.nullable());
      ret += ")";
      break;
    case TopTypeNameType::kArray:
      ret += "Array(";
      TopTypeNameToString(ret, escape, ttn.array());
      ret += ")";
      break;
    case TopTypeNameType::kMap:
      ret += "Map(";
      TopTypeNameToString(ret, escape, ttn.map().key());
      ret += ",";
      TopTypeNameToString(ret, escape, ttn.map().value());
      ret += ")";
      break;
    case TopTypeNameType::kTuple:
      ret += "Tuple(";
      TypeColumnDefToString(ret, escape, ttn.tuple().value1());
      ret += ",";
      TypeColumnDefToString(ret, escape, ttn.tuple().value2());
      for (int i = 0 ; i < ttn.tuple().others_size(); i++) {
        ret += ",";
        TypeColumnDefToString(ret, escape, ttn.tuple().others(i));
      }
      ret += ")";
      break;
    case TopTypeNameType::kNested:
      ret += "Nested(";
      TypeColumnDefToString(ret, escape, ttn.nested().type1());
      for (int i = 0 ; i < ttn.nested().others_size(); i++) {
        ret += ", ";
        TypeColumnDefToString(ret, escape, ttn.nested().others(i));
      }
      ret += ")";
      break;
    case TopTypeNameType::kVariant:
      ret += "Variant(";
      TopTypeNameToString(ret, escape, ttn.variant().value1());
      ret += ",";
      TopTypeNameToString(ret, escape, ttn.variant().value2());
      for (int i = 0 ; i < ttn.variant().others_size(); i++) {
        ret += ",";
        TopTypeNameToString(ret, escape, ttn.variant().others(i));
      }
      ret += ")";
      break;
    default:
      ret += "Int";
  }
}

CONV_FN_ESCAPE(TypeName, tp) {
  TopTypeNameToString(ret, escape, tp.type());
}

CONV_FN(CastExpr, cexpr) {
  ret += "CAST(";
  ExprToString(ret, cexpr.expr());
  ret += ", '";
  TypeNameToString(ret, true, cexpr.type_name());
  ret += "')";
}

CONV_FN(ExprLike, elike) {
  ExprToString(ret, elike.expr1());
  ret += " ";
  if (elike.not_())
    ret += "NOT ";
  ret += "LIKE ";
  ExprToString(ret, elike.expr2());
}

CONV_FN(CondExpr, econd) {
  ret += "((";
  ExprToString(ret, econd.expr1());
  ret += ") ? (";
  ExprToString(ret, econd.expr2());
  ret += ") : (";
  ExprToString(ret, econd.expr3());
  ret += "))";
}

CONV_FN(ExprNullTests, ent) {
  ExprToString(ret, ent.expr());
  ret += " IS";
  ret += ent.not_() ? " NOT" : "";
  ret += " NULL";
}

CONV_FN(ExprBetween, ebetween) {
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

CONV_FN(ExprIn, ein) {
  const sql_query_grammar::ExprList &elist = ein.expr();

  if (elist.extra_exprs_size() == 0) {
    ExprToString(ret, elist.expr());
  } else {
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
  if (ein.has_exprs()) {
    ExprListToString(ret, ein.exprs());
  } else if (ein.has_sel()) {
    SelectToString(ret, ein.sel());
  } else {
    ret += "1";
  }
  ret += ")";
}

CONV_FN(ExprAny, eany) {
  ExprToString(ret, eany.expr());
  BinaryOperatorToString(ret, static_cast<sql_query_grammar::BinaryOperator>(((static_cast<int>(eany.op()) % 8) + 1)));
  ret += eany.anyall() ? "ALL" : "ANY";
  ret += "(";
  SelectToString(ret, eany.sel());
  ret += ")";
}

CONV_FN(ExprExists, exists) {
  if (exists.not_())
    ret += "NOT ";
  ret += "EXISTS (";
  SelectToString(ret, exists.select());
  ret += ")";
}

CONV_FN(ExprWhenThen, ewt) {
  ret += "WHEN ";
  ExprToString(ret, ewt.when_expr());
  ret += " THEN ";
  ExprToString(ret, ewt.then_expr());
}

CONV_FN(ExprCase, ecase) {
  ret += "CASE ";
  if (ecase.has_expr()) {
    ExprToString(ret, ecase.expr());
    ret += " ";
  }
  ExprWhenThenToString(ret, ecase.when_then());
  ret += " ";
  for (int i = 0; i < ecase.extra_when_thens_size(); i++) {
    ExprWhenThenToString(ret, ecase.extra_when_thens(i));
    ret += " ";
  }
  if (ecase.has_else_expr()) {
    ret += "ELSE ";
    ExprToString(ret, ecase.else_expr());
    ret += " ";
  }
  ret += "END";
}

CONV_FN(ColumnList, cols) {
  ColumnToString(ret, cols.col());
  for (int i = 0 ; i < cols.other_cols_size(); i++) {
    ret += ", ";
    ColumnToString(ret, cols.other_cols(i));
  }
}

CONV_FN(LambdaExpr, lambda) {
  ColumnListToString(ret, lambda.args());
  ret += " -> ";
  ExprToString(ret, lambda.expr());
}

CONV_FN(SQLFuncCall, sfc) {
  ret += SQLFunc_Name(sfc.func()).substr(4);
  if (sfc.params_size()) {
    ret += '(';
    for (int i = 0 ; i < sfc.params_size(); i++) {
      if (i != 0) {
        ret += ",";
      }
      ExprToString(ret, sfc.params(i));
    }
    ret += ')';
  }
  ret += '(';
  if (sfc.args_size() == 1 && sfc.distinct()) {
    ret += "DISTINCT ";
  }
  for (int i = 0 ; i < sfc.args_size(); i++) {
    const sql_query_grammar::SQLFuncArg &sfa = sfc.args(i);

    if (i != 0) {
      ret += ",";
    }
    if (sfa.has_lambda()) {
      LambdaExprToString(ret, sfa.lambda());
    } else if (sfa.has_expr()) {
      ExprToString(ret, sfa.expr());
    } else {
      ret += "1";
    }
  }
  ret += ')';
  if (sfc.respect_nulls()) {
    ret += " RESPECT NULLS";
  }
}

CONV_FN(ExprOrderingTerm, eot) {
  ExprToString(ret, eot.expr());
  if (eot.has_asc_desc()) {
    ret += " ";
    ret += ExprOrderingTerm_AscDesc_Name(eot.asc_desc());
  }
  if (eot.with_fill()) {
    ret += " WITH FILL";
  }
}

CONV_FN(OrderByStatement, obs) {
  ret += "ORDER BY ";
  ExprOrderingTermToString(ret, obs.ord_term());
  for (int i = 0; i < obs.extra_ord_terms_size(); i++) {
    ret += ", ";
    ExprOrderingTermToString(ret, obs.extra_ord_terms(i));
  }
}

CONV_FN(SQLWindowCall, wc) {
  ret += WindowFuncs_Name(wc.func()).substr(3);
  ret += '(';
  for (int i = 0 ; i < wc.args_size(); i++) {
    if (i != 0) {
      ret += ",";
    }
    ExprToString(ret, wc.args(i));
  }
  ret += ')';
  if (wc.has_wfn()) {
    std::string next = SQLWindowCall_WindowFuncNulls_Name(wc.wfn());

    std::replace(next.begin(), next.end(), '_', ' ');
    ret += " ";
    ret += next;
  }
}

CONV_FN(FrameSpecSubLeftExpr, fssle) {
  std::string next = FrameSpecSubLeftExpr_Which_Name(fssle.which());

  if (fssle.which() > sql_query_grammar::FrameSpecSubLeftExpr_Which_UNBOUNDED_PRECEDING && fssle.has_expr()) {
    ExprToString(ret, fssle.expr());
    ret += " ";
  }
  std::replace(next.begin(), next.end(), '_', ' ');
  ret += next;
}

CONV_FN(FrameSpecSubRightExpr, fsslr) {
  std::string next = FrameSpecSubRightExpr_Which_Name(fsslr.which());

  if (fsslr.which() > sql_query_grammar::FrameSpecSubRightExpr_Which_UNBOUNDED_FOLLOWING && fsslr.has_expr()) {
    ExprToString(ret, fsslr.expr());
    ret += " ";
  }
  std::replace(next.begin(), next.end(), '_', ' ');
  ret += next;
}

CONV_FN(ExprFrameSpec, efs) {
  const bool has_right = efs.has_right_expr();

  ret += ExprFrameSpec_RangeRows_Name(efs.range_rows());
  if (has_right) {
    ret += " BETWEEN";
  }
  ret += " ";
  FrameSpecSubLeftExprToString(ret, efs.left_expr());
  if (has_right) {
    ret += " AND ";
    FrameSpecSubRightExprToString(ret, efs.right_expr());
  }
}

CONV_FN(WindowDefn, wd) {
  if (wd.partition_exprs_size()) {
    ret += "PARTITION BY ";
    for (int i = 0 ; i < wd.partition_exprs_size(); i++) {
      if (i != 0) {
        ret += ",";
      }
      ExprToString(ret, wd.partition_exprs(i));
    }
  }
  if (wd.has_order_by()) {
    if (wd.partition_exprs_size()) {
      ret += " ";
    }
    OrderByStatementToString(ret, wd.order_by());
  }
  if (wd.has_frame_spec()) {
    if ((wd.partition_exprs_size() && !wd.has_order_by()) || wd.has_order_by()) {
      ret += " ";
    }
    ExprFrameSpecToString(ret, wd.frame_spec());
  }
}

CONV_FN(WindowFuncCall, wc) {
  if (wc.has_win_func()) {
    SQLWindowCallToString(ret, wc.win_func());
  } else if (wc.has_agg_func()) {
    SQLFuncCallToString(ret, wc.agg_func());
  } else {
    ret += "rank()";
  }
  ret += " OVER (";
  if (wc.has_win_defn()) {
    WindowDefnToString(ret, wc.win_defn());
  } else if (wc.has_win_name()) {
    WindowToString(ret, wc.win_name());
  }
  ret += ")";
}

CONV_FN(IntervalExpr, ie) {
  ret += "INTERVAL (";
  ExprToString(ret, ie.expr());
  ret += ") ";
  ret += IntervalExpr_Interval_Name(ie.interval());
}

CONV_FN(ComplicatedExpr, expr) {
  using ExprType = ComplicatedExpr::ComplicatedExprOneofCase;
  switch (expr.complicated_expr_oneof_case()) {
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
    case ExprType::kArray: {
      const sql_query_grammar::ArraySequence &vals = expr.array();

      ret += "[";
      for (int i = 0 ; i < vals.values_size(); i++) {
        if (i != 0) {
          ret += ",";
        }
        ExprToString(ret, vals.values(i));
      }
      ret += "]";
    } break;
    case ExprType::kTuple: {
      const sql_query_grammar::TupleSequence &vals = expr.tuple();

      ret += "(";
      for (int i = 0 ; i < vals.values_size(); i++) {
        if (i != 0) {
          ret += ",";
        }
        ret += "(";
        ExprListToString(ret, vals.values(i));
        ret += ")";
      }
      ret += ")";
    } break;
    default:
      ret += "1";
  }
}

CONV_FN(Expr, expr) {
  if (expr.has_lit_val()) {
    LiteralValueToString(ret, expr.lit_val());
  } else if (expr.has_comp_expr()) {
    ComplicatedExprToString(ret, expr.comp_expr());
  } else {
    ret += "1";
  }
  if (expr.has_field()) {
    FieldAccessToString(ret, expr.field());
  }
}

// ~~~~SELECT~~~~

CONV_FN(ResultColumn, rc) {
  if (rc.has_etc()) {
    ExprSchemaTableColumnToString(ret, rc.etc());
  } else if (rc.has_eca()) {
    ExprColAliasToString(ret, rc.eca());
  } else if (rc.has_table_star()) {
    TableToString(ret, rc.table_star());
    ret += ".*";
  } else {
    ret += "*";
  }
}

CONV_FN(JoinedQuery, tos);
CONV_FN(TableOrSubquery, tos);

CONV_FN(ExprColumnList, cl) {
  ExprColumnToString(ret, cl.col());
  for (int i = 0; i < cl.extra_cols_size(); i++) {
    ret += ", ";
    ExprColumnToString(ret, cl.extra_cols(i));
  }
}

CONV_FN(JoinConstraint, jc) {
  // oneof
  if (jc.has_on_expr()) {
    ret += " ON ";
    ExprToString(ret, jc.on_expr());
  } else if (jc.has_using_expr()) {
    ret += " USING (";
    ExprColumnListToString(ret, jc.using_expr().col_list());
    ret += ")";
  } else {
    ret += " ON TRUE";
  }
}

CONV_FN(JoinCore, jcc) {
  const bool has_cross_or_paste = jcc.join_op() > sql_query_grammar::JoinCore_JoinType::JoinCore_JoinType_FULL;

  if (jcc.global()) {
    ret += " GLOBAL";
  }
  if (jcc.join_op() != sql_query_grammar::JoinCore_JoinType::JoinCore_JoinType_INNER ||
      !jcc.has_join_const() ||
      jcc.join_const() < sql_query_grammar::JoinCore_JoinConst::JoinCore_JoinConst_SEMI) {
    ret += " ";
    ret += JoinCore_JoinType_Name(jcc.join_op());
  }
  if (!has_cross_or_paste && jcc.has_join_const()) {
    ret += " ";
    ret += JoinCore_JoinConst_Name(jcc.join_const());
  }
  ret += " JOIN ";
  TableOrSubqueryToString(ret, jcc.tos());
  if (!has_cross_or_paste) {
    JoinConstraintToString(ret, jcc.join_constraint());
  }
}

CONV_FN(ArrayJoin, aj) {
  if (aj.left()) {
    ret += " LEFT";
  }
  ret += " ARRAY JOIN ";
  ExprColAliasToString(ret, aj.constraint());
}

CONV_FN(JoinClauseCore, jcc) {
  if (jcc.has_core()) {
    JoinCoreToString(ret, jcc.core());
  } else if (jcc.has_arr()) {
    ArrayJoinToString(ret, jcc.arr());
  }
}

CONV_FN(JoinClause, jc) {
  TableOrSubqueryToString(ret, jc.tos());
  for (int i = 0; i < jc.clauses_size(); i++) {
    JoinClauseCoreToString(ret, jc.clauses(i));
  }
}

CONV_FN(JoinedDerivedQuery, tos) {
  ret += "(";
  SelectToString(ret, tos.select());
  ret += ") ";
  TableToString(ret, tos.table_alias());
}

CONV_FN(JoinedTable, jt) {
  ExprSchemaTableToString(ret, jt.est());
  if (jt.has_table_alias()) {
    ret += " ";
    TableToString(ret, jt.table_alias());
  }
  if (jt.final()) {
    ret += " FINAL";
  }
}

CONV_FN(TableOrSubquery, tos) {
  // oneof
  if (tos.has_jt()) {
    JoinedTableToString(ret, tos.jt());
  } else if (tos.has_jq()) {
    JoinedQueryToString(ret, tos.jq());
  } else if (tos.has_jdq()) {
    JoinedDerivedQueryToString(ret, tos.jdq());
  } else {
    ret += "(SELECT 1 c0) t0";
  }
}

CONV_FN(JoinedQuery, tos) {
  if (tos.tos_list_size() > 0) {
   TableOrSubqueryToString(ret, tos.tos_list(0));
    for (int i = 1; i < tos.tos_list_size(); i++) {
      ret += ", ";
      TableOrSubqueryToString(ret, tos.tos_list(i));
    }
  } else {
    JoinClauseToString(ret, tos.join_clause());
  }
}

CONV_FN(FromStatement, fs) {
  ret += "FROM ";
  JoinedQueryToString(ret, fs.tos());
}

CONV_FN(ColumnComparison, cc) {
  ExprSchemaTableColumnToString(ret, cc.col());
  BinaryOperatorToString(ret, cc.op());
  ExprToString(ret, cc.expr());
}

CONV_FN(ExprComparisonHighProbability, echp) {
  if (echp.has_cc()) {
    ColumnComparisonToString(ret, echp.cc());
  } else if (echp.has_expr()) {
    ExprToString(ret, echp.expr());
  }
}

CONV_FN(WhereStatement, ws) {
  ExprComparisonHighProbabilityToString(ret, ws.expr());
}

CONV_FN(GroupByList, gbl) {
  ExprListToString(ret, gbl.exprs());
  if (gbl.has_gs()) {
    ret += " WITH ";
    ret += GroupByList_GroupingSets_Name(gbl.gs());
  }
  if (gbl.with_totals()) {
    ret += " WITH TOTALS";
  }
}


CONV_FN(GroupByStatement, gbs) {
  ret += "GROUP BY ";
  if (gbs.has_glist()) {
    GroupByListToString(ret, gbs.glist());
  } else {
    ret += "ALL";
  }
  if (gbs.has_having_expr()) {
    ret += " HAVING ";
    ExprToString(ret, gbs.having_expr());
  }
}

CONV_FN(LimitStatement, ls) {
  ret += "LIMIT ";
  ret += std::to_string(ls.limit());
  if (ls.has_offset()) {
    ret += ", ";
    ret += std::to_string(ls.offset());
  }
  if (ls.with_ties()) {
    ret += " WITH TIES";
  }
  if (ls.has_limit_by()) {
    ret += " BY ";
    ExprToString(ret, ls.limit_by());
  }
}

CONV_FN(SelectStatementCore, ssc) {
  ret += "SELECT ";
  if (ssc.has_s_or_d()) {
    ret += AllOrDistinct_Name(ssc.s_or_d());
    ret += " ";
  }
  if (ssc.result_columns_size() == 0) {
    ret += "*";
  } else {
    ResultColumnToString(ret, ssc.result_columns(0));
    for (int i = 1; i < ssc.result_columns_size(); i++) {
      ret += ", ";
      ResultColumnToString(ret, ssc.result_columns(i));
    }
  }
  if (ssc.has_from()) {
    ret += " ";
    FromStatementToString(ret, ssc.from());
  }
  if (ssc.has_pre_where()) {
    ret += " PREWHERE ";
    WhereStatementToString(ret, ssc.pre_where());
  }
  if (ssc.has_where()) {
    ret += " WHERE ";
    WhereStatementToString(ret, ssc.where());
  }
  if (ssc.has_groupby()) {
    ret += " ";
    GroupByStatementToString(ret, ssc.groupby());
  }
  /*if (ssc.has_window()) {
    ret += WindowStatementToString(ssc.window());
    ret += " ";
  }*/
  if (ssc.has_orderby()) {
    ret += " ";
    OrderByStatementToString(ret, ssc.orderby());
  }
  if (ssc.has_limit()) {
    ret += " ";
    LimitStatementToString(ret, ssc.limit());
  }
}

CONV_FN(SetQuery, setq) {
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

CONV_FN(CTEquery, cteq) {
  TableToString(ret, cteq.table());
  ret += " AS (";
  SelectToString(ret, cteq.query());
  ret += ")";
}

CONV_FN(Select, select) {
  if (select.ctes_size()) {
    ret += "WITH ";
    for (int i = 0 ; i < select.ctes_size(); i++) {
      if (i != 0) {
        ret += ", ";
      }
      CTEqueryToString(ret, select.ctes(i));
    }
    ret += " ";
  }
  if (select.has_select_core()) {
    SelectStatementCoreToString(ret, select.select_core());
  } else if (select.has_set_query()) {
    SetQueryToString(ret, select.set_query());
  } else {
    ret += "1";
  }
}

CONV_FN(ColumnStatistics, cst) {
  ret += ColumnStat_Name(cst.stat()).substr(4);
  for (int i = 0 ; i < cst.other_stats_size(); i++) {
    ret += ", ";
    ret += ColumnStat_Name(cst.other_stats(i)).substr(4);
  }
}

CONV_FN(CodecParam, cp) {
  ret += CompressionCodec_Name(cp.codec()).substr(4);
  if (cp.params_size()) {
    ret += "(";
    for (int i = 0; i < cp.params_size(); i++) {
      if (i != 0) {
        ret += ", ";
      }
      ret += std::to_string(cp.params(i));
    }
    ret += ")";
  }
}

CONV_FN(SetValue, setv) {
  ret += setv.property();
  ret += " = ";
  ret += setv.value();
}

CONV_FN(SettingValues, setv) {
  SetValueToString(ret, setv.set_value());
  for (int i = 0; i < setv.other_values_size(); i++) {
    ret += ", ";
    SetValueToString(ret, setv.other_values(i));
  }
}

CONV_FN(ColumnDef, cdf) {
  ColumnToString(ret, cdf.col());
  ret += " ";
  TypeNameToString(ret, false, cdf.type());
  if (cdf.has_nullable()) {
    ret += cdf.nullable() ? "" : " NOT";
    ret += " NULL";
  }
  if (cdf.codecs_size()) {
    ret += " CODEC(";
    for (int i = 0; i < cdf.codecs_size(); i++) {
      if (i != 0) {
        ret += ", ";
      }
      CodecParamToString(ret, cdf.codecs(i));
    }
    ret += ")";
  }
  if (cdf.has_stats()) {
    ret += " STATISTICS(";
    ColumnStatisticsToString(ret, cdf.stats());
    ret += ")";
  }
  if (cdf.has_settings()) {
    ret += " SETTINGS(";
    SettingValuesToString(ret, cdf.settings());
    ret += ")";
  }
}

CONV_FN(IndexParam, ip) {
  if (ip.has_ival()) {
    ret += std::to_string(ip.ival());
  } else if (ip.has_dval()) {
    ret += std::to_string(ip.dval());
  } else {
    ret += "0";
  }
}

CONV_FN(IndexDef, idef) {
  ret += "INDEX ";
  IndexToString(ret, idef.idx());
  ret += " ";
  ExprToString(ret, idef.expr());
  ret += " TYPE ";
  ret += IndexType_Name(idef.type()).substr(3);
  if (idef.params_size()) {
    ret += "(";
    for (int i = 0 ; i < idef.params_size(); i++) {
      if (i != 0) {
        ret += ", ";
      }
      IndexParamToString(ret, idef.params(i));
    }
    ret += ")";
  }
  if (idef.has_granularity()) {
    ret += " GRANULARITY ";
    ret += std::to_string(idef.granularity());
  }
}

CONV_FN(ProjectionDef, proj_def) {
  ret += "PROJECTION ";
  ProjectionToString(ret, proj_def.proj());
  ret += " (";
  SelectToString(ret, proj_def.select());
  ret += ")";
}

CONV_FN(ConstraintDef, const_def) {
  ret += "CONSTRAINT ";
  ConstraintToString(ret, const_def.constr());
  ret += " ";
  ret += ConstraintDef_ConstraintType_Name(const_def.ctype());
  ret += " ";
  ExprToString(ret, const_def.expr());
}

CONV_FN(TableDefItem, tdef) {
  using CreateDefType = TableDefItem::CreatedefOneofCase;
  switch (tdef.createdef_oneof_case()) {
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

CONV_FN(TableDef, ct) {
  ColumnDefToString(ret, ct.col_def());
  for (int i = 0 ; i < ct.other_defs_size(); i++) {
    ret += ", ";
    TableDefItemToString(ret, ct.other_defs(i));
  }
}

CONV_FN(TableKey, to) {
  if (to.exprs_size() == 0) {
    ret += "tuple()";
  } else {
    ret += "(";
    for (int i = 0 ; i < to.exprs_size(); i++) {
      if (i != 0) {
        ret += ", ";
      }
      ExprToString(ret, to.exprs(i));
    }
    ret += ")";
  }
}

CONV_FN(TableEngine, te) {
  ret += " ENGINE = ";
  ret += TableEngineValues_Name(te.engine());
  ret += "(";
  for (int i = 0 ; i < te.cols_size(); i++) {
    if (i != 0) {
      ret += ", ";
    }
    ColumnToString(ret, te.cols(i));
  }
  ret += ")";
  if (te.has_order()) {
    ret += " ORDER BY ";
    TableKeyToString(ret, te.order());
  }
  if (te.has_partition_by()) {
    ret += " PARTITION BY ";
    TableKeyToString(ret, te.partition_by());
  }
  if (te.has_primary_key()) {
    ret += " PRIMARY KEY ";
    TableKeyToString(ret, te.primary_key());
  }
}

CONV_FN(CreateTable, create_table) {
  ret += create_table.replace() ? "REPLACE" : "CREATE";
  ret += " ";
  if (create_table.is_temp()) {
    ret += "TEMPORARY ";
  }
  ret += "TABLE ";
  if (create_table.if_not_exists()) {
    ret += "IF NOT EXISTS ";
  }
  ExprSchemaTableToString(ret, create_table.est());
  ret += " ";
  if (create_table.has_table_def()) {
    ret += "(";
    TableDefToString(ret, create_table.table_def());
    ret += ")";
  } else if (create_table.has_table_like()) {
    ret += "AS ";
    ExprSchemaTableToString(ret, create_table.table_like());
  }
  TableEngineToString(ret, create_table.engine());
  if (create_table.has_settings()) {
    ret += " SETTINGS ";
    SettingValuesToString(ret, create_table.settings());
  }
  if (create_table.has_table_def() && create_table.has_as_select_stmt()) {
    ret += " AS (";
    SelectToString(ret, create_table.as_select_stmt());
    ret += ")";
  }
}

CONV_FN(Drop, dt) {
  const bool is_table = dt.wdrop() == sql_query_grammar::Drop_WhatToDrop::Drop_WhatToDrop_TABLE;

  ret += "DROP ";
  if (is_table && dt.is_temp()) {
    ret += "TEMPORARY ";
  }
  ret += Drop_WhatToDrop_Name(dt.wdrop());
  if (dt.if_exists()) {
    ret += " IF EXISTS";
  }
  if (is_table && dt.if_empty()) {
    ret += " IF EMPTY";
  }
  ret += " ";
  ExprSchemaTableToString(ret, dt.est());
  for (int i = 0; i < dt.other_tables_size(); i++) {
    ret += ", ";
    ExprSchemaTableToString(ret, dt.other_tables(i));
  }
  if (dt.sync()) {
    ret += " SYNC";
  }
}

CONV_FN(ValuesStatement, values) {
  ret += "VALUES (";
  ExprListToString(ret, values.expr_list());
  ret += ")";
  for (int i = 0; i < values.extra_expr_lists_size(); i++) {
    ret += ", (";
    ExprListToString(ret, values.extra_expr_lists(i));
    ret += ")";
  }
}

CONV_FN(Insert, insert) {
  ret += "INSERT INTO ";
  ExprSchemaTableToString(ret, insert.est());
  if (insert.cols_size()) {
    ret += " (";
    for (int i = 0; i < insert.cols_size(); i++) {
      if (i != 0) {
        ret += ", ";
      }
      ColumnPathToString(ret, insert.cols(i));
    }
    ret += ")";
  }
  ret += " ";
  if (insert.has_values()) {
    ValuesStatementToString(ret, insert.values());
  } else if (insert.has_select()) {
    ret += "SELECT * FROM (";
    SelectToString(ret, insert.select());
    ret += ")";
  } else if (insert.has_query()) {
    ret += "VALUES ";
    ret += insert.query();
  } else {
    ret += "VALUES (0)";
  }
}

CONV_FN(Delete, del) {
  ret += "DELETE FROM ";
  ExprSchemaTableToString(ret, del.est());
  ret += " WHERE ";
  WhereStatementToString(ret, del.where());
}

CONV_FN(Truncate, trunc) {
  ret += "TRUNCATE ";
  if (trunc.has_est()) {
    ExprSchemaTableToString(ret, trunc.est());
  } else {
    ret += " ALL TABLES FROM s0";
  }
}

CONV_FN(CheckTable, ct) {
  ret += "CHECK TABLE ";
  ExprSchemaTableToString(ret, ct.est());
  if (ct.has_single_result()) {
    ret += " SETTINGS check_query_single_value_result = ";
    ret += ct.single_result() ? "1" : "0";
  }
}

CONV_FN(DescTable, dt) {
  ret += "DESCRIBE TABLE ";
  ExprSchemaTableToString(ret, dt.est());
  if (dt.has_sub_cols()) {
    ret += " SETTINGS describe_include_subcolumns = ";
    ret += dt.sub_cols() ? "1" : "0";
  }
}

CONV_FN(DeduplicateExpr, de) {
  ret += " DEDUPLICATE";
  if (de.has_col_list()) {
    ret += " BY ";
    ExprColumnListToString(ret, de.col_list());
  }
}

CONV_FN(OptimizeTable, ot) {
  ret += "OPTIMIZE TABLE ";
  ExprSchemaTableToString(ret, ot.est());
  if (ot.final()) {
    ret += " FINAL";
  }
  if (ot.has_dedup()) {
    DeduplicateExprToString(ret, ot.dedup());
  }
}

CONV_FN(ExchangeTables, et) {
  ret += "EXCHANGE TABLES ";
  ExprSchemaTableToString(ret, et.est1());
  ret += " AND ";
  ExprSchemaTableToString(ret, et.est2());
}

CONV_FN(UpdateSet, upt) {
  ColumnPathToString(ret, upt.col());
  ret += " = ";
  ExprToString(ret, upt.expr());
}

CONV_FN(Update, upt) {
  UpdateSetToString(ret, upt.update());
  for (int i = 0; i < upt.other_updates_size(); i++) {
    ret += ", ";
    UpdateSetToString(ret, upt.other_updates(i));
  }
  ret += " WHERE ";
  WhereStatementToString(ret, upt.where());
}

CONV_FN(AddWhere, add) {
  if (add.has_col()) {
    ret += " AFTER ";
    ColumnToString(ret, add.col());
  } else if (add.has_idx()) {
    ret += " AFTER ";
    IndexToString(ret, add.idx());
  } else if (add.has_first()) {
    ret += " FIRST";
  }
}

CONV_FN(AddColumn, add) {
  ColumnDefToString(ret, add.new_col());
  AddWhereToString(ret, add.add_where());
}

CONV_FN(AddIndex, add) {
  IndexDefToString(ret, add.new_idx());
  AddWhereToString(ret, add.add_where());
}

CONV_FN(AddStatistics, add) {
  ColumnListToString(ret, add.cols());
  ret += " TYPE ";
  ColumnStatisticsToString(ret, add.stats());
}

CONV_FN(RemoveColumnProperty, rcs) {
  ColumnToString(ret, rcs.col());
  ret += " REMOVE ";
  ret += RemoveColumnProperty_ColumnProperties_Name(rcs.property());
}

CONV_FN(ModifyColumnSetting, mcp) {
  ColumnToString(ret, mcp.col());
  ret += " MODIFY SETTING ";
  SettingValuesToString(ret, mcp.settings());
}

CONV_FN(SettingList, pl) {
  ret += pl.setting();
  for (int i = 0 ; i < pl.other_settings_size(); i++) {
    ret += ", ";
    ret += pl.other_settings(i);
  }
}

CONV_FN(RemoveColumnSetting, rcp) {
  ColumnToString(ret, rcp.col());
  ret += " RESET SETTING ";
  SettingListToString(ret, rcp.settings());
}

CONV_FN(AlterTableItem, alter) {
  ret += "(";
  using AlterType = AlterTableItem::AlterOneofCase;
  switch (alter.alter_oneof_case()) {
    case AlterType::kDel:
      ret += "DELETE WHERE ";
      WhereStatementToString(ret, alter.del());
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
      ColumnToString(ret, alter.materialize_column());
      break;
    case AlterType::kAddColumn:
      ret += "ADD COLUMN ";
      AddColumnToString(ret, alter.add_column());
      break;
    case AlterType::kDropColumn:
      ret += "DROP COLUMN ";
      ColumnToString(ret, alter.drop_column());
      break;
    case AlterType::kRenameColumn:
      ret += "RENAME COLUMN ";
      ColumnToString(ret, alter.rename_column().old_name());
      ret += " TO ";
      ColumnToString(ret, alter.rename_column().new_name());
      break;
    case AlterType::kModifyColumn:
      ret += "MODIFY COLUMN ";
      AddColumnToString(ret, alter.modify_column());
      break;
    case AlterType::kDeleteMask:
      ret += "APPLY DELETED MASK";
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
      ColumnListToString(ret, alter.drop_stats());
      break;
    case AlterType::kClearStats:
      ret += "CLEAR STATISTICS ";
      ColumnListToString(ret, alter.clear_stats());
      break;
    case AlterType::kMatStats:
      ret += "MATERIALIZE STATISTICS ";
      ColumnListToString(ret, alter.mat_stats());
      break;
    case AlterType::kMaterializeIndex:
      ret += "MATERIALIZE INDEX ";
      IndexToString(ret, alter.materialize_index());
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
      IndexToString(ret, alter.clear_index());
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
      ProjectionToString(ret, alter.materialize_projection());
      break;
    case AlterType::kClearProjection:
      ret += "CLEAR PROJECTION ";
      ProjectionToString(ret, alter.clear_projection());
      break;
    case AlterType::kAddConstraint:
      ret += "ADD ";
      ConstraintDefToString(ret, alter.add_constraint());
      break;
    case AlterType::kRemoveConstraint:
      ret += "DROP CONSTRAINT ";
      ConstraintToString(ret, alter.remove_constraint());
      break;
    case AlterType::kModifyQuery:
      ret += "MODIFY QUERY ";
      SelectToString(ret, alter.modify_query());
      break;
    default:
      ret += "DELETE WHERE TRUE";
  }
  ret += ")";
}

CONV_FN(AlterTable, alter_table) {
  ret += "ALTER TABLE ";
  ExprSchemaTableToString(ret, alter_table.est());
  ret += " ";
  AlterTableItemToString(ret, alter_table.alter());
  for (int i = 0; i < alter_table.other_alters_size(); i++) {
    ret += ", ";
    AlterTableItemToString(ret, alter_table.other_alters(i));
  }
}

CONV_FN(TopSelect, top) {
  SelectToString(ret, top.sel());
  if (top.has_format()) {
    ret += " FORMAT ";
    ret += OutFormat_Name(top.format()).substr(4);
  }
}

CONV_FN(CreateView, create_view) {
  const bool materialized = create_view.materialized();

  ret += "CREATE ";
  if (materialized) {
    ret += "MATERIALIZED ";
  }
  ret += "VIEW ";
  ExprSchemaTableToString(ret, create_view.est());
  if (materialized) {
    if (create_view.has_to_est()) {
      ret += " TO ";
      ExprSchemaTableToString(ret, create_view.to_est());
    }
    if (create_view.has_engine()) {
      TableEngineToString(ret, create_view.engine());
    }
    if (create_view.populate()) {
      ret += " POPULATE";
    }
  }
  ret += " AS ";
  SelectToString(ret, create_view.select());
}

CONV_FN(SQLQueryInner, query) {
  using QueryType = SQLQueryInner::QueryInnerOneofCase;
  switch (query.query_inner_oneof_case()) {
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
      DeleteToString(ret, query.del());
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
    default:
      TopSelectToString(ret, query.def_select());
  }
}

CONV_FN(ExplainQuery, explain) {
  ret += "EXPLAIN";
  if (explain.has_expl()) {
    ret += " ";
    ret += ExplainQuery_ExplainValues_Name(explain.expl());
    std::replace(ret.begin(), ret.end(), '_', ' ');
  }
  if (explain.has_expl() && explain.expl() <= sql_query_grammar::ExplainQuery_ExplainValues::ExplainQuery_ExplainValues_QUERY_TREE) {
    for (int i = 0; i < explain.opts_size(); i++) {
      std::string ostr = "";
      const sql_query_grammar::ExplainOption &eopt = explain.opts(i);

      if (i != 0) {
        ret += ",";
      }
      ret += " ";
      switch (explain.expl()) {
        case sql_query_grammar::ExplainQuery_ExplainValues::ExplainQuery_ExplainValues_PLAN:
          ostr += ExpPlanOpt_Name(static_cast<sql_query_grammar::ExpPlanOpt>((eopt.opt() % 5) + 1));
          break;
        case sql_query_grammar::ExplainQuery_ExplainValues::ExplainQuery_ExplainValues_PIPELINE:
          ostr += ExpPipelineOpt_Name(static_cast<sql_query_grammar::ExpPipelineOpt>((eopt.opt() % 3) + 1));
          break;
        case sql_query_grammar::ExplainQuery_ExplainValues::ExplainQuery_ExplainValues_QUERY_TREE:
          ostr += ExpQueryTreeOpt_Name(static_cast<sql_query_grammar::ExpQueryTreeOpt>((eopt.opt() % 3) + 1));
          break;
        default:
          assert(0);
      }
      ostr = ostr.substr(5);
      std::transform(ostr.begin(), ostr.end(), ostr.begin(), [](unsigned char c){ return std::tolower(c); });
      ret += ostr;
      ret += " = ";
      ret += eopt.val() ? "1" : "0";
    }
  }
  ret += " ";
  SQLQueryInnerToString(ret, explain.inner_query());
}

// ~~~~QUERY~~~~
CONV_FN(SQLQuery, query) {
  using QueryType = SQLQuery::QueryOneofCase;
  switch (query.query_oneof_case()) {
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
      SQLQueryInnerToString(ret, query.def_query());
  }
  ret += ";";
}

}
