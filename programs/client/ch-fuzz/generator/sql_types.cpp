#include "sql_types.h"
#include "statement_generator.h"

namespace chfuzz {

SQLType*
TypeDeepCopy(const SQLType *tp) {
	const IntType *it;
	const FloatType *ft;
	const DateType *dt;
	const DecimalType *decp;
	const StringType *st;
	const EnumType *et;
	const DynamicType *ddt;
	const JSONType *jt;
	const Nullable *nl;
	const LowCardinality *lc;
	const ArrayType *at;
	const MapType *mt;
	const TupleType *ttp;
	const VariantType *vtp;
	const NestedType *ntp;

	if (dynamic_cast<const BoolType*>(tp)) {
		return new BoolType();
	} else if ((it = dynamic_cast<const IntType*>(tp))) {
		return new IntType(it->size, it->is_unsigned);
	} else if ((ft = dynamic_cast<const FloatType*>(tp))) {
		return new FloatType(ft->size);
	} else if ((dt = dynamic_cast<const DateType*>(tp))) {
		return new DateType(dt->has_time, dt->extended);
	} else if ((decp = dynamic_cast<const DecimalType*>(tp))) {
		return new DecimalType(decp->precision, decp->scale);
	} else if ((st = dynamic_cast<const StringType*>(tp))) {
		return new StringType(st->precision);
	} else if (dynamic_cast<const UUIDType*>(tp)) {
		return new UUIDType();
	} else if ((et = dynamic_cast<const EnumType*>(tp))) {
		return new EnumType(et->size, et->values);
	} else if ((ddt = dynamic_cast<const DynamicType*>(tp))) {
		return new DynamicType(ddt->ntypes);
	} else if ((jt = dynamic_cast<const JSONType*>(tp))) {
		return new JSONType(jt->desc);
	} else if ((nl = dynamic_cast<const Nullable*>(tp))) {
		return new Nullable(TypeDeepCopy(nl->subtype));
 	} else if ((lc = dynamic_cast<const LowCardinality*>(tp))) {
		return new LowCardinality(TypeDeepCopy(lc->subtype));
	} else if ((at = dynamic_cast<const ArrayType*>(tp))) {
		return new ArrayType(TypeDeepCopy(at->subtype));
	} else if ((mt = dynamic_cast<const MapType*>(tp))) {
		return new MapType(TypeDeepCopy(mt->key), TypeDeepCopy(mt->value));
	} else if ((ttp = dynamic_cast<const TupleType*>(tp))) {
		std::vector<SubType> subtypes;

		for (const auto &entry : ttp->subtypes) {
			subtypes.push_back(SubType(entry.cname, TypeDeepCopy(entry.subtype)));
		}
		return new TupleType(std::move(subtypes));
	} else if ((vtp = dynamic_cast<const VariantType*>(tp))) {
		std::vector<SQLType*> subtypes;

		for (const auto &entry : vtp->subtypes) {
			subtypes.push_back(TypeDeepCopy(entry));
		}
		return new VariantType(std::move(subtypes));
	} else if ((ntp = dynamic_cast<const NestedType*>(tp))) {
		std::vector<NestedSubType> subtypes;

		for (const auto &entry : ntp->subtypes) {
			subtypes.push_back(NestedSubType(entry.cname, TypeDeepCopy(entry.subtype)));
		}
		return new NestedType(std::move(subtypes));
	} else {
		assert(0);
	}
	return nullptr;
}

std::tuple<SQLType*, sql_query_grammar::Integers>
RandomIntType(RandomGenerator &rg) {
	std::uniform_int_distribution<uint32_t> next_dist(1, 12);
	const uint32_t nopt = next_dist(rg.gen);

	switch (nopt) {
		case 1:
			return std::make_tuple(new IntType(8, true), sql_query_grammar::Integers::UInt8);
		case 2:
			return std::make_tuple(new IntType(16, true), sql_query_grammar::Integers::UInt16);
		case 3:
			return std::make_tuple(new IntType(32, true), sql_query_grammar::Integers::UInt32);
		case 4:
			return std::make_tuple(new IntType(64, true), sql_query_grammar::Integers::UInt64);
		case 5:
			return std::make_tuple(new IntType(128, true), sql_query_grammar::Integers::UInt128);
		case 6:
			return std::make_tuple(new IntType(256, true), sql_query_grammar::Integers::UInt256);
		case 7:
			return std::make_tuple(new IntType(8, false), sql_query_grammar::Integers::Int8);
		case 8:
			return std::make_tuple(new IntType(16, false), sql_query_grammar::Integers::Int16);
		case 9:
			return std::make_tuple(new IntType(32, false), sql_query_grammar::Integers::Int32);
		case 10:
			return std::make_tuple(new IntType(64, false), sql_query_grammar::Integers::Int64);
		case 11:
			return std::make_tuple(new IntType(128, false), sql_query_grammar::Integers::Int128);
		case 12:
			return std::make_tuple(new IntType(256, false), sql_query_grammar::Integers::Int256);
		default:
			assert(0);
	}
}

std::tuple<SQLType*, sql_query_grammar::FloatingPoints>
RandomFloatType(RandomGenerator &rg) {
	std::uniform_int_distribution<uint32_t> next_dist(1, 2);
	const uint32_t nopt = next_dist(rg.gen);

	switch (nopt) {
		case 1:
			return std::make_tuple(new FloatType(32), sql_query_grammar::FloatingPoints::Float32);
		case 2:
			return std::make_tuple(new FloatType(64), sql_query_grammar::FloatingPoints::Float64);
		default:
			assert(0);
	}
}

std::tuple<SQLType*, sql_query_grammar::Dates>
RandomDateType(RandomGenerator &rg, const bool low_card) {
	std::uniform_int_distribution<uint32_t> next_dist(1, low_card ? 3 : 4);
	const uint32_t nopt = next_dist(rg.gen);

	switch (nopt) {
		case 1:
			return std::make_tuple(new DateType(false, false), sql_query_grammar::Dates::Date);
		case 2:
			return std::make_tuple(new DateType(false, true), sql_query_grammar::Dates::Date32);
		case 3:
			return std::make_tuple(new DateType(true, false), sql_query_grammar::Dates::DateTime);
		case 4:
			return std::make_tuple(new DateType(true, true), sql_query_grammar::Dates::DateTime64);
		default:
			assert(0);
	}
}

SQLType* StatementGenerator::BottomType(RandomGenerator &rg, const uint32_t allowed_types, const bool low_card, sql_query_grammar::BottomTypeName *tp) {
	SQLType* res = nullptr;
	const uint32_t top_limit = low_card ? 4 : (8 + ((allowed_types & allow_json) ? 2 : 0) + ((allowed_types & allow_dynamic) ? 1 : 0));
	std::uniform_int_distribution<uint32_t> next_dist(1, top_limit);
	const uint32_t nopt = next_dist(rg.gen);

	switch (nopt) {
		case 1: {
			//int
			sql_query_grammar::Integers nint;

			std::tie(res, nint) = RandomIntType(rg);
			if (tp) {
				tp->set_integers(nint);
			}
		} break;
		case 2: {
			//float
			sql_query_grammar::FloatingPoints nflo;

			std::tie(res, nflo) = RandomFloatType(rg);
			if (tp) {
				tp->set_floats(nflo);
			}
		} break;
		case 3: {
			//dates
			sql_query_grammar::Dates dd;

			std::tie(res, dd) = RandomDateType(rg, low_card);
			if (tp) {
				tp->set_dates(dd);
			}
		} break;
		case 4: {
			//string
			std::optional<uint32_t> swidth = std::nullopt;

			if (rg.NextBool()) {
				if (tp) {
					tp->set_sql_string(true);
				}
			} else {
				swidth = std::optional<uint32_t>(rg.NextBool() ? rg.NextSmallNumber() : (rg.NextRandomUInt32() % 100));
				if (tp) {
					tp->set_fixed_string(swidth.value());
				}
			}
			res = new StringType(swidth);
		} break;
		case 5: {
			//decimal
			sql_query_grammar::Decimal *dec = tp ? tp->mutable_decimal() : nullptr;
			std::optional<uint32_t> precision = std::nullopt, scale = std::nullopt;

			if (rg.NextBool()) {
				precision = std::optional<uint32_t>((rg.NextRandomUInt32() % 10) + 1);

				if (dec) {
					dec->set_precision(precision.value());
				}
				if (rg.NextBool()) {
					scale = std::optional<uint32_t>(rg.NextRandomUInt32() % precision.value());
				}
			}
			res = new DecimalType(precision, scale);
		} break;
		case 6:
			//boolean
			if (tp) {
				tp->set_boolean(true);
			}
			res = new BoolType();
			break;
		case 7:
			if (allowed_types & allow_enum) {
				//Enum
				const bool bits = rg.NextBool();
				std::vector<int32_t> enum_values;
				const uint32_t nvalues = rg.NextSmallNumber();
				sql_query_grammar::EnumDef *edef = tp ? tp->mutable_enum_def() : nullptr;

				for (uint32_t i = 0 ; i < nvalues; i++) {
					const int32_t next = static_cast<int32_t>(bits ? rg.NextRandomInt16() : rg.NextRandomInt8());

					if (edef) {
						if (i == 0) {
							edef->set_first_value(next);
							edef->set_bits(bits);
						} else {
							edef->add_other_values(next);
						}
					}
					enum_values.push_back(next);
				}
				res = new EnumType(bits ? 16 : 8, std::move(enum_values));
			} else {
				//int
				sql_query_grammar::Integers nint;

				std::tie(res, nint) = RandomIntType(rg);
				if (tp) {
					tp->set_integers(nint);
				}
			}
			break;
		case 8:
			//uuid
			if (tp) {
				tp->set_uuid(true);
			}
			res = new UUIDType();
			break;
		case 9:
		case 10:
		case 11: {
			if ((allowed_types & allow_json) && (!(allowed_types & allow_dynamic) || nopt < 11)) {
				//json
				std::string desc = "";
				sql_query_grammar::JsonDef *jdef = tp ? tp->mutable_json() : nullptr;
				const uint32_t nclauses = rg.NextMediumNumber() % 7;

				if (nclauses) {
					desc += "(";
				}
				for (uint32_t i = 0 ; i < nclauses; i++) {
					const uint32_t noption = rg.NextSmallNumber();
					sql_query_grammar::JsonDefItem *jdi = tp ? jdef->add_spec() : nullptr;

					if (i != 0) {
						desc += ", ";
					}
					if (noption < 4) {
						const uint32_t max_dpaths = rg.NextBool() ? (rg.NextSmallNumber() % 5) : (rg.NextRandomUInt32() % 1025);

						if (tp) {
							jdi->set_max_dynamic_paths(max_dpaths);
						}
						desc += "max_dynamic_paths=";
						desc += std::to_string(max_dpaths);
					} else if (noption < 7) {
						const uint32_t max_dtypes = rg.NextBool() ? (rg.NextSmallNumber() % 5) : (rg.NextRandomUInt32() % 33);

						if (tp) {
							jdi->set_max_dynamic_types(max_dtypes);
						}
						desc += "max_dynamic_types=";
						desc += std::to_string(max_dtypes);
					} else if (this->depth >= this->max_depth || noption < 9) {
						const uint32_t nskips = (rg.NextMediumNumber() % 4) + 1;
						sql_query_grammar::ColumnPath *cp = tp ? jdi->mutable_skip_path() : nullptr;

						desc += "SKIP ";
						for (uint32_t j = 0 ; j < nskips; j++) {
							std::string nbuf;
							sql_query_grammar::Column *col = tp ? (j == 0 ? cp->mutable_col() : cp->add_sub_cols()) : nullptr;

							if (j != 0) {
								desc += ".";
							}
							desc += '`';
							rg.NextJsonCol(nbuf);
							desc += nbuf;
							desc += '`';
							if (tp) {
								col->set_column(std::move(nbuf));
							}
						}
					} else {
						uint32_t col_counter = 0;
						const uint32_t nskips = (rg.NextMediumNumber() % 4) + 1;
						sql_query_grammar::JsonPathType *jpt = tp ? jdi->mutable_path_type() : nullptr;
						sql_query_grammar::ColumnPath *cp = tp ? jpt->mutable_col() : nullptr;

						for (uint32_t j = 0 ; j < nskips; j++) {
							std::string nbuf;
							sql_query_grammar::Column *col = tp ? (j == 0 ? cp->mutable_col() : cp->add_sub_cols()) : nullptr;

							if (j != 0) {
								desc += ".";
							}
							desc += '`';
							rg.NextJsonCol(nbuf);
							desc += nbuf;
							desc += '`';
							if (tp) {
								col->set_column(std::move(nbuf));
							}
						}
						this->depth++;
						desc += " ";
						SQLType *jtp = RandomNextType(rg, ~(allow_nested|allow_enum), col_counter, tp ? jpt->mutable_type() : nullptr);
						desc += jtp->TypeName(false);
						delete jtp;
						this->depth--;
					}
				}
				if (nclauses) {
					desc += ")";
				}
				res = new JSONType(std::move(desc));
			} else if ((allowed_types & allow_dynamic) && (!(allowed_types & allow_json) || nopt == 11)) {
				//dynamic
				sql_query_grammar::Dynamic *dyn = tp ? tp->mutable_dynamic() : nullptr;
				std::optional<uint32_t> ntypes = std::nullopt;

				if (rg.NextBool()) {
					ntypes = std::optional<uint32_t>(rg.NextBool() ? (rg.NextSmallNumber() - 1) : (rg.NextRandomUInt32() % 100));
					if (dyn) {
						dyn->set_ntypes(ntypes.value());
					}
				}
				res = new DynamicType(ntypes);
			} else {
				assert(0);
			}
		} break;
		default:
			assert(0);
	}
	return res;
}

SQLType* StatementGenerator::GenerateArraytype(RandomGenerator &rg, const uint32_t allowed_types, uint32_t &col_counter, sql_query_grammar::TopTypeName *tp) {
	this->depth++;
	SQLType* k = this->RandomNextType(rg, allowed_types, col_counter, tp);
	this->depth--;
	return new ArrayType(k);
}

SQLType* StatementGenerator::GenerateArraytype(RandomGenerator &rg, const uint32_t allowed_types) {
	uint32_t col_counter = 0;

	return GenerateArraytype(rg, allowed_types, col_counter, nullptr);
}

SQLType* StatementGenerator::RandomNextType(RandomGenerator &rg, const uint32_t allowed_types, uint32_t &col_counter, sql_query_grammar::TopTypeName *tp) {
	const uint32_t noption = rg.NextMediumNumber();

	if ((allowed_types & allow_nullable) && noption < 21) {
		//nullable
		const bool lcard = rg.NextMediumNumber() < 18;
		SQLType* res = new Nullable(BottomType(rg, allowed_types & ~(allow_dynamic|allow_json), lcard, tp ? (lcard ? tp->mutable_nullable_lcard() : tp->mutable_nullable()) : nullptr));
		return lcard ? new LowCardinality(res) : res;
	} else if (noption < 71 || this->depth == this->max_depth) {
		//non nullable
		const bool lcard = rg.NextMediumNumber() < 18;
		SQLType* res = BottomType(rg, allowed_types, lcard, tp ? (lcard ? tp->mutable_non_nullable_lcard() : tp->mutable_non_nullable()) : nullptr);
		return lcard ? new LowCardinality(res) : res;
	} else if (noption < 77 || this->max_width <= this->width + 1) {
		//array
		return GenerateArraytype(rg, allowed_types & ~(allow_nested), col_counter, tp ? tp->mutable_array() : nullptr);
	} else if (noption < 83) {
		//map
		sql_query_grammar::MapType *mt = tp ? tp->mutable_map() : nullptr;

		this->depth++;
		SQLType* k = this->RandomNextType(rg, allowed_types & ~(allow_nullable|allow_nested), col_counter, mt ? mt->mutable_key() : nullptr);
		this->width++;
		SQLType* v = this->RandomNextType(rg, allowed_types & ~(allow_nested), col_counter, mt ? mt->mutable_value() : nullptr);
		this->depth--;
		this->width--;
		return new MapType(k, v);
	} else if (((allowed_types & (allow_variant|allow_nested)) == 0) || noption < 89) {
		//tuple
		sql_query_grammar::TupleType *tt = tp ? tp->mutable_tuple() : nullptr;
		const uint32_t ncols = (rg.NextMediumNumber() % (std::min<uint32_t>(5, this->max_width - this->width))) + UINT32_C(2);
		std::vector<SubType> subtypes;

		this->depth++;
		for (uint32_t i = 0 ; i < ncols ; i++) {
			const uint32_t cname = col_counter++;
			sql_query_grammar::TypeColumnDef *tcd = tp ? (
				(i == 0) ? tt->mutable_value1() : ((i == 1) ? tt->mutable_value2() : tt->add_others())) : nullptr;

			if (tcd) {
				tcd->mutable_col()->set_column("c" + std::to_string(cname));
			}
			SQLType *k = this->RandomNextType(rg, allowed_types & ~(allow_nested), col_counter, tcd ? tcd->mutable_type_name() : nullptr);
			subtypes.push_back(SubType(cname, k));
		}
		this->depth--;
		return new TupleType(subtypes);
	} else if ((allowed_types & allow_variant) && (!(allowed_types & allow_nested) || noption < 95)) {
		//variant
		sql_query_grammar::VariantType *vt = tp ? tp->mutable_variant() : nullptr;
		const uint32_t ncols = (rg.NextMediumNumber() % (std::min<uint32_t>(5, this->max_width - this->width))) + UINT32_C(2);
		std::vector<SQLType*> subtypes;

		this->depth++;
		for (uint32_t i = 0 ; i < ncols ; i++) {
			sql_query_grammar::TopTypeName *ttn = tp ? (
				(i == 0) ? vt->mutable_value1() : ((i == 1) ? vt->mutable_value2() : vt->add_others())) : nullptr;

			subtypes.push_back(this->RandomNextType(rg, allowed_types & ~(allow_nullable|allow_nested|allow_variant|allow_dynamic), col_counter, ttn));
		}
		this->depth--;
		return new VariantType(subtypes);
	} else if ((allowed_types & allow_nested)) {
		//nested
		sql_query_grammar::NestedType *nt = tp ? tp->mutable_nested() : nullptr;
		const uint32_t ncols = (rg.NextMediumNumber() % (std::min<uint32_t>(5, this->max_width - this->width))) + UINT32_C(1);
		std::vector<NestedSubType> subtypes;

		this->depth++;
		for (uint32_t i = 0 ; i < ncols ; i++) {
			const uint32_t cname = col_counter++;
			sql_query_grammar::TypeColumnDef *tcd = tp ? (
				(i == 0) ? nt->mutable_type1() : nt->add_others()) : nullptr;

			if (tcd) {
				tcd->mutable_col()->set_column("c" + std::to_string(cname));
			}
			SQLType *k = this->RandomNextType(rg, allowed_types & ~(allow_nested), col_counter, tcd ? tcd->mutable_type_name() : nullptr);
			subtypes.push_back(NestedSubType(cname, k));
		}
		this->depth--;
		return new NestedType(subtypes);
	} else {
		assert(0);
	}
	return nullptr;
}

SQLType* StatementGenerator::RandomNextType(RandomGenerator &rg, const uint32_t allowed_types) {
	uint32_t col_counter = 0;

	return RandomNextType(rg, allowed_types, col_counter, nullptr);
}

void StatementGenerator::AppendDecimal(RandomGenerator &rg, std::string &ret, const uint32_t left, const uint32_t right) {
	ret += rg.NextBool() ? "-" : "";
	if (left > 0) {
		std::uniform_int_distribution<uint32_t> next_dist(1, left);
		const uint32_t nlen = next_dist(rg.gen);

		ret += std::max<char>(rg.NextDigit(), '1');
		for (uint32_t j = 1; j < nlen; j++) {
			ret += rg.NextDigit();
		}
	} else {
		ret += "0";
	}
	ret += ".";
	if (right > 0) {
		std::uniform_int_distribution<uint32_t> next_dist(1, right);
		const uint32_t nlen = next_dist(rg.gen);

		for (uint32_t j = 0; j < nlen; j++) {
			ret += rg.NextDigit();
		}
	} else {
		ret += "0";
	}
}

void StatementGenerator::StrAppendBottomValue(RandomGenerator &rg, std::string &ret, SQLType* tp) {
	IntType *itp;
	DateType *dtp;
	DecimalType *detp;
	StringType *stp;
	EnumType *etp;

	if ((itp = dynamic_cast<IntType*>(tp))) {
		if (itp->is_unsigned) {
			switch (itp->size) {
				case 8:
					ret += std::to_string(rg.NextRandomUInt8());
					break;
				case 16:
					ret += std::to_string(rg.NextRandomUInt16());
					break;
				case 32:
					ret += std::to_string(rg.NextRandomUInt32());
					break;
				default:
					ret += std::to_string(rg.NextRandomUInt64());
					break;
			}
		} else {
			switch (itp->size) {
				case 8:
					ret += std::to_string(rg.NextRandomInt8());
					break;
				case 16:
					ret += std::to_string(rg.NextRandomInt16());
					break;
				case 32:
					ret += std::to_string(rg.NextRandomInt32());
					break;
				default:
					ret += std::to_string(rg.NextRandomInt64());
					break;
			}
		}
	} else if (dynamic_cast<FloatType*>(tp)) {
		const uint32_t next_option = rg.NextLargeNumber();

		if (next_option < 25) {
			if (next_option < 17) {
				ret += next_option < 9 ? "+" : "-";
			}
			ret += "nan";
		} else if (next_option < 49) {
			if (next_option < 41) {
				ret += next_option < 33 ? "+" : "-";
			}
			ret += "inf";
		} else {
			ret += std::to_string(rg.NextRandomDouble());
		}
	} else if ((dtp = dynamic_cast<DateType*>(tp))) {
		ret += "'";
		if (dtp->has_time) {
			if (dtp->extended) {
				rg.NextDateTime64(ret);
			} else {
				rg.NextDateTime(ret);
			}
		} else {
			if (dtp->extended) {
				rg.NextDate32(ret);
			} else {
				rg.NextDate(ret);
			}
		}
		ret += "'";
	} else if ((detp = dynamic_cast<DecimalType*>(tp))) {
		const uint32_t right = detp->scale.value_or(0), left = detp->precision.value_or(10) - right;

		AppendDecimal(rg, ret, left, right);
	} else if ((stp = dynamic_cast<StringType*>(tp))) {
		const uint32_t limit = stp->precision.value_or(100000);

		ret += "'";
		rg.NextString(ret, limit);
		ret += "'";
	} else if (dynamic_cast<BoolType*>(tp)) {
		ret += rg.NextBool() ? "TRUE" : "FALSE";
	} else if ((etp = dynamic_cast<EnumType*>(tp))) {
		const int32_t nvalue = rg.PickRandomlyFromVector(etp->values);

		ret += "'";
		ret += std::to_string(nvalue);
		ret += "'";
	} else if (dynamic_cast<UUIDType*>(tp)) {
		ret += "'";
		rg.NextUUID(ret);
		ret += "'";
	} else {
		assert(0);
	}
}

void StatementGenerator::StrAppendMap(RandomGenerator &rg, std::string &ret, MapType *mt) {
	const uint32_t limit = (rg.NextSmallNumber() - 1);

	ret += "map(";
	for (uint32_t i = 0 ; i < limit; i++) {
		if (i != 0) {
			ret += ", ";
		}
		StrAppendAnyValueInternal(rg, ret, mt->key);
		ret += ",";
		StrAppendAnyValueInternal(rg, ret, mt->value);
	}
	ret += ")";
}

void StatementGenerator::StrAppendArray(RandomGenerator &rg, std::string &ret, ArrayType *at) {
	const uint32_t limit = (rg.NextSmallNumber() - 1);

	ret += "[";
	for (uint32_t i = 0 ; i < limit; i++) {
		if (i != 0) {
			ret += ", ";
		}
		StrAppendAnyValueInternal(rg, ret, at->subtype);
	}
	ret += "]";
}

void StatementGenerator::StrAppendTuple(RandomGenerator &rg, std::string &ret, TupleType *at) {
	ret += "(";
	for (uint32_t i = 0 ; i < at->subtypes.size(); i++) {
		if (i != 0) {
			ret += ", ";
		}
		StrAppendAnyValueInternal(rg, ret, at->subtypes[i].subtype);
	}
	ret += ")";
}

void StatementGenerator::StrAppendVariant(RandomGenerator &rg, std::string &ret, VariantType *vtp) {
	StrAppendAnyValueInternal(rg, ret, rg.PickRandomlyFromVector(vtp->subtypes));
}

void StatementGenerator::StrBuildJSONArray(RandomGenerator &rg, const int jdepth, const int jwidth, std::string &ret) {
	std::uniform_int_distribution<int> jopt(1, 3);
	int nelems = 0, next_width = 0;

	if (jwidth) {
		std::uniform_int_distribution<int> alen(0, jwidth);
		nelems = alen(rg.gen);
	}
	ret += "[";
	next_width = nelems;
	for (int j = 0 ; j < nelems ; j++) {
		if (j != 0) {
			ret += ",";
		}
		if (jdepth) {
			const int noption = jopt(rg.gen);

			switch (noption) {
			case 1: //object
				StrBuildJSON(rg, jdepth - 1, next_width, ret);
				break;
			case 2: //array
				StrBuildJSONArray(rg, jdepth - 1, next_width, ret);
				break;
			case 3: //others
				StrBuildJSONElement(rg, ret);
				break;
			default:
				assert(0);
			}
		} else {
			StrBuildJSONElement(rg, ret);
		}
		next_width--;
	}
	ret += "]";
}

void StatementGenerator::StrBuildJSONElement(RandomGenerator &rg, std::string &ret) {
	std::uniform_int_distribution<int> opts(1, 12);
	const int noption = opts(rg.gen);

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
		case 4: //large number
			ret += std::to_string(rg.NextRandomInt64());
			break;
		case 5: //large unsigned number
			ret += std::to_string(rg.NextRandomUInt64());
			break;
		case 6:
		case 7: { //small number
			std::uniform_int_distribution<int> numbers(-1000, 1000);
			ret += std::to_string(numbers(rg.gen));
		} break;
		case 8: //date
			ret += '"';
			if (noption < 251) {
				rg.NextDate(ret);
			} else if (noption < 301) {
				rg.NextDate32(ret);
			} else if (noption < 351) {
				rg.NextDateTime(ret);
			} else {
				rg.NextDateTime64(ret);
			}
			ret += '"';
			break;
		case 9: { //decimal
			std::uniform_int_distribution<uint32_t> next_dist(0, 30);
			const uint32_t left = next_dist(rg.gen), right = next_dist(rg.gen);

			AppendDecimal(rg, ret, left, right);
		} break;
		case 10:
		case 11:
		case 12: { //string
			std::uniform_int_distribution<int> slen(0, 200);
			std::uniform_int_distribution<uint8_t> chars(32, 127);
			const int nlen = slen(rg.gen);

			ret += '"';
			for (int i = 0 ; i < nlen ; i++) {
				const uint8_t nchar = chars(rg.gen);

				switch (nchar) {
				case 127:
					ret += "😂";
					break;
				case static_cast<int>('"'):
					ret += "a";
					break;
				case static_cast<int>('\\'):
					ret += "b";
					break;
				case static_cast<int>('\''):
					ret += "''";
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

void StatementGenerator::StrBuildJSON(RandomGenerator &rg, const int jdepth, const int jwidth, std::string &ret) {
	ret += "{";
	if (jdepth && jwidth && rg.NextSmallNumber() < 9) {
		std::uniform_int_distribution<int> childd(1, jwidth);
		const int nchildren = childd(rg.gen);

		for (int i = 0 ; i < nchildren ; i++) {
			std::uniform_int_distribution<int> jopt(1, 3);
			const int noption = jopt(rg.gen);

			if (i != 0) {
				ret += ",";
			}
			ret += "\"";
			rg.NextJsonCol(ret);
			ret += "\":";
			switch (noption) {
			case 1: //object
				StrBuildJSON(rg, jdepth - 1, jwidth, ret);
				break;
			case 2: //array
				StrBuildJSONArray(rg, jdepth - 1, jwidth, ret);
				break;
			case 3: //others
				StrBuildJSONElement(rg, ret);
				break;
			default:
				assert(0);
			}
		}
	}
	ret += "}";
}

void StatementGenerator::StrAppendAnyValueInternal(RandomGenerator &rg, std::string &ret, SQLType *tp) {
	MapType *mt;
	Nullable *nl;
	ArrayType *at;
	TupleType *ttp;
	VariantType *vtp;
	LowCardinality *lc;

	if (rg.NextMediumNumber() < 6) {
		ret += "NULL";
	} else if (dynamic_cast<IntType*>(tp) || dynamic_cast<FloatType*>(tp) || dynamic_cast<DateType*>(tp) ||
			   dynamic_cast<DecimalType*>(tp) || dynamic_cast<StringType*>(tp) || dynamic_cast<BoolType*>(tp) ||
			   dynamic_cast<EnumType*>(tp) || dynamic_cast<UUIDType*>(tp)) {
		StrAppendBottomValue(rg, ret, tp);
	} else if ((lc = dynamic_cast<LowCardinality*>(tp))) {
		StrAppendAnyValueInternal(rg, ret, lc->subtype);
	} else if ((nl = dynamic_cast<Nullable*>(tp))) {
		StrAppendAnyValueInternal(rg, ret, nl->subtype);
	} else if (dynamic_cast<JSONType*>(tp)) {
		std::uniform_int_distribution<int> dopt(1, this->max_depth), wopt(1, this->max_width);

		ret += "'";
		StrBuildJSON(rg, dopt(rg.gen), wopt(rg.gen), ret);
		ret += "'";
	} else if (dynamic_cast<DynamicType*>(tp)) {
		uint32_t col_counter = 0;
		SQLType *next = RandomNextType(rg, allow_nullable|allow_json, col_counter, nullptr);

		StrAppendAnyValueInternal(rg, ret, next);
		delete next;
	} else if (this->depth == this->max_depth) {
		ret += "1";
	} else if ((mt = dynamic_cast<MapType*>(tp))) {
		this->depth++;
		StrAppendMap(rg, ret, mt);
		this->depth--;
	} else if ((at = dynamic_cast<ArrayType*>(tp))) {
		this->depth++;
		StrAppendArray(rg, ret, at);
		this->depth--;
	} else if ((ttp = dynamic_cast<TupleType*>(tp))) {
		this->depth++;
		StrAppendTuple(rg, ret, ttp);
		this->depth--;
	} else if ((vtp = dynamic_cast<VariantType*>(tp))) {
		this->depth++;
		StrAppendVariant(rg, ret, vtp);
		this->depth--;
	} else {
		//no nested types here
		assert(0);
	}
}

void StatementGenerator::StrAppendAnyValue(RandomGenerator &rg, std::string &ret, SQLType *tp) {
	StrAppendAnyValueInternal(rg, ret, tp);
	if (rg.NextSmallNumber() < 7) {
		ret += "::";
		ret += tp->TypeName(false);
	}
}

}
