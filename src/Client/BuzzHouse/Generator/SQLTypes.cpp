#include <cstdint>
#include <sys/types.h>

#include <Client/BuzzHouse/Generator/SQLTypes.h>
#include <Client/BuzzHouse/Generator/StatementGenerator.h>
#include <Client/BuzzHouse/Utils/HugeInt.h>
#include <Client/BuzzHouse/Utils/UHugeInt.h>

namespace BuzzHouse
{

SQLType * TypeDeepCopy(SQLType * tp)
{
    IntType * it;
    FloatType * ft;
    DateType * dt;
    DateTimeType * dtt;
    DecimalType * decp;
    StringType * st;
    EnumType * et;
    DynamicType * ddt;
    JSONType * jt;
    Nullable * nl;
    LowCardinality * lc;
    GeoType * gt;
    ArrayType * at;
    MapType * mt;
    TupleType * ttp;
    VariantType * vtp;
    NestedType * ntp;

    if (dynamic_cast<const BoolType *>(tp))
    {
        return new BoolType();
    }
    else if ((it = dynamic_cast<IntType *>(tp)))
    {
        return new IntType(it->size, it->is_unsigned);
    }
    else if ((ft = dynamic_cast<FloatType *>(tp)))
    {
        return new FloatType(ft->size);
    }
    else if ((dt = dynamic_cast<DateType *>(tp)))
    {
        return new DateType(dt->extended);
    }
    else if ((dtt = dynamic_cast<DateTimeType *>(tp)))
    {
        return new DateTimeType(dtt->extended, dtt->precision, dtt->timezone);
    }
    else if ((decp = dynamic_cast<DecimalType *>(tp)))
    {
        return new DecimalType(decp->precision, decp->scale);
    }
    else if ((st = dynamic_cast<StringType *>(tp)))
    {
        return new StringType(st->precision);
    }
    else if (dynamic_cast<UUIDType *>(tp))
    {
        return new UUIDType();
    }
    else if (dynamic_cast<IPv4Type *>(tp))
    {
        return new IPv4Type();
    }
    else if (dynamic_cast<IPv6Type *>(tp))
    {
        return new IPv6Type();
    }
    else if ((et = dynamic_cast<EnumType *>(tp)))
    {
        return new EnumType(et->size, et->values);
    }
    else if ((ddt = dynamic_cast<DynamicType *>(tp)))
    {
        return new DynamicType(ddt->ntypes);
    }
    else if ((jt = dynamic_cast<JSONType *>(tp)))
    {
        std::vector<JSubType> jsubcols;

        jsubcols.reserve(jt->subcols.size());
        for (const auto & entry : jt->subcols)
        {
            jsubcols.push_back(JSubType(entry.cname, TypeDeepCopy(entry.subtype)));
        }
        return new JSONType(jt->desc, std::move(jsubcols));
    }
    else if ((nl = dynamic_cast<Nullable *>(tp)))
    {
        return new Nullable(TypeDeepCopy(nl->subtype));
    }
    else if ((lc = dynamic_cast<LowCardinality *>(tp)))
    {
        return new LowCardinality(TypeDeepCopy(lc->subtype));
    }
    else if ((gt = dynamic_cast<GeoType *>(tp)))
    {
        return new GeoType(gt->geo_type);
    }
    else if ((at = dynamic_cast<ArrayType *>(tp)))
    {
        return new ArrayType(TypeDeepCopy(at->subtype));
    }
    else if ((mt = dynamic_cast<MapType *>(tp)))
    {
        return new MapType(TypeDeepCopy(mt->key), TypeDeepCopy(mt->value));
    }
    else if ((ttp = dynamic_cast<TupleType *>(tp)))
    {
        std::vector<SubType> subtypes;

        subtypes.reserve(ttp->subtypes.size());
        for (const auto & entry : ttp->subtypes)
        {
            subtypes.push_back(SubType(entry.cname, TypeDeepCopy(entry.subtype)));
        }
        return new TupleType(std::move(subtypes));
    }
    else if ((vtp = dynamic_cast<VariantType *>(tp)))
    {
        std::vector<SQLType *> subtypes;

        subtypes.reserve(vtp->subtypes.size());
        for (const auto & entry : vtp->subtypes)
        {
            subtypes.push_back(TypeDeepCopy(entry));
        }
        return new VariantType(std::move(subtypes));
    }
    else if ((ntp = dynamic_cast<NestedType *>(tp)))
    {
        std::vector<NestedSubType> subtypes;

        subtypes.reserve(ntp->subtypes.size());
        for (const auto & entry : ntp->subtypes)
        {
            subtypes.push_back(NestedSubType(entry.cname, TypeDeepCopy(entry.subtype)));
        }
        return new NestedType(std::move(subtypes));
    }
    else
    {
        assert(0);
    }
    return nullptr;
}

std::tuple<SQLType *, Integers> StatementGenerator::randomIntType(RandomGenerator & rg, const uint32_t allowed_types)
{
    assert(this->ids.empty());

    if ((allowed_types & allow_unsigned_int))
    {
        if ((allowed_types & allow_int8))
        {
            this->ids.push_back(1);
        }
        this->ids.push_back(2);
        this->ids.push_back(3);
        if ((allowed_types & allow_int64))
        {
            this->ids.push_back(4);
        }
        if ((allowed_types & allow_int128))
        {
            this->ids.push_back(5);
            this->ids.push_back(6);
        }
    }
    if ((allowed_types & allow_int8))
    {
        this->ids.push_back(7);
    }
    this->ids.push_back(8);
    this->ids.push_back(9);
    if ((allowed_types & allow_int64))
    {
        this->ids.push_back(10);
    }
    if ((allowed_types & allow_int128))
    {
        this->ids.push_back(11);
        this->ids.push_back(12);
    }
    const uint32_t nopt = rg.pickRandomlyFromVector(this->ids);
    this->ids.clear();
    switch (nopt)
    {
        case 1:
            return std::make_tuple(new IntType(8, true), Integers::UInt8);
        case 2:
            return std::make_tuple(new IntType(16, true), Integers::UInt16);
        case 3:
            return std::make_tuple(new IntType(32, true), Integers::UInt32);
        case 4:
            return std::make_tuple(new IntType(64, true), Integers::UInt64);
        case 5:
            return std::make_tuple(new IntType(128, true), Integers::UInt128);
        case 6:
            return std::make_tuple(new IntType(256, true), Integers::UInt256);
        case 7:
            return std::make_tuple(new IntType(8, false), Integers::Int8);
        case 8:
            return std::make_tuple(new IntType(16, false), Integers::Int16);
        case 9:
            return std::make_tuple(new IntType(32, false), Integers::Int32);
        case 10:
            return std::make_tuple(new IntType(64, false), Integers::Int64);
        case 11:
            return std::make_tuple(new IntType(128, false), Integers::Int128);
        case 12:
            return std::make_tuple(new IntType(256, false), Integers::Int256);
        default:
            assert(0);
    }
    return std::make_tuple(new IntType(32, false), Integers::Int32);
}

std::tuple<SQLType *, FloatingPoints> StatementGenerator::randomFloatType(RandomGenerator & rg) const
{
    const uint32_t nopt = (rg.nextSmallNumber() % 3) + 1; //1 to 3
    return std::make_tuple(new FloatType(1 << (nopt + 3)), static_cast<FloatingPoints>(nopt));
}

std::tuple<SQLType *, Dates> StatementGenerator::randomDateType(RandomGenerator & rg, const uint32_t allowed_types) const
{
    const bool use32 = (allowed_types & allow_date32) && rg.nextBool();
    return std::make_tuple(new DateType(use32), use32 ? Dates::Date32 : Dates::Date);
}

SQLType * StatementGenerator::randomDateTimeType(RandomGenerator & rg, const uint32_t allowed_types, DateTimeTp * dt) const
{
    bool has_precision = false;
    const bool use64 = (allowed_types & allow_datetime64) && rg.nextBool();
    std::optional<uint32_t> precision = std::nullopt;
    std::optional<std::string> timezone = std::nullopt;

    if (dt)
    {
        dt->set_type(use64 ? DateTimes::DateTime64 : DateTimes::DateTime);
    }
    if (use64 && (has_precision = (!(allowed_types & set_any_datetime_precision) || rg.nextSmallNumber() < 5)))
    {
        precision = std::optional<uint32_t>(!(allowed_types & set_any_datetime_precision) ? 6 : (rg.nextSmallNumber() - 1));
        if (dt)
        {
            dt->set_precision(precision.value());
        }
    }
    if ((!use64 || has_precision) && !fc.timezones.empty() && rg.nextSmallNumber() < 5)
    {
        timezone = std::optional<std::string>(rg.pickRandomlyFromVector(fc.timezones));
        if (dt)
        {
            dt->set_timezone(timezone.value());
        }
    }
    return new DateTimeType(use64, precision, timezone);
}

SQLType * StatementGenerator::bottomType(RandomGenerator & rg, const uint32_t allowed_types, const bool low_card, BottomTypeName * tp)
{
    SQLType * res = nullptr;

    const uint32_t int_type = 40;
    const uint32_t floating_point_type
        = 10 * static_cast<uint32_t>((allowed_types & allow_floating_points) != 0 && this->fc.fuzz_floating_points);
    const uint32_t date_type = 15 * static_cast<uint32_t>((allowed_types & allow_dates) != 0);
    const uint32_t datetime_type = 15 * static_cast<uint32_t>((allowed_types & allow_datetimes) != 0);
    const uint32_t string_type = 30 * static_cast<uint32_t>((allowed_types & allow_strings) != 0);
    const uint32_t decimal_type = 20 * static_cast<uint32_t>(!low_card && (allowed_types & allow_decimals) != 0);
    const uint32_t bool_type = 20 * static_cast<uint32_t>(!low_card && (allowed_types & allow_bool) != 0);
    const uint32_t enum_type = 20 * static_cast<uint32_t>(!low_card && (allowed_types & allow_enum) != 0);
    const uint32_t uuid_type = 10 * static_cast<uint32_t>(!low_card && (allowed_types & allow_uuid) != 0);
    const uint32_t ipv4_type = 5 * static_cast<uint32_t>(!low_card && (allowed_types & allow_ipv4) != 0);
    const uint32_t ipv6_type = 5 * static_cast<uint32_t>(!low_card && (allowed_types & allow_ipv6) != 0);
    const uint32_t j_type = 20 * static_cast<uint32_t>(!low_card && (allowed_types & allow_JSON) != 0);
    const uint32_t dynamic_type = 30 * static_cast<uint32_t>(!low_card && (allowed_types & allow_dynamic) != 0);
    const uint32_t prob_space = int_type + floating_point_type + date_type + datetime_type + string_type + decimal_type + bool_type
        + enum_type + uuid_type + ipv4_type + ipv6_type + j_type + dynamic_type;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);

    if (nopt < (int_type + 1))
    {
        Integers nint;

        std::tie(res, nint) = randomIntType(rg, allowed_types);
        if (tp)
        {
            tp->set_integers(nint);
        }
    }
    else if (floating_point_type && nopt < (int_type + floating_point_type + 1))
    {
        FloatingPoints nflo;

        std::tie(res, nflo) = randomFloatType(rg);
        if (tp)
        {
            tp->set_floats(nflo);
        }
    }
    else if (date_type && nopt < (int_type + floating_point_type + date_type + 1))
    {
        Dates dd;

        std::tie(res, dd) = randomDateType(rg, allowed_types);
        if (tp)
        {
            tp->set_dates(dd);
        }
    }
    else if (datetime_type && nopt < (int_type + floating_point_type + date_type + datetime_type + 1))
    {
        DateTimeTp * dtp = tp ? tp->mutable_datetimes() : nullptr;

        res = randomDateTimeType(rg, low_card ? (allowed_types & ~(allow_datetime64)) : allowed_types, dtp);
    }
    else if (string_type && nopt < (int_type + floating_point_type + date_type + datetime_type + string_type + 1))
    {
        std::optional<uint32_t> swidth = std::nullopt;

        if (rg.nextBool())
        {
            if (tp)
            {
                tp->set_standard_string(true);
            }
        }
        else
        {
            swidth = std::optional<uint32_t>(rg.nextBool() ? rg.nextSmallNumber() : (rg.nextRandomUInt32() % 100));
            if (tp)
            {
                tp->set_fixed_string(swidth.value());
            }
        }
        res = new StringType(swidth);
    }
    else if (decimal_type && nopt < (int_type + floating_point_type + date_type + datetime_type + string_type + decimal_type + 1))
    {
        Decimal * dec = tp ? tp->mutable_decimal() : nullptr;
        std::optional<uint32_t> precision = std::nullopt;
        std::optional<uint32_t> scale = std::nullopt;

        if (rg.nextBool())
        {
            precision = std::optional<uint32_t>((rg.nextRandomUInt32() % 10) + 1);

            if (dec)
            {
                dec->set_precision(precision.value());
            }
            if (rg.nextBool())
            {
                scale = std::optional<uint32_t>(rg.nextRandomUInt32() % (precision.value() + 1));
                if (dec)
                {
                    dec->set_scale(scale.value());
                }
            }
        }
        res = new DecimalType(precision, scale);
    }
    else if (bool_type && nopt < (int_type + floating_point_type + date_type + datetime_type + string_type + decimal_type + bool_type + 1))
    {
        if (tp)
        {
            tp->set_boolean(true);
        }
        res = new BoolType();
    }
    else if (
        enum_type
        && nopt < (int_type + floating_point_type + date_type + datetime_type + string_type + decimal_type + bool_type + enum_type + 1))
    {
        const bool bits16 = rg.nextBool();
        std::vector<EnumValue> evs;
        const uint32_t nvalues = (rg.nextLargeNumber() % static_cast<uint32_t>(enum_values.size())) + 1;
        EnumDef * edef = tp ? tp->mutable_enum_def() : nullptr;

        if (edef)
        {
            edef->set_bits(bits16);
        }
        std::shuffle(enum_values.begin(), enum_values.end(), rg.generator);
        if (bits16)
        {
            std::shuffle(enum16_ids.begin(), enum16_ids.end(), rg.generator);
        }
        else
        {
            std::shuffle(enum8_ids.begin(), enum8_ids.end(), rg.generator);
        }
        for (uint32_t i = 0; i < nvalues; i++)
        {
            const std::string & nval = enum_values[i];
            const int32_t num = static_cast<const int32_t>(bits16 ? enum16_ids[i] : enum8_ids[i]);

            if (edef)
            {
                EnumDefValue * edf = i == 0 ? edef->mutable_first_value() : edef->add_other_values();

                edf->set_number(num);
                edf->set_enumv(nval);
            }
            evs.push_back(EnumValue(nval, num));
        }
        res = new EnumType(bits16 ? 16 : 8, evs);
    }
    else if (
        uuid_type
        && nopt
            < (int_type + floating_point_type + date_type + datetime_type + string_type + decimal_type + bool_type + enum_type + uuid_type
               + 1))
    {
        if (tp)
        {
            tp->set_uuid(true);
        }
        res = new UUIDType();
    }
    else if (
        ipv4_type
        && nopt
            < (int_type + floating_point_type + date_type + datetime_type + string_type + decimal_type + bool_type + enum_type + uuid_type
               + ipv4_type + 1))
    {
        if (tp)
        {
            tp->set_ipv4(true);
        }
        res = new IPv4Type();
    }
    else if (
        ipv6_type
        && nopt
            < (int_type + floating_point_type + date_type + datetime_type + string_type + decimal_type + bool_type + enum_type + uuid_type
               + ipv4_type + ipv6_type + 1))
    {
        if (tp)
        {
            tp->set_ipv6(true);
        }
        res = new IPv6Type();
    }
    else if (
        j_type
        && nopt
            < (int_type + floating_point_type + date_type + datetime_type + string_type + decimal_type + bool_type + enum_type + uuid_type
               + ipv4_type + ipv6_type + j_type + 1))
    {
        std::string desc;
        std::vector<JSubType> subcols;
        JSONDef * jdef = tp ? tp->mutable_jdef() : nullptr;
        const uint32_t nclauses = rg.nextMediumNumber() % 7;

        if (nclauses)
        {
            desc += "(";
        }
        this->depth++;
        for (uint32_t i = 0; i < nclauses; i++)
        {
            const uint32_t noption = rg.nextSmallNumber();
            JSONDefItem * jdi = tp ? jdef->add_spec() : nullptr;

            if (i != 0)
            {
                desc += ", ";
            }
            if (noption < 4)
            {
                const uint32_t max_dpaths = rg.nextBool() ? (rg.nextSmallNumber() % 5) : (rg.nextRandomUInt32() % 1025);

                if (tp)
                {
                    jdi->set_max_dynamic_paths(max_dpaths);
                }
                desc += "max_dynamic_paths=";
                desc += std::to_string(max_dpaths);
            }
            else if (this->depth >= this->fc.max_depth || noption < 8)
            {
                const uint32_t max_dtypes = rg.nextBool() ? (rg.nextSmallNumber() % 5) : (rg.nextRandomUInt32() % 33);

                if (tp)
                {
                    jdi->set_max_dynamic_types(max_dtypes);
                }
                desc += "max_dynamic_types=";
                desc += std::to_string(max_dtypes);
            }
            else
            {
                uint32_t col_counter = 0;
                const uint32_t ncols = (rg.nextMediumNumber() % 4) + 1;
                JSONPathType * jpt = tp ? jdi->mutable_path_type() : nullptr;
                ColumnPath * cp = tp ? jpt->mutable_col() : nullptr;
                std::string npath;

                for (uint32_t j = 0; j < ncols; j++)
                {
                    std::string nbuf;
                    Column * col = tp ? (j == 0 ? cp->mutable_col() : cp->add_sub_cols()) : nullptr;

                    if (j != 0)
                    {
                        desc += ".";
                        npath += ".";
                    }
                    desc += '`';
                    rg.nextJSONCol(nbuf);
                    npath += nbuf;
                    desc += nbuf;
                    desc += '`';
                    if (tp)
                    {
                        col->set_column(std::move(nbuf));
                    }
                }
                desc += " ";
                SQLType * jtp = randomNextType(rg, ~(allow_nested | allow_enum), col_counter, tp ? jpt->mutable_type() : nullptr);
                jtp->typeName(desc, false);
                subcols.push_back(JSubType(npath, jtp));
            }
        }
        this->depth--;
        if (nclauses)
        {
            desc += ")";
        }
        res = new JSONType(desc, subcols);
    }
    else if (
        dynamic_type
        && nopt
            < (int_type + floating_point_type + date_type + datetime_type + string_type + decimal_type + bool_type + enum_type + uuid_type
               + ipv4_type + ipv6_type + j_type + dynamic_type + 1))
    {
        Dynamic * dyn = tp ? tp->mutable_dynamic() : nullptr;
        std::optional<uint32_t> ntypes = std::nullopt;

        if (rg.nextBool())
        {
            ntypes = std::optional<uint32_t>(rg.nextBool() ? rg.nextSmallNumber() : ((rg.nextRandomUInt32() % 100) + 1));
            if (dyn)
            {
                dyn->set_ntypes(ntypes.value());
            }
        }
        res = new DynamicType(ntypes);
    }
    else
    {
        assert(0);
    }
    return res;
}

SQLType *
StatementGenerator::generateArraytype(RandomGenerator & rg, const uint32_t allowed_types, uint32_t & col_counter, TopTypeName * tp)
{
    this->depth++;
    SQLType * k = this->randomNextType(rg, allowed_types, col_counter, tp);
    this->depth--;
    return new ArrayType(k);
}

SQLType * StatementGenerator::generateArraytype(RandomGenerator & rg, const uint32_t allowed_types)
{
    uint32_t col_counter = 0;

    return generateArraytype(rg, allowed_types, col_counter, nullptr);
}

SQLType * StatementGenerator::randomNextType(RandomGenerator & rg, const uint32_t allowed_types, uint32_t & col_counter, TopTypeName * tp)
{
    const uint32_t non_nullable_type = 60;
    const uint32_t nullable_type = 25 * static_cast<uint32_t>((allowed_types & allow_nullable) != 0);
    const uint32_t array_type = 10 * static_cast<uint32_t>((allowed_types & allow_array) != 0 && this->depth < this->fc.max_depth);
    const uint32_t map_type = 10
        * static_cast<uint32_t>((allowed_types & allow_map) != 0 && this->depth < this->fc.max_depth && this->width < this->fc.max_width);
    const uint32_t tuple_type = 10 * static_cast<uint32_t>((allowed_types & allow_tuple) != 0 && this->depth < this->fc.max_depth);
    const uint32_t variant_type = 10 * static_cast<uint32_t>((allowed_types & allow_variant) != 0 && this->depth < this->fc.max_depth);
    const uint32_t nested_type = 10
        * static_cast<uint32_t>((allowed_types & allow_nested) != 0 && this->depth < this->fc.max_depth
                                && this->width < this->fc.max_width);
    const uint32_t geo_type = 10 * static_cast<uint32_t>((allowed_types & allow_geo) != 0 && this->fc.fuzz_floating_points);
    const uint32_t prob_space
        = nullable_type + non_nullable_type + array_type + map_type + tuple_type + variant_type + nested_type + geo_type;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t nopt = next_dist(rg.generator);

    if (/*non_nullable_type && */ nopt < (non_nullable_type + 1))
    {
        //non nullable
        const bool lcard = (allowed_types & allow_low_cardinality) != 0 && rg.nextMediumNumber() < 18;
        SQLType * res
            = bottomType(rg, allowed_types, lcard, tp ? (lcard ? tp->mutable_non_nullable_lcard() : tp->mutable_non_nullable()) : nullptr);
        return lcard ? new LowCardinality(res) : res;
    }
    else if (nullable_type && nopt < (non_nullable_type + nullable_type + 1))
    {
        //nullable
        const bool lcard = (allowed_types & allow_low_cardinality) != 0 && rg.nextMediumNumber() < 18;
        SQLType * res = new Nullable(bottomType(
            rg,
            allowed_types & ~(allow_dynamic | allow_JSON),
            lcard,
            tp ? (lcard ? tp->mutable_nullable_lcard() : tp->mutable_nullable()) : nullptr));
        return lcard ? new LowCardinality(res) : res;
    }
    else if (array_type && nopt < (nullable_type + non_nullable_type + array_type + 1))
    {
        //array
        const uint32_t nallowed = allowed_types & ~(allow_nested | ((allowed_types & allow_nullable_inside_array) ? 0 : allow_nullable));
        return generateArraytype(rg, nallowed, col_counter, tp ? tp->mutable_array() : nullptr);
    }
    else if (map_type && nopt < (nullable_type + non_nullable_type + array_type + map_type + 1))
    {
        //map
        MapTypeDef * mt = tp ? tp->mutable_map() : nullptr;

        this->depth++;
        SQLType * k
            = this->randomNextType(rg, allowed_types & ~(allow_nullable | allow_nested), col_counter, mt ? mt->mutable_key() : nullptr);
        this->width++;
        SQLType * v = this->randomNextType(rg, allowed_types & ~(allow_nested), col_counter, mt ? mt->mutable_value() : nullptr);
        this->depth--;
        this->width--;
        return new MapType(k, v);
    }
    else if (tuple_type && nopt < (nullable_type + non_nullable_type + array_type + map_type + tuple_type + 1))
    {
        //tuple
        std::vector<SubType> subtypes;
        const bool with_names = rg.nextBool();
        TupleTypeDef * tt = tp ? tp->mutable_tuple() : nullptr;
        TupleWithColumnNames * twcn = (tp && with_names) ? tt->mutable_with_names() : nullptr;
        TupleWithOutColumnNames * twocn = (tp && !with_names) ? tt->mutable_no_names() : nullptr;
        const uint32_t ncols
            = this->width >= this->fc.max_width ? 0 : (rg.nextMediumNumber() % std::min<uint32_t>(5, this->fc.max_width - this->width));

        this->depth++;
        for (uint32_t i = 0; i < ncols; i++)
        {
            std::optional<uint32_t> opt_cname = std::nullopt;
            TypeColumnDef * tcd = twcn ? twcn->add_values() : nullptr;
            TopTypeName * ttn = twocn ? twocn->add_values() : nullptr;

            if (tcd)
            {
                const uint32_t ncname = col_counter++;

                tcd->mutable_col()->set_column("c" + std::to_string(ncname));
                opt_cname = std::optional<uint32_t>(ncname);
            }
            SQLType * k = this->randomNextType(rg, allowed_types & ~(allow_nested), col_counter, tcd ? tcd->mutable_type_name() : ttn);
            subtypes.push_back(SubType(opt_cname, k));
        }
        this->depth--;
        return new TupleType(subtypes);
    }
    else if (variant_type && nopt < (nullable_type + non_nullable_type + array_type + map_type + tuple_type + variant_type + 1))
    {
        //variant
        std::vector<SQLType *> subtypes;
        TupleWithOutColumnNames * twocn = tp ? tp->mutable_variant() : nullptr;
        const uint32_t ncols
            = (this->width >= this->fc.max_width ? 0 : (rg.nextMediumNumber() % std::min<uint32_t>(5, this->fc.max_width - this->width)))
            + UINT32_C(1);

        this->depth++;
        for (uint32_t i = 0; i < ncols; i++)
        {
            TopTypeName * ttn = tp ? twocn->add_values() : nullptr;

            subtypes.push_back(this->randomNextType(
                rg, allowed_types & ~(allow_nullable | allow_nested | allow_variant | allow_dynamic), col_counter, ttn));
        }
        this->depth--;
        return new VariantType(subtypes);
    }
    else if (
        nested_type && nopt < (nullable_type + non_nullable_type + array_type + map_type + tuple_type + variant_type + nested_type + 1))
    {
        //nested
        std::vector<NestedSubType> subtypes;
        NestedTypeDef * nt = tp ? tp->mutable_nested() : nullptr;
        const uint32_t ncols = (rg.nextMediumNumber() % (std::min<uint32_t>(5, this->fc.max_width - this->width))) + UINT32_C(1);

        this->depth++;
        for (uint32_t i = 0; i < ncols; i++)
        {
            const uint32_t cname = col_counter++;
            TypeColumnDef * tcd = tp ? ((i == 0) ? nt->mutable_type1() : nt->add_others()) : nullptr;

            if (tcd)
            {
                tcd->mutable_col()->set_column("c" + std::to_string(cname));
            }
            SQLType * k = this->randomNextType(rg, allowed_types & ~(allow_nested), col_counter, tcd ? tcd->mutable_type_name() : nullptr);
            subtypes.push_back(NestedSubType(cname, k));
        }
        this->depth--;
        return new NestedType(subtypes);
    }
    else if (
        geo_type
        && nopt < (nullable_type + non_nullable_type + array_type + map_type + tuple_type + variant_type + nested_type + geo_type + 1))
    {
        //geo
        const GeoTypes gt = static_cast<GeoTypes>((rg.nextRandomUInt32() % static_cast<uint32_t>(GeoTypes_MAX)) + 1);

        if (tp)
        {
            tp->set_geo(gt);
        }
        return new GeoType(gt);
    }
    else
    {
        assert(0);
    }
    return nullptr;
}

SQLType * StatementGenerator::randomNextType(RandomGenerator & rg, const uint32_t allowed_types)
{
    uint32_t col_counter = 0;

    return randomNextType(rg, allowed_types, col_counter, nullptr);
}

void appendDecimal(RandomGenerator & rg, std::string & ret, const uint32_t left, const uint32_t right)
{
    ret += rg.nextBool() ? "-" : "";
    if (left > 0)
    {
        std::uniform_int_distribution<uint32_t> next_dist(1, left);
        const uint32_t nlen = next_dist(rg.generator);

        ret += std::max<char>(rg.nextDigit(), '1');
        for (uint32_t j = 1; j < nlen; j++)
        {
            ret += rg.nextDigit();
        }
    }
    else
    {
        ret += "0";
    }
    ret += ".";
    if (right > 0)
    {
        std::uniform_int_distribution<uint32_t> next_dist(1, right);
        const uint32_t nlen = next_dist(rg.generator);

        for (uint32_t j = 0; j < nlen; j++)
        {
            ret += rg.nextDigit();
        }
    }
    else
    {
        ret += "0";
    }
}

template <bool Extremes>
static inline void nextFloatingPoint(RandomGenerator & rg, std::string & ret)
{
    const uint32_t next_option = rg.nextLargeNumber();

    if (Extremes && next_option < 25)
    {
        if (next_option < 17)
        {
            ret += next_option < 9 ? "+" : "-";
        }
        ret += "nan";
    }
    else if (Extremes && next_option < 49)
    {
        if (next_option < 41)
        {
            ret += next_option < 33 ? "+" : "-";
        }
        ret += "inf";
    }
    else if (Extremes && next_option < 73)
    {
        if (next_option < 65)
        {
            ret += next_option < 57 ? "+" : "-";
        }
        ret += "0.0";
    }
    else if (next_option < 373)
    {
        ret += std::to_string(rg.nextRandomInt32());
    }
    else if (next_option < 673)
    {
        ret += std::to_string(rg.nextRandomInt64());
    }
    else
    {
        std::uniform_int_distribution<uint32_t> next_dist(0, 9);
        const uint32_t left = next_dist(rg.generator);
        const uint32_t right = next_dist(rg.generator);

        appendDecimal(rg, ret, left, right);
    }
}

void strAppendGeoValue(RandomGenerator & rg, std::string & ret, const GeoTypes & geo_type)
{
    const uint32_t limit = rg.nextLargeNumber() % 10;

    switch (geo_type)
    {
        case GeoTypes::Point:
            ret += "(";
            nextFloatingPoint<false>(rg, ret);
            ret += ",";
            nextFloatingPoint<false>(rg, ret);
            ret += ")";
            break;
        case GeoTypes::Ring:
        case GeoTypes::LineString:
            ret += "[";
            for (uint32_t i = 0; i < limit; i++)
            {
                if (i != 0)
                {
                    ret += ", ";
                }
                ret += "(";
                nextFloatingPoint<false>(rg, ret);
                ret += ",";
                nextFloatingPoint<false>(rg, ret);
                ret += ")";
            }
            ret += "]";
            break;
        case GeoTypes::MultiLineString:
        case GeoTypes::Polygon:
            ret += "[";
            for (uint32_t i = 0; i < limit; i++)
            {
                const uint32_t nlines = rg.nextLargeNumber() % 10;

                if (i != 0)
                {
                    ret += ", ";
                }
                ret += "[";
                for (uint32_t j = 0; j < nlines; j++)
                {
                    if (j != 0)
                    {
                        ret += ", ";
                    }
                    ret += "(";
                    nextFloatingPoint<false>(rg, ret);
                    ret += ",";
                    nextFloatingPoint<false>(rg, ret);
                    ret += ")";
                }
                ret += "]";
            }
            ret += "]";
            break;
        case GeoTypes::MultiPolygon:
            ret += "[";
            for (uint32_t i = 0; i < limit; i++)
            {
                const uint32_t npoligons = rg.nextLargeNumber() % 10;

                if (i != 0)
                {
                    ret += ", ";
                }
                ret += "[";
                for (uint32_t j = 0; j < npoligons; j++)
                {
                    const uint32_t nlines = rg.nextLargeNumber() % 10;

                    if (j != 0)
                    {
                        ret += ", ";
                    }
                    ret += "[";
                    for (uint32_t k = 0; k < nlines; k++)
                    {
                        if (k != 0)
                        {
                            ret += ", ";
                        }
                        ret += "(";
                        nextFloatingPoint<false>(rg, ret);
                        ret += ",";
                        nextFloatingPoint<false>(rg, ret);
                        ret += ")";
                    }
                    ret += "]";
                }
                ret += "]";
            }
            ret += "]";
            break;
    }
}

void StatementGenerator::strAppendBottomValue(RandomGenerator & rg, std::string & ret, SQLType * tp)
{
    IntType * itp;
    DateType * dtp;
    DateTimeType * dttp;
    DecimalType * detp;
    StringType * stp;
    EnumType * etp;

    if ((itp = dynamic_cast<IntType *>(tp)))
    {
        if (itp->is_unsigned)
        {
            switch (itp->size)
            {
                case 8:
                    ret += std::to_string(rg.nextRandomUInt8());
                    break;
                case 16:
                    ret += std::to_string(rg.nextRandomUInt16());
                    break;
                case 32:
                    ret += std::to_string(rg.nextRandomUInt32());
                    break;
                case 64:
                    ret += std::to_string(rg.nextRandomUInt64());
                    break;
                default: {
                    UHugeInt val(rg.nextRandomUInt64(), rg.nextRandomUInt64());
                    val.toString(ret);
                }
            }
        }
        else
        {
            switch (itp->size)
            {
                case 8:
                    ret += std::to_string(rg.nextRandomInt8());
                    break;
                case 16:
                    ret += std::to_string(rg.nextRandomInt16());
                    break;
                case 32:
                    ret += std::to_string(rg.nextRandomInt32());
                    break;
                case 64:
                    ret += std::to_string(rg.nextRandomInt64());
                    break;
                default: {
                    HugeInt val(rg.nextRandomInt64(), rg.nextRandomUInt64());
                    val.toString(ret);
                }
            }
        }
    }
    else if (dynamic_cast<FloatType *>(tp))
    {
        nextFloatingPoint<true>(rg, ret);
    }
    else if ((dtp = dynamic_cast<DateType *>(tp)))
    {
        ret += "'";
        if (dtp->extended)
        {
            rg.nextDate32(ret);
        }
        else
        {
            rg.nextDate(ret);
        }
        ret += "'";
    }
    else if ((dttp = dynamic_cast<DateTimeType *>(tp)))
    {
        ret += "'";
        if (dttp->extended)
        {
            rg.nextDateTime64(ret);
        }
        else
        {
            rg.nextDateTime(ret);
        }
        ret += "'";
    }
    else if ((detp = dynamic_cast<DecimalType *>(tp)))
    {
        const uint32_t right = detp->scale.value_or(0);
        const uint32_t left = detp->precision.value_or(10) - right;

        appendDecimal(rg, ret, left, right);
    }
    else if ((stp = dynamic_cast<StringType *>(tp)))
    {
        const uint32_t limit = stp->precision.value_or(rg.nextRandomUInt32() % 1009);

        rg.nextString(ret, "'", true, limit);
    }
    else if (dynamic_cast<const BoolType *>(tp))
    {
        ret += rg.nextBool() ? "TRUE" : "FALSE";
    }
    else if ((etp = dynamic_cast<EnumType *>(tp)))
    {
        const EnumValue & nvalue = rg.pickRandomlyFromVector(etp->values);

        ret += nvalue.val;
    }
    else if (dynamic_cast<UUIDType *>(tp))
    {
        ret += "'";
        rg.nextUUID(ret);
        ret += "'";
    }
    else if (dynamic_cast<IPv4Type *>(tp))
    {
        ret += "'";
        rg.nextIPv4(ret);
        ret += "'";
    }
    else if (dynamic_cast<IPv6Type *>(tp))
    {
        ret += "'";
        rg.nextIPv6(ret);
        ret += "'";
    }
    else
    {
        assert(0);
    }
}

void StatementGenerator::strAppendMap(RandomGenerator & rg, std::string & ret, MapType * mt)
{
    std::uniform_int_distribution<uint64_t> rows_dist(0, fc.max_nested_rows);
    const uint64_t limit = rows_dist(rg.generator);

    ret += "map(";
    for (uint64_t i = 0; i < limit; i++)
    {
        if (i != 0)
        {
            ret += ", ";
        }
        strAppendAnyValueInternal(rg, ret, mt->key);
        ret += ",";
        strAppendAnyValueInternal(rg, ret, mt->value);
    }
    ret += ")";
}

void StatementGenerator::strAppendArray(RandomGenerator & rg, std::string & ret, SQLType * tp, const uint64_t limit)
{
    ret += "[";
    for (uint64_t i = 0; i < limit; i++)
    {
        if (i != 0)
        {
            ret += ", ";
        }
        strAppendAnyValueInternal(rg, ret, tp);
    }
    ret += "]";
}

void StatementGenerator::strAppendArray(RandomGenerator & rg, std::string & ret, ArrayType * at)
{
    std::uniform_int_distribution<uint64_t> rows_dist(0, fc.max_nested_rows);

    strAppendArray(rg, ret, at->subtype, rows_dist(rg.generator));
}

void StatementGenerator::strAppendTuple(RandomGenerator & rg, std::string & ret, TupleType * at)
{
    ret += "(";
    for (const auto & entry : at->subtypes)
    {
        strAppendAnyValueInternal(rg, ret, entry.subtype);
        ret += ", ";
    }
    ret += ")";
}

void StatementGenerator::strAppendVariant(RandomGenerator & rg, std::string & ret, VariantType * vtp)
{
    if (vtp->subtypes.empty())
    {
        ret += "NULL";
    }
    else
    {
        strAppendAnyValueInternal(rg, ret, rg.pickRandomlyFromVector(vtp->subtypes));
    }
}

void strBuildJSONArray(RandomGenerator & rg, const int jdepth, const int jwidth, std::string & ret)
{
    std::uniform_int_distribution<int> jopt(1, 3);
    int nelems = 0;
    int next_width = 0;

    if (jwidth)
    {
        std::uniform_int_distribution<int> alen(0, jwidth);
        nelems = alen(rg.generator);
    }
    ret += "[";
    next_width = nelems;
    for (int j = 0; j < nelems; j++)
    {
        if (j != 0)
        {
            ret += ",";
        }
        if (jdepth)
        {
            switch (jopt(rg.generator))
            {
                case 1: //object
                    strBuildJSON(rg, jdepth - 1, next_width, ret);
                    break;
                case 2: //array
                    strBuildJSONArray(rg, jdepth - 1, next_width, ret);
                    break;
                case 3: //others
                    strBuildJSONElement(rg, ret);
                    break;
                default:
                    assert(0);
            }
        }
        else
        {
            strBuildJSONElement(rg, ret);
        }
        next_width--;
    }
    ret += "]";
}

void strBuildJSONElement(RandomGenerator & rg, std::string & ret)
{
    std::uniform_int_distribution<int> opts(1, 20);

    switch (opts(rg.generator))
    {
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
            ret += std::to_string(rg.nextRandomInt64());
            break;
        case 5: //large unsigned number
            ret += std::to_string(rg.nextRandomUInt64());
            break;
        case 6:
        case 7:
        case 8: { //small number
            std::uniform_int_distribution<int> numbers(-1000, 1000);
            ret += std::to_string(numbers(rg.generator));
        }
        break;
        case 9:
        case 10: { //decimal
            std::uniform_int_distribution<uint32_t> next_dist(0, 8);
            const uint32_t left = next_dist(rg.generator);
            const uint32_t right = next_dist(rg.generator);

            appendDecimal(rg, ret, left, right);
        }
        break;
        case 11:
        case 12:
        case 13: //string
            rg.nextString(ret, "\"", false, rg.nextRandomUInt32() % 1009);
            break;
        case 14: //date
            ret += '"';
            rg.nextDate(ret);
            ret += '"';
            break;
        case 15: //date32
            ret += '"';
            rg.nextDate32(ret);
            ret += '"';
            break;
        case 16: //datetime
            ret += '"';
            rg.nextDateTime(ret);
            ret += '"';
            break;
        case 17: //datetime64
            ret += '"';
            rg.nextDateTime64(ret);
            ret += '"';
            break;
        case 18: //uuid
            ret += '"';
            rg.nextUUID(ret);
            ret += '"';
            break;
        case 19: //ipv4
            ret += '"';
            rg.nextIPv4(ret);
            ret += '"';
            break;
        case 20: //ipv6
            ret += '"';
            rg.nextIPv6(ret);
            ret += '"';
            break;
        default:
            assert(0);
    }
}

void strBuildJSON(RandomGenerator & rg, const int jdepth, const int jwidth, std::string & ret)
{
    ret += "{";
    if (jdepth && jwidth && rg.nextSmallNumber() < 9)
    {
        std::uniform_int_distribution<int> childd(1, jwidth);
        const int nchildren = childd(rg.generator);

        for (int i = 0; i < nchildren; i++)
        {
            std::uniform_int_distribution<int> jopt(1, 3);

            if (i != 0)
            {
                ret += ",";
            }
            ret += "\"";
            rg.nextJSONCol(ret);
            ret += "\":";
            switch (jopt(rg.generator))
            {
                case 1: //object
                    strBuildJSON(rg, jdepth - 1, jwidth, ret);
                    break;
                case 2: //array
                    strBuildJSONArray(rg, jdepth - 1, jwidth, ret);
                    break;
                case 3: //others
                    strBuildJSONElement(rg, ret);
                    break;
                default:
                    assert(0);
            }
        }
    }
    ret += "}";
}

void StatementGenerator::strAppendAnyValueInternal(RandomGenerator & rg, std::string & ret, SQLType * tp)
{
    MapType * mt;
    Nullable * nl;
    ArrayType * at;
    TupleType * ttp;
    VariantType * vtp;
    LowCardinality * lc;
    GeoType * gtp;

    if (dynamic_cast<IntType *>(tp) || dynamic_cast<FloatType *>(tp) || dynamic_cast<DateType *>(tp) || dynamic_cast<DateTimeType *>(tp)
        || dynamic_cast<DecimalType *>(tp) || dynamic_cast<StringType *>(tp) || dynamic_cast<const BoolType *>(tp)
        || dynamic_cast<EnumType *>(tp) || dynamic_cast<UUIDType *>(tp) || dynamic_cast<IPv4Type *>(tp) || dynamic_cast<IPv6Type *>(tp))
    {
        strAppendBottomValue(rg, ret, tp);
    }
    else if ((lc = dynamic_cast<LowCardinality *>(tp)))
    {
        strAppendAnyValueInternal(rg, ret, lc->subtype);
    }
    else if ((nl = dynamic_cast<Nullable *>(tp)))
    {
        if (rg.nextMediumNumber() < 6)
        {
            ret += "NULL";
        }
        else
        {
            strAppendAnyValueInternal(rg, ret, nl->subtype);
        }
    }
    else if (dynamic_cast<JSONType *>(tp))
    {
        std::uniform_int_distribution<int> dopt(1, this->fc.max_depth);
        std::uniform_int_distribution<int> wopt(1, this->fc.max_width);

        ret += "'";
        strBuildJSON(rg, dopt(rg.generator), wopt(rg.generator), ret);
        ret += "'";
    }
    else if (dynamic_cast<DynamicType *>(tp))
    {
        uint32_t col_counter = 0;
        SQLType * next = randomNextType(rg, allow_nullable | allow_JSON, col_counter, nullptr);

        strAppendAnyValueInternal(rg, ret, next);
        if (rg.nextMediumNumber() < 4)
        {
            ret += "::";
            next->typeName(ret, false);
        }
        delete next;
    }
    else if ((gtp = dynamic_cast<GeoType *>(tp)))
    {
        strAppendGeoValue(rg, ret, gtp->geo_type);
    }
    else if (this->depth == this->fc.max_depth)
    {
        ret += "1";
    }
    else if ((mt = dynamic_cast<MapType *>(tp)))
    {
        this->depth++;
        strAppendMap(rg, ret, mt);
        this->depth--;
    }
    else if ((at = dynamic_cast<ArrayType *>(tp)))
    {
        this->depth++;
        strAppendArray(rg, ret, at);
        this->depth--;
    }
    else if ((ttp = dynamic_cast<TupleType *>(tp)))
    {
        this->depth++;
        strAppendTuple(rg, ret, ttp);
        this->depth--;
    }
    else if ((vtp = dynamic_cast<VariantType *>(tp)))
    {
        this->depth++;
        strAppendVariant(rg, ret, vtp);
        this->depth--;
    }
    else
    {
        //no nested types here
        assert(0);
    }
}

void StatementGenerator::strAppendAnyValue(RandomGenerator & rg, std::string & ret, SQLType * tp)
{
    strAppendAnyValueInternal(rg, ret, tp);
    if (rg.nextSmallNumber() < 7)
    {
        ret += "::";
        tp->typeName(ret, false);
    }
}

}
