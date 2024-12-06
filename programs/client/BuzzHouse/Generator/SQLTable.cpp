#include "SQLCatalog.h"
#include "SQLTypes.h"
#include "StatementGenerator.h"

namespace BuzzHouse
{

void StatementGenerator::addTableRelation(
    RandomGenerator & rg, const bool allow_internal_cols, const std::string & rel_name, const SQLTable & t)
{
    const NestedType * ntp = nullptr;
    SQLRelation rel(rel_name);

    for (const auto & entry : t.cols)
    {
        if ((ntp = dynamic_cast<const NestedType *>(entry.second.tp)))
        {
            for (const auto & entry2 : ntp->subtypes)
            {
                rel.cols.push_back(SQLRelationCol(
                    rel_name, "c" + std::to_string(entry.first), std::optional<std::string>("c" + std::to_string(entry2.cname))));
            }
        }
        else
        {
            rel.cols.push_back(SQLRelationCol(rel_name, "c" + std::to_string(entry.first), std::nullopt));
        }
    }
    if (allow_internal_cols && rg.nextSmallNumber() < 3)
    {
        if (t.isMergeTreeFamily())
        {
            if (this->allow_not_deterministic)
            {
                rel.cols.push_back(SQLRelationCol(rel_name, "_block_number", std::nullopt));
            }
            rel.cols.push_back(SQLRelationCol(rel_name, "_part", std::nullopt));
            rel.cols.push_back(SQLRelationCol(rel_name, "_part_data_version", std::nullopt));
            rel.cols.push_back(SQLRelationCol(rel_name, "_part_index", std::nullopt));
            rel.cols.push_back(SQLRelationCol(rel_name, "_part_offset", std::nullopt));
            rel.cols.push_back(SQLRelationCol(rel_name, "_part_uuid", std::nullopt));
            rel.cols.push_back(SQLRelationCol(rel_name, "_partition_id", std::nullopt));
            rel.cols.push_back(SQLRelationCol(rel_name, "_partition_value", std::nullopt));
            rel.cols.push_back(SQLRelationCol(rel_name, "_sample_factor", std::nullopt));
        }
        else if (t.isAnyS3Engine() || t.isFileEngine())
        {
            rel.cols.push_back(SQLRelationCol(rel_name, "_path", std::nullopt));
            rel.cols.push_back(SQLRelationCol(rel_name, "_file", std::nullopt));
            if (t.isS3Engine())
            {
                rel.cols.push_back(SQLRelationCol(rel_name, "_size", std::nullopt));
                rel.cols.push_back(SQLRelationCol(rel_name, "_time", std::nullopt));
                rel.cols.push_back(SQLRelationCol(rel_name, "_etag", std::nullopt));
            }
        }
    }
    if (rel_name.empty())
    {
        this->levels[this->current_level] = QueryLevel(this->current_level);
    }
    this->levels[this->current_level].rels.push_back(std::move(rel));
}

int StatementGenerator::generateNextStatistics(RandomGenerator & rg, ColumnStatistics * cstats)
{
    const size_t nstats = (rg.nextMediumNumber() % static_cast<uint32_t>(ColumnStat_MAX)) + 1;

    for (uint32_t i = 1; i <= ColumnStat_MAX; i++)
    {
        ids.push_back(i);
    }
    std::shuffle(ids.begin(), ids.end(), rg.generator);
    for (size_t i = 0; i < nstats; i++)
    {
        const ColumnStat nstat = static_cast<ColumnStat>(ids[i]);

        if (i == 0)
        {
            cstats->set_stat(nstat);
        }
        else
        {
            cstats->add_other_stats(nstat);
        }
    }
    ids.clear();
    return 0;
}

int StatementGenerator::pickUpNextCols(RandomGenerator & rg, const SQLTable & t, ColumnList * clist)
{
    const NestedType * ntp = nullptr;
    const size_t ncols = (rg.nextMediumNumber() % std::min<uint32_t>(static_cast<uint32_t>(t.realNumberOfColumns()), UINT32_C(4))) + 1;

    assert(this->ids.empty());
    for (const auto & entry : t.cols)
    {
        if ((ntp = dynamic_cast<const NestedType *>(entry.second.tp)))
        {
            for (const auto & entry2 : ntp->subtypes)
            {
                ids.push_back(entry2.cname);
            }
        }
        else
        {
            ids.push_back(entry.first);
        }
    }
    std::shuffle(ids.begin(), ids.end(), rg.generator);
    for (size_t i = 0; i < ncols; i++)
    {
        Column * col = i == 0 ? clist->mutable_col() : clist->add_other_cols();

        col->set_column("c" + std::to_string(ids[i]));
    }
    ids.clear();
    return 0;
}

const std::vector<SQLFunc> multicol_hash
    = {SQLFunc::FUNChalfMD5,
       SQLFunc::FUNCsipHash64,
       SQLFunc::FUNCsipHash128,
       SQLFunc::FUNCsipHash128Reference,
       SQLFunc::FUNCcityHash64,
       SQLFunc::FUNCfarmFingerprint64,
       SQLFunc::FUNCfarmHash64,
       SQLFunc::FUNCmetroHash64,
       SQLFunc::FUNCmurmurHash2_32,
       SQLFunc::FUNCmurmurHash2_64,
       SQLFunc::FUNCgccMurmurHash,
       SQLFunc::FUNCkafkaMurmurHash,
       SQLFunc::FUNCmurmurHash3_32,
       SQLFunc::FUNCmurmurHash3_64,
       SQLFunc::FUNCmurmurHash3_128};

const std::vector<SQLFunc> dates_hash
    = {SQLFunc::FUNCtoYear,
       SQLFunc::FUNCtoQuarter,
       SQLFunc::FUNCtoMonth,
       SQLFunc::FUNCtoDayOfYear,
       SQLFunc::FUNCtoDayOfMonth,
       SQLFunc::FUNCtoDayOfWeek,
       SQLFunc::FUNCtoHour,
       SQLFunc::FUNCtoMinute,
       SQLFunc::FUNCtoSecond,
       SQLFunc::FUNCtoMillisecond,
       SQLFunc::FUNCtoUnixTimestamp,
       SQLFunc::FUNCtoStartOfYear,
       SQLFunc::FUNCtoStartOfISOYear,
       SQLFunc::FUNCtoStartOfQuarter,
       SQLFunc::FUNCtoStartOfMonth,
       SQLFunc::FUNCtoLastDayOfMonth,
       SQLFunc::FUNCtoMonday,
       SQLFunc::FUNCtoStartOfWeek,
       SQLFunc::FUNCtoLastDayOfWeek,
       SQLFunc::FUNCtoStartOfDay,
       SQLFunc::FUNCtoStartOfHour,
       SQLFunc::FUNCtoStartOfMinute,
       SQLFunc::FUNCtoStartOfSecond,
       SQLFunc::FUNCtoStartOfMillisecond,
       SQLFunc::FUNCtoStartOfMicrosecond,
       SQLFunc::FUNCtoStartOfNanosecond,
       SQLFunc::FUNCtoStartOfFiveMinutes,
       SQLFunc::FUNCtoStartOfTenMinutes,
       SQLFunc::FUNCtoStartOfFifteenMinutes,
       SQLFunc::FUNCtoTime,
       SQLFunc::FUNCtoRelativeYearNum,
       SQLFunc::FUNCtoRelativeQuarterNum,
       SQLFunc::FUNCtoRelativeMonthNum,
       SQLFunc::FUNCtoRelativeWeekNum,
       SQLFunc::FUNCtoRelativeDayNum,
       SQLFunc::FUNCtoRelativeHourNum,
       SQLFunc::FUNCtoRelativeMinuteNum,
       SQLFunc::FUNCtoRelativeSecondNum,
       SQLFunc::FUNCtoISOYear,
       SQLFunc::FUNCtoISOWeek,
       SQLFunc::FUNCtoWeek,
       SQLFunc::FUNCtoYearWeek,
       SQLFunc::FUNCtoDaysSinceYearZero,
       SQLFunc::FUNCtoday,
       SQLFunc::FUNCyesterday,
       SQLFunc::FUNCtimeSlot,
       SQLFunc::FUNCtoYYYYMM,
       SQLFunc::FUNCtoYYYYMMDD,
       SQLFunc::FUNCtoYYYYMMDDhhmmss,
       SQLFunc::FUNCmonthName,
       SQLFunc::FUNCtoModifiedJulianDay,
       SQLFunc::FUNCtoModifiedJulianDayOrNull,
       SQLFunc::FUNCtoUTCTimestamp};

void StatementGenerator::insertEntryRef(const InsertEntry & entry, Expr * expr) const
{
    ExprColumn * ecol = expr->mutable_comp_expr()->mutable_expr_stc()->mutable_col();

    ecol->mutable_col()->set_column("c" + std::to_string(entry.cname1));
    if (entry.cname2.has_value())
    {
        ecol->mutable_subcol()->set_column("c" + std::to_string(entry.cname2.value()));
    }
}

void StatementGenerator::insertEntryRefCP(const InsertEntry & entry, ColumnPath * cp) const
{
    cp->mutable_col()->set_column("c" + std::to_string(entry.cname1));
    if (entry.cname2.has_value())
    {
        cp->add_sub_cols()->set_column("c" + std::to_string(entry.cname2.value()));
    }
}

int StatementGenerator::generateTableKey(RandomGenerator & rg, const TableEngineValues teng, TableKey * tkey)
{
    if (!entries.empty() && rg.nextSmallNumber() < 7)
    {
        const size_t ocols = (rg.nextMediumNumber() % std::min<size_t>(entries.size(), UINT32_C(3))) + 1;

        std::shuffle(entries.begin(), entries.end(), rg.generator);
        if (teng != TableEngineValues::SummingMergeTree && rg.nextSmallNumber() < 3)
        {
            //Use a single expression for the entire table
            //See https://github.com/ClickHouse/ClickHouse/issues/72043 for SummingMergeTree exception
            SQLFuncCall * func_call = tkey->add_exprs()->mutable_comp_expr()->mutable_func_call();

            func_call->mutable_func()->set_catalog_func(rg.pickRandomlyFromVector(multicol_hash));
            for (size_t i = 0; i < ocols; i++)
            {
                insertEntryRef(this->entries[i], func_call->add_args()->mutable_expr());
            }
        }
        else
        {
            for (size_t i = 0; i < ocols; i++)
            {
                const InsertEntry & entry = this->entries[i];

                if ((hasType<DateType, false, true>(entry.tp) || hasType<DateTimeType, false, true>(entry.tp)) && rg.nextBool())
                {
                    //Use date functions for partitioning/keys
                    SQLFuncCall * func_call = tkey->add_exprs()->mutable_comp_expr()->mutable_func_call();

                    func_call->mutable_func()->set_catalog_func(rg.pickRandomlyFromVector(dates_hash));
                    insertEntryRef(entry, func_call->add_args()->mutable_expr());
                }
                else if (hasType<IntType, true, true>(entry.tp) && rg.nextBool())
                {
                    //Use modulo function for partitioning/keys
                    BinaryExpr * bexpr = tkey->add_exprs()->mutable_comp_expr()->mutable_binary_expr();

                    insertEntryRef(entry, bexpr->mutable_lhs());
                    bexpr->set_op(BinaryOperator::BINOP_PERCENT);
                    bexpr->mutable_rhs()->mutable_lit_val()->mutable_int_lit()->set_uint_lit(
                        rg.nextRandomUInt32() % (rg.nextBool() ? 1024 : 65536));
                }
                else if (teng != TableEngineValues::SummingMergeTree && rg.nextMediumNumber() < 6)
                {
                    //Use hash
                    SQLFuncCall * func_call = tkey->add_exprs()->mutable_comp_expr()->mutable_func_call();

                    func_call->mutable_func()->set_catalog_func(rg.pickRandomlyFromVector(multicol_hash));
                    insertEntryRef(entry, func_call->add_args()->mutable_expr());
                }
                else
                {
                    insertEntryRef(entry, tkey->add_exprs());
                }
            }
        }
    }
    return 0;
}

int StatementGenerator::generateMergeTreeEngineDetails(
    RandomGenerator & rg, const TableEngineValues teng, const bool add_pkey, TableEngine * te)
{
    if (rg.nextSmallNumber() < 6)
    {
        generateTableKey(rg, teng, te->mutable_order());
    }
    if (te->has_order() && add_pkey && rg.nextSmallNumber() < 5)
    {
        //pkey is a subset of order by
        TableKey * tkey = te->mutable_primary_key();

        if (te->order().exprs_size())
        {
            std::uniform_int_distribution<uint32_t> table_order_by(1, te->order().exprs_size());
            const uint32_t pkey_size = table_order_by(rg.generator);

            for (uint32_t i = 0; i < pkey_size; i++)
            {
                tkey->add_exprs()->CopyFrom(te->order().exprs(i));
            }
        }
    }
    else if (!te->has_order() && add_pkey)
    {
        generateTableKey(rg, teng, te->mutable_primary_key());
    }
    if (rg.nextBool())
    {
        generateTableKey(rg, teng, te->mutable_partition_by());
    }

    const int npkey = te->primary_key().exprs_size();
    if (npkey && rg.nextSmallNumber() < 5)
    {
        //try to add sample key
        assert(this->ids.empty());
        for (const auto & entry : this->entries)
        {
            const IntType * itp = nullptr;

            if (!entry.cname2.has_value() && (itp = dynamic_cast<const IntType *>(entry.tp)) && itp->is_unsigned)
            {
                const TableKey & tpk = te->primary_key();

                //must be in pkey
                for (int j = 0; j < npkey; j++)
                {
                    if (tpk.exprs(j).has_comp_expr() && tpk.exprs(j).comp_expr().has_expr_stc())
                    {
                        const ExprColumn & oecol = tpk.exprs(j).comp_expr().expr_stc().col();

                        if (!oecol.has_subcol() && static_cast<uint32_t>(std::stoul(oecol.col().column().substr(1))) == entry.cname1)
                        {
                            this->ids.push_back(entry.cname1);
                            break;
                        }
                    }
                }
            }
        }
        if (!this->ids.empty())
        {
            TableKey * tkey = te->mutable_sample_by();
            const size_t ncols = (rg.nextMediumNumber() % std::min<size_t>(this->ids.size(), UINT32_C(3))) + 1;

            std::shuffle(ids.begin(), ids.end(), rg.generator);
            for (size_t i = 0; i < ncols; i++)
            {
                ExprColumn * ecol = tkey->add_exprs()->mutable_comp_expr()->mutable_expr_stc()->mutable_col();

                ecol->mutable_col()->set_column("c" + std::to_string(ids[i]));
            }
            this->ids.clear();
        }
    }
    if (teng == TableEngineValues::SummingMergeTree && rg.nextSmallNumber() < 4)
    {
        ColumnPathList * clist = te->add_params()->mutable_col_list();
        const size_t ncols = (rg.nextMediumNumber() % std::min<uint32_t>(static_cast<uint32_t>(entries.size()), UINT32_C(4))) + 1;

        std::shuffle(entries.begin(), entries.end(), rg.generator);
        for (size_t i = 0; i < ncols; i++)
        {
            insertEntryRefCP(entries[i], i == 0 ? clist->mutable_col() : clist->add_other_cols());
        }
    }
    return 0;
}

const std::vector<std::string> & s3_compress = {"none", "gzip", "gz", "brotli", "br", "xz", "LZMA", "zstd", "zst"};

int StatementGenerator::generateEngineDetails(RandomGenerator & rg, SQLBase & b, const bool add_pkey, TableEngine * te)
{
    if (b.isMergeTreeFamily())
    {
        if (rg.nextSmallNumber() < 4)
        {
            b.toption = supports_cloud_features && rg.nextBool() ? TableEngineOption::TShared : TableEngineOption::TReplicated;
            te->set_toption(b.toption.value());
        }
        generateMergeTreeEngineDetails(rg, b.teng, add_pkey, te);
    }
    else if (b.isFileEngine())
    {
        const uint32_t noption = rg.nextSmallNumber();
        TableEngineParam * tep = te->add_params();

        if (rg.nextSmallNumber() < 5)
        {
            generateTableKey(rg, b.teng, te->mutable_partition_by());
        }
        if (noption < 9)
        {
            tep->set_in_out(static_cast<InOutFormat>((rg.nextRandomUInt32() % static_cast<uint32_t>(InOutFormat_MAX)) + 1));
        }
        else if (noption == 9)
        {
            tep->set_in(static_cast<InFormat>((rg.nextRandomUInt32() % static_cast<uint32_t>(InFormat_MAX)) + 1));
        }
        else
        {
            tep->set_out(static_cast<OutFormat>((rg.nextRandomUInt32() % static_cast<uint32_t>(OutFormat_MAX)) + 1));
        }
    }
    else if (b.isJoinEngine())
    {
        const size_t ncols = (rg.nextMediumNumber() % std::min<uint32_t>(static_cast<uint32_t>(entries.size()), UINT32_C(3))) + 1;
        JoinType jt = static_cast<JoinType>((rg.nextRandomUInt32() % static_cast<uint32_t>(J_FULL)) + 1);
        TableEngineParam * tep = te->add_params();

        switch (jt)
        {
            case JoinType::J_LEFT:
            case JoinType::J_INNER:
                tep->set_join_const(static_cast<JoinConst>((rg.nextRandomUInt32() % static_cast<uint32_t>(JoinConst_MAX)) + 1));
                break;
            case JoinType::J_RIGHT:
                tep->set_join_const(static_cast<JoinConst>((rg.nextRandomUInt32() % static_cast<uint32_t>(JoinConst::J_ANTI)) + 1));
                break;
            case JoinType::J_FULL:
                tep->set_join_const(JoinConst::J_ALL);
                break;
            default:
                assert(0);
                break;
        }
        te->add_params()->set_join_op(jt);

        std::shuffle(entries.begin(), entries.end(), rg.generator);
        for (size_t i = 0; i < ncols; i++)
        {
            insertEntryRefCP(entries[i], te->add_params()->mutable_cols());
        }
    }
    else if (b.isBufferEngine())
    {
        const bool has_tables
            = collectionHas<SQLTable>([](const SQLTable & t)
                                      { return t.db && t.db->attached == DetachStatus::ATTACHED && t.attached == DetachStatus::ATTACHED; }),
            has_views = collectionHas<SQLView>(
                [](const SQLView & v) { return v.db && v.db->attached == DetachStatus::ATTACHED && v.attached == DetachStatus::ATTACHED; });

        if (has_tables && (!has_views || rg.nextSmallNumber() < 8))
        {
            const SQLTable & t = rg.pickRandomlyFromVector(filterCollection<SQLTable>(
                [](const SQLTable & tt)
                { return tt.db && tt.db->attached == DetachStatus::ATTACHED && tt.attached == DetachStatus::ATTACHED; }));

            te->add_params()->mutable_database()->set_database("d" + std::to_string(t.db->dname));
            te->add_params()->mutable_table()->set_table("t" + std::to_string(t.tname));
        }
        else
        {
            const SQLView & v = rg.pickRandomlyFromVector(filterCollection<SQLView>(
                [](const SQLView & vv)
                { return vv.db && vv.db->attached == DetachStatus::ATTACHED && vv.attached == DetachStatus::ATTACHED; }));

            te->add_params()->mutable_database()->set_database("d" + std::to_string(v.db->dname));
            te->add_params()->mutable_table()->set_table("v" + std::to_string(v.tname));
        }
        //num_layers
        te->add_params()->set_num(static_cast<int32_t>(rg.nextRandomUInt32() % 101));
        //min_time, max_time, min_rows, max_rows, min_bytes, max_bytes
        for (int i = 0; i < 6; i++)
        {
            te->add_params()->set_num(static_cast<int32_t>(rg.nextRandomUInt32() % 1001));
        }
        if (rg.nextSmallNumber() < 7)
        {
            //flush_time
            te->add_params()->set_num(static_cast<int32_t>(rg.nextRandomUInt32() % 61));
        }
        if (rg.nextSmallNumber() < 7)
        {
            //flush_rows
            te->add_params()->set_num(static_cast<int32_t>(rg.nextRandomUInt32() % 1001));
        }
        if (rg.nextSmallNumber() < 7)
        {
            //flush_bytes
            te->add_params()->set_num(static_cast<int32_t>(rg.nextRandomUInt32() % 1001));
        }
    }
    else if (b.isMySQLEngine() || b.isPostgreSQLEngine() || b.isSQLiteEngine() || b.isMongoDBEngine() || b.isRedisEngine())
    {
        IntegrationCall next = IntegrationCall::IntMinIO;

        if (b.isMySQLEngine())
        {
            next = IntegrationCall::IntMySQL;
        }
        else if (b.isPostgreSQLEngine())
        {
            next = IntegrationCall::IntPostgreSQL;
        }
        else if (b.isSQLiteEngine())
        {
            next = IntegrationCall::IntSQLite;
        }
        else if (b.isMongoDBEngine())
        {
            next = IntegrationCall::IntMongoDB;
        }
        else if (b.isRedisEngine())
        {
            next = IntegrationCall::IntRedis;
        }
        else
        {
            assert(0);
        }
        connections.createExternalDatabaseTable(rg, next, b, entries, te);
    }
    else if (b.isAnyS3Engine() || b.isHudiEngine() || b.isDeltaLakeEngine() || b.isIcebergEngine())
    {
        connections.createExternalDatabaseTable(rg, IntegrationCall::IntMinIO, b, entries, te);
        if (b.isAnyS3Engine() || b.isIcebergEngine())
        {
            te->add_params()->set_in_out(static_cast<InOutFormat>((rg.nextRandomUInt32() % static_cast<uint32_t>(InOutFormat_MAX)) + 1));
            if (rg.nextSmallNumber() < 4)
            {
                te->add_params()->set_svalue(rg.pickRandomlyFromVector(s3_compress));
            }
            if (b.isAnyS3Engine() && rg.nextSmallNumber() < 5)
            {
                generateTableKey(rg, b.teng, te->mutable_partition_by());
            }
        }
    }
    if ((b.isRocksEngine() || b.isRedisEngine()) && add_pkey && !entries.empty())
    {
        insertEntryRef(rg.pickRandomlyFromVector(entries), te->mutable_primary_key()->add_exprs());
    }
    return 0;
}

int StatementGenerator::addTableColumn(
    RandomGenerator & rg,
    SQLTable & t,
    const uint32_t cname,
    const bool staged,
    const bool modify,
    const bool is_pk,
    const ColumnSpecial special,
    ColumnDef * cd)
{
    SQLColumn col;
    const SQLType * tp = nullptr;
    auto & to_add = staged ? t.staged_cols : t.cols;
    uint32_t possible_types = std::numeric_limits<uint32_t>::max();

    if (t.isMySQLEngine() || t.hasMySQLPeer())
    {
        possible_types
            &= ~(allow_hugeint | allow_dynamic | allow_array | allow_map | allow_tuple | allow_variant | allow_nested | allow_geo);
    }
    if (t.isPostgreSQLEngine() || t.hasPostgreSQLPeer())
    {
        possible_types
            &= ~(allow_hugeint | allow_unsigned_int | allow_dynamic | allow_map | allow_tuple | allow_variant | allow_nested | allow_geo);
    }
    if (t.isSQLiteEngine() || t.hasSQLitePeer())
    {
        possible_types &= ~(
            allow_hugeint | allow_unsigned_int | allow_dynamic | allow_array | allow_map | allow_tuple | allow_variant | allow_nested
            | allow_geo);
    }
    if (t.isMongoDBEngine())
    {
        possible_types &= ~(allow_dynamic | allow_map | allow_tuple | allow_variant | allow_nested);
    }

    col.cname = cname;
    cd->mutable_col()->set_column("c" + std::to_string(cname));
    if (special == ColumnSpecial::SIGN || special == ColumnSpecial::IS_DELETED)
    {
        tp = new IntType(8, special == ColumnSpecial::IS_DELETED);
        cd->mutable_type()->mutable_type()->mutable_non_nullable()->set_integers(
            special == ColumnSpecial::IS_DELETED ? Integers::UInt8 : Integers::Int8);
    }
    else if (special == ColumnSpecial::VERSION)
    {
        if (((possible_types & (allow_dates | allow_datetimes)) == 0) || rg.nextBool())
        {
            Integers nint;

            std::tie(tp, nint) = randomIntType(rg, possible_types);
            cd->mutable_type()->mutable_type()->mutable_non_nullable()->set_integers(nint);
        }
        else if (((possible_types & allow_datetimes) == 0) || rg.nextBool())
        {
            Dates dd;

            std::tie(tp, dd) = randomDateType(rg, possible_types);
            cd->mutable_type()->mutable_type()->mutable_non_nullable()->set_dates(dd);
        }
        else
        {
            tp = randomDateTimeType(rg, possible_types, cd->mutable_type()->mutable_type()->mutable_non_nullable()->mutable_datetimes());
        }
    }
    else
    {
        tp = randomNextType(rg, possible_types, t.col_counter, cd->mutable_type()->mutable_type());
    }
    col.tp = tp;
    col.special = special;
    if (!modify && col.special == ColumnSpecial::NONE
        && (dynamic_cast<const IntType *>(tp) || dynamic_cast<const FloatType *>(tp) || dynamic_cast<const DateType *>(tp)
            || dynamic_cast<const DateTimeType *>(tp) || dynamic_cast<const DecimalType *>(tp) || dynamic_cast<const StringType *>(tp)
            || dynamic_cast<const BoolType *>(tp) || dynamic_cast<const UUIDType *>(tp) || dynamic_cast<const IPv4Type *>(tp)
            || dynamic_cast<const IPv6Type *>(tp))
        && rg.nextSmallNumber() < 3)
    {
        cd->set_nullable(rg.nextBool());
        col.nullable = std::optional<bool>(cd->nullable());
    }
    if (rg.nextMediumNumber() < 16)
    {
        generateNextStatistics(rg, cd->mutable_stats());
    }
    if (col.special == ColumnSpecial::NONE && rg.nextSmallNumber() < 2)
    {
        DefaultModifier * def_value = cd->mutable_defaultv();
        DModifier dmod = static_cast<DModifier>((rg.nextRandomUInt32() % static_cast<uint32_t>(DModifier_MAX)) + 1);

        def_value->set_dvalue(dmod);
        col.dmod = std::optional<DModifier>(dmod);
        if (dmod != DModifier::DEF_EPHEMERAL || rg.nextMediumNumber() < 21)
        {
            addTableRelation(rg, false, "", t);
            this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs
                = this->allow_in_expression_alias = this->allow_subqueries = false;
            generateExpression(rg, def_value->mutable_expr());
            this->allow_in_expression_alias = this->allow_subqueries = true;
            this->levels.clear();
        }
    }
    if (t.isMergeTreeFamily())
    {
        const auto & csettings = allColumnSettings.at(t.teng);

        if ((!col.dmod.has_value() || col.dmod.value() != DModifier::DEF_ALIAS) && rg.nextMediumNumber() < 16)
        {
            const uint32_t ncodecs = (rg.nextMediumNumber() % UINT32_C(3)) + 1;

            for (uint32_t i = 0; i < ncodecs; i++)
            {
                CodecParam * cp = cd->add_codecs();
                CompressionCodec cc
                    = static_cast<CompressionCodec>((rg.nextRandomUInt32() % static_cast<uint32_t>(CompressionCodec_MAX)) + 1);

                cp->set_codec(cc);
                switch (cc)
                {
                    case COMP_LZ4HC:
                    case COMP_ZSTD_QAT:
                        if (rg.nextBool())
                        {
                            std::uniform_int_distribution<uint32_t> next_dist(1, 12);
                            cp->add_params(next_dist(rg.generator));
                        }
                        break;
                    case COMP_ZSTD:
                        if (rg.nextBool())
                        {
                            std::uniform_int_distribution<uint32_t> next_dist(1, 22);
                            cp->add_params(next_dist(rg.generator));
                        }
                        break;
                    case COMP_Delta:
                    case COMP_DoubleDelta:
                    case COMP_Gorilla:
                        if (rg.nextBool())
                        {
                            std::uniform_int_distribution<uint32_t> next_dist(0, 3);
                            cp->add_params(UINT32_C(1) << next_dist(rg.generator));
                        }
                        break;
                    case COMP_FPC:
                        if (rg.nextBool())
                        {
                            std::uniform_int_distribution<uint32_t> next_dist1(1, 28);
                            cp->add_params(next_dist1(rg.generator));
                            cp->add_params(rg.nextBool() ? 4 : 9);
                        }
                        break;
                    default:
                        break;
                }
            }
        }
        if ((!col.dmod.has_value() || col.dmod.value() != DModifier::DEF_EPHEMERAL) && !csettings.empty() && rg.nextMediumNumber() < 16)
        {
            generateSettingValues(rg, csettings, cd->mutable_settings());
        }
        cd->set_is_pkey(is_pk);
    }
    if (rg.nextSmallNumber() < 3)
    {
        buf.resize(0);
        rg.nextString(buf, "'", true, rg.nextRandomUInt32() % 1009);
        cd->set_comment(buf);
    }
    to_add[cname] = std::move(col);
    return 0;
}

int StatementGenerator::addTableIndex(RandomGenerator & rg, SQLTable & t, const bool staged, IndexDef * idef)
{
    SQLIndex idx;
    const uint32_t iname = t.idx_counter++;
    Expr * expr = idef->mutable_expr();
    IndexType itpe = static_cast<IndexType>((rg.nextRandomUInt32() % static_cast<uint32_t>(IndexType_MAX)) + 1);
    auto & to_add = staged ? t.staged_idxs : t.idxs;

    idx.iname = iname;
    idef->mutable_idx()->set_index("i" + std::to_string(iname));
    idef->set_type(itpe);
    if (rg.nextSmallNumber() < 9)
    {
        const NestedType * ntp = nullptr;

        assert(this->entries.empty());
        for (const auto & entry : t.cols)
        {
            if ((ntp = dynamic_cast<const NestedType *>(entry.second.tp)))
            {
                for (const auto & entry2 : ntp->subtypes)
                {
                    if (itpe < IndexType::IDX_ngrambf_v1 || hasType<StringType, true, false>(entry2.subtype))
                    {
                        entries.push_back(InsertEntry(
                            entry.second.nullable,
                            ColumnSpecial::NONE,
                            entry.second.cname,
                            std::optional<uint32_t>(entry2.cname),
                            entry2.array_subtype,
                            entry.second.dmod));
                    }
                }
            }
            else if (itpe < IndexType::IDX_ngrambf_v1 || hasType<StringType, true, false>(entry.second.tp))
            {
                entries.push_back(InsertEntry(
                    entry.second.nullable, entry.second.special, entry.second.cname, std::nullopt, entry.second.tp, entry.second.dmod));
            }
        }
    }
    if (!entries.empty())
    {
        std::shuffle(entries.begin(), entries.end(), rg.generator);

        if (itpe == IndexType::IDX_hypothesis && entries.size() > 1 && rg.nextSmallNumber() < 9)
        {
            BinaryExpr * bexpr = expr->mutable_comp_expr()->mutable_binary_expr();
            Expr *expr1 = bexpr->mutable_lhs(), *expr2 = bexpr->mutable_rhs();
            ExprSchemaTableColumn *estc1 = expr1->mutable_comp_expr()->mutable_expr_stc(),
                                  *estc2 = expr2->mutable_comp_expr()->mutable_expr_stc();
            ExprColumn *ecol1 = estc1->mutable_col(), *ecol2 = estc2->mutable_col();
            const InsertEntry &entry1 = this->entries[0], &entry2 = this->entries[1];

            bexpr->set_op(
                rg.nextSmallNumber() < 8
                    ? BinaryOperator::BINOP_EQ
                    : static_cast<BinaryOperator>((rg.nextRandomUInt32() % static_cast<uint32_t>(BinaryOperator::BINOP_LEGR)) + 1));
            ecol1->mutable_col()->set_column("c" + std::to_string(entry1.cname1));
            if (entry1.cname2.has_value())
            {
                ecol1->mutable_subcol()->set_column("c" + std::to_string(entry1.cname2.value()));
            }
            ecol2->mutable_col()->set_column("c" + std::to_string(entry2.cname1));
            if (entry2.cname2.has_value())
            {
                ecol2->mutable_subcol()->set_column("c" + std::to_string(entry2.cname2.value()));
            }
            addFieldAccess(rg, expr1, 11);
            addFieldAccess(rg, expr2, 11);
            addColNestedAccess(rg, ecol1, 21);
            addColNestedAccess(rg, ecol2, 21);
        }
        else
        {
            insertEntryRef(this->entries[0], expr);
        }
        entries.clear();
    }
    else
    {
        addTableRelation(rg, false, "", t);
        this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs
            = this->allow_in_expression_alias = this->allow_subqueries = false;
        generateExpression(rg, expr);
        this->allow_in_expression_alias = this->allow_subqueries = true;
        this->levels.clear();
    }
    switch (itpe)
    {
        case IndexType::IDX_set:
            if (rg.nextSmallNumber() < 7)
            {
                idef->add_params()->set_ival(0);
            }
            else
            {
                std::uniform_int_distribution<uint32_t> next_dist(1, 1000);
                idef->add_params()->set_ival(next_dist(rg.generator));
            }
            break;
        case IndexType::IDX_bloom_filter: {
            std::uniform_int_distribution<uint32_t> next_dist(1, 1000);
            idef->add_params()->set_dval(static_cast<double>(next_dist(rg.generator)) / static_cast<double>(1000));
        }
        break;
        case IndexType::IDX_ngrambf_v1:
        case IndexType::IDX_tokenbf_v1: {
            std::uniform_int_distribution<uint32_t> next_dist1(1, 1000), next_dist2(1, 5);

            if (itpe == IndexType::IDX_ngrambf_v1)
            {
                idef->add_params()->set_ival(next_dist1(rg.generator));
            }
            idef->add_params()->set_ival(next_dist1(rg.generator));
            idef->add_params()->set_ival(next_dist2(rg.generator));
            idef->add_params()->set_ival(next_dist1(rg.generator));
        }
        break;
        case IndexType::IDX_full_text:
        case IndexType::IDX_inverted: {
            std::uniform_int_distribution<uint32_t> next_dist(0, 10);
            idef->add_params()->set_ival(next_dist(rg.generator));
        }
        break;
        case IndexType::IDX_minmax:
        case IndexType::IDX_hypothesis:
            break;
    }
    if (rg.nextSmallNumber() < 7)
    {
        std::uniform_int_distribution<uint32_t> next_dist(1, 1000);
        idef->set_granularity(next_dist(rg.generator));
    }
    to_add[iname] = std::move(idx);
    return 0;
}

int StatementGenerator::addTableProjection(RandomGenerator & rg, SQLTable & t, const bool staged, ProjectionDef * pdef)
{
    const uint32_t pname = t.proj_counter++,
                   ncols = std::max(std::min(this->fc.max_width - this->width, (rg.nextMediumNumber() % UINT32_C(3)) + 1), UINT32_C(1));
    auto & to_add = staged ? t.staged_projs : t.projs;

    pdef->mutable_proj()->set_projection("p" + std::to_string(pname));
    this->inside_projection = true;
    addTableRelation(rg, false, "", t);
    generateSelect(rg, true, false, ncols, allow_groupby | allow_orderby, pdef->mutable_select());
    this->levels.clear();
    this->inside_projection = false;
    to_add.insert(pname);
    return 0;
}

int StatementGenerator::addTableConstraint(RandomGenerator & rg, SQLTable & t, const bool staged, ConstraintDef * cdef)
{
    const uint32_t crname = t.constr_counter++;
    auto & to_add = staged ? t.staged_constrs : t.constrs;

    cdef->mutable_constr()->set_constraint("c" + std::to_string(crname));
    cdef->set_ctype(
        static_cast<ConstraintDef_ConstraintType>((rg.nextRandomUInt32() % static_cast<uint32_t>(ConstraintDef::ConstraintType_MAX)) + 1));
    addTableRelation(rg, false, "", t);
    this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs = false;
    this->generateWherePredicate(rg, cdef->mutable_expr());
    this->levels.clear();
    to_add.insert(crname);
    return 0;
}

PeerTableDatabase StatementGenerator::getNextPeerTableDatabase(RandomGenerator & rg, TableEngineValues teng)
{
    assert(this->ids.empty());
    if (teng != TableEngineValues::Set)
    {
        if (teng != TableEngineValues::MySQL && connections.hasMySQLConnection())
        {
            this->ids.push_back(PeerTableDatabase::PeerMySQL);
        }
        if (teng != TableEngineValues::PostgreSQL && connections.hasPostgreSQLConnection())
        {
            this->ids.push_back(PeerTableDatabase::PeerPostgreSQL);
        }
        if (teng != TableEngineValues::SQLite && connections.hasSQLiteConnection())
        {
            this->ids.push_back(PeerTableDatabase::PeerSQLite);
        }
        if (teng >= TableEngineValues::MergeTree && teng <= TableEngineValues::VersionedCollapsingMergeTree
            && connections.hasClickHouseExtraServerConnection())
        {
            this->ids.push_back(PeerTableDatabase::PeerClickHouse);
            this->ids.push_back(PeerTableDatabase::PeerClickHouse); // give more probability
        }
    }
    const auto res = (this->ids.empty() || rg.nextBool()) ? PeerTableDatabase::PeerNone
                                                          : static_cast<PeerTableDatabase>(rg.pickRandomlyFromVector(this->ids));
    this->ids.clear();
    return res;
}

TableEngineValues StatementGenerator::getNextTableEngine(RandomGenerator & rg, const bool use_external_integrations)
{
    if (rg.nextSmallNumber() < 9)
    {
        std::uniform_int_distribution<uint32_t> table_engine(1, TableEngineValues::VersionedCollapsingMergeTree);
        return static_cast<TableEngineValues>(table_engine(rg.generator));
    }
    assert(this->ids.empty());
    this->ids.push_back(MergeTree);
    this->ids.push_back(ReplacingMergeTree);
    this->ids.push_back(SummingMergeTree);
    this->ids.push_back(AggregatingMergeTree);
    this->ids.push_back(CollapsingMergeTree);
    this->ids.push_back(VersionedCollapsingMergeTree);
    this->ids.push_back(File);
    this->ids.push_back(Null);
    this->ids.push_back(Set);
    this->ids.push_back(Join);
    this->ids.push_back(Memory);
    this->ids.push_back(StripeLog);
    this->ids.push_back(Log);
    this->ids.push_back(TinyLog);
    this->ids.push_back(EmbeddedRocksDB);
    if (collectionHas<SQLTable>([](const SQLTable & t)
                                { return t.db && t.db->attached == DetachStatus::ATTACHED && t.attached == DetachStatus::ATTACHED; })
        || collectionHas<SQLView>([](const SQLView & v)
                                  { return v.db && v.db->attached == DetachStatus::ATTACHED && v.attached == DetachStatus::ATTACHED; }))
    {
        this->ids.push_back(Buffer);
    }
    if (use_external_integrations)
    {
        if (connections.hasMySQLConnection())
        {
            this->ids.push_back(MySQL);
        }
        if (connections.hasPostgreSQLConnection())
        {
            this->ids.push_back(PostgreSQL);
        }
        if (connections.hasSQLiteConnection())
        {
            this->ids.push_back(SQLite);
        }
        if (connections.hasMongoDBConnection())
        {
            this->ids.push_back(MongoDB);
        }
        if (connections.hasRedisConnection())
        {
            this->ids.push_back(Redis);
        }
        if (connections.hasMinIOConnection())
        {
            this->ids.push_back(S3);
            //this->ids.push_back(S3Queue);
            this->ids.push_back(Hudi);
            this->ids.push_back(DeltaLake);
            //this->ids.push_back(IcebergS3);
        }
    }

    const auto res = static_cast<TableEngineValues>(rg.pickRandomlyFromVector(this->ids));
    this->ids.clear();
    return res;
}

const std::vector<TableEngineValues> like_engs
    = {TableEngineValues::MergeTree,
       TableEngineValues::ReplacingMergeTree,
       TableEngineValues::SummingMergeTree,
       TableEngineValues::AggregatingMergeTree,
       TableEngineValues::File,
       TableEngineValues::Null,
       TableEngineValues::Set,
       TableEngineValues::Join,
       TableEngineValues::Memory,
       TableEngineValues::StripeLog,
       TableEngineValues::Log,
       TableEngineValues::TinyLog,
       TableEngineValues::EmbeddedRocksDB};

int StatementGenerator::generateNextCreateTable(RandomGenerator & rg, CreateTable * ct)
{
    SQLTable next;
    uint32_t tname = 0;
    bool added_pkey = false;
    const NestedType * ntp = nullptr;
    TableEngine * te = ct->mutable_engine();
    ExprSchemaTable * est = ct->mutable_est();
    SettingValues * svs = nullptr;
    const bool replace = collectionCount<SQLTable>(
                             [](const SQLTable & tt)
                             {
                                 return (!tt.db || tt.db->attached == DetachStatus::ATTACHED) && tt.attached == DetachStatus::ATTACHED
                                     && !tt.hasDatabasePeer();
                             })
            > 3
        && rg.nextMediumNumber() < 16;

    next.is_temp = rg.nextMediumNumber() < 22;
    ct->set_is_temp(next.is_temp);
    if (replace)
    {
        const SQLTable & t = rg.pickRandomlyFromVector(filterCollection<SQLTable>(
            [](const SQLTable & tt)
            {
                return (!tt.db || tt.db->attached == DetachStatus::ATTACHED) && tt.attached == DetachStatus::ATTACHED
                    && !tt.hasDatabasePeer();
            }));

        next.db = t.db;
        tname = next.tname = t.tname;
    }
    else
    {
        if (!next.is_temp && rg.nextSmallNumber() < 9)
        {
            next.db = rg.pickRandomlyFromVector(filterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));
        }
        tname = next.tname = this->table_counter++;
    }
    ct->set_replace(replace);
    if (next.db)
    {
        est->mutable_database()->set_database("d" + std::to_string(next.db->dname));
    }
    est->mutable_table()->set_table("t" + std::to_string(next.tname));
    if (!collectionHas<SQLTable>(attached_tables) || rg.nextSmallNumber() < 9)
    {
        //create table with definition
        TableDef * colsdef = ct->mutable_table_def();

        next.teng = getNextTableEngine(rg, true);
        te->set_engine(next.teng);
        next.peer_table = getNextPeerTableDatabase(rg, next.teng);
        added_pkey |= (!next.isMergeTreeFamily() && !next.isRocksEngine() && !next.isRedisEngine());
        const bool add_version_to_replacing = next.teng == TableEngineValues::ReplacingMergeTree && !next.hasPostgreSQLPeer()
            && !next.hasSQLitePeer() && rg.nextSmallNumber() < 4;
        uint32_t added_cols = 0, added_idxs = 0, added_projs = 0, added_consts = 0, added_sign = 0, added_is_deleted = 0, added_version = 0;
        const uint32_t to_addcols
            = (rg.nextMediumNumber() % 5) + 1,
            to_addidxs = (rg.nextMediumNumber() % 4) * static_cast<uint32_t>(next.isMergeTreeFamily() && rg.nextSmallNumber() < 4),
            to_addprojs = (rg.nextMediumNumber() % 3) * static_cast<uint32_t>(next.isMergeTreeFamily() && rg.nextSmallNumber() < 5),
            to_addconsts = (rg.nextMediumNumber() % 3) * static_cast<uint32_t>(rg.nextSmallNumber() < 3),
            to_add_sign = static_cast<uint32_t>(next.hasSignColumn()),
            to_add_version = static_cast<uint32_t>(next.hasVersionColumn() || add_version_to_replacing),
            to_add_is_deleted = static_cast<uint32_t>(add_version_to_replacing && rg.nextSmallNumber() < 4),
            total_to_add = to_addcols + to_addidxs + to_addprojs + to_addconsts + to_add_sign + to_add_version + to_add_is_deleted;

        for (uint32_t i = 0; i < total_to_add; i++)
        {
            const uint32_t add_idx = 4 * static_cast<uint32_t>(!next.cols.empty() && added_idxs < to_addidxs),
                           add_proj = 4 * static_cast<uint32_t>(!next.cols.empty() && added_projs < to_addprojs),
                           add_const = 4 * static_cast<uint32_t>(!next.cols.empty() && added_consts < to_addconsts),
                           add_col = 8 * static_cast<uint32_t>(added_cols < to_addcols),
                           add_sign = 2 * static_cast<uint32_t>(added_sign < to_add_sign),
                           add_version = 2 * static_cast<uint32_t>(added_version < to_add_version && added_sign == to_add_sign),
                           add_is_deleted
                = 2 * static_cast<uint32_t>(added_is_deleted < to_add_is_deleted && added_version == to_add_version),
                           prob_space = add_idx + add_proj + add_const + add_col + add_sign + add_version + add_is_deleted;
            std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
            const uint32_t nopt = next_dist(rg.generator);

            if (add_idx && nopt < (add_idx + 1))
            {
                addTableIndex(rg, next, false, colsdef->add_other_defs()->mutable_idx_def());
                added_idxs++;
            }
            else if (add_proj && nopt < (add_idx + add_proj + 1))
            {
                addTableProjection(rg, next, false, colsdef->add_other_defs()->mutable_proj_def());
                added_projs++;
            }
            else if (add_const && nopt < (add_idx + add_proj + add_const + 1))
            {
                addTableConstraint(rg, next, false, colsdef->add_other_defs()->mutable_const_def());
                added_consts++;
            }
            else if (add_col && nopt < (add_idx + add_proj + add_const + add_col + 1))
            {
                const bool add_pkey = !added_pkey && rg.nextMediumNumber() < 4;
                ColumnDef * cd = i == 0 ? colsdef->mutable_col_def() : colsdef->add_other_defs()->mutable_col_def();

                addTableColumn(rg, next, next.col_counter++, false, false, add_pkey, ColumnSpecial::NONE, cd);
                added_pkey |= add_pkey;
                added_cols++;
            }
            else
            {
                const uint32_t cname = next.col_counter++;
                const bool add_pkey = !added_pkey && rg.nextMediumNumber() < 4,
                           add_version_col = add_version && nopt < (add_idx + add_proj + add_const + add_col + add_version + 1);
                ColumnDef * cd = i == 0 ? colsdef->mutable_col_def() : colsdef->add_other_defs()->mutable_col_def();

                addTableColumn(
                    rg,
                    next,
                    cname,
                    false,
                    false,
                    add_pkey,
                    add_version_col ? ColumnSpecial::VERSION : (add_sign ? ColumnSpecial::SIGN : ColumnSpecial::IS_DELETED),
                    cd);
                added_pkey |= add_pkey;
                te->add_params()->mutable_cols()->mutable_col()->set_column("c" + std::to_string(cname));
                if (add_version_col)
                {
                    added_version++;
                }
                else if (add_sign)
                {
                    assert(!add_is_deleted);
                    added_sign++;
                }
                else
                {
                    assert(add_is_deleted);
                    added_is_deleted++;
                }
            }
        }
        if (rg.nextMediumNumber() < 16)
        {
            this->levels[this->current_level] = QueryLevel(this->current_level);
            generateSelect(
                rg,
                true,
                false,
                static_cast<uint32_t>(next.numberOfInsertableColumns()),
                std::numeric_limits<uint32_t>::max(),
                ct->mutable_as_select_stmt());
        }
    }
    else
    {
        //create table as
        CreateTableAs * cta = ct->mutable_table_as();
        ExprSchemaTable * aest = cta->mutable_est();
        const SQLTable & t = rg.pickRandomlyFromVector(filterCollection<SQLTable>(attached_tables));
        std::uniform_int_distribution<size_t> table_engine(0, rg.nextSmallNumber() < 8 ? 3 : (like_engs.size() - 1));
        TableEngineValues val = like_engs[table_engine(rg.generator)];

        next.teng = val;
        te->set_engine(val);
        cta->set_clone(next.isMergeTreeFamily() && rg.nextBool());
        if (t.db)
        {
            aest->mutable_database()->set_database("d" + std::to_string(t.db->dname));
        }
        aest->mutable_table()->set_table("t" + std::to_string(t.tname));
        for (const auto & col : t.cols)
        {
            next.cols[col.first] = col.second;
        }
        for (const auto & idx : t.idxs)
        {
            next.idxs[idx.first] = idx.second;
        }
        next.projs.insert(t.projs.begin(), t.projs.end());
        next.constrs.insert(t.constrs.begin(), t.constrs.end());
        next.col_counter = t.col_counter;
        next.idx_counter = t.idx_counter;
        next.proj_counter = t.proj_counter;
        next.constr_counter = t.constr_counter;
        next.is_temp = t.is_temp;
    }

    assert(this->entries.empty());
    for (const auto & entry : next.cols)
    {
        if ((ntp = dynamic_cast<const NestedType *>(entry.second.tp)))
        {
            for (const auto & entry2 : ntp->subtypes)
            {
                entries.push_back(InsertEntry(
                    std::nullopt,
                    ColumnSpecial::NONE,
                    entry.second.cname,
                    std::optional<uint32_t>(entry2.cname),
                    entry2.array_subtype,
                    entry.second.dmod));
            }
        }
        else
        {
            entries.push_back(InsertEntry(
                entry.second.nullable, entry.second.special, entry.second.cname, std::nullopt, entry.second.tp, entry.second.dmod));
        }
    }
    generateEngineDetails(rg, next, !added_pkey, te);

    const auto & tsettings = allTableSettings.at(next.teng);
    if (!tsettings.empty() && rg.nextSmallNumber() < 5)
    {
        svs = ct->mutable_settings();
        generateSettingValues(rg, tsettings, svs);
    }
    if (next.isMergeTreeFamily() || next.isAnyS3Engine())
    {
        if (!svs)
        {
            svs = ct->mutable_settings();
        }
        SetValue * sv = svs->has_set_value() ? svs->add_other_values() : svs->mutable_set_value();

        if (next.isMergeTreeFamily())
        {
            sv->set_property("allow_nullable_key");
            sv->set_value("1");

            if (next.toption.has_value() && next.toption.value() == TableEngineOption::TShared)
            {
                //requires keeper storage
                bool found = false;
                auto & ovals = const_cast<decltype(svs->other_values())&>(svs->other_values());

                for (auto it = ovals.begin(); it != ovals.end() && !found; it++)
                {
                    if (it->property() == "storage_policy")
                    {
                        auto & prop = const_cast<SetValue&>(*it);
                        prop.set_value("'s3_with_keeper'");
                        found = true;
                    }
                }
                if (!found)
                {
                    SetValue * sv2 = svs->add_other_values();

                    sv2->set_property("storage_policy");
                    sv2->set_value("'s3_with_keeper'");
                }
            }
        }
        else
        {
            sv->set_property("input_format_with_names_use_header");
            sv->set_value("0");
            if (next.isS3QueueEngine())
            {
                SetValue * sv2 = svs->add_other_values();

                sv2->set_property("mode");
                sv2->set_value(rg.nextBool() ? "'ordered'" : "'unordered'");
            }
        }
    }
    if (rg.nextSmallNumber() < 3)
    {
        buf.resize(0);
        rg.nextString(buf, "'", true, rg.nextRandomUInt32() % 1009);
        ct->set_comment(buf);
    }
    if (next.hasDatabasePeer())
    {
        connections.createPeerTable(rg, next.peer_table, next, ct, entries);
    }
    entries.clear();
    assert(!next.toption.has_value() || next.isMergeTreeFamily());
    this->staged_tables[tname] = std::move(next);
    return 0;
}

}
