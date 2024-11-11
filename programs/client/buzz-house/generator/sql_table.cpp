#include "sql_catalog.h"
#include "statement_generator.h"

namespace buzzhouse
{

void StatementGenerator::AddTableRelation(
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
    if (allow_internal_cols && rg.NextSmallNumber() < 3)
    {
        if (t.IsMergeTreeFamily())
        {
            rel.cols.push_back(SQLRelationCol(rel_name, "_block_number", std::nullopt));
            rel.cols.push_back(SQLRelationCol(rel_name, "_part", std::nullopt));
            rel.cols.push_back(SQLRelationCol(rel_name, "_part_data_version", std::nullopt));
            rel.cols.push_back(SQLRelationCol(rel_name, "_part_index", std::nullopt));
            rel.cols.push_back(SQLRelationCol(rel_name, "_part_offset", std::nullopt));
            rel.cols.push_back(SQLRelationCol(rel_name, "_part_uuid", std::nullopt));
            rel.cols.push_back(SQLRelationCol(rel_name, "_partition_id", std::nullopt));
            rel.cols.push_back(SQLRelationCol(rel_name, "_partition_value", std::nullopt));
            rel.cols.push_back(SQLRelationCol(rel_name, "_sample_factor", std::nullopt));
        }
        else if (t.IsAnyS3Engine() || t.IsFileEngine())
        {
            rel.cols.push_back(SQLRelationCol(rel_name, "_path", std::nullopt));
            rel.cols.push_back(SQLRelationCol(rel_name, "_file", std::nullopt));
            if (t.IsS3Engine())
            {
                rel.cols.push_back(SQLRelationCol(rel_name, "_size", std::nullopt));
                rel.cols.push_back(SQLRelationCol(rel_name, "_time", std::nullopt));
                rel.cols.push_back(SQLRelationCol(rel_name, "_etag", std::nullopt));
            }
        }
    }
    if (rel_name == "")
    {
        this->levels[this->current_level] = QueryLevel(this->current_level);
    }
    this->levels[this->current_level].rels.push_back(std::move(rel));
}

int StatementGenerator::GenerateNextStatistics(RandomGenerator & rg, sql_query_grammar::ColumnStatistics * cstats)
{
    const size_t nstats = (rg.NextMediumNumber() % static_cast<uint32_t>(sql_query_grammar::ColumnStat_MAX)) + 1;

    for (uint32_t i = 1; i <= sql_query_grammar::ColumnStat_MAX; i++)
    {
        ids.push_back(i);
    }
    std::shuffle(ids.begin(), ids.end(), rg.gen);
    for (size_t i = 0; i < nstats; i++)
    {
        const sql_query_grammar::ColumnStat nstat = static_cast<sql_query_grammar::ColumnStat>(ids[i]);

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

int StatementGenerator::PickUpNextCols(RandomGenerator & rg, const SQLTable & t, sql_query_grammar::ColumnList * clist)
{
    const NestedType * ntp = nullptr;
    const size_t ncols = (rg.NextMediumNumber() % std::min<uint32_t>(static_cast<uint32_t>(t.RealNumberOfColumns()), UINT32_C(4))) + 1;

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
    std::shuffle(ids.begin(), ids.end(), rg.gen);
    for (size_t i = 0; i < ncols; i++)
    {
        sql_query_grammar::Column * col = i == 0 ? clist->mutable_col() : clist->add_other_cols();

        col->set_column("c" + std::to_string(ids[i]));
    }
    ids.clear();
    return 0;
}

const std::vector<sql_query_grammar::SQLFunc> multicol_hash
    = {sql_query_grammar::SQLFunc::FUNChalfMD5,
       sql_query_grammar::SQLFunc::FUNCsipHash64,
       sql_query_grammar::SQLFunc::FUNCsipHash128,
       sql_query_grammar::SQLFunc::FUNCsipHash128Reference,
       sql_query_grammar::SQLFunc::FUNCcityHash64,
       sql_query_grammar::SQLFunc::FUNCfarmFingerprint64,
       sql_query_grammar::SQLFunc::FUNCfarmHash64,
       sql_query_grammar::SQLFunc::FUNCmetroHash64,
       sql_query_grammar::SQLFunc::FUNCmurmurHash2_32,
       sql_query_grammar::SQLFunc::FUNCmurmurHash2_64,
       sql_query_grammar::SQLFunc::FUNCgccMurmurHash,
       sql_query_grammar::SQLFunc::FUNCkafkaMurmurHash,
       sql_query_grammar::SQLFunc::FUNCmurmurHash3_32,
       sql_query_grammar::SQLFunc::FUNCmurmurHash3_64,
       sql_query_grammar::SQLFunc::FUNCmurmurHash3_128};

const std::vector<sql_query_grammar::SQLFunc> dates_hash
    = {sql_query_grammar::SQLFunc::FUNCtoYear,
       sql_query_grammar::SQLFunc::FUNCtoQuarter,
       sql_query_grammar::SQLFunc::FUNCtoMonth,
       sql_query_grammar::SQLFunc::FUNCtoDayOfYear,
       sql_query_grammar::SQLFunc::FUNCtoDayOfMonth,
       sql_query_grammar::SQLFunc::FUNCtoDayOfWeek,
       sql_query_grammar::SQLFunc::FUNCtoHour,
       sql_query_grammar::SQLFunc::FUNCtoMinute,
       sql_query_grammar::SQLFunc::FUNCtoSecond,
       sql_query_grammar::SQLFunc::FUNCtoMillisecond,
       sql_query_grammar::SQLFunc::FUNCtoUnixTimestamp,
       sql_query_grammar::SQLFunc::FUNCtoStartOfYear,
       sql_query_grammar::SQLFunc::FUNCtoStartOfISOYear,
       sql_query_grammar::SQLFunc::FUNCtoStartOfQuarter,
       sql_query_grammar::SQLFunc::FUNCtoStartOfMonth,
       sql_query_grammar::SQLFunc::FUNCtoLastDayOfMonth,
       sql_query_grammar::SQLFunc::FUNCtoMonday,
       sql_query_grammar::SQLFunc::FUNCtoStartOfWeek,
       sql_query_grammar::SQLFunc::FUNCtoLastDayOfWeek,
       sql_query_grammar::SQLFunc::FUNCtoStartOfDay,
       sql_query_grammar::SQLFunc::FUNCtoStartOfHour,
       sql_query_grammar::SQLFunc::FUNCtoStartOfMinute,
       sql_query_grammar::SQLFunc::FUNCtoStartOfSecond,
       sql_query_grammar::SQLFunc::FUNCtoStartOfMillisecond,
       sql_query_grammar::SQLFunc::FUNCtoStartOfMicrosecond,
       sql_query_grammar::SQLFunc::FUNCtoStartOfNanosecond,
       sql_query_grammar::SQLFunc::FUNCtoStartOfFiveMinutes,
       sql_query_grammar::SQLFunc::FUNCtoStartOfTenMinutes,
       sql_query_grammar::SQLFunc::FUNCtoStartOfFifteenMinutes,
       sql_query_grammar::SQLFunc::FUNCtoTime,
       sql_query_grammar::SQLFunc::FUNCtoRelativeYearNum,
       sql_query_grammar::SQLFunc::FUNCtoRelativeQuarterNum,
       sql_query_grammar::SQLFunc::FUNCtoRelativeMonthNum,
       sql_query_grammar::SQLFunc::FUNCtoRelativeWeekNum,
       sql_query_grammar::SQLFunc::FUNCtoRelativeDayNum,
       sql_query_grammar::SQLFunc::FUNCtoRelativeHourNum,
       sql_query_grammar::SQLFunc::FUNCtoRelativeMinuteNum,
       sql_query_grammar::SQLFunc::FUNCtoRelativeSecondNum,
       sql_query_grammar::SQLFunc::FUNCtoISOYear,
       sql_query_grammar::SQLFunc::FUNCtoISOWeek,
       sql_query_grammar::SQLFunc::FUNCtoWeek,
       sql_query_grammar::SQLFunc::FUNCtoYearWeek,
       sql_query_grammar::SQLFunc::FUNCtoDaysSinceYearZero,
       sql_query_grammar::SQLFunc::FUNCtoday,
       sql_query_grammar::SQLFunc::FUNCyesterday,
       sql_query_grammar::SQLFunc::FUNCtimeSlot,
       sql_query_grammar::SQLFunc::FUNCtoYYYYMM,
       sql_query_grammar::SQLFunc::FUNCtoYYYYMMDD,
       sql_query_grammar::SQLFunc::FUNCtoYYYYMMDDhhmmss,
       sql_query_grammar::SQLFunc::FUNCmonthName,
       sql_query_grammar::SQLFunc::FUNCtoModifiedJulianDay,
       sql_query_grammar::SQLFunc::FUNCtoModifiedJulianDayOrNull,
       sql_query_grammar::SQLFunc::FUNCtoUTCTimestamp};

void StatementGenerator::InsertEntryRef(const InsertEntry & entry, sql_query_grammar::Expr * expr)
{
    sql_query_grammar::ExprColumn * ecol = expr->mutable_comp_expr()->mutable_expr_stc()->mutable_col();

    ecol->mutable_col()->set_column("c" + std::to_string(entry.cname1));
    if (entry.cname2.has_value())
    {
        ecol->mutable_subcol()->set_column("c" + std::to_string(entry.cname2.value()));
    }
}

void StatementGenerator::InsertEntryRefCP(const InsertEntry & entry, sql_query_grammar::ColumnPath * cp)
{
    cp->mutable_col()->set_column("c" + std::to_string(entry.cname1));
    if (entry.cname2.has_value())
    {
        cp->add_sub_cols()->set_column("c" + std::to_string(entry.cname2.value()));
    }
}

int StatementGenerator::GenerateTableKey(RandomGenerator & rg, sql_query_grammar::TableKey * tkey)
{
    if (!entries.empty() && rg.NextSmallNumber() < 7)
    {
        const size_t ocols = (rg.NextMediumNumber() % std::min<size_t>(entries.size(), UINT32_C(3))) + 1;

        std::shuffle(entries.begin(), entries.end(), rg.gen);
        if (rg.NextSmallNumber() < 3)
        {
            //Use a single expression for the entire table
            sql_query_grammar::SQLFuncCall * func_call = tkey->add_exprs()->mutable_comp_expr()->mutable_func_call();

            func_call->mutable_func()->set_catalog_func(rg.PickRandomlyFromVector(multicol_hash));
            for (size_t i = 0; i < ocols; i++)
            {
                InsertEntryRef(this->entries[i], func_call->add_args()->mutable_expr());
            }
        }
        else
        {
            for (size_t i = 0; i < ocols; i++)
            {
                const InsertEntry & entry = this->entries[i];

                if ((HasType<DateType, false>(entry.tp) || HasType<DateTimeType, false>(entry.tp)) && rg.NextBool())
                {
                    sql_query_grammar::SQLFuncCall * func_call = tkey->add_exprs()->mutable_comp_expr()->mutable_func_call();

                    func_call->mutable_func()->set_catalog_func(rg.PickRandomlyFromVector(dates_hash));
                    InsertEntryRef(entry, func_call->add_args()->mutable_expr());
                }
                else
                {
                    InsertEntryRef(entry, tkey->add_exprs());
                }
            }
        }
    }
    return 0;
}

int StatementGenerator::GenerateMergeTreeEngineDetails(
    RandomGenerator & rg, const sql_query_grammar::TableEngineValues teng, const bool add_pkey, sql_query_grammar::TableEngine * te)
{
    if (rg.NextSmallNumber() < 6)
    {
        GenerateTableKey(rg, te->mutable_order());
    }
    if (te->order().exprs_size() && add_pkey && rg.NextSmallNumber() < 5)
    {
        //pkey is a subset of order by
        sql_query_grammar::TableKey * tkey = te->mutable_primary_key();
        std::uniform_int_distribution<uint32_t> table_order_by(1, te->order().exprs_size());
        const uint32_t pkey_size = table_order_by(rg.gen);

        for (uint32_t i = 0; i < pkey_size; i++)
        {
            tkey->add_exprs()->CopyFrom(te->order().exprs(i));
        }
    }
    else if (!te->order().exprs_size() && add_pkey)
    {
        GenerateTableKey(rg, te->mutable_primary_key());
    }
    if (rg.NextSmallNumber() < 5)
    {
        GenerateTableKey(rg, te->mutable_partition_by());
    }

    const size_t npkey = te->primary_key().exprs_size();
    if (npkey && rg.NextSmallNumber() < 5)
    {
        //try to add sample key
        assert(this->ids.empty());
        for (size_t i = 0; i < this->entries.size(); i++)
        {
            const IntType * itp = nullptr;
            const InsertEntry & entry = this->entries[i];

            if (!entry.cname2.has_value() && (itp = dynamic_cast<const IntType *>(entry.tp)) && itp->is_unsigned)
            {
                const sql_query_grammar::TableKey & tpk = te->primary_key();

                //must be in pkey
                for (size_t j = 0; j < npkey; j++)
                {
                    if (tpk.exprs(j).has_comp_expr() && tpk.exprs(j).comp_expr().has_expr_stc())
                    {
                        const sql_query_grammar::ExprColumn & oecol = tpk.exprs(j).comp_expr().expr_stc().col();

                        if (!oecol.has_subcol() && std::stoul(oecol.col().column().substr(1)) == entry.cname1)
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
            sql_query_grammar::TableKey * tkey = te->mutable_sample_by();
            const size_t ncols = (rg.NextMediumNumber() % std::min<size_t>(this->ids.size(), UINT32_C(3))) + 1;

            std::shuffle(ids.begin(), ids.end(), rg.gen);
            for (size_t i = 0; i < ncols; i++)
            {
                sql_query_grammar::ExprColumn * ecol = tkey->add_exprs()->mutable_comp_expr()->mutable_expr_stc()->mutable_col();

                ecol->mutable_col()->set_column("c" + std::to_string(ids[i]));
            }
            this->ids.clear();
        }
    }
    if (teng == sql_query_grammar::TableEngineValues::SummingMergeTree && rg.NextSmallNumber() < 4)
    {
        const size_t ncols = (rg.NextMediumNumber() % std::min<uint32_t>(static_cast<uint32_t>(entries.size()), UINT32_C(4))) + 1;

        std::shuffle(entries.begin(), entries.end(), rg.gen);
        for (size_t i = 0; i < ncols; i++)
        {
            InsertEntryRefCP(entries[i], te->add_params()->mutable_cols());
        }
    }
    return 0;
}

const std::vector<std::string> & s3_compress = {"none", "gzip", "gz", "brotli", "br", "xz", "LZMA", "zstd", "zst"};

int StatementGenerator::GenerateEngineDetails(RandomGenerator & rg, SQLBase & b, const bool add_pkey, sql_query_grammar::TableEngine * te)
{
    if (b.IsMergeTreeFamily())
    {
        if (rg.NextSmallNumber() < 4)
        {
            b.toption = supports_cloud_features && rg.NextBool() ? sql_query_grammar::TableEngineOption::TShared
                                                                 : sql_query_grammar::TableEngineOption::TReplicated;
            te->set_toption(b.toption.value());
        }
        GenerateMergeTreeEngineDetails(rg, b.teng, add_pkey, te);
    }
    else if (b.IsFileEngine())
    {
        const uint32_t noption = rg.NextSmallNumber();
        sql_query_grammar::TableEngineParam * tep = te->add_params();

        if (rg.NextSmallNumber() < 5)
        {
            GenerateTableKey(rg, te->mutable_partition_by());
        }
        if (noption < 9)
        {
            tep->set_in_out(static_cast<sql_query_grammar::InOutFormat>(
                (rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::InOutFormat_MAX)) + 1));
        }
        else if (noption == 9)
        {
            tep->set_in(static_cast<sql_query_grammar::InFormat>(
                (rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::InFormat_MAX)) + 1));
        }
        else
        {
            tep->set_out(static_cast<sql_query_grammar::OutFormat>(
                (rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::OutFormat_MAX)) + 1));
        }
    }
    else if (b.IsJoinEngine())
    {
        const size_t ncols = (rg.NextMediumNumber() % std::min<uint32_t>(static_cast<uint32_t>(entries.size()), UINT32_C(3))) + 1;
        sql_query_grammar::JoinType jt
            = static_cast<sql_query_grammar::JoinType>((rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::J_FULL)) + 1);
        sql_query_grammar::TableEngineParam * tep = te->add_params();

        switch (jt)
        {
            case sql_query_grammar::JoinType::J_LEFT:
            case sql_query_grammar::JoinType::J_INNER:
                tep->set_join_const(static_cast<sql_query_grammar::JoinConst>(
                    (rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::JoinConst_MAX)) + 1));
                break;
            case sql_query_grammar::JoinType::J_RIGHT:
                tep->set_join_const(static_cast<sql_query_grammar::JoinConst>(
                    (rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::JoinConst::J_ANTI)) + 1));
                break;
            case sql_query_grammar::JoinType::J_FULL:
                tep->set_join_const(sql_query_grammar::JoinConst::J_ALL);
                break;
            default:
                assert(0);
                break;
        }
        te->add_params()->set_join_op(jt);

        std::shuffle(entries.begin(), entries.end(), rg.gen);
        for (size_t i = 0; i < ncols; i++)
        {
            InsertEntryRefCP(entries[i], te->add_params()->mutable_cols());
        }
    }
    else if (b.IsBufferEngine())
    {
        const bool has_tables
            = CollectionHas<SQLTable>([](const SQLTable & t)
                                      { return t.db && t.db->attached == DetachStatus::ATTACHED && t.attached == DetachStatus::ATTACHED; }),
            has_views = CollectionHas<SQLView>(
                [](const SQLView & v) { return v.db && v.db->attached == DetachStatus::ATTACHED && v.attached == DetachStatus::ATTACHED; });

        if (has_tables && (!has_views || rg.NextSmallNumber() < 8))
        {
            const SQLTable & t = rg.PickRandomlyFromVector(FilterCollection<SQLTable>(
                [](const SQLTable & tt)
                { return tt.db && tt.db->attached == DetachStatus::ATTACHED && tt.attached == DetachStatus::ATTACHED; }));

            te->add_params()->mutable_database()->set_database("d" + std::to_string(t.db->dname));
            te->add_params()->mutable_table()->set_table("t" + std::to_string(t.tname));
        }
        else
        {
            const SQLView & v = rg.PickRandomlyFromVector(FilterCollection<SQLView>(
                [](const SQLView & vv)
                { return vv.db && vv.db->attached == DetachStatus::ATTACHED && vv.attached == DetachStatus::ATTACHED; }));

            te->add_params()->mutable_database()->set_database("d" + std::to_string(v.db->dname));
            te->add_params()->mutable_table()->set_table("v" + std::to_string(v.tname));
        }
        //num_layers
        te->add_params()->set_num(static_cast<int32_t>(rg.NextRandomUInt32() % 101));
        //min_time, max_time, min_rows, max_rows, min_bytes, max_bytes
        for (int i = 0; i < 6; i++)
        {
            te->add_params()->set_num(static_cast<int32_t>(rg.NextRandomUInt32() % 1001));
        }
        if (rg.NextSmallNumber() < 7)
        {
            //flush_time
            te->add_params()->set_num(static_cast<int32_t>(rg.NextRandomUInt32() % 61));
        }
        if (rg.NextSmallNumber() < 7)
        {
            //flush_rows
            te->add_params()->set_num(static_cast<int32_t>(rg.NextRandomUInt32() % 1001));
        }
        if (rg.NextSmallNumber() < 7)
        {
            //flush_bytes
            te->add_params()->set_num(static_cast<int32_t>(rg.NextRandomUInt32() % 1001));
        }
    }
    else if (b.IsMySQLEngine() || b.IsPostgreSQLEngine())
    {
        const ServerCredentials & sc = b.IsMySQLEngine() ? fc.mysql_server : fc.postgresql_server;

        te->add_params()->set_svalue(sc.hostname + ":" + std::to_string(sc.port));
        te->add_params()->set_svalue(sc.database);
        te->add_params()->set_svalue("t" + std::to_string(b.tname));
        te->add_params()->set_svalue(sc.user);
        te->add_params()->set_svalue(sc.password);
        if (b.IsPostgreSQLEngine())
        {
            te->add_params()->set_svalue("test"); //PostgreSQL schema
            if (rg.NextSmallNumber() < 4)
            {
                te->add_params()->set_svalue("ON CONFLICT DO NOTHING");
            }
        }
        else if (rg.NextBool())
        {
            const uint32_t first_optional_value = rg.NextBool() ? 1 : 0;

            te->add_params()->set_num(first_optional_value);
            /*if (b.IsMySQLEngine() && !first_optional_value && rg.NextBool()) {
                te->add_params()->set_svalue(rg.NextBool() ? "replace_query" : "on_duplicate_clause");
            }*/
        }

        connections.CreateExternalDatabaseTable(
            rg, b.IsMySQLEngine() ? IntegrationCall::MySQL : IntegrationCall::PostgreSQL, b.tname, entries);
    }
    else if (b.IsSQLiteEngine())
    {
        te->add_params()->set_svalue(connections.GetSQLiteDBPath().generic_string());
        te->add_params()->set_svalue("t" + std::to_string(b.tname));

        connections.CreateExternalDatabaseTable(rg, IntegrationCall::SQLite, b.tname, entries);
    }
    else if (b.IsMongoDBEngine())
    {
        const ServerCredentials & sc = fc.mongodb_server;

        te->add_params()->set_svalue(sc.hostname + ":" + std::to_string(sc.port));
        te->add_params()->set_svalue(sc.database);
        te->add_params()->set_svalue("t" + std::to_string(b.tname));
        te->add_params()->set_svalue(sc.user);
        te->add_params()->set_svalue(sc.password);

        connections.CreateExternalDatabaseTable(rg, IntegrationCall::MongoDB, b.tname, entries);
    }
    else if (b.IsRedisEngine())
    {
        const ServerCredentials & sc = fc.redis_server;

        te->add_params()->set_svalue(sc.hostname + ":" + std::to_string(sc.port));
        te->add_params()->set_num(rg.NextBool() ? 0 : rg.NextLargeNumber() % 16);
        te->add_params()->set_svalue(fc.redis_server.password);
        te->add_params()->set_num(rg.NextBool() ? 16 : rg.NextLargeNumber() % 33);

        connections.CreateExternalDatabaseTable(rg, IntegrationCall::Redis, b.tname, entries);
    }
    else if (b.IsAnyS3Engine() || b.IsHudiEngine() || b.IsDeltaLakeEngine() || b.IsIcebergEngine())
    {
        const ServerCredentials & sc = fc.minio_server;
        const std::string nresource = sc.database + "/file" + std::to_string(b.tname) + (b.IsS3QueueEngine() ? "/*" : "");

        te->add_params()->set_svalue("http://" + sc.hostname + ":" + std::to_string(sc.port) + nresource);
        te->add_params()->set_svalue(sc.user);
        te->add_params()->set_svalue(sc.password);
        if (b.IsAnyS3Engine() || b.IsIcebergEngine())
        {
            te->add_params()->set_in_out(static_cast<sql_query_grammar::InOutFormat>(
                (rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::InOutFormat_MAX)) + 1));
            if (rg.NextSmallNumber() < 4)
            {
                te->add_params()->set_svalue(rg.PickRandomlyFromVector(s3_compress));
            }
            if (b.IsAnyS3Engine() && rg.NextSmallNumber() < 5)
            {
                GenerateTableKey(rg, te->mutable_partition_by());
            }
        }
        connections.CreateExternalDatabaseTable(rg, IntegrationCall::MinIO, b.tname, entries);
    }
    if ((b.IsRocksEngine() || b.IsRedisEngine()) && add_pkey && !entries.empty())
    {
        InsertEntryRef(rg.PickRandomlyFromVector(entries), te->mutable_primary_key()->add_exprs());
    }
    return 0;
}

int StatementGenerator::AddTableColumn(
    RandomGenerator & rg,
    SQLTable & t,
    const uint32_t cname,
    const bool staged,
    const bool modify,
    const bool is_pk,
    const ColumnSpecial special,
    sql_query_grammar::ColumnDef * cd)
{
    SQLColumn col;
    const SQLType * tp = nullptr;
    auto & to_add = staged ? t.staged_cols : t.cols;

    col.cname = cname;
    cd->mutable_col()->set_column("c" + std::to_string(cname));
    if (special == ColumnSpecial::SIGN || special == ColumnSpecial::IS_DELETED)
    {
        tp = new IntType(8, special == ColumnSpecial::IS_DELETED);
        cd->mutable_type()->mutable_type()->mutable_non_nullable()->set_integers(
            special == ColumnSpecial::IS_DELETED ? sql_query_grammar::Integers::UInt8 : sql_query_grammar::Integers::Int8);
    }
    else if (special == ColumnSpecial::VERSION && rg.NextBool())
    {
        sql_query_grammar::Integers nint;

        std::tie(tp, nint) = RandomIntType(rg, std::numeric_limits<uint32_t>::max());
        cd->mutable_type()->mutable_type()->mutable_non_nullable()->set_integers(nint);
    }
    else if (special == ColumnSpecial::VERSION)
    {
        if (rg.NextBool())
        {
            sql_query_grammar::Dates dd;

            std::tie(tp, dd) = RandomDateType(rg, std::numeric_limits<uint32_t>::max());
            cd->mutable_type()->mutable_type()->mutable_non_nullable()->set_dates(dd);
        }
        else
        {
            tp = RandomDateTimeType(
                rg, std::numeric_limits<uint32_t>::max(), cd->mutable_type()->mutable_type()->mutable_non_nullable()->mutable_datetimes());
        }
    }
    else
    {
        uint32_t possible_types = std::numeric_limits<uint32_t>::max();

        if (t.IsMySQLEngine())
        {
            possible_types
                = ~(allow_hugeint | allow_date32 | allow_datetime64 | allow_enum | allow_dynamic | allow_json | allow_low_cardinality
                    | allow_array | allow_map | allow_tuple | allow_variant | allow_nested | allow_ipv4 | allow_ipv6 | allow_geo);
        }
        else if (t.IsPostgreSQLEngine())
        {
            possible_types = ~(
                allow_unsigned_int | allow_int8 | allow_hugeint | allow_date32 | allow_datetime64 | allow_enum | allow_dynamic | allow_json
                | allow_low_cardinality | allow_map | allow_tuple | allow_variant | allow_nested | allow_ipv4 | allow_ipv6 | allow_geo);
        }
        else if (t.IsSQLiteEngine())
        {
            possible_types
                = ~(allow_unsigned_int | allow_hugeint | allow_floating_points | allow_dates | allow_datetimes | allow_enum | allow_dynamic
                    | allow_json | allow_low_cardinality | allow_array | allow_map | allow_tuple | allow_variant | allow_nested | allow_ipv4
                    | allow_ipv6 | allow_geo);
        }
        else if (t.IsMongoDBEngine())
        {
            possible_types = ~(allow_map | allow_tuple | allow_dynamic | allow_nested);
        }

        tp = RandomNextType(rg, possible_types, t.col_counter, cd->mutable_type()->mutable_type());
    }
    col.tp = tp;
    col.special = special;
    if (!modify && col.special == ColumnSpecial::NONE
        && (dynamic_cast<const IntType *>(tp) || dynamic_cast<const FloatType *>(tp) || dynamic_cast<const DateType *>(tp)
            || dynamic_cast<const DateTimeType *>(tp) || dynamic_cast<const DecimalType *>(tp) || dynamic_cast<const StringType *>(tp)
            || dynamic_cast<const BoolType *>(tp) || dynamic_cast<const UUIDType *>(tp) || dynamic_cast<const IPv4Type *>(tp)
            || dynamic_cast<const IPv6Type *>(tp))
        && rg.NextSmallNumber() < 3)
    {
        cd->set_nullable(rg.NextBool());
        col.nullable = std::optional<bool>(cd->nullable());
    }
    if (rg.NextSmallNumber() < 3)
    {
        GenerateNextStatistics(rg, cd->mutable_stats());
    }
    if (rg.NextSmallNumber() < 2)
    {
        sql_query_grammar::DefaultModifier * def_value = cd->mutable_defaultv();
        sql_query_grammar::DModifier dmod = static_cast<sql_query_grammar::DModifier>(
            (rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::DModifier_MAX)) + 1);

        def_value->set_dvalue(dmod);
        col.dmod = std::optional<sql_query_grammar::DModifier>(dmod);
        if (dmod != sql_query_grammar::DModifier::DEF_EPHEMERAL || rg.NextSmallNumber() < 4)
        {
            AddTableRelation(rg, false, "", t);
            this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs = false;
            GenerateExpression(rg, def_value->mutable_expr());
            this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs = true;
            this->levels.clear();
        }
    }
    if (t.IsMergeTreeFamily())
    {
        const auto & csettings = AllColumnSettings.at(t.teng);

        if (rg.NextSmallNumber() < 3)
        {
            const uint32_t ncodecs = (rg.NextMediumNumber() % UINT32_C(3)) + 1;

            for (uint32_t i = 0; i < ncodecs; i++)
            {
                sql_query_grammar::CodecParam * cp = cd->add_codecs();
                sql_query_grammar::CompressionCodec cc = static_cast<sql_query_grammar::CompressionCodec>(
                    (rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::CompressionCodec_MAX)) + 1);

                cp->set_codec(cc);
                switch (cc)
                {
                    case sql_query_grammar::COMP_LZ4HC:
                    case sql_query_grammar::COMP_ZSTD_QAT:
                        if (rg.NextBool())
                        {
                            std::uniform_int_distribution<uint32_t> next_dist(1, 12);
                            cp->add_params(next_dist(rg.gen));
                        }
                        break;
                    case sql_query_grammar::COMP_ZSTD:
                        if (rg.NextBool())
                        {
                            std::uniform_int_distribution<uint32_t> next_dist(1, 22);
                            cp->add_params(next_dist(rg.gen));
                        }
                        break;
                    case sql_query_grammar::COMP_Delta:
                    case sql_query_grammar::COMP_DoubleDelta:
                    case sql_query_grammar::COMP_Gorilla:
                        if (rg.NextBool())
                        {
                            std::uniform_int_distribution<uint32_t> next_dist(0, 3);
                            cp->add_params(UINT32_C(1) << next_dist(rg.gen));
                        }
                        break;
                    case sql_query_grammar::COMP_FPC:
                        if (rg.NextBool())
                        {
                            std::uniform_int_distribution<uint32_t> next_dist1(1, 28);
                            cp->add_params(next_dist1(rg.gen));
                            cp->add_params(rg.NextBool() ? 4 : 9);
                        }
                        break;
                    default:
                        break;
                }
            }
        }
        if (!csettings.empty() && rg.NextSmallNumber() < 3)
        {
            GenerateSettingValues(rg, csettings, cd->mutable_settings());
        }
        cd->set_is_pkey(is_pk);
    }
    to_add[cname] = std::move(col);
    return 0;
}

int StatementGenerator::AddTableIndex(RandomGenerator & rg, SQLTable & t, const bool staged, sql_query_grammar::IndexDef * idef)
{
    SQLIndex idx;
    const uint32_t iname = t.idx_counter++;
    sql_query_grammar::Expr * expr = idef->mutable_expr();
    sql_query_grammar::IndexType itpe
        = static_cast<sql_query_grammar::IndexType>((rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::IndexType_MAX)) + 1);
    auto & to_add = staged ? t.staged_idxs : t.idxs;

    idx.iname = iname;
    idef->mutable_idx()->set_index("i" + std::to_string(iname));
    idef->set_type(itpe);
    if (rg.NextSmallNumber() < 9)
    {
        const NestedType * ntp = nullptr;

        assert(this->entries.empty());
        for (const auto & entry : t.cols)
        {
            if ((ntp = dynamic_cast<const NestedType *>(entry.second.tp)))
            {
                for (const auto & entry2 : ntp->subtypes)
                {
                    if (itpe < sql_query_grammar::IndexType::IDX_ngrambf_v1 || HasType<StringType, true>(entry2.subtype))
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
            else if (itpe < sql_query_grammar::IndexType::IDX_ngrambf_v1 || HasType<StringType, true>(entry.second.tp))
            {
                entries.push_back(InsertEntry(
                    entry.second.nullable, entry.second.special, entry.second.cname, std::nullopt, entry.second.tp, entry.second.dmod));
            }
        }
    }
    if (!entries.empty())
    {
        std::shuffle(entries.begin(), entries.end(), rg.gen);

        if (itpe == sql_query_grammar::IndexType::IDX_hypothesis && entries.size() > 1 && rg.NextSmallNumber() < 9)
        {
            sql_query_grammar::BinaryExpr * bexpr = expr->mutable_comp_expr()->mutable_binary_expr();
            sql_query_grammar::Expr *expr1 = bexpr->mutable_lhs(), *expr2 = bexpr->mutable_rhs();
            sql_query_grammar::ExprSchemaTableColumn *estc1 = expr1->mutable_comp_expr()->mutable_expr_stc(),
                                                     *estc2 = expr2->mutable_comp_expr()->mutable_expr_stc();
            sql_query_grammar::ExprColumn *ecol1 = estc1->mutable_col(), *ecol2 = estc2->mutable_col();
            const InsertEntry &entry1 = this->entries[0], &entry2 = this->entries[1];

            bexpr->set_op(
                rg.NextSmallNumber() < 8
                    ? sql_query_grammar::BinaryOperator::BINOP_EQ
                    : static_cast<sql_query_grammar::BinaryOperator>(
                          (rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::BinaryOperator::BINOP_LEGR)) + 1));
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
            AddFieldAccess(rg, expr1, 11);
            AddFieldAccess(rg, expr2, 11);
            AddColNestedAccess(rg, ecol1, 21);
            AddColNestedAccess(rg, ecol2, 21);
        }
        else
        {
            InsertEntryRef(this->entries[0], expr);
        }
        entries.clear();
    }
    else
    {
        AddTableRelation(rg, false, "", t);
        this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs = false;
        GenerateExpression(rg, expr);
        this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs = true;
        this->levels.clear();
    }
    switch (itpe)
    {
        case sql_query_grammar::IndexType::IDX_set:
            if (rg.NextSmallNumber() < 7)
            {
                idef->add_params()->set_ival(0);
            }
            else
            {
                std::uniform_int_distribution<uint32_t> next_dist(1, 1000);
                idef->add_params()->set_ival(next_dist(rg.gen));
            }
            break;
        case sql_query_grammar::IndexType::IDX_bloom_filter: {
            std::uniform_int_distribution<uint32_t> next_dist(1, 1000);
            idef->add_params()->set_dval(static_cast<double>(next_dist(rg.gen)) / static_cast<double>(1000));
        }
        break;
        case sql_query_grammar::IndexType::IDX_ngrambf_v1:
        case sql_query_grammar::IndexType::IDX_tokenbf_v1: {
            std::uniform_int_distribution<uint32_t> next_dist1(1, 1000), next_dist2(1, 5);

            if (itpe == sql_query_grammar::IndexType::IDX_ngrambf_v1)
            {
                idef->add_params()->set_ival(next_dist1(rg.gen));
            }
            idef->add_params()->set_ival(next_dist1(rg.gen));
            idef->add_params()->set_ival(next_dist2(rg.gen));
            idef->add_params()->set_ival(next_dist1(rg.gen));
        }
        break;
        case sql_query_grammar::IndexType::IDX_full_text:
        case sql_query_grammar::IndexType::IDX_inverted: {
            std::uniform_int_distribution<uint32_t> next_dist(0, 10);
            idef->add_params()->set_ival(next_dist(rg.gen));
        }
        break;
        case sql_query_grammar::IndexType::IDX_minmax:
        case sql_query_grammar::IndexType::IDX_hypothesis:
            break;
    }
    if (rg.NextSmallNumber() < 7)
    {
        std::uniform_int_distribution<uint32_t> next_dist(1, 1000);
        idef->set_granularity(next_dist(rg.gen));
    }
    to_add[iname] = std::move(idx);
    return 0;
}

int StatementGenerator::AddTableProjection(RandomGenerator & rg, SQLTable & t, const bool staged, sql_query_grammar::ProjectionDef * pdef)
{
    const uint32_t pname = t.proj_counter++,
                   ncols = std::max(std::min(this->fc.max_width - this->width, (rg.NextMediumNumber() % UINT32_C(3)) + 1), UINT32_C(1));
    auto & to_add = staged ? t.staged_projs : t.projs;

    pdef->mutable_proj()->set_projection("p" + std::to_string(pname));
    this->inside_projection = true;
    AddTableRelation(rg, false, "", t);
    GenerateSelect(rg, true, ncols, allow_groupby | allow_orderby, pdef->mutable_select());
    this->levels.clear();
    this->inside_projection = false;
    to_add.insert(pname);
    return 0;
}

int StatementGenerator::AddTableConstraint(RandomGenerator & rg, SQLTable & t, const bool staged, sql_query_grammar::ConstraintDef * cdef)
{
    const uint32_t crname = t.constr_counter++;
    auto & to_add = staged ? t.staged_constrs : t.constrs;

    cdef->mutable_constr()->set_constraint("c" + std::to_string(crname));
    cdef->set_ctype(static_cast<sql_query_grammar::ConstraintDef_ConstraintType>(
        (rg.NextRandomUInt32() % static_cast<uint32_t>(sql_query_grammar::ConstraintDef::ConstraintType_MAX)) + 1));
    AddTableRelation(rg, false, "", t);
    this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs = false;
    this->GenerateWherePredicate(rg, cdef->mutable_expr());
    this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs = true;
    this->levels.clear();
    to_add.insert(crname);
    return 0;
}

sql_query_grammar::TableEngineValues StatementGenerator::GetNextTableEngine(RandomGenerator & rg, const bool use_external_integrations)
{
    if (rg.NextSmallNumber() < 5)
    {
        std::uniform_int_distribution<uint32_t> table_engine(1, sql_query_grammar::TableEngineValues::VersionedCollapsingMergeTree);
        return static_cast<sql_query_grammar::TableEngineValues>(table_engine(rg.gen));
    }
    assert(this->ids.empty());
    this->ids.push_back(sql_query_grammar::MergeTree);
    this->ids.push_back(sql_query_grammar::ReplacingMergeTree);
    this->ids.push_back(sql_query_grammar::SummingMergeTree);
    this->ids.push_back(sql_query_grammar::AggregatingMergeTree);
    this->ids.push_back(sql_query_grammar::CollapsingMergeTree);
    this->ids.push_back(sql_query_grammar::VersionedCollapsingMergeTree);
    this->ids.push_back(sql_query_grammar::File);
    this->ids.push_back(sql_query_grammar::Null);
    this->ids.push_back(sql_query_grammar::Set);
    this->ids.push_back(sql_query_grammar::Join);
    this->ids.push_back(sql_query_grammar::Memory);
    this->ids.push_back(sql_query_grammar::StripeLog);
    this->ids.push_back(sql_query_grammar::Log);
    this->ids.push_back(sql_query_grammar::TinyLog);
    this->ids.push_back(sql_query_grammar::EmbeddedRocksDB);
    if (CollectionHas<SQLTable>([](const SQLTable & t)
                                { return t.db && t.db->attached == DetachStatus::ATTACHED && t.attached == DetachStatus::ATTACHED; })
        || CollectionHas<SQLView>([](const SQLView & v)
                                  { return v.db && v.db->attached == DetachStatus::ATTACHED && v.attached == DetachStatus::ATTACHED; }))
    {
        this->ids.push_back(sql_query_grammar::Buffer);
    }
    if (use_external_integrations)
    {
        if (connections.HasMySQLConnection())
        {
            this->ids.push_back(sql_query_grammar::MySQL);
        }
        if (connections.HasPostgreSQLConnection())
        {
            this->ids.push_back(sql_query_grammar::PostgreSQL);
        }
        if (connections.HasSQLiteConnection())
        {
            this->ids.push_back(sql_query_grammar::SQLite);
        }
        if (connections.HasMongoDBConnection())
        {
            this->ids.push_back(sql_query_grammar::MongoDB);
        }
        if (connections.HasRedisConnection())
        {
            this->ids.push_back(sql_query_grammar::Redis);
        }
        if (connections.HasMinIOConnection())
        {
            this->ids.push_back(sql_query_grammar::S3);
            //this->ids.push_back(sql_query_grammar::S3Queue);
            this->ids.push_back(sql_query_grammar::Hudi);
            this->ids.push_back(sql_query_grammar::DeltaLake);
            //this->ids.push_back(sql_query_grammar::IcebergS3);
        }
    }

    const auto res = static_cast<sql_query_grammar::TableEngineValues>(rg.PickRandomlyFromVector(this->ids));
    this->ids.clear();
    return res;
}

const std::vector<sql_query_grammar::TableEngineValues> like_engs
    = {sql_query_grammar::TableEngineValues::MergeTree,
       sql_query_grammar::TableEngineValues::ReplacingMergeTree,
       sql_query_grammar::TableEngineValues::SummingMergeTree,
       sql_query_grammar::TableEngineValues::AggregatingMergeTree,
       sql_query_grammar::TableEngineValues::File,
       sql_query_grammar::TableEngineValues::Null,
       sql_query_grammar::TableEngineValues::Set,
       sql_query_grammar::TableEngineValues::Join,
       sql_query_grammar::TableEngineValues::Memory,
       sql_query_grammar::TableEngineValues::StripeLog,
       sql_query_grammar::TableEngineValues::Log,
       sql_query_grammar::TableEngineValues::TinyLog,
       sql_query_grammar::TableEngineValues::EmbeddedRocksDB};

int StatementGenerator::GenerateNextCreateTable(RandomGenerator & rg, sql_query_grammar::CreateTable * ct)
{
    SQLTable next;
    uint32_t tname = 0;
    bool added_pkey = false;
    const NestedType * ntp = nullptr;
    sql_query_grammar::TableEngine * te = ct->mutable_engine();
    sql_query_grammar::ExprSchemaTable * est = ct->mutable_est();
    sql_query_grammar::SettingValues * svs = nullptr;
    const bool replace = CollectionCount<SQLTable>(attached_tables) > 3 && rg.NextMediumNumber() < 16;

    next.is_temp = rg.NextMediumNumber() < 22;
    ct->set_is_temp(next.is_temp);
    if (replace)
    {
        const SQLTable & t = rg.PickRandomlyFromVector(FilterCollection<SQLTable>(attached_tables));

        next.db = t.db;
        tname = next.tname = t.tname;
    }
    else
    {
        if (!next.is_temp && rg.NextSmallNumber() < 9)
        {
            next.db = rg.PickRandomlyFromVector(FilterCollection<std::shared_ptr<SQLDatabase>>(attached_databases));
        }
        tname = next.tname = this->table_counter++;
    }
    ct->set_replace(replace);
    if (next.db)
    {
        est->mutable_database()->set_database("d" + std::to_string(next.db->dname));
    }
    est->mutable_table()->set_table("t" + std::to_string(next.tname));
    if (!CollectionHas<SQLTable>(attached_tables) || rg.NextSmallNumber() < 9)
    {
        //create table with definition
        sql_query_grammar::TableDef * colsdef = ct->mutable_table_def();

        next.teng = GetNextTableEngine(rg, true);
        te->set_engine(next.teng);
        added_pkey |= (!next.IsMergeTreeFamily() && !next.IsRocksEngine() && !next.IsRedisEngine());
        const bool add_version_to_replacing
            = next.teng == sql_query_grammar::TableEngineValues::ReplacingMergeTree && rg.NextSmallNumber() < 4;
        uint32_t added_cols = 0, added_idxs = 0, added_projs = 0, added_consts = 0, added_sign = 0, added_is_deleted = 0, added_version = 0;
        const uint32_t to_addcols
            = (rg.NextMediumNumber() % (rg.NextBool() ? 5 : 30)) + 1,
            to_addidxs = (rg.NextMediumNumber() % 4) * static_cast<uint32_t>(next.IsMergeTreeFamily() && rg.NextSmallNumber() < 4),
            to_addprojs = (rg.NextMediumNumber() % 3) * static_cast<uint32_t>(next.IsMergeTreeFamily() && rg.NextSmallNumber() < 5),
            to_addconsts = (rg.NextMediumNumber() % 3) * static_cast<uint32_t>(rg.NextSmallNumber() < 3),
            to_add_sign = static_cast<uint32_t>(next.HasSignColumn()),
            to_add_version = static_cast<uint32_t>(next.HasVersionColumn() || add_version_to_replacing),
            to_add_is_deleted = static_cast<uint32_t>(add_version_to_replacing && rg.NextSmallNumber() < 4),
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
            const uint32_t nopt = next_dist(rg.gen);

            if (add_idx && nopt < (add_idx + 1))
            {
                AddTableIndex(rg, next, false, colsdef->add_other_defs()->mutable_idx_def());
                added_idxs++;
            }
            else if (add_proj && nopt < (add_idx + add_proj + 1))
            {
                AddTableProjection(rg, next, false, colsdef->add_other_defs()->mutable_proj_def());
                added_projs++;
            }
            else if (add_const && nopt < (add_idx + add_proj + add_const + 1))
            {
                AddTableConstraint(rg, next, false, colsdef->add_other_defs()->mutable_const_def());
                added_consts++;
            }
            else if (add_col && nopt < (add_idx + add_proj + add_const + add_col + 1))
            {
                const bool add_pkey = !added_pkey && rg.NextMediumNumber() < 4;
                sql_query_grammar::ColumnDef * cd = i == 0 ? colsdef->mutable_col_def() : colsdef->add_other_defs()->mutable_col_def();

                AddTableColumn(rg, next, next.col_counter++, false, false, add_pkey, ColumnSpecial::NONE, cd);
                added_pkey |= add_pkey;
                added_cols++;
            }
            else
            {
                const uint32_t cname = next.col_counter++;
                const bool add_pkey = !added_pkey && rg.NextMediumNumber() < 4,
                           add_version_col = add_version && nopt < (add_idx + add_proj + add_const + add_col + add_version + 1);
                sql_query_grammar::ColumnDef * cd = i == 0 ? colsdef->mutable_col_def() : colsdef->add_other_defs()->mutable_col_def();

                AddTableColumn(
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
        if (rg.NextSmallNumber() < 3)
        {
            this->levels[this->current_level] = QueryLevel(this->current_level);
            GenerateSelect(
                rg,
                true,
                static_cast<uint32_t>(next.RealNumberOfColumns()),
                std::numeric_limits<uint32_t>::max(),
                ct->mutable_as_select_stmt());
        }
    }
    else
    {
        //create table as
        sql_query_grammar::CreateTableAs * cta = ct->mutable_table_as();
        sql_query_grammar::ExprSchemaTable * aest = cta->mutable_est();
        const SQLTable & t = rg.PickRandomlyFromVector(FilterCollection<SQLTable>(attached_tables));
        std::uniform_int_distribution<size_t> table_engine(0, like_engs.size() - 1);
        sql_query_grammar::TableEngineValues val = like_engs[table_engine(rg.gen)];

        next.teng = val;
        te->set_engine(val);
        cta->set_clone(next.IsMergeTreeFamily() && rg.NextBool());
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
    GenerateEngineDetails(rg, next, !added_pkey, te);
    this->entries.clear();

    const auto & tsettings = AllTableSettings.at(next.teng);
    if (!tsettings.empty() && rg.NextSmallNumber() < 5)
    {
        svs = ct->mutable_settings();
        GenerateSettingValues(rg, tsettings, svs);
    }
    if (next.IsMergeTreeFamily() || next.IsAnyS3Engine())
    {
        if (!svs)
        {
            svs = ct->mutable_settings();
        }
        sql_query_grammar::SetValue * sv = svs->has_set_value() ? svs->add_other_values() : svs->mutable_set_value();

        if (next.IsMergeTreeFamily())
        {
            sv->set_property("allow_nullable_key");
            sv->set_value("1");

            if (next.toption.has_value() && next.toption.value() == sql_query_grammar::TableEngineOption::TShared)
            {
                sql_query_grammar::SetValue * sv2 = svs->add_other_values();

                sv2->set_property("storage_policy");
                sv2->set_value("'s3_with_keeper'");
            }
        }
        else
        {
            sv->set_property("input_format_with_names_use_header");
            sv->set_value("0");
            if (next.IsS3QueueEngine())
            {
                sql_query_grammar::SetValue * sv2 = svs->add_other_values();

                sv2->set_property("mode");
                sv2->set_value(rg.NextBool() ? "'ordered'" : "'unordered'");
            }
        }
    }
    assert(!next.toption.has_value() || next.IsMergeTreeFamily());
    this->staged_tables[tname] = std::move(next);
    return 0;
}

}
