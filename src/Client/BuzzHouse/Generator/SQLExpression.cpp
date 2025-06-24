#include <Client/BuzzHouse/Generator/SQLCatalog.h>
#include <Client/BuzzHouse/Generator/SQLFuncs.h>
#include <Client/BuzzHouse/Generator/SQLTypes.h>
#include <Client/BuzzHouse/Generator/StatementGenerator.h>

namespace BuzzHouse
{

void StatementGenerator::addFieldAccess(RandomGenerator & rg, Expr * expr, const uint32_t nested_prob)
{
    if (rg.nextMediumNumber() < nested_prob)
    {
        const uint32_t noption = rg.nextMediumNumber();
        FieldAccess * fa = expr->mutable_field();

        this->depth++;
        if (noption < 41)
        {
            fa->set_array_index(rg.nextRandomUInt32() % 5);
        }
        else if (noption < 71)
        {
            fa->set_tuple_index(rg.nextRandomUInt32() % 5);
        }
        else if (this->depth >= this->fc.max_depth || noption < 81)
        {
            fa->mutable_array_key()->set_column(rg.nextJSONCol());
        }
        else
        {
            this->generateExpression(rg, fa->mutable_array_expr());
        }
        this->depth--;
    }
}

void StatementGenerator::addColNestedAccess(RandomGenerator & rg, ExprColumn * expr, const uint32_t nested_prob)
{
    const uint32_t nsuboption = rg.nextLargeNumber();
    const String & last_col
        = expr->path().sub_cols_size() ? expr->path().sub_cols(expr->path().sub_cols_size() - 1).column() : expr->path().col().column();
    const bool has_nested = last_col == "keys" || last_col == "values" || last_col == "null" || startsWith(last_col, "size");

    if (!has_nested)
    {
        ColumnPath & cp = const_cast<ColumnPath &>(expr->path());

        this->depth++;
        if (rg.nextMediumNumber() < nested_prob)
        {
            TypeName * tpn = nullptr;
            JSONColumns * subcols = expr->mutable_subcols();
            const uint32_t noption = rg.nextMediumNumber();
            const uint32_t nvalues = std::max(std::min(this->fc.max_width - this->width, rg.nextMediumNumber() % 4), UINT32_C(1));

            for (uint32_t i = 0; i < nvalues; i++)
            {
                const uint32_t noption2 = rg.nextMediumNumber();
                JSONColumn * jcol = i == 0 ? subcols->mutable_jcol() : subcols->add_other_jcols();

                this->width++;
                if (noption2 < 31)
                {
                    jcol->set_jcol(true);
                }
                else if (noption2 < 61)
                {
                    jcol->set_jarray(0);
                }
                jcol->mutable_col()->set_column(rg.nextJSONCol());
            }
            if (noption < 4)
            {
                tpn = subcols->mutable_jcast();
            }
            else if (noption < 8)
            {
                tpn = subcols->mutable_jreinterpret();
            }
            this->width -= nvalues;
            if (tpn)
            {
                uint32_t col_counter = 0;

                const uint32_t type_mask_backup = this->next_type_mask;
                this->next_type_mask = fc.type_mask & ~(allow_nested);
                auto tp = std::unique_ptr<SQLType>(randomNextType(rg, this->next_type_mask, col_counter, tpn->mutable_type()));
                this->next_type_mask = type_mask_backup;
            }
        }
        if (rg.nextMediumNumber() < nested_prob)
        {
            uint32_t col_counter = 0;

            const uint32_t type_mask_backup = this->next_type_mask;
            this->next_type_mask = fc.type_mask & ~(allow_nested);
            auto tp = std::unique_ptr<SQLType>(
                randomNextType(rg, this->next_type_mask, col_counter, expr->mutable_dynamic_subtype()->mutable_type()));
            this->next_type_mask = type_mask_backup;
        }
        if (nsuboption < 6)
        {
            cp.add_sub_cols()->set_column("null");
        }
        else if (nsuboption < 11)
        {
            cp.add_sub_cols()->set_column("keys");
        }
        else if (nsuboption < 16)
        {
            cp.add_sub_cols()->set_column("values");
        }
        else if (nsuboption < 21)
        {
            cp.add_sub_cols()->set_column("size" + std::to_string(rg.nextMediumNumber() % 3));
        }
        this->depth--;
    }
}

void StatementGenerator::addSargableColRef(RandomGenerator & rg, const SQLRelationCol & rel_col, Expr * expr)
{
    if (rg.nextMediumNumber() < 6)
    {
        /// Add non sargable reference
        SQLFuncCall * sfc = expr->mutable_comp_expr()->mutable_func_call();

        sfc->mutable_func()->set_catalog_func(static_cast<SQLFunc>(rg.pickRandomly(this->one_arg_funcs).fnum));
        expr = sfc->add_args()->mutable_expr();
    }
    ExprSchemaTableColumn * estc = expr->mutable_comp_expr()->mutable_expr_stc();
    ExprColumn * ecol = estc->mutable_col();

    if (!rel_col.rel_name.empty())
    {
        estc->mutable_table()->set_table(rel_col.rel_name);
    }
    rel_col.AddRef(ecol);
    addFieldAccess(rg, expr, 6);
    addColNestedAccess(rg, ecol, 6);
}

void StatementGenerator::refColumn(RandomGenerator & rg, const GroupCol & gcol, Expr * expr)
{
    chassert(gcol.col.has_value() || gcol.gexpr);
    if (gcol.gexpr)
    {
        /// Use grouping column
        expr->CopyFrom(*(gcol.gexpr));
    }
    else
    {
        //// Otherwise reference the column
        addSargableColRef(rg, gcol.col.value(), expr);
    }
}

void StatementGenerator::generateLiteralValueInternal(RandomGenerator & rg, const bool complex, Expr * expr)
{
    uint32_t nested_prob = 0;
    LiteralValue * lv = expr->mutable_lit_val();
    const uint32_t hugeint_lit = 20;
    const uint32_t uhugeint_lit = 20;
    const uint32_t int_lit = 80;
    const uint32_t uint_lit = 80;
    const uint32_t time_lit = 25 * static_cast<uint32_t>((this->fc.type_mask & allow_time) != 0);
    const uint32_t date_lit = 25 * static_cast<uint32_t>((this->fc.type_mask & allow_dates) != 0);
    const uint32_t datetime_lit = 25 * static_cast<uint32_t>((this->fc.type_mask & allow_datetimes) != 0);
    const uint32_t dec_lit = 50 * static_cast<uint32_t>((this->fc.type_mask & allow_decimals) != 0);
    const uint32_t random_str = 30 * static_cast<uint32_t>(complex && this->allow_not_deterministic);
    const uint32_t uuid_lit = 20 * static_cast<uint32_t>((this->fc.type_mask & allow_uuid) != 0);
    const uint32_t ipv4_lit = 20 * static_cast<uint32_t>((this->fc.type_mask & allow_ipv4) != 0);
    const uint32_t ipv6_lit = 20 * static_cast<uint32_t>((this->fc.type_mask & allow_ipv6) != 0);
    const uint32_t geo_lit = 20 * static_cast<uint32_t>((this->fc.type_mask & allow_geo) != 0);
    const uint32_t str_lit = 50;
    const uint32_t special_val = 20;
    const uint32_t json_lit = 20 * static_cast<uint32_t>((this->fc.type_mask & allow_JSON) != 0);
    const uint32_t null_lit = 10;
    const uint32_t prob_space = hugeint_lit + uhugeint_lit + int_lit + uint_lit + time_lit + date_lit + datetime_lit + dec_lit + random_str
        + uuid_lit + ipv4_lit + ipv6_lit + geo_lit + str_lit + special_val + json_lit + null_lit;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t noption = next_dist(rg.generator);

    if (hugeint_lit && (noption < hugeint_lit + 1))
    {
        IntLiteral * il = lv->mutable_int_lit();
        HugeIntLiteral * huge = il->mutable_huge_lit();

        huge->set_upper(rg.nextRandomInt64());
        huge->set_lower(rg.nextRandomUInt64());
        if (complex && rg.nextSmallNumber() < 9)
        {
            il->set_integers(rg.nextBool() ? Integers::Int128 : Integers::Int256);
        }
    }
    else if (uhugeint_lit && (noption < hugeint_lit + uhugeint_lit + 1))
    {
        IntLiteral * il = lv->mutable_int_lit();
        UHugeIntLiteral * uhuge = il->mutable_uhuge_lit();

        uhuge->set_upper(rg.nextRandomUInt64());
        uhuge->set_lower(rg.nextRandomUInt64());
        if (complex && rg.nextSmallNumber() < 9)
        {
            il->set_integers(rg.nextBool() ? Integers::UInt128 : Integers::UInt256);
        }
    }
    else if (int_lit && (noption < hugeint_lit + uhugeint_lit + int_lit + 1))
    {
        IntLiteral * il = lv->mutable_int_lit();

        il->set_int_lit(rg.nextRandomInt64());
        if (complex && rg.nextSmallNumber() < 9)
        {
            il->set_integers(
                static_cast<Integers>(
                    (rg.nextRandomUInt32() % static_cast<uint32_t>(Integers::Int - Integers::UInt256))
                    + static_cast<uint32_t>(Integers::Int8)));
        }
    }
    else if (uint_lit && (noption < hugeint_lit + uhugeint_lit + int_lit + uint_lit + 1))
    {
        IntLiteral * il = lv->mutable_int_lit();

        il->set_uint_lit(rg.nextRandomUInt64());
        if (complex && rg.nextSmallNumber() < 9)
        {
            std::uniform_int_distribution<uint32_t> integers_range(1, static_cast<uint32_t>(Integers_MAX));

            il->set_integers(static_cast<Integers>(integers_range(rg.generator)));
        }
    }
    else if (time_lit && (noption < hugeint_lit + uhugeint_lit + int_lit + uint_lit + time_lit + 1))
    {
        const SQLType * tp = randomTimeType(rg, std::numeric_limits<uint32_t>::max(), nullptr);

        lv->set_no_quote_str(
            fmt::format("{}{}{}", tp->appendRandomRawValue(rg, *this), complex ? "::" : "", complex ? tp->typeName(false) : ""));
        delete tp;
    }
    else if (date_lit && (noption < hugeint_lit + uhugeint_lit + int_lit + uint_lit + time_lit + date_lit + 1))
    {
        const SQLType * tp;

        std::tie(tp, std::ignore) = randomDateType(rg, std::numeric_limits<uint32_t>::max());
        lv->set_no_quote_str(
            fmt::format("{}{}{}", tp->appendRandomRawValue(rg, *this), complex ? "::" : "", complex ? tp->typeName(false) : ""));
        delete tp;
    }
    else if (datetime_lit && (noption < hugeint_lit + uhugeint_lit + int_lit + uint_lit + time_lit + date_lit + datetime_lit + 1))
    {
        const SQLType * tp = randomDateTimeType(rg, std::numeric_limits<uint32_t>::max(), nullptr);

        lv->set_no_quote_str(
            fmt::format("{}{}{}", tp->appendRandomRawValue(rg, *this), complex ? "::" : "", complex ? tp->typeName(false) : ""));
        delete tp;
    }
    else if (dec_lit && (noption < hugeint_lit + uhugeint_lit + int_lit + uint_lit + time_lit + date_lit + datetime_lit + dec_lit + 1))
    {
        const DecimalType * tp = static_cast<DecimalType *>(randomDecimalType(rg, std::numeric_limits<uint32_t>::max(), nullptr));

        lv->set_no_quote_str(DecimalType::appendDecimalValue(rg, complex && rg.nextSmallNumber() < 9, tp));
        delete tp;
    }
    else if (
        random_str
        && (noption < hugeint_lit + uhugeint_lit + int_lit + uint_lit + time_lit + date_lit + datetime_lit + dec_lit + random_str + 1))
    {
        static const DB::Strings & funcs = {"randomString", "randomFixedString", "randomPrintableASCII", "randomStringUTF8"};

        lv->set_no_quote_str(fmt::format("{}({})", rg.pickRandomly(funcs), rg.nextLargeNumber()));
    }
    else if (
        uuid_lit
        && (noption
            < hugeint_lit + uhugeint_lit + int_lit + uint_lit + time_lit + date_lit + datetime_lit + dec_lit + random_str + uuid_lit + 1))
    {
        lv->set_no_quote_str(fmt::format("'{}'{}", rg.nextUUID(), complex ? "::UUID" : ""));
    }
    else if (
        ipv4_lit
        && (noption < hugeint_lit + uhugeint_lit + int_lit + uint_lit + time_lit + date_lit + datetime_lit + dec_lit + random_str + uuid_lit
                + ipv4_lit + 1))
    {
        lv->set_no_quote_str(fmt::format("'{}'{}", rg.nextIPv4(), complex ? "::IPv4" : ""));
    }
    else if (
        ipv6_lit
        && (noption < hugeint_lit + uhugeint_lit + int_lit + uint_lit + time_lit + date_lit + datetime_lit + dec_lit + random_str + uuid_lit
                + ipv4_lit + ipv6_lit + 1))
    {
        lv->set_no_quote_str(fmt::format("'{}'{}", rg.nextIPv6(), complex ? "::IPv6" : ""));
    }
    else if (
        geo_lit
        && (noption < hugeint_lit + uhugeint_lit + int_lit + uint_lit + time_lit + date_lit + datetime_lit + dec_lit + random_str + uuid_lit
                + ipv4_lit + ipv6_lit + geo_lit + 1))
    {
        std::uniform_int_distribution<uint32_t> geo_range(1, static_cast<uint32_t>(GeoTypes_MAX));
        const GeoTypes gt = static_cast<GeoTypes>(geo_range(rg.generator));

        lv->set_no_quote_str(fmt::format("'{}'{}{}", strAppendGeoValue(rg, gt), complex ? "::" : "", complex ? GeoTypes_Name(gt) : ""));
    }
    else if (
        str_lit
        && (noption < hugeint_lit + uhugeint_lit + int_lit + uint_lit + time_lit + date_lit + datetime_lit + dec_lit + random_str + uuid_lit
                + ipv4_lit + ipv6_lit + geo_lit + str_lit + 1))
    {
        std::uniform_int_distribution<uint32_t> strlens(0, fc.max_string_length);

        lv->set_no_quote_str(rg.nextString("'", true, strlens(rg.generator)));
    }
    else if (
        special_val
        && (noption < hugeint_lit + uhugeint_lit + int_lit + uint_lit + time_lit + date_lit + datetime_lit + dec_lit + random_str + uuid_lit
                + ipv4_lit + ipv6_lit + geo_lit + str_lit + special_val + 1))
    {
        SpecialVal * val = lv->mutable_special_val();
        std::uniform_int_distribution<uint32_t> special_range(1, static_cast<uint32_t>(SpecialVal::SpecialValEnum_MAX));

        val->set_val(static_cast<SpecialVal_SpecialValEnum>(special_range(rg.generator)));
        val->set_paren(complex && rg.nextBool());
        nested_prob = 3;
    }
    else if (
        json_lit
        && (noption < hugeint_lit + uhugeint_lit + int_lit + uint_lit + time_lit + date_lit + datetime_lit + dec_lit + random_str + uuid_lit
                + ipv4_lit + ipv6_lit + geo_lit + str_lit + special_val + json_lit + 1))
    {
        std::uniform_int_distribution<int> dopt(1, 3);
        std::uniform_int_distribution<int> wopt(1, 3);

        lv->set_no_quote_str(fmt::format("'{}'{}", strBuildJSON(rg, dopt(rg.generator), wopt(rg.generator)), complex ? "::JSON" : ""));
    }
    else if (
        null_lit
        && (noption < hugeint_lit + uhugeint_lit + int_lit + uint_lit + time_lit + date_lit + datetime_lit + dec_lit + random_str + uuid_lit
                + ipv4_lit + ipv6_lit + geo_lit + str_lit + special_val + json_lit + null_lit + 1))
    {
        lv->mutable_special_val()->set_val(SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_NULL);
    }
    else
    {
        chassert(0);
    }
    addFieldAccess(rg, expr, nested_prob);
}

void StatementGenerator::generateLiteralValue(RandomGenerator & rg, const bool complex, Expr * expr)
{
    if (this->width < this->fc.max_width && rg.nextMediumNumber() < 16)
    {
        /// Generate a few arrays/tuples with literal values
        ExprList * elist
            = (!complex || rg.nextBool()) ? expr->mutable_comp_expr()->mutable_array() : expr->mutable_comp_expr()->mutable_tuple();
        const uint32_t nvalues = std::min(this->fc.max_width - this->width, rg.nextMediumNumber() % 8);

        for (uint32_t i = 0; i < nvalues; i++)
        {
            /// There are no recursive calls here, so don't bother about width and depth
            this->generateLiteralValueInternal(rg, complex, i == 0 ? elist->mutable_expr() : elist->add_extra_exprs());
        }
    }
    else
    {
        this->generateLiteralValueInternal(rg, complex, expr);
    }
}

void StatementGenerator::generateColRef(RandomGenerator & rg, Expr * expr)
{
    std::vector<GroupCol> available_cols;

    if ((this->levels[this->current_level].gcols.empty() && !this->levels[this->current_level].global_aggregate)
        || this->levels[this->current_level].inside_aggregate)
    {
        for (const auto & entry : this->levels[this->current_level].rels)
        {
            for (const auto & col : entry.cols)
            {
                available_cols.emplace_back(GroupCol(col, nullptr));
            }
        }
    }
    else if (!this->levels[this->current_level].gcols.empty())
    {
        for (const auto & entry : this->levels[this->current_level].gcols)
        {
            available_cols.push_back(entry);
        }
    }

    if (available_cols.empty())
    {
        this->generateLiteralValue(rg, true, expr);
    }
    else
    {
        refColumn(rg, rg.pickRandomly(available_cols), expr);
    }
}

void StatementGenerator::generateSubquery(RandomGenerator & rg, ExplainQuery * eq)
{
    if (rg.nextMediumNumber() < 6)
    {
        prepareNextExplain(rg, eq);
    }
    else
    {
        this->current_level++;
        this->levels[this->current_level] = QueryLevel(this->current_level);

        if (rg.nextBool())
        {
            /// Make the subquery correlated
            for (const auto & rel : this->levels[this->current_level - 1].rels)
            {
                this->levels[this->current_level].rels.push_back(rel);
            }
            if (rg.nextBool())
            {
                for (const auto & gcol : this->levels[this->current_level - 1].gcols)
                {
                    this->levels[this->current_level].gcols.push_back(gcol);
                }
            }
        }
        this->generateSelect(
            rg, true, false, 1, std::numeric_limits<uint32_t>::max(), eq->mutable_inner_query()->mutable_select()->mutable_sel());
        this->current_level--;
    }
}

void StatementGenerator::generatePredicate(RandomGenerator & rg, Expr * expr)
{
    if (this->depth < this->fc.max_depth)
    {
        const uint32_t unary_expr = 100;
        const uint32_t binary_expr = 200 * static_cast<uint32_t>(this->fc.max_width > (this->width + 1));
        const uint32_t between_expr = 80 * static_cast<uint32_t>(this->fc.max_width > (this->width + 2));
        const uint32_t in_expr = 100 * static_cast<uint32_t>(this->fc.max_width > this->width);
        const uint32_t any_expr = 100 * static_cast<uint32_t>(this->fc.max_width > (this->width + 1));
        const uint32_t is_null_expr = 100;
        const uint32_t exists_expr = 100 * static_cast<uint32_t>(this->allow_subqueries);
        const uint32_t like_expr = 100;
        const uint32_t other_expr = 10;
        const uint32_t prob_space
            = unary_expr + binary_expr + between_expr + in_expr + any_expr + is_null_expr + exists_expr + like_expr + other_expr;
        std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
        const uint32_t noption = next_dist(rg.generator);

        if (unary_expr && noption < (unary_expr + 1))
        {
            ComplicatedExpr * cexpr = expr->mutable_comp_expr();
            UnaryExpr * unexp = cexpr->mutable_unary_expr();

            unexp->set_unary_op(UnaryOperator::UNOP_NOT);
            this->depth++;
            if (rg.nextSmallNumber() < 5)
            {
                this->generatePredicate(rg, unexp->mutable_expr());
            }
            else
            {
                this->generateExpression(rg, unexp->mutable_expr());
            }
            this->depth--;
        }
        else if (binary_expr && noption < (unary_expr + binary_expr + 1))
        {
            ComplicatedExpr * cexpr = expr->mutable_comp_expr();
            BinaryExpr * bexpr = cexpr->mutable_binary_expr();

            if (rg.nextBool())
            {
                bexpr->set_op(rg.nextBool() ? BinaryOperator::BINOP_AND : BinaryOperator::BINOP_OR);
            }
            else
            {
                bexpr->set_op(static_cast<BinaryOperator>((rg.nextRandomUInt32() % static_cast<uint32_t>(BinaryOperator_MAX)) + 1));
            }
            this->depth++;
            this->generateExpression(rg, bexpr->mutable_lhs());
            this->width++;
            this->generateExpression(rg, bexpr->mutable_rhs());
            this->width--;
            this->depth--;
        }
        else if (between_expr && noption < (unary_expr + binary_expr + between_expr + 1))
        {
            ComplicatedExpr * cexpr = expr->mutable_comp_expr();
            ExprBetween * bexpr = cexpr->mutable_expr_between();

            bexpr->set_not_(rg.nextBool());
            this->depth++;
            this->generateExpression(rg, bexpr->mutable_expr1());
            this->width++;
            this->generateExpression(rg, bexpr->mutable_expr2());
            this->width++;
            this->generateExpression(rg, bexpr->mutable_expr3());
            this->width -= 2;
            this->depth--;
        }
        else if (in_expr && noption < (unary_expr + binary_expr + between_expr + in_expr + 1))
        {
            const uint32_t nopt2 = rg.nextSmallNumber();
            const uint32_t nclauses = std::min(this->fc.max_width - this->width, (rg.nextMediumNumber() % 4) + 1);
            ComplicatedExpr * cexpr = expr->mutable_comp_expr();
            ExprIn * ein = cexpr->mutable_expr_in();
            ExprList * elist = ein->mutable_expr();

            ein->set_not_(rg.nextBool());
            ein->set_global(rg.nextBool());

            this->depth++;
            for (uint32_t i = 0; i < nclauses; i++)
            {
                this->generateExpression(rg, i == 0 ? elist->mutable_expr() : elist->add_extra_exprs());
            }
            if (nopt2 < 5)
            {
                this->generateSubquery(rg, ein->mutable_sel());
            }
            else if (nopt2 < 8)
            {
                ExprList * elist2 = ein->mutable_exprs();

                for (uint32_t i = 0; i < nclauses; i++)
                {
                    this->generateExpression(rg, i == 0 ? elist2->mutable_expr() : elist2->add_extra_exprs());
                }
            }
            else
            {
                this->generateExpression(rg, ein->mutable_single_expr());
            }
            this->depth--;
        }
        else if (any_expr && noption < (unary_expr + binary_expr + between_expr + in_expr + any_expr + 1))
        {
            ComplicatedExpr * cexpr = expr->mutable_comp_expr();
            ExprAny * eany = cexpr->mutable_expr_any();

            eany->set_op(static_cast<BinaryOperator>((rg.nextRandomUInt32() % static_cast<uint32_t>(BinaryOperator::BINOP_LEGR)) + 1));
            eany->set_anyall(rg.nextBool());
            this->depth++;
            this->generateExpression(rg, eany->mutable_expr());
            this->width++;
            generateSubquery(rg, eany->mutable_sel());
            this->width--;
            this->depth--;
        }
        else if (is_null_expr && noption < (unary_expr + binary_expr + between_expr + in_expr + any_expr + is_null_expr + 1))
        {
            ComplicatedExpr * cexpr = expr->mutable_comp_expr();
            ExprNullTests * enull = cexpr->mutable_expr_null_tests();

            enull->set_not_(rg.nextBool());
            this->depth++;
            this->generateExpression(rg, enull->mutable_expr());
            this->depth--;
        }
        else if (exists_expr && noption < (unary_expr + binary_expr + between_expr + in_expr + any_expr + is_null_expr + exists_expr + 1))
        {
            ComplicatedExpr * cexpr = expr->mutable_comp_expr();
            ExprExists * exists = cexpr->mutable_expr_exists();

            exists->set_not_(rg.nextBool());
            this->depth++;
            generateSubquery(rg, exists->mutable_select());
            this->depth--;
        }
        else if (
            like_expr
            && noption < (unary_expr + binary_expr + between_expr + in_expr + any_expr + is_null_expr + exists_expr + like_expr + 1))
        {
            ComplicatedExpr * cexpr = expr->mutable_comp_expr();
            ExprLike * elike = cexpr->mutable_expr_like();
            std::uniform_int_distribution<uint32_t> like_range(1, static_cast<uint32_t>(ExprLike::PossibleKeywords_MAX));

            elike->set_keyword(static_cast<ExprLike_PossibleKeywords>(like_range(rg.generator)));
            elike->set_not_(rg.nextBool());
            this->depth++;
            this->generateExpression(rg, elike->mutable_expr1());
            this->width++;
            this->generateExpression(rg, elike->mutable_expr2());
            this->width--;
            this->depth--;
        }
        else if (
            other_expr
            && noption
                < (unary_expr + binary_expr + between_expr + in_expr + any_expr + is_null_expr + exists_expr + like_expr + other_expr + 1))
        {
            this->depth++;
            this->generateExpression(rg, expr);
            this->depth--;
        }
        else
        {
            chassert(0);
        }
        addFieldAccess(rg, expr, 0);
    }
    else
    {
        this->generateLiteralValue(rg, true, expr);
    }
}

void StatementGenerator::generateLambdaCall(RandomGenerator & rg, const uint32_t nparams, LambdaExpr * lexpr)
{
    SQLRelation rel("");
    std::unordered_map<uint32_t, QueryLevel> levels_backup;

    for (const auto & entry : this->levels)
    {
        levels_backup[entry.first] = entry.second;
    }
    this->levels.clear();

    this->levels[this->current_level] = QueryLevel(this->current_level);
    for (uint32_t i = 0; i < nparams; i++)
    {
        const String buf = "p" + std::to_string(i);
        lexpr->add_args()->set_column(buf);
        rel.cols.emplace_back(SQLRelationCol("", {buf}));
    }
    this->levels[this->current_level].rels.emplace_back(rel);
    this->generateExpression(rg, lexpr->mutable_expr());

    this->levels.clear();
    for (const auto & entry : levels_backup)
    {
        this->levels[entry.first] = entry.second;
    }
}

void StatementGenerator::generateFuncCall(RandomGenerator & rg, const bool allow_funcs, const bool allow_aggr, SQLFuncCall * func_call)
{
    const size_t funcs_size = this->allow_not_deterministic ? CHFuncs.size() : this->deterministic_funcs_limit;
    const bool nallow_funcs = allow_funcs && (!allow_aggr || rg.nextSmallNumber() < 8);
    const uint32_t nfuncs = static_cast<uint32_t>(
        (nallow_funcs ? funcs_size : 0)
        + (allow_aggr ? (this->allow_not_deterministic ? CHAggrs.size() : (CHAggrs.size() - this->deterministic_aggrs_limit)) : 0));
    std::uniform_int_distribution<uint32_t> next_dist(0, nfuncs - 1);
    uint32_t generated_params = 0;

    chassert(nallow_funcs || allow_aggr);
    const uint32_t nopt = next_dist(rg.generator);
    if (!nallow_funcs || nopt >= static_cast<uint32_t>(funcs_size))
    {
        /// Aggregate
        const uint32_t next_off
            = rg.nextSmallNumber() < 2 ? (rg.nextLargeNumber() % 5) : (nopt - static_cast<uint32_t>(nallow_funcs ? funcs_size : 0));
        const CHAggregate & agg = CHAggrs[next_off];
        const uint32_t agg_max_params = std::min(agg.max_params, UINT32_C(5));
        const uint32_t max_params = std::min(this->fc.max_width - this->width, agg_max_params);
        const uint32_t agg_max_args = std::min(agg.max_args, UINT32_C(5));
        const uint32_t max_args = std::min(this->fc.max_width - this->width, agg_max_args);
        const uint32_t ncombinators
            = rg.nextSmallNumber() < 4 ? std::min(this->fc.max_width - this->width, (rg.nextSmallNumber() % 3) + 1) : 0;
        const bool prev_inside_aggregate = this->levels[this->current_level].inside_aggregate;
        const bool prev_allow_window_funcs = this->levels[this->current_level].allow_window_funcs;
        std::uniform_int_distribution<uint32_t> comb_range(
            1, static_cast<uint32_t>(this->allow_not_deterministic ? SQLFuncCall::AggregateCombinator_MAX : SQLFuncCall::ArgMax));

        /// Most of the times disallow nested aggregates, and window functions inside aggregates
        this->levels[this->current_level].inside_aggregate = rg.nextSmallNumber() < 9;
        this->levels[this->current_level].allow_window_funcs = rg.nextSmallNumber() < 3;
        if (max_params > 0 && max_params >= agg.min_params)
        {
            std::uniform_int_distribution<uint32_t> nparams(agg.min_params, max_params);
            const uint32_t nagg_params = nparams(rg.generator);

            for (uint32_t i = 0; i < nagg_params; i++)
            {
                this->generateExpression(rg, func_call->add_params());
                this->width++;
                generated_params++;
            }
        }
        else if (agg.min_params > 0)
        {
            for (uint32_t i = 0; i < agg.min_params; i++)
            {
                generateLiteralValue(rg, true, func_call->add_params());
            }
        }

        if (max_args > 0 && max_args >= agg.min_args)
        {
            std::uniform_int_distribution<uint32_t> nparams(agg.min_args, max_args);
            const uint32_t nagg_args = nparams(rg.generator);

            for (uint32_t i = 0; i < nagg_args; i++)
            {
                this->generateExpression(rg, func_call->add_args()->mutable_expr());
                this->width++;
                generated_params++;
            }
        }
        else if (agg.min_args > 0)
        {
            for (uint32_t i = 0; i < agg.min_args; i++)
            {
                generateLiteralValue(rg, true, func_call->add_args()->mutable_expr());
            }
        }

        for (uint32_t i = 0; i < ncombinators; i++)
        {
            const SQLFuncCall_AggregateCombinator comb = static_cast<SQLFuncCall_AggregateCombinator>(comb_range(rg.generator));

            switch (comb)
            {
                case SQLFuncCall_AggregateCombinator::SQLFuncCall_AggregateCombinator_If:
                    if (rg.nextSmallNumber() < 9)
                    {
                        this->generatePredicate(rg, func_call->add_args()->mutable_expr());
                    }
                    else
                    {
                        this->generateExpression(rg, func_call->add_args()->mutable_expr());
                    }
                    this->width++;
                    generated_params++;
                    break;
                case SQLFuncCall_AggregateCombinator::SQLFuncCall_AggregateCombinator_ArgMin:
                case SQLFuncCall_AggregateCombinator::SQLFuncCall_AggregateCombinator_ArgMax:
                    this->generateExpression(rg, func_call->add_args()->mutable_expr());
                    this->width++;
                    generated_params++;
                    break;
                case SQLFuncCall_AggregateCombinator::SQLFuncCall_AggregateCombinator_Resample:
                    this->generateExpression(rg, func_call->add_params());
                    this->width++;
                    this->generateExpression(rg, func_call->add_params());
                    this->width++;
                    this->generateExpression(rg, func_call->add_params());
                    this->width++;
                    this->generateExpression(rg, func_call->add_args()->mutable_expr());
                    this->width++;
                    generated_params += 4;
                    break;
                default:
                    break;
            }
            func_call->add_combinators(comb);
        }
        this->levels[this->current_level].inside_aggregate = prev_inside_aggregate;
        this->levels[this->current_level].allow_window_funcs = prev_allow_window_funcs;

        func_call->set_distinct(func_call->args_size() > 0 && rg.nextSmallNumber() < 4);
        if (agg.support_nulls_clause && rg.nextSmallNumber() < 7)
        {
            func_call->set_fnulls(rg.nextBool() ? FuncNulls::NRESPECT : FuncNulls::NIGNORE);
        }
        func_call->mutable_func()->set_catalog_func(static_cast<SQLFunc>(agg.fnum));
    }
    else
    {
        /// Function
        uint32_t n_lambda = 0;
        uint32_t min_args = 0;
        uint32_t max_args = 0;
        SQLFuncName * sfn = func_call->mutable_func();

        if (!this->functions.empty() && this->peer_query != PeerQuery::ClickHouseOnly
            && (this->allow_not_deterministic || collectionHas<SQLFunction>(StatementGenerator::funcDeterministicLambda))
            && rg.nextSmallNumber() < 3)
        {
            /// Use a function from the user
            const std::reference_wrapper<const SQLFunction> & func = this->allow_not_deterministic
                ? std::ref<const SQLFunction>(rg.pickValueRandomlyFromMap(this->functions))
                : rg.pickRandomly(filterCollection<SQLFunction>(StatementGenerator::funcDeterministicLambda));

            min_args = max_args = func.get().nargs;
            func.get().setName(sfn->mutable_function());
        }
        else
        {
            /// Use a default catalog function
            const CHFunction & func = rg.nextMediumNumber() < 5 ? materialize : CHFuncs[nopt];
            const uint32_t func_max_args = std::min(func.max_args, UINT32_C(5));

            n_lambda = std::max(func.min_lambda_param, func.max_lambda_param > 0 ? (rg.nextSmallNumber() % func.max_lambda_param) : 0);
            min_args = func.min_args;
            max_args = std::min(this->fc.max_width - this->width, func_max_args);
            sfn->set_catalog_func(static_cast<SQLFunc>(func.fnum));
        }

        if (n_lambda > 0)
        {
            chassert(n_lambda == 1);
            generateLambdaCall(rg, (rg.nextSmallNumber() % 3) + 1, func_call->add_args()->mutable_lambda());
            this->width++;
            generated_params++;
        }
        if (max_args > 0 && max_args >= min_args)
        {
            std::uniform_int_distribution<uint32_t> nparams(min_args, max_args);
            const uint32_t nfunc_args = nparams(rg.generator);

            for (uint32_t i = 0; i < nfunc_args; i++)
            {
                this->generateExpression(rg, func_call->add_args()->mutable_expr());
                this->width++;
                generated_params++;
            }
        }
        else if (min_args > 0)
        {
            for (uint32_t i = 0; i < min_args; i++)
            {
                generateLiteralValue(rg, true, func_call->add_args()->mutable_expr());
            }
        }
    }
    this->width -= generated_params;
}

/// Don't forget to clear levels!
void StatementGenerator::generateTableFuncCall(RandomGenerator & rg, SQLTableFuncCall * tfunc_call)
{
    const size_t funcs_size = CHTableFuncs.size();
    std::uniform_int_distribution<size_t> next_dist(0, funcs_size - 1);
    const CHFunction & func = CHTableFuncs[next_dist(rg.generator)];
    const uint32_t func_max_args = std::min(func.max_args, UINT32_C(5));
    uint32_t generated_params = 0;
    uint32_t n_lambda = std::max(func.min_lambda_param, func.max_lambda_param > 0 ? (rg.nextSmallNumber() % func.max_lambda_param) : 0);
    uint32_t min_args = func.min_args;
    uint32_t max_args = std::min(this->fc.max_width - this->width, func_max_args);

    tfunc_call->set_func(static_cast<SQLTableFunc>(func.fnum));
    if (n_lambda > 0)
    {
        chassert(n_lambda == 1);
        generateLambdaCall(rg, (rg.nextSmallNumber() % 3) + 1, tfunc_call->add_args()->mutable_lambda());
        this->width++;
        generated_params++;
    }
    if (max_args > 0 && max_args >= min_args)
    {
        std::uniform_int_distribution<uint32_t> nparams(min_args, max_args);
        const uint32_t nfunc_args = nparams(rg.generator);

        for (uint32_t i = 0; i < nfunc_args; i++)
        {
            this->generateExpression(rg, tfunc_call->add_args()->mutable_expr());
            this->width++;
            generated_params++;
        }
    }
    else if (min_args > 0)
    {
        for (uint32_t i = 0; i < min_args; i++)
        {
            generateLiteralValue(rg, true, tfunc_call->add_args()->mutable_expr());
        }
    }
    this->width -= generated_params;
}

void StatementGenerator::generateFrameBound(RandomGenerator & rg, Expr * expr)
{
    if (rg.nextBool())
    {
        expr->mutable_lit_val()->mutable_int_lit()->set_int_lit(rg.nextRandomInt64());
    }
    else
    {
        std::unordered_map<uint32_t, QueryLevel> levels_backup;

        for (const auto & entry : this->levels)
        {
            levels_backup[entry.first] = entry.second;
        }
        this->levels.clear();
        this->generateExpression(rg, expr);
        for (const auto & entry : levels_backup)
        {
            this->levels[entry.first] = entry.second;
        }
    }
}

void StatementGenerator::generateWindowDefinition(RandomGenerator & rg, WindowDefn * wdef)
{
    if (this->width < this->fc.max_width && rg.nextSmallNumber() < 4)
    {
        const uint32_t nclauses = std::min(this->fc.max_width - this->width, (rg.nextMediumNumber() % 4) + 1);

        for (uint32_t i = 0; i < nclauses; i++)
        {
            this->generateExpression(rg, wdef->add_partition_exprs());
            this->width++;
        }
        this->width -= nclauses;
    }
    if (!this->allow_not_deterministic || (this->width < this->fc.max_width && rg.nextSmallNumber() < 4))
    {
        generateOrderBy(rg, 0, true, true, wdef->mutable_order_by());
    }
    if (this->width < this->fc.max_width && rg.nextSmallNumber() < 4)
    {
        ExprFrameSpec * efs = wdef->mutable_frame_spec();
        FrameSpecSubLeftExpr * fssle = efs->mutable_left_expr();
        std::uniform_int_distribution<uint32_t> fspec_range1(1, static_cast<uint32_t>(FrameSpecSubLeftExpr_Which_Which_MAX));
        const FrameSpecSubLeftExpr_Which fspec = static_cast<FrameSpecSubLeftExpr_Which>(fspec_range1(rg.generator));

        efs->set_range_rows(rg.nextBool() ? ExprFrameSpec_RangeRows_RANGE : ExprFrameSpec_RangeRows_ROWS);
        fssle->set_which(fspec);
        if (fspec > FrameSpecSubLeftExpr_Which_UNBOUNDED_PRECEDING)
        {
            this->generateFrameBound(rg, fssle->mutable_expr());
        }
        if (rg.nextBool())
        {
            FrameSpecSubRightExpr * fsslr = efs->mutable_right_expr();
            std::uniform_int_distribution<uint32_t> fspec_range2(1, static_cast<uint32_t>(FrameSpecSubRightExpr_Which_Which_MAX));
            const FrameSpecSubRightExpr_Which fspec2 = static_cast<FrameSpecSubRightExpr_Which>(fspec_range2(rg.generator));

            fsslr->set_which(fspec2);
            if (fspec2 > FrameSpecSubRightExpr_Which_UNBOUNDED_FOLLOWING)
            {
                this->generateFrameBound(rg, fsslr->mutable_expr());
            }
        }
    }
}

static const auto has_rel_name_lambda = [](const SQLRelation & rel) { return !rel.name.empty(); };

void StatementGenerator::generateExpression(RandomGenerator & rg, Expr * expr)
{
    ExprColAlias * eca = nullptr;
    const auto & level_rels = this->levels[this->current_level].rels;

    const uint32_t literal_value = this->inside_projection ? 50 : 100;
    const uint32_t col_ref_expr = 300;
    const uint32_t predicate_expr = 50 * static_cast<uint32_t>(this->fc.max_depth > this->depth);
    const uint32_t cast_expr = 25 * static_cast<uint32_t>(this->fc.max_depth > this->depth);
    const uint32_t unary_expr = 30 * static_cast<uint32_t>(this->fc.max_depth > this->depth);
    const uint32_t interval_expr = 5 * static_cast<uint32_t>(this->fc.max_depth > this->depth);
    const uint32_t columns_expr = 10 * static_cast<uint32_t>(this->fc.max_depth > this->depth && this->allow_not_deterministic);
    const uint32_t cond_expr = 10 * static_cast<uint32_t>(this->fc.max_depth > this->depth && this->fc.max_width > this->width);
    const uint32_t case_expr = 10 * static_cast<uint32_t>(this->fc.max_depth > this->depth && this->fc.max_width > this->width);
    const uint32_t subquery_expr = 30 * static_cast<uint32_t>(this->fc.max_depth > this->depth && this->allow_subqueries);
    const uint32_t binary_expr = 50 * static_cast<uint32_t>(this->fc.max_depth > this->depth && this->fc.max_width > (this->width + 1));
    const uint32_t array_tuple_expr = 50 * static_cast<uint32_t>(this->fc.max_depth > this->depth && this->fc.max_width > this->width);
    const uint32_t func_expr = 150 * static_cast<uint32_t>(this->fc.max_depth > this->depth);
    const uint32_t window_func_expr = 75
        * static_cast<uint32_t>(this->fc.max_depth > this->depth && this->levels[this->current_level].allow_window_funcs
                                && !this->levels[this->current_level].inside_aggregate);
    const uint32_t table_star_expr
        = 10 * static_cast<uint32_t>(std::find_if(level_rels.begin(), level_rels.end(), has_rel_name_lambda) != level_rels.end());
    const uint32_t prob_space = literal_value + col_ref_expr + predicate_expr + cast_expr + unary_expr + interval_expr + columns_expr
        + cond_expr + case_expr + subquery_expr + binary_expr + array_tuple_expr + func_expr + window_func_expr + table_star_expr;
    std::uniform_int_distribution<uint32_t> next_dist(1, prob_space);
    const uint32_t noption = next_dist(rg.generator);

    if (rg.nextSmallNumber() < 3)
    {
        eca = expr->mutable_comp_expr()->mutable_alias_expr();
        expr = eca->mutable_expr();
    }

    if (literal_value && noption < (literal_value + 1))
    {
        generateLiteralValue(rg, true, expr);
    }
    else if (col_ref_expr && noption < (literal_value + col_ref_expr + 1))
    {
        generateColRef(rg, expr);
    }
    else if (predicate_expr && noption < (literal_value + col_ref_expr + predicate_expr + 1))
    {
        generatePredicate(rg, expr);
    }
    else if (cast_expr && noption < (literal_value + col_ref_expr + predicate_expr + cast_expr + 1))
    {
        uint32_t col_counter = 0;
        const uint32_t type_mask_backup = this->next_type_mask;
        CastExpr * casexpr = expr->mutable_comp_expr()->mutable_cast_expr();

        this->depth++;
        this->next_type_mask = fc.type_mask & ~(allow_nested);
        auto tp
            = std::unique_ptr<SQLType>(randomNextType(rg, this->next_type_mask, col_counter, casexpr->mutable_type_name()->mutable_type()));
        this->next_type_mask = type_mask_backup;
        this->generateExpression(rg, casexpr->mutable_expr());
        this->depth--;
    }
    else if (unary_expr && noption < (literal_value + col_ref_expr + predicate_expr + cast_expr + unary_expr + 1))
    {
        UnaryExpr * uexpr = expr->mutable_comp_expr()->mutable_unary_expr();

        this->depth++;
        uexpr->set_unary_op(static_cast<UnaryOperator>((rg.nextRandomUInt32() % static_cast<uint32_t>(UnaryOperator::UNOP_PLUS)) + 1));
        this->generateExpression(rg, uexpr->mutable_expr());
        this->depth--;
    }
    else if (interval_expr && noption < (literal_value + col_ref_expr + predicate_expr + cast_expr + unary_expr + interval_expr + 1))
    {
        IntervalExpr * inter = expr->mutable_comp_expr()->mutable_interval();
        std::uniform_int_distribution<uint32_t> int_range(1, static_cast<uint32_t>(IntervalExpr::Interval_MAX));

        inter->set_interval(static_cast<IntervalExpr_Interval>(int_range(rg.generator)));
        this->depth++;
        this->generateExpression(rg, inter->mutable_expr());
        this->depth--;
    }
    else if (
        columns_expr
        && noption < (literal_value + col_ref_expr + predicate_expr + cast_expr + unary_expr + interval_expr + columns_expr + 1))
    {
        String ret;
        const uint32_t nopt2 = rg.nextSmallNumber();

        if (nopt2 < 6)
        {
            ret = std::to_string(rg.nextSmallNumber() - 1);
        }
        else if (nopt2 < 10)
        {
            const uint32_t first = rg.nextSmallNumber() - 1;
            const uint32_t second = std::max(rg.nextSmallNumber() - 1, first);

            ret = fmt::format("[{}-{}]", first, second);
        }
        expr->mutable_comp_expr()->set_columns(ret);
    }
    else if (
        cond_expr
        && noption
            < (literal_value + col_ref_expr + predicate_expr + cast_expr + unary_expr + interval_expr + columns_expr + cond_expr + 1))
    {
        CondExpr * conexpr = expr->mutable_comp_expr()->mutable_expr_cond();

        this->depth++;
        this->generateExpression(rg, conexpr->mutable_expr1());
        this->width++;
        this->generateExpression(rg, conexpr->mutable_expr2());
        this->width++;
        this->generateExpression(rg, conexpr->mutable_expr3());
        this->width -= 2;
        this->depth--;
    }
    else if (
        case_expr
        && noption
            < (literal_value + col_ref_expr + predicate_expr + cast_expr + unary_expr + interval_expr + columns_expr + cond_expr + case_expr
               + 1))
    {
        ExprCase * caseexp = expr->mutable_comp_expr()->mutable_expr_case();
        const uint32_t nwhen = std::min(this->fc.max_width - this->width, rg.nextMediumNumber() % 4);

        this->depth++;
        if (rg.nextSmallNumber() < 5)
        {
            this->generateExpression(rg, caseexp->mutable_expr());
        }
        for (uint32_t i = 0; i < nwhen; i++)
        {
            ExprWhenThen * wt = i == 0 ? caseexp->mutable_when_then() : caseexp->add_extra_when_thens();

            this->generateExpression(rg, wt->mutable_when_expr());
            this->generateExpression(rg, wt->mutable_then_expr());
            this->width++;
        }
        this->width -= nwhen;
        if (rg.nextSmallNumber() < 5)
        {
            this->generateExpression(rg, caseexp->mutable_else_expr());
        }
        this->depth--;
    }
    else if (
        subquery_expr
        && noption
            < (literal_value + col_ref_expr + predicate_expr + cast_expr + unary_expr + interval_expr + columns_expr + cond_expr + case_expr
               + subquery_expr + 1))
    {
        this->depth++;
        generateSubquery(rg, expr->mutable_comp_expr()->mutable_subquery());
        this->depth--;
    }
    else if (
        binary_expr
        && noption
            < (literal_value + col_ref_expr + predicate_expr + cast_expr + unary_expr + interval_expr + columns_expr + cond_expr + case_expr
               + subquery_expr + binary_expr + 1))
    {
        BinaryExpr * bexpr = expr->mutable_comp_expr()->mutable_binary_expr();

        this->depth++;
        if (rg.nextSmallNumber() < 9)
        {
            bexpr->set_op(static_cast<BinaryOperator>((rg.nextRandomUInt32() % 6) + 13));
        }
        else
        {
            std::uniform_int_distribution<uint32_t> binop_range(1, static_cast<uint32_t>(BinaryOperator_MAX));

            bexpr->set_op(static_cast<BinaryOperator>(binop_range(rg.generator)));
        }
        this->generateExpression(rg, bexpr->mutable_lhs());
        this->width++;
        this->generateExpression(rg, bexpr->mutable_rhs());
        this->width--;
        this->depth--;
    }
    else if (
        array_tuple_expr
        && noption
            < (literal_value + col_ref_expr + predicate_expr + cast_expr + unary_expr + interval_expr + columns_expr + cond_expr + case_expr
               + subquery_expr + binary_expr + array_tuple_expr + 1))
    {
        ExprList * elist = rg.nextBool() ? expr->mutable_comp_expr()->mutable_array() : expr->mutable_comp_expr()->mutable_tuple();
        const uint32_t nvalues = std::min(this->fc.max_width - this->width, rg.nextMediumNumber() % 8);

        this->depth++;
        for (uint32_t i = 0; i < nvalues; i++)
        {
            this->generateExpression(rg, i == 0 ? elist->mutable_expr() : elist->add_extra_exprs());
            this->width++;
        }
        this->depth--;
        this->width -= nvalues;
    }
    else if (
        func_expr
        && noption
            < (literal_value + col_ref_expr + predicate_expr + cast_expr + unary_expr + interval_expr + columns_expr + cond_expr + case_expr
               + subquery_expr + binary_expr + array_tuple_expr + func_expr + 1))
    {
        /// Func
        const bool allow_aggr
            = (!this->levels[this->current_level].inside_aggregate && this->levels[this->current_level].allow_aggregates
               && (!this->levels[this->current_level].gcols.empty() || this->levels[this->current_level].global_aggregate))
            || rg.nextSmallNumber() < 3;

        this->depth++;
        generateFuncCall(rg, true, allow_aggr, expr->mutable_comp_expr()->mutable_func_call());
        this->depth--;
    }
    else if (
        window_func_expr
        && noption
            < (literal_value + col_ref_expr + predicate_expr + cast_expr + unary_expr + interval_expr + columns_expr + cond_expr + case_expr
               + subquery_expr + binary_expr + array_tuple_expr + func_expr + window_func_expr + 1))
    {
        /// Window func
        WindowFuncCall * wfc = expr->mutable_comp_expr()->mutable_window_call();
        const bool prev_allow_window_funcs = this->levels[this->current_level].allow_window_funcs;

        this->depth++;
        /// Most of the times disallow nested window functions
        this->levels[this->current_level].allow_window_funcs = rg.nextSmallNumber() < 3;
        if (rg.nextSmallNumber() < 7)
        {
            generateFuncCall(rg, false, true, wfc->mutable_agg_func());
        }
        else
        {
            uint32_t nargs = 0;
            SQLWindowCall * wc = wfc->mutable_win_func();

            chassert(this->ids.empty());
            if (this->fc.max_width - this->width > 1)
            {
                this->ids.emplace_back(static_cast<uint32_t>(WINnth_value));
            }
            if (this->fc.max_width > this->width)
            {
                this->ids.emplace_back(static_cast<uint32_t>(WINfirst_value));
                this->ids.emplace_back(static_cast<uint32_t>(WINlast_value));
                this->ids.emplace_back(static_cast<uint32_t>(WINntile));
                this->ids.emplace_back(static_cast<uint32_t>(WINlag));
                this->ids.emplace_back(static_cast<uint32_t>(WINlagInFrame));
                this->ids.emplace_back(static_cast<uint32_t>(WINlead));
                this->ids.emplace_back(static_cast<uint32_t>(WINleadInFrame));
            }
            this->ids.emplace_back(static_cast<uint32_t>(WINdense_rank));
            this->ids.emplace_back(static_cast<uint32_t>(WINnth_value));
            this->ids.emplace_back(static_cast<uint32_t>(WINpercent_rank));
            this->ids.emplace_back(static_cast<uint32_t>(WINrank));
            this->ids.emplace_back(static_cast<uint32_t>(WINrow_number));
            const WindowFuncs wfs = static_cast<WindowFuncs>(rg.pickRandomly(this->ids));

            this->ids.clear();
            switch (wfs)
            {
                case WINfirst_value:
                case WINlast_value:
                    if (rg.nextSmallNumber() < 7)
                    {
                        wc->set_fnulls(rg.nextBool() ? FuncNulls::NRESPECT : FuncNulls::NIGNORE);
                    }
                    nargs = 1;
                    break;
                case WINntile:
                    nargs = 1;
                    break;
                case WINnth_value:
                    nargs = 2;
                    break;
                case WINlag:
                case WINlagInFrame:
                case WINlead:
                case WINleadInFrame:
                    nargs = std::min(this->fc.max_width - this->width, (rg.nextMediumNumber() % 3) + 1);
                    break;
                default:
                    break;
            }
            wc->set_func(wfs);
            for (uint32_t i = 0; i < nargs; i++)
            {
                this->generateExpression(rg, wc->add_args());
                this->width++;
            }
            this->width -= nargs;
        }
        if (this->levels[this->current_level].window_counter > 0 && rg.nextBool())
        {
            wfc->mutable_window()->set_window(
                "w" + std::to_string(rg.nextRandomUInt32() % this->levels[this->current_level].window_counter));
        }
        else
        {
            generateWindowDefinition(rg, wfc->mutable_win_defn());
        }
        this->depth--;
        this->levels[this->current_level].allow_window_funcs = prev_allow_window_funcs;
    }
    else if (
        table_star_expr
        && noption
            < (literal_value + col_ref_expr + predicate_expr + cast_expr + unary_expr + interval_expr + columns_expr + cond_expr + case_expr
               + subquery_expr + binary_expr + array_tuple_expr + func_expr + window_func_expr + table_star_expr + 1))
    {
        filtered_relations.clear();
        for (const auto & entry : level_rels)
        {
            if (has_rel_name_lambda(entry))
            {
                filtered_relations.emplace_back(std::ref(entry));
            }
        }
        expr->mutable_comp_expr()->mutable_table()->set_table(rg.pickRandomly(filtered_relations).get().name);
    }
    else
    {
        chassert(0);
    }

    addFieldAccess(rg, expr, 6);
    if (eca && this->allow_in_expression_alias && !this->inside_projection && rg.nextSmallNumber() < 4)
    {
        SQLRelation rel("");
        const String ncname = this->getNextAlias();

        rel.cols.emplace_back(SQLRelationCol("", {ncname}));
        this->levels[this->current_level].rels.emplace_back(rel);
        eca->mutable_col_alias()->set_column(ncname);
        this->levels[this->current_level].projections.emplace_back(ncname);
        eca->set_use_parenthesis(rg.nextMediumNumber() < 98);
    }
}

}
