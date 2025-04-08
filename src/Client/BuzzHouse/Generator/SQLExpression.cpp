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
            const uint32_t nvalues = std::max(std::min(this->fc.max_width - this->width, rg.nextSmallNumber() % 5), UINT32_C(1));

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

void StatementGenerator::generateLiteralValue(RandomGenerator & rg, const bool complex, Expr * expr)
{
    const uint32_t noption = rg.nextLargeNumber();
    LiteralValue * lv = expr->mutable_lit_val();
    uint32_t nested_prob = 0;

    if (noption < 201)
    {
        IntLiteral * il = lv->mutable_int_lit();

        if (noption < 21)
        {
            /// Hugeint
            HugeIntLiteral * huge = il->mutable_huge_lit();

            huge->set_upper(rg.nextRandomInt64());
            huge->set_lower(rg.nextRandomUInt64());
            if (complex && rg.nextSmallNumber() < 9)
            {
                il->set_integers(rg.nextBool() ? Integers::Int128 : Integers::Int256);
            }
        }
        else if (noption < 41)
        {
            /// UHugeint
            UHugeIntLiteral * uhuge = il->mutable_uhuge_lit();

            uhuge->set_upper(rg.nextRandomUInt64());
            uhuge->set_lower(rg.nextRandomUInt64());
            if (complex && rg.nextSmallNumber() < 9)
            {
                il->set_integers(rg.nextBool() ? Integers::UInt128 : Integers::UInt256);
            }
        }
        else if (noption < 121)
        {
            il->set_int_lit(rg.nextRandomInt64());
            if (complex && rg.nextSmallNumber() < 9)
            {
                il->set_integers(static_cast<Integers>(
                    (rg.nextRandomUInt32() % static_cast<uint32_t>(Integers::Int - Integers::UInt256))
                    + static_cast<uint32_t>(Integers::Int8)));
            }
        }
        else
        {
            il->set_uint_lit(rg.nextRandomUInt64());
            if (complex && rg.nextSmallNumber() < 9)
            {
                il->set_integers(static_cast<Integers>((rg.nextRandomUInt32() % static_cast<uint32_t>(Integers_MAX)) + 1));
            }
        }
    }
    else if (noption < 401)
    {
        String ret;

        if (noption < 251)
        {
            ret = fmt::format("'{}'{}", rg.nextDate(), complex ? "::Date" : "");
        }
        else if (noption < 301)
        {
            ret = fmt::format("'{}'{}", rg.nextDate32(), complex ? "::Date32" : "");
        }
        else if (noption < 351)
        {
            ret = fmt::format("'{}'{}", rg.nextDateTime(), complex ? "::DateTime" : "");
        }
        else
        {
            ret = fmt::format("'{}'{}", rg.nextDateTime64(), complex ? "::DateTime64" : "");
        }
        lv->set_no_quote_str(ret);
    }
    else if (noption < 501)
    {
        String buf;
        std::uniform_int_distribution<uint32_t> next_dist(0, 30);
        const uint32_t left = next_dist(rg.generator);
        const uint32_t right = next_dist(rg.generator);

        buf += complex ? "(" : "";
        buf += appendDecimal(rg, left, right);
        buf += complex ? ")" : "";
        lv->set_no_quote_str(std::move(buf));
    }
    else if (complex && this->allow_not_deterministic && noption < 551)
    {
        String ret;
        const uint32_t nlen = rg.nextLargeNumber();
        const uint32_t noption2 = rg.nextSmallNumber();

        if (noption2 < 3)
        {
            ret = "randomString";
        }
        else if (noption2 < 5)
        {
            ret = "randomFixedString";
        }
        else if (noption2 < 7)
        {
            ret = "randomPrintableASCII";
        }
        else
        {
            ret = "randomStringUTF8";
        }
        lv->set_no_quote_str(fmt::format("{}({})", ret, nlen));
    }
    else if (noption < 601)
    {
        String ret;
        const uint32_t nopt = rg.nextLargeNumber();

        if (nopt < 31)
        {
            ret = fmt::format("'{}'{}", rg.nextUUID(), complex ? "::UUID" : "");
        }
        else if (nopt < 51)
        {
            ret = fmt::format("'{}'{}", rg.nextIPv4(), complex ? "::IPv4" : "");
        }
        else if (nopt < 71)
        {
            ret = fmt::format("'{}'{}", rg.nextIPv6(), complex ? "::IPv6" : "");
        }
        else if (nopt < 101)
        {
            const GeoTypes gt = static_cast<GeoTypes>((rg.nextRandomUInt32() % static_cast<uint32_t>(GeoTypes_MAX)) + 1);

            ret = fmt::format("'{}'{}{}", strAppendGeoValue(rg, gt), complex ? "::" : "", complex ? GeoTypes_Name(gt) : "");
        }
        else
        {
            ret = rg.nextString("'", true, rg.nextRandomUInt32() % 1009);
        }
        lv->set_no_quote_str(ret);
    }
    else if (noption < 701)
    {
        lv->set_special_val(static_cast<SpecialVal>((rg.nextRandomUInt32() % static_cast<uint32_t>(SpecialVal_MAX)) + 1));
        nested_prob = 3;
    }
    else if (noption < 801)
    {
        lv->set_special_val(rg.nextBool() ? SpecialVal::VAL_ONE : SpecialVal::VAL_ZERO);
    }
    else if (noption < 951)
    {
        std::uniform_int_distribution<int> dopt(1, 3);
        std::uniform_int_distribution<int> wopt(1, 3);

        lv->set_no_quote_str(fmt::format("'{}'{}", strBuildJSON(rg, dopt(rg.generator), wopt(rg.generator)), complex ? "::JSON" : ""));
    }
    else
    {
        lv->set_special_val(SpecialVal::VAL_NULL);
    }
    addFieldAccess(rg, expr, nested_prob);
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
        const bool prev_inside_aggregate = this->levels[this->current_level].inside_aggregate;
        const bool prev_allow_aggregates = this->levels[this->current_level].allow_aggregates;
        const bool prev_allow_window_funcs = this->levels[this->current_level].allow_window_funcs;

        this->levels[this->current_level].inside_aggregate = false;
        this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs = true;

        this->current_level++;
        this->levels[this->current_level] = QueryLevel(this->current_level);
        this->generateSelect(
            rg, true, false, 1, std::numeric_limits<uint32_t>::max(), eq->mutable_inner_query()->mutable_select()->mutable_sel());
        this->current_level--;

        this->levels[this->current_level].inside_aggregate = prev_inside_aggregate;
        this->levels[this->current_level].allow_aggregates = prev_allow_aggregates;
        this->levels[this->current_level].allow_window_funcs = prev_allow_window_funcs;
    }
}

void StatementGenerator::generatePredicate(RandomGenerator & rg, Expr * expr)
{
    if (this->depth < this->fc.max_depth)
    {
        const uint32_t noption = rg.nextLargeNumber();

        if (noption < 101)
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
        else if (this->fc.max_width > this->width + 1 && noption < 301)
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
        else if (this->fc.max_width > this->width + 2 && noption < 401)
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
        else if (this->width < this->fc.max_width && noption < 501)
        {
            const uint32_t nopt2 = rg.nextSmallNumber();
            const uint32_t nclauses = std::min(this->fc.max_width - this->width, (rg.nextSmallNumber() % 4) + 1);
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
        else if (this->fc.max_width > this->width + 1 && noption < 601)
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
        else if (noption < 701)
        {
            ComplicatedExpr * cexpr = expr->mutable_comp_expr();
            ExprNullTests * enull = cexpr->mutable_expr_null_tests();

            enull->set_not_(rg.nextBool());
            this->depth++;
            this->generateExpression(rg, enull->mutable_expr());
            this->depth--;
        }
        else if (this->allow_subqueries && noption < 801)
        {
            ComplicatedExpr * cexpr = expr->mutable_comp_expr();
            ExprExists * exists = cexpr->mutable_expr_exists();

            exists->set_not_(rg.nextBool());
            this->depth++;
            generateSubquery(rg, exists->mutable_select());
            this->depth--;
        }
        else if (this->fc.max_width > this->width + 1 && noption < 901)
        {
            ComplicatedExpr * cexpr = expr->mutable_comp_expr();
            ExprLike * elike = cexpr->mutable_expr_like();

            elike->set_not_(rg.nextBool());
            elike->set_keyword(static_cast<ExprLike_PossibleKeywords>(
                (rg.nextRandomUInt32() % static_cast<uint32_t>(ExprLike::PossibleKeywords_MAX)) + 1));
            this->depth++;
            this->generateExpression(rg, elike->mutable_expr1());
            this->width++;
            this->generateExpression(rg, elike->mutable_expr2());
            this->width--;
            this->depth--;
        }
        else
        {
            this->depth++;
            this->generateExpression(rg, expr);
            this->depth--;
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
    const bool prev_inside_aggregate = this->levels[this->current_level].inside_aggregate;
    const bool prev_allow_aggregates = this->levels[this->current_level].allow_aggregates;
    const bool prev_allow_window_funcs = this->levels[this->current_level].allow_window_funcs;

    for (const auto & entry : this->levels)
    {
        levels_backup[entry.first] = entry.second;
    }
    this->levels.clear();
    this->levels[this->current_level].inside_aggregate = false;
    this->levels[this->current_level].allow_aggregates = this->levels[this->current_level].allow_window_funcs = true;

    for (uint32_t i = 0; i < nparams; i++)
    {
        const String buf = String(1, 'a' + i);
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
    this->levels[this->current_level].inside_aggregate = prev_inside_aggregate;
    this->levels[this->current_level].allow_aggregates = prev_allow_aggregates;
    this->levels[this->current_level].allow_window_funcs = prev_allow_window_funcs;
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
            const SQLFuncCall_AggregateCombinator comb = static_cast<SQLFuncCall_AggregateCombinator>(
                (rg.nextRandomUInt32()
                 % static_cast<uint32_t>(this->allow_not_deterministic ? SQLFuncCall::AggregateCombinator_MAX : SQLFuncCall::ArgMax))
                + 1);

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
        const uint32_t nclauses = std::min(this->fc.max_width - this->width, (rg.nextSmallNumber() % 4) + 1);

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
        const FrameSpecSubLeftExpr_Which fspec = static_cast<FrameSpecSubLeftExpr_Which>(
            (rg.nextRandomUInt32() % static_cast<uint32_t>(FrameSpecSubLeftExpr_Which_Which_MAX)) + 1);

        efs->set_range_rows(rg.nextBool() ? ExprFrameSpec_RangeRows_RANGE : ExprFrameSpec_RangeRows_ROWS);
        fssle->set_which(fspec);
        if (fspec > FrameSpecSubLeftExpr_Which_UNBOUNDED_PRECEDING)
        {
            this->generateFrameBound(rg, fssle->mutable_expr());
        }
        if (rg.nextBool())
        {
            FrameSpecSubRightExpr * fsslr = efs->mutable_right_expr();
            const FrameSpecSubRightExpr_Which fspec2 = static_cast<FrameSpecSubRightExpr_Which>(
                (rg.nextRandomUInt32() % static_cast<uint32_t>(FrameSpecSubRightExpr_Which_Which_MAX)) + 1);

            fsslr->set_which(fspec2);
            if (fspec2 > FrameSpecSubRightExpr_Which_UNBOUNDED_FOLLOWING)
            {
                this->generateFrameBound(rg, fsslr->mutable_expr());
            }
        }
    }
}

void StatementGenerator::generateExpression(RandomGenerator & rg, Expr * expr)
{
    const uint32_t noption = rg.nextLargeNumber();
    ExprColAlias * eca = nullptr;

    if (rg.nextSmallNumber() < 3)
    {
        ParenthesesExpr * paren = expr->mutable_comp_expr()->mutable_par_expr();

        eca = paren->mutable_expr();
        expr = eca->mutable_expr();
    }

    if (noption < (this->inside_projection ? 76 : 101))
    {
        generateLiteralValue(rg, true, expr);
    }
    else if (this->depth >= this->fc.max_depth || noption < 401)
    {
        generateColRef(rg, expr);
    }
    else if (noption < 451)
    {
        generatePredicate(rg, expr);
    }
    else if (noption < 501)
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
    else if (noption < 526)
    {
        UnaryExpr * uexpr = expr->mutable_comp_expr()->mutable_unary_expr();

        this->depth++;
        uexpr->set_unary_op(static_cast<UnaryOperator>((rg.nextRandomUInt32() % static_cast<uint32_t>(UnaryOperator::UNOP_PLUS)) + 1));
        this->generateExpression(rg, uexpr->mutable_expr());
        this->depth--;
    }
    else if (noption < 551)
    {
        IntervalExpr * inter = expr->mutable_comp_expr()->mutable_interval();

        this->depth++;
        inter->set_interval(
            static_cast<IntervalExpr_Interval>((rg.nextRandomUInt32() % static_cast<uint32_t>(IntervalExpr::Interval_MAX)) + 1));
        this->generateExpression(rg, inter->mutable_expr());
        this->depth--;
    }
    else if (this->allow_not_deterministic && noption < 556)
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
    else if (this->fc.max_width > this->width && noption < 576)
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
    else if (this->fc.max_width > this->width + 1 && noption < 601)
    {
        ExprCase * caseexp = expr->mutable_comp_expr()->mutable_expr_case();
        const uint32_t nwhen = std::min(this->fc.max_width - this->width, rg.nextSmallNumber() % 4);

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
    else if (this->allow_subqueries && noption < 651)
    {
        this->depth++;
        generateSubquery(rg, expr->mutable_comp_expr()->mutable_subquery());
        this->depth--;
    }
    else if (this->fc.max_width > this->width + 1 && noption < 701)
    {
        BinaryExpr * bexpr = expr->mutable_comp_expr()->mutable_binary_expr();

        this->depth++;
        if (rg.nextSmallNumber() < 9)
        {
            bexpr->set_op(static_cast<BinaryOperator>((rg.nextRandomUInt32() % 6) + 13));
        }
        else
        {
            bexpr->set_op(static_cast<BinaryOperator>((rg.nextRandomUInt32() % static_cast<uint32_t>(BinaryOperator_MAX)) + 1));
        }
        this->generateExpression(rg, bexpr->mutable_lhs());
        this->width++;
        this->generateExpression(rg, bexpr->mutable_rhs());
        this->width--;
        this->depth--;
    }
    else if (this->width < this->fc.max_width && noption < 751)
    {
        ArraySequence * arr = expr->mutable_comp_expr()->mutable_array();
        const uint32_t nvalues = std::min(this->fc.max_width - this->width, rg.nextSmallNumber() % 8);

        this->depth++;
        for (uint32_t i = 0; i < nvalues; i++)
        {
            this->generateExpression(rg, arr->add_values());
            this->width++;
        }
        this->depth--;
        this->width -= nvalues;
    }
    else if (this->width < this->fc.max_width && noption < 801)
    {
        TupleSequence * tupl = expr->mutable_comp_expr()->mutable_tuple();
        const uint32_t nvalues = std::min(this->fc.max_width - this->width, rg.nextSmallNumber() % 8);
        const uint32_t ncols = std::min(this->fc.max_width - this->width, (rg.nextSmallNumber() % 4) + 1);

        this->depth++;
        for (uint32_t i = 0; i < ncols; i++)
        {
            ExprList * elist = tupl->add_values();

            for (uint32_t j = 0; j < nvalues; j++)
            {
                Expr * el = j == 0 ? elist->mutable_expr() : elist->add_extra_exprs();

                this->generateExpression(rg, el);
                this->width++;
            }
            this->width -= nvalues;
            this->width++;
        }
        this->width -= ncols;
        this->depth--;
    }
    else if (!this->levels[this->current_level].allow_window_funcs || this->levels[this->current_level].inside_aggregate || noption < 951)
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
    else
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
                this->ids.emplace_back(static_cast<uint32_t>(WINlagInFrame));
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
                case WINlagInFrame:
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
    addFieldAccess(rg, expr, 6);
    if (eca && this->allow_in_expression_alias && !this->inside_projection && rg.nextSmallNumber() < 4)
    {
        SQLRelation rel("");
        const String ncname = this->getNextAlias();

        rel.cols.emplace_back(SQLRelationCol("", {ncname}));
        this->levels[this->current_level].rels.emplace_back(rel);
        eca->mutable_col_alias()->set_column(ncname);
        this->levels[this->current_level].projections.emplace_back(ncname);
    }
}

}
