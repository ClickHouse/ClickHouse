#include <Common/StringUtils.h>

#include <Client/BuzzHouse/Generator/SQLCatalog.h>
#include <Client/BuzzHouse/Generator/SQLFuncs.h>
#include <Client/BuzzHouse/Generator/SQLTypes.h>
#include <Client/BuzzHouse/Generator/StatementGenerator.h>

namespace BuzzHouse
{

String StatementGenerator::getNextAlias(RandomGenerator & rg)
{
    /// Most of the times, use a new alias
    const uint32_t noption = rg.nextMediumNumber();

    if (noption < 81)
    {
        return "a" + std::to_string(aliases_counter++);
    }
    return (noption < 91 ? "a" : "c") + std::to_string(rg.randomInt<uint32_t>(0, noption < 91 ? 2 : 3));
}

void StatementGenerator::addFieldAccess(RandomGenerator & rg, Expr * expr, const uint32_t nested_prob)
{
    if (rg.nextMediumNumber() < nested_prob)
    {
        const uint32_t noption = rg.nextMediumNumber();
        FieldAccess * fa = expr->mutable_field();

        this->depth++;
        if (noption < 41)
        {
            fa->set_array_index(rg.randomInt<int32_t>(-4, 4));
        }
        else if (noption < 71)
        {
            fa->set_tuple_index(rg.randomInt<int32_t>(-4, 4));
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
            const uint32_t nvalues = std::max(std::min(this->fc.max_width - this->width, rg.randomInt<uint32_t>(0, 3)), UINT32_C(1));

            for (uint32_t i = 0; i < nvalues; i++)
            {
                const uint32_t noption2 = rg.nextMediumNumber();
                JSONColumn * jcol = i == 0 ? subcols->mutable_jcol() : subcols->add_other_jcols();

                if (noption2 < 31)
                {
                    jcol->set_jcol(true);
                }
                else if (noption2 < 61)
                {
                    jcol->set_jarray(0);
                }
                jcol->mutable_col()->set_column(rg.nextJSONCol());
                this->width++;
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

                const uint64_t type_mask_backup = this->next_type_mask;
                this->next_type_mask = fc.type_mask & ~(allow_nested);
                auto tp = std::unique_ptr<SQLType>(randomNextType(rg, this->next_type_mask, col_counter, tpn->mutable_type()));
                this->next_type_mask = type_mask_backup;
            }
        }
        if (rg.nextMediumNumber() < nested_prob)
        {
            uint32_t col_counter = 0;

            const uint64_t type_mask_backup = this->next_type_mask;
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
        else if (nsuboption < 20)
        {
            cp.add_sub_cols()->set_column("size");
        }
        else if (nsuboption < 24)
        {
            cp.add_sub_cols()->set_column("size" + std::to_string(rg.randomInt<uint32_t>(0, 2)));
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

    if (!rel_col.rel_name.empty() && rg.nextMediumNumber() < 86)
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
    const bool allow_floats
        = (this->next_type_mask & (allow_bfloat16 | allow_float32 | allow_float64)) != 0 && this->fc.fuzz_floating_points;
    uint32_t nested_prob = 0;
    LiteralValue * lv = expr->mutable_lit_val();

    litMask[static_cast<size_t>(LitOp::LitHugeInt)] = (this->next_type_mask & allow_int128) != 0;
    litMask[static_cast<size_t>(LitOp::LitUHugeInt)] = (this->next_type_mask & allow_int128) != 0;
    /// litMask[static_cast<size_t>(LitOp::LitInt)] = true;
    /// litMask[static_cast<size_t>(LitOp::LitUInt)] = true;
    litMask[static_cast<size_t>(LitOp::LitTime)] = (this->next_type_mask & allow_time) != 0;
    litMask[static_cast<size_t>(LitOp::LitDate)] = (this->next_type_mask & allow_dates) != 0;
    litMask[static_cast<size_t>(LitOp::LitDateTime)] = (this->next_type_mask & allow_datetimes) != 0;
    litMask[static_cast<size_t>(LitOp::LitDecimal)] = (this->next_type_mask & allow_decimals) != 0;
    litMask[static_cast<size_t>(LitOp::LitRandStr)] = complex && this->allow_not_deterministic;
    litMask[static_cast<size_t>(LitOp::LitUUID)] = (this->next_type_mask & allow_uuid) != 0;
    litMask[static_cast<size_t>(LitOp::LitIPv4)] = (this->next_type_mask & allow_ipv4) != 0;
    litMask[static_cast<size_t>(LitOp::LitIPv6)] = (this->next_type_mask & allow_ipv6) != 0;
    litMask[static_cast<size_t>(LitOp::LitGeo)] = (this->next_type_mask & allow_geo) != 0 && allow_floats;
    /// litMask[static_cast<size_t>(LitOp::LitStr)] = true;
    /// litMask[static_cast<size_t>(LitOp::LitSpecial)] = true;
    litMask[static_cast<size_t>(LitOp::LitJSON)] = (this->next_type_mask & allow_JSON) != 0;
    /// litMask[static_cast<size_t>(LitOp::LitNULLVal)] = true;
    /// litMask[static_cast<size_t>(LitOp::LitFraction)] = true;
    litGen.setEnabled(litMask);

    switch (static_cast<LitOp>(litGen.nextOp())) /// drifts over time
    {
        case LitOp::LitHugeInt: {
            IntLiteral * il = lv->mutable_int_lit();
            HugeIntLiteral * huge = il->mutable_huge_lit();

            huge->set_upper(rg.nextRandomInt64());
            huge->set_lower(rg.nextRandomUInt64());
            if (complex && rg.nextSmallNumber() < 9)
            {
                il->set_integers(rg.nextBool() ? Integers::Int128 : Integers::Int256);
            }
        }
        break;
        case LitOp::LitUHugeInt: {
            IntLiteral * il = lv->mutable_int_lit();
            UHugeIntLiteral * uhuge = il->mutable_uhuge_lit();

            uhuge->set_upper(rg.nextRandomUInt64());
            uhuge->set_lower(rg.nextRandomUInt64());
            if (complex && rg.nextSmallNumber() < 9)
            {
                il->set_integers(rg.nextBool() ? Integers::UInt128 : Integers::UInt256);
            }
        }
        break;
        case LitOp::LitInt: {
            IntLiteral * il = lv->mutable_int_lit();

            il->set_int_lit(rg.nextRandomInt64());
            if (complex && rg.nextSmallNumber() < 9)
            {
                std::uniform_int_distribution<uint32_t> integers_range(
                    static_cast<uint32_t>(Integers::Int8), static_cast<uint32_t>(Integers::Int));

                il->set_integers(static_cast<Integers>(integers_range(rg.generator)));
            }
        }
        break;
        case LitOp::LitUInt: {
            IntLiteral * il = lv->mutable_int_lit();

            il->set_uint_lit(rg.nextRandomUInt64());
            if (complex && rg.nextSmallNumber() < 9)
            {
                std::uniform_int_distribution<uint32_t> integers_range(1, static_cast<uint32_t>(Integers_MAX));

                il->set_integers(static_cast<Integers>(integers_range(rg.generator)));
            }
        }
        break;
        case LitOp::LitTime: {
            const SQLType * tp = randomTimeType(rg, std::numeric_limits<uint32_t>::max(), nullptr);
            const bool prev_allow_not_deterministic = this->allow_not_deterministic;

            this->allow_not_deterministic &= complex;
            lv->set_no_quote_str(tp->appendRandomRawValue(rg, *this));
            this->allow_not_deterministic = prev_allow_not_deterministic;
            delete tp;
        }
        break;
        case LitOp::LitDate: {
            const SQLType * tp;
            const bool prev_allow_not_deterministic = this->allow_not_deterministic;
            std::tie(tp, std::ignore) = randomDateType(rg, std::numeric_limits<uint32_t>::max());

            this->allow_not_deterministic &= complex;
            lv->set_no_quote_str(tp->appendRandomRawValue(rg, *this));
            this->allow_not_deterministic = prev_allow_not_deterministic;
            delete tp;
        }
        break;
        case LitOp::LitDateTime: {
            const SQLType * tp = randomDateTimeType(rg, std::numeric_limits<uint32_t>::max(), nullptr);
            const bool prev_allow_not_deterministic = this->allow_not_deterministic;

            this->allow_not_deterministic &= complex;
            lv->set_no_quote_str(tp->appendRandomRawValue(rg, *this));
            this->allow_not_deterministic = prev_allow_not_deterministic;
            delete tp;
        }
        break;
        case LitOp::LitDecimal: {
            const DecimalType * tp = static_cast<DecimalType *>(randomDecimalType(rg, std::numeric_limits<uint32_t>::max(), nullptr));

            lv->set_no_quote_str(DecimalType::appendDecimalValue(rg, complex && rg.nextSmallNumber() < 9, tp));
            delete tp;
        }
        break;
        case LitOp::LitRandStr: {
            static const DB::Strings & funcs = {"randomString", "randomFixedString", "randomPrintableASCII", "randomStringUTF8"};

            lv->set_no_quote_str(fmt::format("{}({})", rg.pickRandomly(funcs), rg.nextStrlen()));
        }
        break;
        case LitOp::LitUUID:
            lv->set_no_quote_str(fmt::format("'{}'{}", rg.nextUUID(), complex ? "::UUID" : ""));
            break;
        case LitOp::LitIPv4:
            lv->set_no_quote_str(fmt::format("'{}'{}", rg.nextIPv4(), complex ? "::IPv4" : ""));
            break;
        case LitOp::LitIPv6:
            lv->set_no_quote_str(fmt::format("'{}'{}", rg.nextIPv6(), complex ? "::IPv6" : ""));
            break;
        case LitOp::LitGeo: {
            std::uniform_int_distribution<uint32_t> geo_range(1, static_cast<uint32_t>(GeoTypes_MAX));
            const GeoTypes gt = static_cast<GeoTypes>(geo_range(rg.generator));

            lv->set_no_quote_str(fmt::format("'{}'{}{}", strAppendGeoValue(rg, gt), complex ? "::" : "", complex ? GeoTypes_Name(gt) : ""));
        }
        break;
        case LitOp::LitStr:
            lv->set_string_lit(rg.nextString("'", true, rg.nextStrlen()));
            break;
        case LitOp::LitSpecial: {
            SpecialVal * val = lv->mutable_special_val();
            std::uniform_int_distribution<uint32_t> special_range(1, static_cast<uint32_t>(SpecialVal::SpecialValEnum_MAX));
            const SpecialVal_SpecialValEnum sval = static_cast<SpecialVal_SpecialValEnum>(special_range(rg.generator));

            /// Can't use `*` on query oracles
            val->set_val(
                (sval != SpecialVal_SpecialValEnum_VAL_STAR || this->allow_not_deterministic
                 || this->levels[this->current_level].inside_aggregate)
                    ? sval
                    : SpecialVal_SpecialValEnum_VAL_NULL);
            val->set_paren(complex && rg.nextBool());
            nested_prob = 3;
        }
        break;
        case LitOp::LitJSON: {
            std::uniform_int_distribution<int> jrange(1, 10);

            lv->set_no_quote_str(
                fmt::format("'{}'{}", strBuildJSON(rg, jrange(rg.generator), jrange(rg.generator)), complex ? "::JSON" : ""));
        }
        break;
        case LitOp::LitNULLVal:
            lv->mutable_special_val()->set_val(SpecialVal_SpecialValEnum::SpecialVal_SpecialValEnum_VAL_NULL);
            break;
        case LitOp::LitFraction: {
            std::uniform_int_distribution<uint32_t> frange(1, 999);

            lv->set_no_quote_str(fmt::format("0.{}", frange(rg.generator)));
        }
        break;
    }
    addFieldAccess(rg, expr, nested_prob);
}

void StatementGenerator::generateLiteralValue(RandomGenerator & rg, const bool complex, Expr * expr)
{
    const uint32_t nopt = rg.nextMediumNumber();

    if (nopt < 15 && ((this->next_type_mask & allow_array) != 0 || (complex && ((this->next_type_mask & allow_tuple) != 0))))
    {
        /// Generate a few arrays/tuples with literal values
        ExprList * elist = ((this->next_type_mask & allow_tuple) == 0 || !complex || rg.nextBool())
            ? expr->mutable_comp_expr()->mutable_array()
            : expr->mutable_comp_expr()->mutable_tuple();
        const uint32_t nvalues = std::min(this->fc.max_width - this->width, rg.randomInt<uint32_t>(0, 8));

        this->depth++;
        for (uint32_t i = 0; i < nvalues; i++)
        {
            /// There are no recursive calls here, so don't bother about width and depth
            this->generateLiteralValue(rg, complex, i == 0 ? elist->mutable_expr() : elist->add_extra_exprs());
        }
        this->depth--;
    }
    else if (nopt < 20 && (this->next_type_mask & allow_map) != 0)
    {
        /// Generate a few map key/value pairs
        const uint32_t nvalues = std::min(this->fc.max_width - this->width, rg.randomInt<uint32_t>(0, 4));
        SQLFuncCall * fcall = expr->mutable_comp_expr()->mutable_func_call();

        fcall->mutable_func()->set_catalog_func(SQLFunc::FUNCmap);
        this->depth++;
        for (uint32_t i = 0; i < nvalues; i++)
        {
            /// There are no recursive calls here, so don't bother about width and depth
            this->generateLiteralValue(rg, complex, fcall->add_args()->mutable_expr());
            this->generateLiteralValue(rg, complex, fcall->add_args()->mutable_expr());
        }
        this->depth--;
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
    this->current_level++;
    if (rg.nextMediumNumber() < 6)
    {
        prepareNextExplain(rg, eq);
    }
    else
    {
        this->levels[this->current_level] = QueryLevel(this->current_level);

        /// When running oracles with global aggregates, a correlated column can give false positives
        if ((this->allow_not_deterministic || !this->levels[this->current_level - 1].global_aggregate) && rg.nextBool())
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
            rg,
            true,
            false,
            1,
            std::numeric_limits<uint32_t>::max(),
            std::nullopt,
            eq->mutable_inner_query()->mutable_select()->mutable_sel());
    }
    this->current_level--;
}

void StatementGenerator::generateLikeExpr(RandomGenerator & rg, Expr * expr)
{
    if (rg.nextBool())
    {
        String buf;
        std::uniform_int_distribution<uint32_t> count_dist(1, 5);
        const uint32_t count = count_dist(rg.generator);

        for (uint32_t i = 0; i < count; i++)
        {
            const uint32_t nopt = rg.nextSmallNumber();

            if (nopt < 4)
            {
                buf += "%";
            }
            else if (nopt < 7)
            {
                buf += "_";
            }
            else
            {
                buf += rg.nextTokenString();
            }
        }
        expr->mutable_lit_val()->set_string_lit(std::move(buf));
    }
    else
    {
        this->generateExpression(rg, expr);
    }
}

Expr * StatementGenerator::generatePartialSearchExpr(RandomGenerator & rg, Expr * expr) const
{
    /// Use search functions more often
    SQLFuncCall * sfc = expr->mutable_comp_expr()->mutable_func_call();
    static const auto & searchFuncs
        = {SQLFunc::FUNCendsWith,
           SQLFunc::FUNChas,
           SQLFunc::FUNChasToken,
           SQLFunc::FUNChasTokenOrNull,
           SQLFunc::FUNCmapContains,
           SQLFunc::FUNCmatch,
           SQLFunc::FUNChasAllTokens,
           SQLFunc::FUNChasAnyTokens,
           SQLFunc::FUNCstartsWith};
    const auto & nfunc = rg.pickRandomly(searchFuncs);

    sfc->mutable_func()->set_catalog_func(nfunc);
    Expr * res = sfc->add_args()->mutable_expr();
    Expr * expr2 = sfc->add_args()->mutable_expr();
    if ((nfunc == SQLFunc::FUNChasAnyTokens || nfunc == SQLFunc::FUNChasAllTokens) && rg.nextBool())
    {
        ExprList * elist = expr2->mutable_comp_expr()->mutable_array();
        const uint32_t nvalues = std::min(this->fc.max_width - this->width, rg.randomInt<uint32_t>(0, 5)) + 1;

        for (uint32_t i = 0; i < nvalues; i++)
        {
            Expr * next = i == 0 ? elist->mutable_expr() : elist->add_extra_exprs();
            next->mutable_lit_val()->set_string_lit(rg.nextTokenString());
        }
    }
    else
    {
        expr2->mutable_lit_val()->set_string_lit(rg.nextTokenString());
    }
    return res;
}

void StatementGenerator::generatePredicate(RandomGenerator & rg, Expr * expr)
{
    if (this->fc.max_depth <= this->depth)
    {
        /// In the worst case, not a single option can be picked
        this->generateLiteralValue(rg, true, expr);
        return;
    }

    predMask[static_cast<size_t>(PredOp::UnaryExpr)] = this->fc.max_depth > this->depth;
    predMask[static_cast<size_t>(PredOp::BinaryExpr)] = this->fc.max_depth > this->depth && this->fc.max_width > (this->width + 1);
    predMask[static_cast<size_t>(PredOp::BetweenExpr)] = this->fc.max_depth > this->depth && this->fc.max_width > (this->width + 2);
    predMask[static_cast<size_t>(PredOp::InExpr)] = this->fc.max_depth > this->depth && this->fc.max_width > this->width;
    predMask[static_cast<size_t>(PredOp::AnyExpr)] = this->fc.max_depth > this->depth && this->fc.max_width > (this->width + 1);
    predMask[static_cast<size_t>(PredOp::IsNullExpr)] = this->fc.max_depth > this->depth;
    predMask[static_cast<size_t>(PredOp::ExistsExpr)] = this->fc.max_depth > this->depth && this->allow_subqueries;
    predMask[static_cast<size_t>(PredOp::LikeExpr)] = this->fc.max_depth > this->depth;
    predMask[static_cast<size_t>(PredOp::SearchExpr)] = this->fc.max_depth > this->depth;
    predMask[static_cast<size_t>(PredOp::OtherExpr)] = this->fc.max_depth > this->depth;
    predGen.setEnabled(predMask);

    switch (static_cast<PredOp>(predGen.nextOp())) /// drifts over time
    {
        case PredOp::UnaryExpr: {
            ComplicatedExpr * cexpr = expr->mutable_comp_expr();
            UnaryExpr * unexp = cexpr->mutable_unary_expr();

            unexp->set_paren(rg.nextMediumNumber() < 96);
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
        break;
        case PredOp::BinaryExpr: {
            const bool limited = rg.nextBool();
            ComplicatedExpr * cexpr = expr->mutable_comp_expr();
            BinaryExpr * bexpr = cexpr->mutable_binary_expr();
            std::uniform_int_distribution<uint32_t> op_range(
                limited ? static_cast<uint32_t>(BinaryOperator::BINOP_AND) : 1,
                static_cast<uint32_t>(limited ? BinaryOperator::BINOP_OR : BinaryOperator_MAX));

            bexpr->set_op(static_cast<BinaryOperator>(op_range(rg.generator)));
            this->depth++;
            this->generateExpression(rg, bexpr->mutable_lhs());
            this->width++;
            this->generateExpression(rg, bexpr->mutable_rhs());
            this->width--;
            this->depth--;
        }
        break;
        case PredOp::BetweenExpr: {
            ComplicatedExpr * cexpr = expr->mutable_comp_expr();
            ExprBetween * bexpr = cexpr->mutable_expr_between();

            bexpr->set_paren(rg.nextMediumNumber() < 96);
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
        break;
        case PredOp::InExpr: {
            const uint32_t nopt2 = rg.nextSmallNumber();
            const uint32_t nclauses = std::min(this->fc.max_width - this->width, rg.randomInt<uint32_t>(1, 4));
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
            else if (nopt2 < 9)
            {
                ExprList * elist2 = rg.nextBool() ? ein->mutable_tuple() : ein->mutable_array();
                const uint32_t nclauses2
                    = rg.nextSmallNumber() < 9 ? nclauses : std::min(this->fc.max_width - this->width, rg.randomInt<uint32_t>(1, 4));

                for (uint32_t i = 0; i < nclauses2; i++)
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
        break;
        case PredOp::AnyExpr: {
            ComplicatedExpr * cexpr = expr->mutable_comp_expr();
            ExprAny * eany = cexpr->mutable_expr_any();
            std::uniform_int_distribution<uint32_t> op_range(
                1, static_cast<uint32_t>(rg.nextLargeNumber() < 5 ? BinaryOperator_MAX : BinaryOperator::BINOP_LEEQGR));

            eany->set_op(static_cast<BinaryOperator>(op_range(rg.generator)));
            eany->set_anyall(rg.nextBool());
            this->depth++;
            this->generateExpression(rg, eany->mutable_expr());
            this->width++;
            generateSubquery(rg, eany->mutable_sel());
            this->width--;
            this->depth--;
        }
        break;
        case PredOp::IsNullExpr: {
            ComplicatedExpr * cexpr = expr->mutable_comp_expr();
            ExprNullTests * enull = cexpr->mutable_expr_null_tests();

            enull->set_not_(rg.nextBool());
            this->depth++;
            this->generateExpression(rg, enull->mutable_expr());
            this->depth--;
        }
        break;
        case PredOp::ExistsExpr: {
            ComplicatedExpr * cexpr = expr->mutable_comp_expr();
            ExprExists * exists = cexpr->mutable_expr_exists();

            exists->set_not_(rg.nextBool());
            this->depth++;
            generateSubquery(rg, exists->mutable_select());
            this->depth--;
        }
        break;
        case PredOp::LikeExpr: {
            ComplicatedExpr * cexpr = expr->mutable_comp_expr();
            ExprLike * elike = cexpr->mutable_expr_like();
            std::uniform_int_distribution<uint32_t> like_range(1, static_cast<uint32_t>(ExprLike::PossibleKeywords_MAX));

            elike->set_keyword(static_cast<ExprLike_PossibleKeywords>(like_range(rg.generator)));
            elike->set_not_(rg.nextBool());
            this->depth++;
            this->generateExpression(rg, elike->mutable_expr1());
            this->width++;
            this->generateLikeExpr(rg, elike->mutable_expr2());
            this->width--;
            this->depth--;
        }
        break;
        case PredOp::SearchExpr:
            /// Use search functions more often
            this->depth++;
            this->generateExpression(rg, generatePartialSearchExpr(rg, expr));
            this->depth--;
            break;
        case PredOp::OtherExpr:
            this->depth++;
            this->generateExpression(rg, expr);
            this->depth--;
            break;
    }
    addFieldAccess(rg, expr, 0);
}

void StatementGenerator::generateLambdaCall(RandomGenerator & rg, const uint32_t nparams, LambdaExpr * lexpr)
{
    std::unordered_map<uint32_t, QueryLevel> levels_backup;
    std::unordered_map<uint32_t, std::unordered_map<String, SQLRelation>> ctes_backup;

    for (const auto & [key, val] : this->levels)
    {
        levels_backup[key] = val;
    }
    for (const auto & [key, val] : this->ctes)
    {
        ctes_backup[key] = val;
    }
    this->levels.clear();
    this->ctes.clear();

    this->levels[this->current_level] = QueryLevel(this->current_level);
    if (nparams > 0)
    {
        SQLRelation rel("");

        for (uint32_t i = 0; i < nparams; i++)
        {
            const String buf = "p" + std::to_string(i);
            lexpr->add_args()->set_column(buf);
            rel.cols.emplace_back(SQLRelationCol("", {buf}));
        }
        this->levels[this->current_level].rels.emplace_back(rel);
    }
    this->generateExpression(rg, lexpr->mutable_expr());

    this->levels.clear();
    this->ctes.clear();
    for (const auto & [key, val] : levels_backup)
    {
        this->levels[key] = val;
    }
    for (const auto & [key, val] : ctes_backup)
    {
        this->ctes[key] = val;
    }
}

void StatementGenerator::generateFuncCall(RandomGenerator & rg, const bool allow_funcs, const bool allow_aggr, SQLFuncCall * func_call)
{
    const size_t funcs_size = this->allow_not_deterministic ? CHFuncs.size() : this->deterministic_funcs_limit;
    const bool nallow_funcs = allow_funcs && (!allow_aggr || rg.nextSmallNumber() < (this->inside_projection ? 3 : 7));
    const uint32_t nfuncs = static_cast<uint32_t>(
        (nallow_funcs ? funcs_size : 0)
        + (allow_aggr ? (this->allow_not_deterministic ? CHAggrs.size() : (CHAggrs.size() - this->deterministic_aggrs_limit)) : 0));
    std::uniform_int_distribution<uint32_t> next_dist(0, nfuncs - 1);
    uint32_t generated_params = 0;
    uint32_t n_lambda = 0;
    uint32_t min_args = 0;
    uint32_t max_args = 0;

    chassert(nallow_funcs || allow_aggr);
    const uint32_t nopt = next_dist(rg.generator);
    if (!nallow_funcs || nopt >= static_cast<uint32_t>(funcs_size))
    {
        /// Aggregate
        const uint32_t next_off
            = rg.nextSmallNumber() < 2 ? (rg.nextLargeNumber() % 5) : (nopt - static_cast<uint32_t>(nallow_funcs ? funcs_size : 0));
        const CHAggregate & agg = CHAggrs[next_off];
        const uint32_t ncombinators
            = rg.nextSmallNumber() < 4 ? std::min(this->fc.max_width - this->width, rg.randomInt<uint32_t>(1, 3)) : 0;
        const bool prev_inside_aggregate = this->levels[this->current_level].inside_aggregate;
        const bool prev_allow_window_funcs = this->levels[this->current_level].allow_window_funcs;
        std::uniform_int_distribution<uint32_t> comb_range(
            1, static_cast<uint32_t>(this->allow_not_deterministic ? SQLFuncCall::AggregateCombinator_MAX : SQLFuncCall::ArgMax));
        uint32_t min_params = 0;
        uint32_t max_params = 0;

        if (rg.nextLargeNumber() < 21)
        {
            /// Go random
            min_params = rg.randomInt<uint32_t>(0, 3);
            max_params = std::min(this->fc.max_width - this->width, min_params + rg.randomInt<uint32_t>(0, 3));
            min_args = rg.randomInt<uint32_t>(0, 3);
            max_args = std::min(this->fc.max_width - this->width, min_args + rg.randomInt<uint32_t>(0, 3));
        }
        else
        {
            min_params = agg.min_params;
            const uint32_t max_possible_params = std::min(agg.max_params, UINT32_C(5));
            max_params = std::min(this->fc.max_width - this->width, max_possible_params);
            min_args = agg.min_args;
            const uint32_t max_possible_args = std::min(agg.max_args, UINT32_C(5));
            max_args = std::min(this->fc.max_width - this->width, max_possible_args);
        }
        /// Most of the times disallow nested aggregates, and window functions inside aggregates
        this->levels[this->current_level].inside_aggregate = rg.nextSmallNumber() < 9;
        this->levels[this->current_level].allow_window_funcs = rg.nextSmallNumber() < 3;
        if (max_params > 0 && max_params >= min_params)
        {
            std::uniform_int_distribution<uint32_t> nparams(min_params, max_params);
            const uint32_t nagg_params = nparams(rg.generator);

            for (uint32_t i = 0; i < nagg_params; i++)
            {
                this->generateExpression(rg, func_call->add_params());
                this->width++;
                generated_params++;
            }
        }
        else if (min_params > 0)
        {
            for (uint32_t i = 0; i < min_params; i++)
            {
                generateLiteralValue(rg, true, func_call->add_params());
            }
        }

        if (max_args > 0 && max_args >= min_args)
        {
            std::uniform_int_distribution<uint32_t> nparams(min_args, max_args);
            const uint32_t nagg_args = nparams(rg.generator);

            for (uint32_t i = 0; i < nagg_args; i++)
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
        SQLFuncName * sfn = func_call->mutable_func();

        if (!this->functions.empty() && this->peer_query != PeerQuery::ClickHouseOnly
            && (this->allow_not_deterministic || collectionHas<SQLFunction>(StatementGenerator::funcDeterministicLambda))
            && rg.nextSmallNumber() < 3)
        {
            /// Use a function from the user
            const std::reference_wrapper<const SQLFunction> & func = this->allow_not_deterministic
                ? std::ref<const SQLFunction>(rg.pickValueRandomlyFromMap(this->functions))
                : rg.pickRandomly(filterCollection<SQLFunction>(StatementGenerator::funcDeterministicLambda));

            min_args = max_args = (rg.nextLargeNumber() < 21 ? rg.randomInt<uint32_t>(0, 3) : func.get().nargs);
            func.get().setName(sfn->mutable_function());
        }
        else
        {
            /// Use a default catalog function
            const CHFunction & func = rg.nextMediumNumber() < 10 ? rg.pickRandomly(CommonCHFuncs) : CHFuncs[nopt];

            n_lambda = ((func.min_lambda_param == func.max_lambda_param && func.max_lambda_param == 1)
                        || (func.max_lambda_param == 1 && rg.nextBool()))
                ? 1
                : 0;
            if (rg.nextLargeNumber() < 21)
            {
                /// Go random
                min_args = rg.randomInt<uint32_t>(0, 3);
                max_args = std::min(this->fc.max_width - this->width, min_args + rg.randomInt<uint32_t>(0, 3));
            }
            else
            {
                min_args = func.min_args;
                const uint32_t max_possible_args = std::min(func.max_args, UINT32_C(5));
                max_args = std::min(this->fc.max_width - this->width, max_possible_args);
            }
            sfn->set_catalog_func(static_cast<SQLFunc>(func.fnum));
        }

        if (n_lambda > 0)
        {
            generateLambdaCall(rg, rg.nextBool() ? 1 : rg.randomInt<uint32_t>(1, 3), func_call->add_args()->mutable_lambda());
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
    uint32_t generated_params = 0;
    uint32_t min_args = 0;
    uint32_t max_args = 0;

    tfunc_call->set_func(static_cast<SQLTableFunc>(func.fnum));
    chassert(func.min_lambda_param == func.max_lambda_param && func.max_lambda_param == 0);
    if (rg.nextBool())
    {
        /// Completely random
        min_args = rg.randomInt<uint32_t>(0, 2);
        max_args = std::min(this->fc.max_width - this->width, min_args + rg.randomInt<uint32_t>(0, 2));
    }
    else
    {
        min_args = func.min_args;
        max_args = std::min(this->fc.max_width - this->width, func.max_args);
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
        std::unordered_map<uint32_t, std::unordered_map<String, SQLRelation>> ctes_backup;

        for (const auto & [key, val] : this->levels)
        {
            levels_backup[key] = val;
        }
        for (const auto & [key, val] : this->ctes)
        {
            ctes_backup[key] = val;
        }
        this->levels.clear();
        this->ctes.clear();

        this->generateExpression(rg, expr);

        this->levels.clear();
        this->ctes.clear();
        for (const auto & [key, val] : levels_backup)
        {
            this->levels[key] = val;
        }
        for (const auto & [key, val] : ctes_backup)
        {
            this->ctes[key] = val;
        }
    }
}

void StatementGenerator::generateWindowDefinition(RandomGenerator & rg, WindowDefn * wdef)
{
    if (this->width < this->fc.max_width && rg.nextSmallNumber() < 4)
    {
        const uint32_t nclauses = std::min(this->fc.max_width - this->width, rg.randomInt<uint32_t>(1, 4));

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
    const bool allTables = rg.nextMediumNumber() < 4;
    const auto & level_rels = this->levels[this->current_level].rels;
    const auto has_dictionary_lambda
        = [&](const SQLDictionary & d) { return d.isAttached() && (d.is_deterministic || this->allow_not_deterministic); };
    const auto has_join_table_lambda = [&](const SQLTable & t)
    { return t.isAttached() && (allTables || t.isJoinEngine()) && (t.is_deterministic || this->allow_not_deterministic); };

    /// expMask[static_cast<size_t>(ExpOp::Literal)] = true;
    /// expMask[static_cast<size_t>(ExpOp::ColumnRef)] = true;
    expMask[static_cast<size_t>(ExpOp::Predicate)] = this->fc.max_depth > this->depth;
    expMask[static_cast<size_t>(ExpOp::CastExpr)] = this->fc.max_depth > this->depth;
    expMask[static_cast<size_t>(ExpOp::UnaryExpr)] = this->fc.max_depth > this->depth;
    expMask[static_cast<size_t>(ExpOp::IntervalExpr)] = this->fc.max_depth > this->depth;
    expMask[static_cast<size_t>(ExpOp::ColumnsExpr)]
        = this->fc.max_depth > this->depth && (this->allow_not_deterministic || this->levels[this->current_level].inside_aggregate);
    expMask[static_cast<size_t>(ExpOp::CondExpr)] = this->fc.max_depth > this->depth && this->fc.max_width > this->width;
    expMask[static_cast<size_t>(ExpOp::CaseExpr)] = this->fc.max_depth > this->depth && this->fc.max_width > this->width;
    expMask[static_cast<size_t>(ExpOp::SubqueryExpr)] = this->fc.max_depth > this->depth && this->allow_subqueries;
    expMask[static_cast<size_t>(ExpOp::BinaryExpr)] = this->fc.max_depth > this->depth && this->fc.max_width > (this->width + 1);
    expMask[static_cast<size_t>(ExpOp::ArrayTupleExpr)] = this->fc.max_depth > this->depth && this->fc.max_width > this->width;
    expMask[static_cast<size_t>(ExpOp::FuncExpr)] = this->fc.max_depth > this->depth;
    expMask[static_cast<size_t>(ExpOp::WindowFuncExpr)] = this->fc.max_depth > this->depth
        && this->levels[this->current_level].allow_window_funcs && !this->levels[this->current_level].inside_aggregate;
    expMask[static_cast<size_t>(ExpOp::TableStarExpr)]
        = (this->allow_not_deterministic || this->levels[this->current_level].inside_aggregate)
        && std::find_if(level_rels.begin(), level_rels.end(), has_rel_name_lambda) != level_rels.end();
    expMask[static_cast<size_t>(ExpOp::LambdaExpr)] = this->fc.max_depth > this->depth && this->fc.max_width > this->width;
    expMask[static_cast<size_t>(ExpOp::ProjectionExpr)] = this->inside_projection;
    expMask[static_cast<size_t>(ExpOp::DictExpr)] = this->fc.max_depth > this->depth && collectionHas<SQLDictionary>(has_dictionary_lambda);
    expMask[static_cast<size_t>(ExpOp::JoinExpr)] = this->fc.max_depth > this->depth && collectionHas<SQLTable>(has_join_table_lambda);
    expMask[static_cast<size_t>(ExpOp::StarExpr)] = this->allow_not_deterministic || this->levels[this->current_level].inside_aggregate;
    expGen.setEnabled(expMask);

    if (rg.nextSmallNumber() < 3)
    {
        eca = expr->mutable_comp_expr()->mutable_alias_expr();
        expr = eca->mutable_expr();
    }

    switch (static_cast<ExpOp>(expGen.nextOp())) /// drifts over time
    {
        case ExpOp::Literal:
            generateLiteralValue(rg, true, expr);
            break;
        case ExpOp::ColumnRef:
            generateColRef(rg, expr);
            break;
        case ExpOp::Predicate:
            generatePredicate(rg, expr);
            break;
        case ExpOp::CastExpr: {
            uint32_t col_counter = 0;
            const uint64_t type_mask_backup = this->next_type_mask;
            CastExpr * casexpr = expr->mutable_comp_expr()->mutable_cast_expr();

            casexpr->set_simple(rg.nextMediumNumber() < 16);
            this->depth++;
            this->next_type_mask = fc.type_mask & ~(allow_nested);
            auto tp = std::unique_ptr<SQLType>(
                randomNextType(rg, this->next_type_mask, col_counter, casexpr->mutable_type_name()->mutable_type()));
            this->next_type_mask = type_mask_backup;
            this->generateExpression(rg, casexpr->mutable_expr());
            this->depth--;
        }
        break;
        case ExpOp::UnaryExpr: {
            UnaryExpr * uexpr = expr->mutable_comp_expr()->mutable_unary_expr();
            std::uniform_int_distribution<uint32_t> op_range(1, static_cast<uint32_t>(UnaryOperator::UNOP_PLUS));

            this->depth++;
            uexpr->set_paren(rg.nextMediumNumber() < 96);
            uexpr->set_unary_op(static_cast<UnaryOperator>(op_range(rg.generator)));
            this->generateExpression(rg, uexpr->mutable_expr());
            this->depth--;
        }
        break;
        case ExpOp::IntervalExpr: {
            IntervalExpr * inter = expr->mutable_comp_expr()->mutable_interval();
            std::uniform_int_distribution<uint32_t> int_range(1, static_cast<uint32_t>(IntervalExpr::Interval_MAX));

            inter->set_interval(static_cast<IntervalExpr_Interval>(int_range(rg.generator)));
            this->depth++;
            this->generateExpression(rg, inter->mutable_expr());
            this->depth--;
        }
        break;
        case ExpOp::ColumnsExpr: {
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
        break;
        case ExpOp::CondExpr: {
            CondExpr * conexpr = expr->mutable_comp_expr()->mutable_expr_cond();

            conexpr->set_paren(rg.nextMediumNumber() < 96);
            this->depth++;
            this->generateExpression(rg, conexpr->mutable_expr1());
            this->width++;
            this->generateExpression(rg, conexpr->mutable_expr2());
            this->width++;
            this->generateExpression(rg, conexpr->mutable_expr3());
            this->width -= 2;
            this->depth--;
        }
        break;
        case ExpOp::CaseExpr: {
            ExprCase * caseexp = expr->mutable_comp_expr()->mutable_expr_case();
            const uint32_t nwhen = std::min(this->fc.max_width - this->width, rg.randomInt<uint32_t>(0, 3));

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
        break;
        case ExpOp::SubqueryExpr:
            this->depth++;
            generateSubquery(rg, expr->mutable_comp_expr()->mutable_subquery());
            this->depth--;
            break;
        case ExpOp::BinaryExpr: {
            BinaryExpr * bexpr = expr->mutable_comp_expr()->mutable_binary_expr();
            std::uniform_int_distribution<uint32_t> binop_range(
                rg.nextSmallNumber() < 9 ? static_cast<uint32_t>(BinaryOperator::BINOP_CONCAT) : 1,
                static_cast<uint32_t>(BinaryOperator_MAX));

            bexpr->set_op(static_cast<BinaryOperator>(binop_range(rg.generator)));
            this->depth++;
            this->generateExpression(rg, bexpr->mutable_lhs());
            this->width++;
            this->generateExpression(rg, bexpr->mutable_rhs());
            this->width--;
            this->depth--;
        }
        break;
        case ExpOp::ArrayTupleExpr: {
            const bool has_tuple = rg.nextBool();
            ExprList * elist = has_tuple ? expr->mutable_comp_expr()->mutable_tuple() : expr->mutable_comp_expr()->mutable_array();
            const uint32_t nvalues = std::min(this->fc.max_width - this->width, rg.randomInt<uint32_t>(0, 8));
            const bool tuple_cols = has_tuple && rg.nextSmallNumber() < 5;

            this->depth++;
            for (uint32_t i = 0; i < nvalues; i++)
            {
                Expr * next = i == 0 ? elist->mutable_expr() : elist->add_extra_exprs();

                if (tuple_cols)
                {
                    generateColRef(rg, next);
                }
                else
                {
                    generateExpression(rg, next);
                }
                this->width++;
            }
            this->depth--;
            this->width -= nvalues;
        }
        break;
        case ExpOp::FuncExpr: {
            /// Function call
            const bool allow_aggr
                = (!this->levels[this->current_level].inside_aggregate && this->levels[this->current_level].allow_aggregates
                   && (!this->levels[this->current_level].gcols.empty() || this->levels[this->current_level].global_aggregate))
                || rg.nextSmallNumber() < (this->inside_projection ? 8 : 3);

            this->depth++;
            generateFuncCall(rg, true, allow_aggr, expr->mutable_comp_expr()->mutable_func_call());
            this->depth--;
        }
        break;
        case ExpOp::WindowFuncExpr: {
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
                this->ids.emplace_back(static_cast<uint32_t>(WINcume_dist));
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
                        nargs = std::min(this->fc.max_width - this->width, rg.randomInt<uint32_t>(1, 3));
                        break;
                    default:
                        break;
                }
                wc->set_func(wfs);
                if (rg.nextLargeNumber() < 21)
                {
                    /// Generate random arguments for window function
                    nargs = rg.randomInt<uint32_t>(0, 3);
                }
                for (uint32_t i = 0; i < nargs; i++)
                {
                    this->generateExpression(rg, wc->add_args());
                    this->width++;
                }
                this->width -= nargs;
            }
            if (this->levels[this->current_level].window_counter > 0 && rg.nextBool())
            {
                std::uniform_int_distribution<uint32_t> w_range(0, this->levels[this->current_level].window_counter);

                wfc->mutable_window()->set_window("w" + std::to_string(w_range(rg.generator)));
            }
            else
            {
                generateWindowDefinition(rg, wfc->mutable_win_defn());
            }
            this->depth--;
            this->levels[this->current_level].allow_window_funcs = prev_allow_window_funcs;
        }
        break;
        case ExpOp::TableStarExpr:
            filtered_relations.clear();
            for (const auto & entry : level_rels)
            {
                if (has_rel_name_lambda(entry))
                {
                    filtered_relations.emplace_back(std::ref(entry));
                }
            }
            expr->mutable_comp_expr()->mutable_table()->set_table(rg.pickRandomly(filtered_relations).get().name);
            break;
        case ExpOp::LambdaExpr: {
            const uint32_t nexprs = std::min(this->fc.max_width - this->width, rg.randomInt<uint32_t>(0, 3));

            this->depth++;
            generateLambdaCall(rg, nexprs, expr->mutable_comp_expr()->mutable_lambda());
            this->depth--;
        }
        break;
        case ExpOp::ProjectionExpr: {
            /// Use count(*) in projections more often
            SQLFuncCall * sfc = expr->mutable_comp_expr()->mutable_func_call();

            sfc->mutable_func()->set_catalog_func(SQLFunc::FUNCcount);
        }
        break;
        case ExpOp::DictExpr: {
            /// Dictionary functions
            SQLFuncCall * sfc = expr->mutable_comp_expr()->mutable_func_call();
            const SQLDictionary & d = rg.pickRandomly(filterCollection<SQLDictionary>(has_dictionary_lambda));
            const auto dfunc = rg.pickRandomly(dictFuncs);

            sfc->mutable_func()->set_catalog_func(dfunc);
            sfc->add_args()->mutable_expr()->mutable_lit_val()->set_no_quote_str("'" + d.getFullName(true) + "'");
            flatTableColumnPath(
                flat_tuple | flat_nested | flat_json | to_table_entries | collect_generated,
                d.cols,
                [](const SQLColumn &) { return true; });
            sfc->add_args()->mutable_expr()->mutable_lit_val()->set_no_quote_str(rg.pickRandomly(this->table_entries).columnPathRef("'"));
            this->table_entries.clear();
            this->depth++;
            /// May break total width
            for (uint32_t i = 0; i < dictFuncs.at(dfunc); i++)
            {
                this->generateExpression(rg, sfc->add_args()->mutable_expr());
            }
            this->depth--;
        }
        break;
        case ExpOp::JoinExpr: {
            /// Join table functions
            SQLFuncCall * sfc = expr->mutable_comp_expr()->mutable_func_call();
            const SQLTable & t = rg.pickRandomly(filterCollection<SQLTable>(has_join_table_lambda));
            const uint32_t nkeys
                = std::max<uint32_t>(1, std::min<uint32_t>(this->fc.max_width - this->width, rg.randomInt<uint32_t>(1, 3)));

            sfc->mutable_func()->set_catalog_func(rg.nextBool() ? SQLFunc::FUNCjoinGet : SQLFunc::FUNCjoinGetOrNull);
            sfc->add_args()->mutable_expr()->mutable_lit_val()->set_no_quote_str("'" + t.getFullName(true) + "'");
            flatTableColumnPath(
                flat_tuple | flat_nested | flat_json | to_table_entries | collect_generated,
                t.cols,
                [](const SQLColumn &) { return true; });
            sfc->add_args()->mutable_expr()->mutable_lit_val()->set_no_quote_str(rg.pickRandomly(this->table_entries).columnPathRef("'"));
            this->table_entries.clear();
            this->depth++;
            for (uint32_t i = 0; i < nkeys; i++)
            {
                this->generateExpression(rg, sfc->add_args()->mutable_expr());
            }
            this->depth--;
        }
        break;
        case ExpOp::StarExpr:
            /// Star expression
            expr->mutable_lit_val()->mutable_special_val()->set_val(SpecialVal_SpecialValEnum_VAL_STAR);
            break;
    }

    addFieldAccess(rg, expr, 6);
    if (eca && this->allow_in_expression_alias && !this->inside_projection && rg.nextSmallNumber() < 4)
    {
        SQLRelation rel("");
        const String ncname = this->getNextAlias(rg);

        rel.cols.emplace_back(SQLRelationCol("", {ncname}));
        this->levels[this->current_level].rels.emplace_back(rel);
        eca->mutable_col_alias()->set_column(ncname);
        this->levels[this->current_level].projections.emplace_back(ncname);
        eca->set_use_parenthesis(rg.nextMediumNumber() < 98);
    }
}

}
